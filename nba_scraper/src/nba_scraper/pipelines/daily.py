"""Pipeline for daily incremental NBA data ingestion."""

import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from .season_pipeline import SeasonPipeline
from .game_pipeline import GamePipeline
from ..io_clients import BRefClient, NBAStatsClient, GamebooksClient
from ..rate_limit import RateLimiter
from ..nba_logging import get_logger
from ..db import get_connection

logger = get_logger(__name__)


@dataclass
class DailyPipelineResult:
    """Result of a daily pipeline execution."""
    success: bool
    date_processed: date
    games_processed: int
    games_failed: int
    records_updated: Dict[str, int]
    duration_seconds: float
    error: Optional[str] = None


class DailyPipeline:
    """Pipeline for daily incremental NBA data ingestion."""
    
    def __init__(self):
        """Initialize daily pipeline with all required dependencies."""
        # Initialize clients
        self.bref_client = BRefClient()
        self.nba_stats_client = NBAStatsClient()
        self.gamebooks_client = GamebooksClient()
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter()
        
        # Initialize game pipeline
        self.game_pipeline = GamePipeline(
            bref_client=self.bref_client,
            nba_stats_client=self.nba_stats_client,
            gamebooks_client=self.gamebooks_client,
            rate_limiter=self.rate_limiter
        )
        
        # Initialize season pipeline
        self.season_pipeline = SeasonPipeline(
            game_pipeline=self.game_pipeline,
            rate_limiter=self.rate_limiter
        )
        
    async def run_daily(
        self, 
        target_date: date, 
        force: bool = False, 
        dry_run: bool = False
    ) -> DailyPipelineResult:
        """Run daily ingestion for a specific date.
        
        Args:
            target_date: Date to process games for
            force: Whether to force reprocessing of existing data
            dry_run: Whether to run in dry-run mode (no database writes)
            
        Returns:
            DailyPipelineResult with execution details
        """
        start_time = datetime.utcnow()
        
        result = DailyPipelineResult(
            success=False,
            date_processed=target_date,
            games_processed=0,
            games_failed=0,
            records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Starting daily ingestion", target_date=target_date, force=force, dry_run=dry_run)
            
            # Get games for the target date
            games = await self._get_games_for_date(target_date)
            
            if not games:
                logger.info("No games found for date", target_date=target_date)
                result.success = True
                result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
                return result
            
            logger.info("Found games for processing", target_date=target_date, game_count=len(games))
            
            # Process games concurrently
            tasks = []
            for game in games:
                task = self._process_daily_game(game, force=force, dry_run=dry_run)
                tasks.append(task)
            
            # Execute all game processing tasks
            game_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate results
            for i, game_result in enumerate(game_results):
                if isinstance(game_result, Exception):
                    result.games_failed += 1
                    logger.error("Daily game processing failed", 
                               game_id=games[i].get('game_id'), 
                               error=str(game_result))
                    continue
                
                if game_result.success:
                    result.games_processed += 1
                    # Aggregate records updated
                    for table, count in game_result.records_updated.items():
                        result.records_updated[table] = (
                            result.records_updated.get(table, 0) + count
                        )
                else:
                    result.games_failed += 1
            
            result.success = result.games_processed > 0 or len(games) == 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Daily ingestion completed",
                       target_date=target_date,
                       games_processed=result.games_processed,
                       games_failed=result.games_failed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Daily ingestion failed", target_date=target_date, error=str(e))
        
        return result
    
    async def _get_games_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Get list of games for a specific date.
        
        Args:
            target_date: Date to get games for
            
        Returns:
            List of game dictionaries with basic info
        """
        try:
            conn = await get_connection()
            
            # Query for games on the target date
            query = """
            SELECT game_id, home_team_tricode, away_team_tricode, 
                   game_date_local, status, season
            FROM games 
            WHERE game_date_local = $1
            ORDER BY game_id
            """
            
            rows = await conn.fetch(query, target_date)
            
            games = []
            for row in rows:
                games.append({
                    'game_id': row['game_id'],
                    'home_team_tricode': row['home_team_tricode'],
                    'away_team_tricode': row['away_team_tricode'],
                    'game_date_local': row['game_date_local'],
                    'status': row['status'],
                    'season': row['season']
                })
            
            return games
            
        except Exception as e:
            logger.error("Failed to get games for date", target_date=target_date, error=str(e))
            return []
    
    async def _process_daily_game(
        self, 
        game: Dict[str, Any], 
        force: bool = False, 
        dry_run: bool = False
    ) -> DailyPipelineResult:
        """Process a single game for daily ingestion.
        
        Args:
            game: Game dictionary with basic info
            force: Whether to force reprocessing
            dry_run: Whether to run in dry-run mode
            
        Returns:
            DailyPipelineResult for the single game
        """
        game_id = game['game_id']
        start_time = datetime.utcnow()
        
        result = DailyPipelineResult(
            success=False,
            date_processed=game['game_date_local'],
            games_processed=0,
            games_failed=0,
            records_updated={},
            duration_seconds=0
        )
        
        try:
            # Use game pipeline to process this specific game
            game_result = await self.game_pipeline.process_game(
                game_id=game_id,
                sources=['nba_stats', 'bref', 'gamebooks'],  # All sources for daily ingestion
                force_refresh=force,
                dry_run=dry_run
            )
            
            if game_result.success:
                result.success = True
                result.games_processed = 1
                result.records_updated = game_result.records_updated
            else:
                result.games_failed = 1
                result.error = game_result.error
                
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
        except Exception as e:
            result.error = str(e)
            result.games_failed = 1
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Failed to process daily game", game_id=game_id, error=str(e))
        
        return result
    
    async def process_recent_games(
        self, 
        days_back: int = 3, 
        force: bool = False, 
        dry_run: bool = False
    ) -> DailyPipelineResult:
        """Process recent games (useful for catching up on missed data).
        
        Args:
            days_back: Number of days back to process
            force: Whether to force reprocessing
            dry_run: Whether to run in dry-run mode
            
        Returns:
            DailyPipelineResult with aggregated results
        """
        start_time = datetime.utcnow()
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        result = DailyPipelineResult(
            success=False,
            date_processed=end_date,
            games_processed=0,
            games_failed=0,
            records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Processing recent games", 
                       start_date=start_date, 
                       end_date=end_date, 
                       days_back=days_back)
            
            # Process each day in the range
            current_date = start_date
            while current_date <= end_date:
                daily_result = await self.run_daily(current_date, force=force, dry_run=dry_run)
                
                # Aggregate results
                result.games_processed += daily_result.games_processed
                result.games_failed += daily_result.games_failed
                
                for table, count in daily_result.records_updated.items():
                    result.records_updated[table] = (
                        result.records_updated.get(table, 0) + count
                    )
                
                current_date += timedelta(days=1)
            
            result.success = result.games_processed > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Recent games processing completed",
                       start_date=start_date,
                       end_date=end_date,
                       games_processed=result.games_processed,
                       games_failed=result.games_failed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Recent games processing failed", error=str(e))
        
        return result