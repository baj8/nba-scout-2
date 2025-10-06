"""Pipeline for processing entire NBA seasons with batch coordination."""

import asyncio
from datetime import datetime, date, UTC
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from .game_pipeline import GamePipeline, GamePipelineResult
from ..models import GameStatus
from ..nba_logging import get_logger
from ..rate_limit import RateLimiter

logger = get_logger(__name__)


@dataclass
class SeasonPipelineResult:
    """Result of a season pipeline execution."""
    season: str
    success: bool
    games_processed: int
    games_failed: int
    total_records_updated: Dict[str, int]
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class SeasonPipeline:
    """Orchestrates processing of entire NBA seasons with intelligent batching."""
    
    def __init__(
        self,
        game_pipeline: GamePipeline,
        rate_limiter: RateLimiter,
        batch_size: int = 10,
        max_concurrent: int = 5
    ):
        self.game_pipeline = game_pipeline
        self.rate_limiter = rate_limiter
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
    
    async def process_season(
        self,
        season: str,
        sources: Optional[List[str]] = None,
        force_refresh: bool = False,
        date_range: Optional[tuple[date, date]] = None
    ) -> SeasonPipelineResult:
        """Process an entire NBA season with intelligent batching.
        
        Args:
            season: Season identifier (e.g., '2023-24')
            sources: List of sources to process per game
            force_refresh: Whether to force re-extraction of existing games
            date_range: Optional tuple of (start_date, end_date) to limit processing
            
        Returns:
            SeasonPipelineResult with processing summary
        """
        start_time = datetime.now(UTC)
        
        result = SeasonPipelineResult(
            season=season,
            success=False,
            games_processed=0,
            games_failed=0,
            total_records_updated={}
        )
        
        try:
            logger.info("Starting season pipeline", season=season, sources=sources)
            
            # Get list of games for the season
            game_ids = await self._get_season_games(season, date_range)
            if not game_ids:
                logger.warning("No games found for season", season=season)
                return result
            
            logger.info("Found games for season", season=season, count=len(game_ids))
            
            # Filter games that need processing
            if not force_refresh:
                game_ids = await self._filter_games_needing_processing(game_ids)
                logger.info("Games needing processing", season=season, count=len(game_ids))
            
            # Process games in batches
            semaphore = asyncio.Semaphore(self.max_concurrent)
            all_results = []
            
            for i in range(0, len(game_ids), self.batch_size):
                batch = game_ids[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (len(game_ids) + self.batch_size - 1) // self.batch_size
                
                logger.info("Processing batch", 
                           season=season,
                           batch=batch_num,
                           total_batches=total_batches,
                           games_in_batch=len(batch))
                
                # Process batch with concurrency control
                batch_tasks = [
                    self._process_game_with_semaphore(semaphore, game_id, sources, force_refresh)
                    for game_id in batch
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                all_results.extend(batch_results)
                
                # Add delay between batches to be respectful to sources
                if i + self.batch_size < len(game_ids):
                    await asyncio.sleep(2)
            
            # Aggregate results
            for game_result in all_results:
                if isinstance(game_result, Exception):
                    result.games_failed += 1
                    logger.error("Game processing exception", error=str(game_result))
                    continue
                
                if game_result.success:
                    result.games_processed += 1
                    # Aggregate record counts
                    for table, count in game_result.records_updated.items():
                        result.total_records_updated[table] = (
                            result.total_records_updated.get(table, 0) + count
                        )
                else:
                    result.games_failed += 1
            
            result.success = result.games_processed > 0
            result.duration_seconds = (datetime.now(UTC) - start_time).total_seconds()
            
            logger.info("Season pipeline completed",
                       season=season,
                       processed=result.games_processed,
                       failed=result.games_failed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.now(UTC) - start_time).total_seconds()
            logger.error("Season pipeline failed", season=season, error=str(e))
        
        return result
    
    async def _process_game_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        game_id: str,
        sources: Optional[List[str]],
        force_refresh: bool
    ) -> GamePipelineResult:
        """Process a single game with concurrency control."""
        async with semaphore:
            return await self.game_pipeline.process_game(game_id, sources, force_refresh)
    
    async def _get_season_games(
        self, 
        season: str, 
        date_range: Optional[tuple[date, date]] = None
    ) -> List[str]:
        """Get list of game IDs for a season."""
        try:
            from ..db import get_connection
            
            conn = await get_connection()
            
            # Base query
            query = "SELECT DISTINCT game_id FROM games WHERE season = $1"
            params = [season]
            
            # Add date filtering if specified
            if date_range:
                query += " AND game_date >= $2 AND game_date <= $3"
                params.extend([date_range[0], date_range[1]])
            
            query += " ORDER BY game_id"
            
            rows = await conn.fetch(query, *params)
            return [row['game_id'] for row in rows]
            
        except Exception as e:
            logger.error("Failed to get season games", season=season, error=str(e))
            return []
    
    async def _filter_games_needing_processing(self, game_ids: List[str]) -> List[str]:
        """Filter to games that need processing (not final or missing data)."""
        filtered_games = []
        
        for game_id in game_ids:
            should_process = await self.game_pipeline.should_process_game(game_id)
            if should_process:
                filtered_games.append(game_id)
        
        return filtered_games
    
    async def get_season_processing_stats(self, season: str) -> Dict[str, Any]:
        """Get processing statistics for a season."""
        try:
            from ..db import get_connection
            
            conn = await get_connection()
            
            # Get game status distribution
            status_query = """
            SELECT status, COUNT(*) as count
            FROM games 
            WHERE season = $1 
            GROUP BY status
            ORDER BY status
            """
            
            status_rows = await conn.fetch(status_query, season)
            status_distribution = {row['status']: row['count'] for row in status_rows}
            
            # Get data completeness stats
            completeness_query = """
            SELECT 
                COUNT(*) as total_games,
                COUNT(CASE WHEN EXISTS(
                    SELECT 1 FROM ref_assignments WHERE ref_assignments.game_id = games.game_id
                )) as games_with_refs,
                COUNT(CASE WHEN EXISTS(
                    SELECT 1 FROM starting_lineups WHERE starting_lineups.game_id = games.game_id
                )) as games_with_lineups,
                COUNT(CASE WHEN EXISTS(
                    SELECT 1 FROM pbp_events WHERE pbp_events.game_id = games.game_id
                )) as games_with_pbp
            FROM games 
            WHERE season = $1
            """
            
            completeness_row = await conn.fetchrow(completeness_query, season)
            
            return {
                'season': season,
                'status_distribution': status_distribution,
                'total_games': completeness_row['total_games'],
                'games_with_refs': completeness_row['games_with_refs'],
                'games_with_lineups': completeness_row['games_with_lineups'],
                'games_with_pbp': completeness_row['games_with_pbp'],
                'completeness_pct': {
                    'refs': (completeness_row['games_with_refs'] / completeness_row['total_games'] * 100) if completeness_row['total_games'] > 0 else 0,
                    'lineups': (completeness_row['games_with_lineups'] / completeness_row['total_games'] * 100) if completeness_row['total_games'] > 0 else 0,
                    'pbp': (completeness_row['games_with_pbp'] / completeness_row['total_games'] * 100) if completeness_row['total_games'] > 0 else 0,
                }
            }
            
        except Exception as e:
            logger.error("Failed to get season stats", season=season, error=str(e))
            return {}
    
    async def process_recent_games(
        self,
        days_back: int = 3,
        sources: Optional[List[str]] = None
    ) -> SeasonPipelineResult:
        """Process recent games (useful for daily updates)."""
        try:
            from ..db import get_connection
            
            conn = await get_connection()
            
            # Get recent games
            query = """
            SELECT DISTINCT game_id, season
            FROM games 
            WHERE game_date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY game_date DESC, game_id
            """ % days_back
            
            rows = await conn.fetch(query)
            game_ids = [row['game_id'] for row in rows]
            
            if not game_ids:
                return SeasonPipelineResult(
                    season="recent",
                    success=True,
                    games_processed=0,
                    games_failed=0,
                    total_records_updated={}
                )
            
            # Determine season for logging (use most recent)
            season = rows[0]['season'] if rows else "recent"
            
            logger.info("Processing recent games", days_back=days_back, count=len(game_ids))
            
            # Process using existing season logic
            start_time = datetime.now(UTC)
            
            result = SeasonPipelineResult(
                season=season,
                success=False,
                games_processed=0,
                games_failed=0,
                total_records_updated={}
            )
            
            # Process games with concurrency control
            semaphore = asyncio.Semaphore(self.max_concurrent)
            tasks = [
                self._process_game_with_semaphore(semaphore, game_id, sources, False)
                for game_id in game_ids  # Don't force refresh for recent games
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate results
            for game_result in results:
                if isinstance(game_result, Exception):
                    result.games_failed += 1
                    continue
                
                if game_result.success:
                    result.games_processed += 1
                    for table, count in game_result.records_updated.items():
                        result.total_records_updated[table] = (
                            result.total_records_updated.get(table, 0) + count
                        )
                else:
                    result.games_failed += 1
            
            result.success = result.games_processed > 0
            result.duration_seconds = (datetime.now(UTC) - start_time).total_seconds()
            
            return result
            
        except Exception as e:
            logger.error("Failed to process recent games", error=str(e))
            return SeasonPipelineResult(
                season="recent",
                success=False,
                games_processed=0,
                games_failed=0,
                total_records_updated={},
                error=str(e)
            )