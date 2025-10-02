"""Pipeline for backfilling historical NBA data across multiple seasons."""

import asyncio
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from .season_pipeline import SeasonPipeline, SeasonPipelineResult
from ..logging import get_logger
from ..config import get_settings
from ..db import get_connection

logger = get_logger(__name__)


@dataclass
class BackfillPipelineResult:
    """Result of a backfill pipeline execution."""
    success: bool
    seasons_processed: List[str]
    seasons_failed: List[str]
    total_games_processed: int
    total_games_failed: int
    total_records_updated: Dict[str, int]
    duration_seconds: float
    error: Optional[str] = None


class BackfillPipeline:
    """Pipeline for backfilling historical NBA data across multiple seasons."""
    
    def __init__(self, max_concurrent_seasons: int = 2):
        """Initialize backfill pipeline.
        
        Args:
            max_concurrent_seasons: Maximum number of seasons to process concurrently
        """
        self.max_concurrent_seasons = max_concurrent_seasons
        
        # Initialize clients
        from ..io_clients import BRefClient, NBAStatsClient, GamebooksClient
        self.bref_client = BRefClient()
        self.nba_stats_client = NBAStatsClient()
        self.gamebooks_client = GamebooksClient()
        
        # Initialize rate limiter
        from ..rate_limit import RateLimiter
        self.rate_limiter = RateLimiter()
        
        # Initialize game pipeline with all dependencies
        from .game_pipeline import GamePipeline
        self.game_pipeline = GamePipeline(
            bref_client=self.bref_client,
            nba_stats_client=self.nba_stats_client,
            gamebooks_client=self.gamebooks_client,
            rate_limiter=self.rate_limiter
        )
        
        # Initialize season pipeline with game pipeline and rate limiter
        self.season_pipeline = SeasonPipeline(
            game_pipeline=self.game_pipeline,
            rate_limiter=self.rate_limiter
        )
        
    async def backfill_games(
        self, 
        seasons: List[str], 
        resume: bool = False, 
        dry_run: bool = False
    ) -> BackfillPipelineResult:
        """Backfill games data for specified seasons.
        
        Args:
            seasons: List of season strings (e.g., ["2021-22", "2022-23"])
            resume: Whether to resume from last checkpoint
            dry_run: Whether to run in dry-run mode (no database writes)
            
        Returns:
            BackfillPipelineResult with execution details
        """
        start_time = datetime.utcnow()
        
        result = BackfillPipelineResult(
            success=False,
            seasons_processed=[],
            seasons_failed=[],
            total_games_processed=0,
            total_games_failed=0,
            total_records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Starting games backfill", seasons=seasons, resume=resume, dry_run=dry_run)
            
            # Process seasons in batches to avoid overwhelming APIs
            for i in range(0, len(seasons), self.max_concurrent_seasons):
                batch = seasons[i:i + self.max_concurrent_seasons]
                
                # Create tasks for concurrent processing
                tasks = []
                for season in batch:
                    task = self._process_season_games(season, resume=resume, dry_run=dry_run)
                    tasks.append(task)
                
                # Execute batch
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Aggregate batch results
                for j, season_result in enumerate(batch_results):
                    season = batch[j]
                    
                    if isinstance(season_result, Exception):
                        result.seasons_failed.append(season)
                        logger.error("Season games backfill failed", season=season, error=str(season_result))
                        continue
                    
                    if season_result.success:
                        result.seasons_processed.append(season)
                        result.total_games_processed += season_result.games_processed
                        
                        # Aggregate records updated
                        for table, count in season_result.total_records_updated.items():
                            result.total_records_updated[table] = (
                                result.total_records_updated.get(table, 0) + count
                            )
                    else:
                        result.seasons_failed.append(season)
                        result.total_games_failed += season_result.games_failed
            
            result.success = len(result.seasons_processed) > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Games backfill completed",
                       seasons_processed=len(result.seasons_processed),
                       seasons_failed=len(result.seasons_failed),
                       total_games=result.total_games_processed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Games backfill failed", error=str(e))
        
        return result
    
    async def backfill_refs(
        self, 
        seasons: List[str], 
        resume: bool = False, 
        dry_run: bool = False
    ) -> BackfillPipelineResult:
        """Backfill referee data for specified seasons."""
        start_time = datetime.utcnow()
        
        result = BackfillPipelineResult(
            success=False,
            seasons_processed=[],
            seasons_failed=[],
            total_games_processed=0,
            total_games_failed=0,
            total_records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Starting refs backfill", seasons=seasons, resume=resume, dry_run=dry_run)
            
            # Process seasons sequentially for referee data (slower, more careful)
            for season in seasons:
                season_result = await self._process_season_refs(season, resume=resume, dry_run=dry_run)
                
                if season_result.success:
                    result.seasons_processed.append(season)
                    result.total_games_processed += season_result.games_processed
                    
                    # Aggregate records updated
                    for table, count in season_result.total_records_updated.items():
                        result.total_records_updated[table] = (
                            result.total_records_updated.get(table, 0) + count
                        )
                else:
                    result.seasons_failed.append(season)
                    result.total_games_failed += season_result.games_failed
            
            result.success = len(result.seasons_processed) > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Refs backfill completed",
                       seasons_processed=len(result.seasons_processed),
                       seasons_failed=len(result.seasons_failed),
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Refs backfill failed", error=str(e))
        
        return result
    
    async def backfill_lineups_and_injuries(
        self, 
        seasons: List[str], 
        resume: bool = False, 
        dry_run: bool = False
    ) -> BackfillPipelineResult:
        """Backfill lineups and injury data for specified seasons."""
        start_time = datetime.utcnow()
        
        result = BackfillPipelineResult(
            success=False,
            seasons_processed=[],
            seasons_failed=[],
            total_games_processed=0,
            total_games_failed=0,
            total_records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Starting lineups/injuries backfill", seasons=seasons, resume=resume, dry_run=dry_run)
            
            # Process seasons with moderate concurrency
            for i in range(0, len(seasons), max(1, self.max_concurrent_seasons // 2)):
                batch = seasons[i:i + max(1, self.max_concurrent_seasons // 2)]
                
                tasks = []
                for season in batch:
                    task = self._process_season_lineups(season, resume=resume, dry_run=dry_run)
                    tasks.append(task)
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Aggregate batch results
                for j, season_result in enumerate(batch_results):
                    season = batch[j]
                    
                    if isinstance(season_result, Exception):
                        result.seasons_failed.append(season)
                        logger.error("Season lineups backfill failed", season=season, error=str(season_result))
                        continue
                    
                    if season_result.success:
                        result.seasons_processed.append(season)
                        result.total_games_processed += season_result.games_processed
                        
                        for table, count in season_result.total_records_updated.items():
                            result.total_records_updated[table] = (
                                result.total_records_updated.get(table, 0) + count
                            )
                    else:
                        result.seasons_failed.append(season)
                        result.total_games_failed += season_result.games_failed
            
            result.success = len(result.seasons_processed) > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Lineups/injuries backfill completed",
                       seasons_processed=len(result.seasons_processed),
                       seasons_failed=len(result.seasons_failed),
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Lineups/injuries backfill failed", error=str(e))
        
        return result
    
    async def backfill_pbp(
        self, 
        seasons: List[str], 
        resume: bool = False, 
        dry_run: bool = False
    ) -> BackfillPipelineResult:
        """Backfill play-by-play data for specified seasons."""
        start_time = datetime.utcnow()
        
        result = BackfillPipelineResult(
            success=False,
            seasons_processed=[],
            seasons_failed=[],
            total_games_processed=0,
            total_games_failed=0,
            total_records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Starting PBP backfill", seasons=seasons, resume=resume, dry_run=dry_run)
            
            # Process seasons with limited concurrency (PBP data is large)
            for season in seasons:
                season_result = await self._process_season_pbp(season, resume=resume, dry_run=dry_run)
                
                if season_result.success:
                    result.seasons_processed.append(season)
                    result.total_games_processed += season_result.games_processed
                    
                    for table, count in season_result.total_records_updated.items():
                        result.total_records_updated[table] = (
                            result.total_records_updated.get(table, 0) + count
                        )
                else:
                    result.seasons_failed.append(season)
                    result.total_games_failed += season_result.games_failed
            
            result.success = len(result.seasons_processed) > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("PBP backfill completed",
                       seasons_processed=len(result.seasons_processed),
                       seasons_failed=len(result.seasons_failed),
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("PBP backfill failed", error=str(e))
        
        return result
    
    async def _process_season_games(self, season: str, resume: bool = False, dry_run: bool = False) -> SeasonPipelineResult:
        """Process games for a single season."""
        try:
            # Note: process_season doesn't support resume/dry_run params yet
            return await self.season_pipeline.process_season(
                season=season,
                sources=['nba_stats', 'bref'],  # Games from both sources
                force_refresh=not resume  # If not resuming, force refresh
            )
        except Exception as e:
            logger.error("Failed to process season games", season=season, error=str(e))
            return SeasonPipelineResult(
                season=season,
                success=False,
                games_processed=0,
                games_failed=1,
                total_records_updated={},
                error=str(e)
            )
    
    async def _process_season_refs(self, season: str, resume: bool = False, dry_run: bool = False) -> SeasonPipelineResult:
        """Process referee data for a single season."""
        try:
            return await self.season_pipeline.process_season(
                season=season,
                sources=['gamebooks'],  # Refs from gamebooks only
                force_refresh=not resume
            )
        except Exception as e:
            logger.error("Failed to process season refs", season=season, error=str(e))
            return SeasonPipelineResult(
                season=season,
                success=False,
                games_processed=0,
                games_failed=1,
                total_records_updated={},
                error=str(e)
            )
    
    async def _process_season_lineups(self, season: str, resume: bool = False, dry_run: bool = False) -> SeasonPipelineResult:
        """Process lineups and injuries for a single season."""
        try:
            return await self.season_pipeline.process_season(
                season=season,
                sources=['bref'],  # Lineups from Basketball Reference
                force_refresh=not resume
            )
        except Exception as e:
            logger.error("Failed to process season lineups", season=season, error=str(e))
            return SeasonPipelineResult(
                season=season,
                success=False,
                games_processed=0,
                games_failed=1,
                total_records_updated={},
                error=str(e)
            )
    
    async def _process_season_pbp(self, season: str, resume: bool = False, dry_run: bool = False) -> SeasonPipelineResult:
        """Process play-by-play data for a single season."""
        try:
            return await self.season_pipeline.process_season(
                season=season,
                sources=['nba_stats'],  # PBP primarily from NBA Stats
                force_refresh=not resume
            )
        except Exception as e:
            logger.error("Failed to process season PBP", season=season, error=str(e))
            return SeasonPipelineResult(
                season=season,
                success=False,
                games_processed=0,
                games_failed=1,
                total_records_updated={},
                error=str(e)
            )