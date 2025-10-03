"""Source-specific pipelines for NBA data processing."""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass

from ..models import GameStatus
from ..extractors import (
    extract_pbp_from_response,
    extract_boxscore_lineups,
    extract_game_outcomes,
    extract_starting_lineups,
    extract_injury_notes,
    extract_referee_assignments,
    extract_referee_alternates,
)
from ..io_clients import BRefClient, NBAStatsClient, GamebooksClient
from ..loaders import GameLoader, RefLoader, LineupLoader, PbpLoader
from ..transformers import GameTransformer, RefTransformer, LineupTransformer, PbpTransformer
from ..nba_logging import get_logger
from ..rate_limit import RateLimiter

logger = get_logger(__name__)


@dataclass
class SourcePipelineResult:
    """Result of a source-specific pipeline execution."""
    game_id: str
    source: str
    success: bool
    data_types_processed: List[str]
    records_updated: Dict[str, int]
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


class BaseSourcePipeline(ABC):
    """Base class for source-specific pipelines."""
    
    def __init__(self, source: str, rate_limiter: RateLimiter):
        self.source = source
        self.rate_limiter = rate_limiter
        
        # Initialize common loaders
        self.game_loader = GameLoader()
        self.ref_loader = RefLoader()
        self.lineup_loader = LineupLoader()
        self.pbp_loader = PbpLoader()
        
        # Initialize source-specific transformers
        self.game_transformer = GameTransformer(source=source)
        self.ref_transformer = RefTransformer(source=source)
        self.lineup_transformer = LineupTransformer(source=source)
        self.pbp_transformer = PbpTransformer(source=source)
    
    @abstractmethod
    async def process_game(
        self,
        game_id: str,
        force_refresh: bool = False,
        dry_run: bool = False
    ) -> SourcePipelineResult:
        """Process game data from this source."""
        pass
    
    @abstractmethod
    def get_supported_data_types(self) -> Set[str]:
        """Return the data types this source can provide."""
        pass
    
    @abstractmethod
    def get_priority_data_types(self) -> Set[str]:
        """Return the data types this source is the preferred provider for."""
        pass
    
    async def _create_result(self, game_id: str, start_time: datetime) -> SourcePipelineResult:
        """Create a base result object."""
        return SourcePipelineResult(
            game_id=game_id,
            source=self.source,
            success=False,
            data_types_processed=[],
            records_updated={},
            duration_seconds=(datetime.utcnow() - start_time).total_seconds()
        )


class NBAStatsPipeline(BaseSourcePipeline):
    """Pipeline for NBA Stats API data - rich PBP and real-time data."""
    
    def __init__(self, client: NBAStatsClient, rate_limiter: RateLimiter):
        super().__init__('nba_stats', rate_limiter)
        self.client = client
    
    def get_supported_data_types(self) -> Set[str]:
        """NBA Stats provides comprehensive game data."""
        return {
            'games', 'pbp_events', 'starting_lineups', 
            'boxscore_stats', 'player_stats'
        }
    
    def get_priority_data_types(self) -> Set[str]:
        """NBA Stats is the primary source for PBP and real-time data."""
        return {'pbp_events', 'boxscore_stats', 'player_stats'}
    
    async def process_game(
        self,
        game_id: str,
        force_refresh: bool = False,
        dry_run: bool = False
    ) -> SourcePipelineResult:
        """Process NBA Stats data for a game."""
        start_time = datetime.utcnow()
        result = await self._create_result(game_id, start_time)
        
        try:
            logger.info("Processing NBA Stats data", game_id=game_id, dry_run=dry_run)
            
            if dry_run:
                logger.info("DRY RUN: Would process NBA Stats data", game_id=game_id)
                result.success = True
                result.data_types_processed = list(self.get_supported_data_types())
                return result
            
            await self.rate_limiter.acquire('nba_stats')
            
            # Process PBP data (NBA Stats specialty)
            try:
                pbp_response = await self.client.fetch_pbp(game_id)
                if pbp_response:
                    pbp_events = extract_pbp_from_response(
                        pbp_response, game_id, 
                        f"https://stats.nba.com/game/{game_id}"
                    )
                    if pbp_events:
                        count = await self.pbp_loader.upsert_events(pbp_events)
                        result.records_updated['pbp_events'] = count
                        result.data_types_processed.append('pbp_events')
                        logger.info("Processed NBA Stats PBP", game_id=game_id, count=count)
            except Exception as e:
                logger.warning("Failed to process NBA Stats PBP", 
                             game_id=game_id, error=str(e))
            
            # Process boxscore lineups
            try:
                boxscore_response = await self.client.fetch_boxscore(game_id)
                if boxscore_response:
                    lineups = extract_boxscore_lineups(
                        boxscore_response, game_id,
                        f"https://stats.nba.com/game/{game_id}"
                    )
                    if lineups:
                        count = await self.lineup_loader.upsert_lineups(lineups)
                        result.records_updated['starting_lineups'] = count
                        result.data_types_processed.append('starting_lineups')
                        logger.info("Processed NBA Stats lineups", game_id=game_id, count=count)
            except Exception as e:
                logger.warning("Failed to process NBA Stats lineups", 
                             game_id=game_id, error=str(e))
            
            # Process game metadata (as backup to other sources)
            try:
                game_response = await self.client.fetch_game_header(game_id)
                if game_response:
                    games = self.game_transformer.transform(
                        game_response, 
                        game_id=game_id,
                        source_url=f"https://stats.nba.com/game/{game_id}"
                    )
                    if games:
                        count = await self.game_loader.upsert_games(games)
                        result.records_updated['games'] = count
                        result.data_types_processed.append('games')
                        logger.info("Processed NBA Stats game data", game_id=game_id, count=count)
            except Exception as e:
                logger.warning("Failed to process NBA Stats game data", 
                             game_id=game_id, error=str(e))
            
            result.success = len(result.data_types_processed) > 0
            
        except Exception as e:
            result.error = str(e)
            logger.error("NBA Stats pipeline failed", game_id=game_id, error=str(e))
        
        result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        return result


class BRefPipeline(BaseSourcePipeline):
    """Pipeline for Basketball Reference data - historical accuracy and outcomes."""
    
    def __init__(self, client: BRefClient, rate_limiter: RateLimiter):
        super().__init__('bref', rate_limiter)
        self.client = client
    
    def get_supported_data_types(self) -> Set[str]:
        """B-Ref provides historical game outcomes and lineups."""
        return {
            'games', 'starting_lineups', 'injury_status', 
            'game_outcomes', 'team_stats'
        }
    
    def get_priority_data_types(self) -> Set[str]:
        """B-Ref is preferred for historical accuracy and injury data."""
        return {'game_outcomes', 'injury_status', 'team_stats'}
    
    async def process_game(
        self,
        game_id: str,
        force_refresh: bool = False,
        dry_run: bool = False
    ) -> SourcePipelineResult:
        """Process Basketball Reference data for a game."""
        start_time = datetime.utcnow()
        result = await self._create_result(game_id, start_time)
        
        try:
            logger.info("Processing B-Ref data", game_id=game_id, dry_run=dry_run)
            
            if dry_run:
                logger.info("DRY RUN: Would process B-Ref data", game_id=game_id)
                result.success = True
                result.data_types_processed = list(self.get_supported_data_types())
                return result
            
            await self.rate_limiter.acquire('bref')
            
            # Fetch the main box score page once
            box_html = await self.client.fetch_bref_box(game_id)
            if not box_html:
                logger.warning("No B-Ref box score data", game_id=game_id)
                return result
            
            source_url = f"https://www.basketball-reference.com/boxscores/{game_id}.html"
            
            # Process game outcomes (B-Ref specialty)
            try:
                outcomes = extract_game_outcomes(box_html, game_id, source_url)
                if outcomes:
                    count = await self.game_loader.upsert_games(outcomes)
                    result.records_updated['games'] = count
                    result.data_types_processed.append('games')
                    logger.info("Processed B-Ref outcomes", game_id=game_id, count=count)
            except Exception as e:
                logger.warning("Failed to process B-Ref outcomes", 
                             game_id=game_id, error=str(e))
            
            # Process starting lineups
            try:
                lineups = extract_starting_lineups(box_html, game_id, source_url)
                if lineups:
                    count = await self.lineup_loader.upsert_lineups(lineups)
                    result.records_updated['starting_lineups'] = count
                    result.data_types_processed.append('starting_lineups')
                    logger.info("Processed B-Ref lineups", game_id=game_id, count=count)
            except Exception as e:
                logger.warning("Failed to process B-Ref lineups", 
                             game_id=game_id, error=str(e))
            
            # Process injury status (B-Ref specialty)
            try:
                injuries = extract_injury_notes(box_html, game_id, source_url)
                if injuries:
                    count = await self.lineup_loader.upsert_injuries(injuries)
                    result.records_updated['injury_status'] = count
                    result.data_types_processed.append('injury_status')
                    logger.info("Processed B-Ref injuries", game_id=game_id, count=count)
            except Exception as e:
                logger.warning("Failed to process B-Ref injuries", 
                             game_id=game_id, error=str(e))
            
            result.success = len(result.data_types_processed) > 0
            
        except Exception as e:
            result.error = str(e)
            logger.error("B-Ref pipeline failed", game_id=game_id, error=str(e))
        
        result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        return result


class GamebooksPipeline(BaseSourcePipeline):
    """Pipeline for NBA Gamebooks data - official referee and technical data."""
    
    def __init__(self, client: GamebooksClient, rate_limiter: RateLimiter):
        super().__init__('gamebooks', rate_limiter)
        self.client = client
    
    def get_supported_data_types(self) -> Set[str]:
        """Gamebooks provide official referee and technical data."""
        return {
            'ref_assignments', 'ref_alternates', 'technical_fouls', 
            'ejections', 'official_notes'
        }
    
    def get_priority_data_types(self) -> Set[str]:
        """Gamebooks are the authoritative source for official data."""
        return {
            'ref_assignments', 'ref_alternates', 'technical_fouls', 
            'ejections', 'official_notes'
        }
    
    async def process_game(
        self,
        game_id: str,
        force_refresh: bool = False,
        dry_run: bool = False
    ) -> SourcePipelineResult:
        """Process NBA Gamebooks data for a game."""
        start_time = datetime.utcnow()
        result = await self._create_result(game_id, start_time)
        
        try:
            logger.info("Processing Gamebooks data", game_id=game_id, dry_run=dry_run)
            
            if dry_run:
                logger.info("DRY RUN: Would process Gamebooks data", game_id=game_id)
                result.success = True
                result.data_types_processed = list(self.get_supported_data_types())
                return result
            
            await self.rate_limiter.acquire('gamebooks')
            
            # Parse game_id to get date for gamebook lookup
            game_date = self._parse_game_date(game_id)
            if not game_date:
                logger.warning("Could not parse game date", game_id=game_id)
                return result
            
            # Find and process gamebook
            try:
                gamebook_urls = await self.client.list_gamebooks(game_date)
                matching_url = None
                
                for url in gamebook_urls:
                    if game_id in url or self._matches_game(url, game_id):
                        matching_url = url
                        break
                
                if not matching_url:
                    logger.info("No gamebook found", game_id=game_id, date=game_date)
                    return result
                
                # Download and parse gamebook
                pdf_path = await self.client.download_gamebook(matching_url)
                parsed_data = self.client.parse_gamebook_pdf(pdf_path)
                
                # Process referee assignments (Gamebooks specialty)
                if parsed_data.get('refs'):
                    try:
                        ref_assignments = extract_referee_assignments(
                            parsed_data, game_id, matching_url
                        )
                        if ref_assignments:
                            count = await self.ref_loader.upsert_assignments(ref_assignments)
                            result.records_updated['ref_assignments'] = count
                            result.data_types_processed.append('ref_assignments')
                            logger.info("Processed Gamebooks refs", game_id=game_id, count=count)
                    except Exception as e:
                        logger.warning("Failed to process ref assignments", 
                                     game_id=game_id, error=str(e))
                
                # Process referee alternates
                if parsed_data.get('alternates'):
                    try:
                        ref_alternates = extract_referee_alternates(
                            parsed_data, game_id, matching_url
                        )
                        if ref_alternates:
                            count = await self.ref_loader.upsert_alternates(ref_alternates)
                            result.records_updated['ref_alternates'] = count
                            result.data_types_processed.append('ref_alternates')
                            logger.info("Processed Gamebooks alternates", game_id=game_id, count=count)
                    except Exception as e:
                        logger.warning("Failed to process ref alternates", 
                                     game_id=game_id, error=str(e))
                
            except Exception as e:
                logger.warning("Failed to process gamebook", 
                             game_id=game_id, error=str(e))
            
            result.success = len(result.data_types_processed) > 0
            
        except Exception as e:
            result.error = str(e)
            logger.error("Gamebooks pipeline failed", game_id=game_id, error=str(e))
        
        result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
        return result
    
    def _parse_game_date(self, game_id: str) -> Optional[date]:
        """Parse game date from game_id format."""
        try:
            # Assuming format like "0022300123" where "20230" represents date info
            # This is a simplified implementation - adjust based on actual format
            if len(game_id) >= 10:
                # Extract date portion and convert
                date_part = game_id[3:7]  # Simplified - adjust as needed
                year = 2000 + int(date_part[:2])
                month_day = date_part[2:]
                # This needs proper implementation based on actual game_id format
                return date.today()  # Placeholder
            return None
        except Exception:
            return None
    
    def _matches_game(self, url: str, game_id: str) -> bool:
        """Check if gamebook URL matches the game."""
        # Implement logic to match gamebook URLs to game IDs
        # This could involve parsing team names, dates, etc.
        return False  # Placeholder


class SourcePipelineOrchestrator:
    """Orchestrates multiple source pipelines for a game."""
    
    def __init__(
        self,
        nba_stats_pipeline: NBAStatsPipeline,
        bref_pipeline: BRefPipeline,
        gamebooks_pipeline: GamebooksPipeline
    ):
        self.pipelines = {
            'nba_stats': nba_stats_pipeline,
            'bref': bref_pipeline,
            'gamebooks': gamebooks_pipeline
        }
    
    async def process_game(
        self,
        game_id: str,
        sources: Optional[List[str]] = None,
        force_refresh: bool = False,
        dry_run: bool = False,
        strategy: str = 'parallel'  # 'parallel', 'sequential', 'priority'
    ) -> Dict[str, SourcePipelineResult]:
        """Process a game using multiple source pipelines."""
        sources = sources or list(self.pipelines.keys())
        results = {}
        
        logger.info("Starting source pipeline orchestration", 
                   game_id=game_id, sources=sources, strategy=strategy)
        
        if strategy == 'parallel':
            # Process all sources concurrently
            tasks = []
            for source in sources:
                if source in self.pipelines:
                    pipeline = self.pipelines[source]
                    task = pipeline.process_game(game_id, force_refresh, dry_run)
                    tasks.append((source, task))
            
            # Execute all tasks
            for source, task in tasks:
                try:
                    result = await task
                    results[source] = result
                except Exception as e:
                    logger.error("Pipeline failed", source=source, game_id=game_id, error=str(e))
                    results[source] = SourcePipelineResult(
                        game_id=game_id,
                        source=source,
                        success=False,
                        data_types_processed=[],
                        records_updated={},
                        error=str(e)
                    )
                    
        elif strategy == 'sequential':
            # Process sources one by one
            for source in sources:
                if source in self.pipelines:
                    try:
                        pipeline = self.pipelines[source]
                        result = await pipeline.process_game(game_id, force_refresh, dry_run)
                        results[source] = result
                    except Exception as e:
                        logger.error("Pipeline failed", source=source, game_id=game_id, error=str(e))
                        results[source] = SourcePipelineResult(
                            game_id=game_id,
                            source=source,
                            success=False,
                            data_types_processed=[],
                            records_updated={},
                            error=str(e)
                        )
                        
        elif strategy == 'priority':
            # Process sources based on their priority data types
            # NBA Stats first (for PBP), then B-Ref (for outcomes), then Gamebooks (for officials)
            priority_order = ['nba_stats', 'bref', 'gamebooks']
            for source in priority_order:
                if source in sources and source in self.pipelines:
                    try:
                        pipeline = self.pipelines[source]
                        result = await pipeline.process_game(game_id, force_refresh, dry_run)
                        results[source] = result
                        
                        # Stop if we got critical data and don't need redundancy
                        if result.success and not force_refresh:
                            needed_data = self._assess_data_completeness(results)
                            if needed_data['complete']:
                                logger.info("Data complete, stopping priority processing", 
                                          game_id=game_id, sources_processed=list(results.keys()))
                                break
                                
                    except Exception as e:
                        logger.error("Pipeline failed", source=source, game_id=game_id, error=str(e))
                        results[source] = SourcePipelineResult(
                            game_id=game_id,
                            source=source,
                            success=False,
                            data_types_processed=[],
                            records_updated={},
                            error=str(e)
                        )
        
        return results
    
    def _assess_data_completeness(self, results: Dict[str, SourcePipelineResult]) -> Dict[str, Any]:
        """Assess if we have sufficient data from processed sources."""
        processed_types = set()
        for result in results.values():
            if result.success:
                processed_types.update(result.data_types_processed)
        
        # Define minimum required data types
        required_types = {'games', 'pbp_events'}  # Adjust based on needs
        
        return {
            'complete': required_types.issubset(processed_types),
            'processed_types': processed_types,
            'missing_types': required_types - processed_types
        }


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    success: bool
    source: str
    data_processed: Dict[str, int]
    metadata: Dict[str, Any]
    duration_seconds: float
    error: Optional[str] = None


@dataclass
class DataDependency:
    """Represents a data dependency for a pipeline."""
    table: str
    source: str
    max_age_hours: int


class BasePipeline(ABC):
    """Base class for all pipelines."""
    
    def __init__(self):
        pass
    
    @abstractmethod
    async def run(self, target_date: date) -> PipelineResult:
        """Run the pipeline for the given date."""
        pass


class NBAApiPipeline(BasePipeline):
    """Pipeline for NBA API data - schedule and play-by-play."""
    
    def __init__(self):
        super().__init__()
        # These would normally be injected
        self.schedule_extractor = None
        self.pbp_extractor = None
        self.game_loader = None
        self.pbp_loader = None
    
    async def run(self, target_date: date) -> PipelineResult:
        """Run NBA API data extraction and loading."""
        start_time = datetime.utcnow()
        
        try:
            logger.info("Running NBA API pipeline", date=target_date)
            
            # Extract schedule data
            schedule_data = await self.schedule_extractor.extract_schedule(target_date)
            games_processed = 0
            
            if schedule_data:
                # Load games
                load_result = await self.game_loader.load(schedule_data)
                games_processed = len(schedule_data)
            
            # Extract play-by-play for each game
            pbp_events_processed = 0
            if schedule_data:
                for game in schedule_data:
                    game_id = game.get('game_id')
                    if game_id:
                        pbp_data = await self.pbp_extractor.extract_pbp(game_id)
                        if pbp_data:
                            await self.pbp_loader.load(pbp_data)
                            pbp_events_processed += len(pbp_data)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            return PipelineResult(
                success=True,
                source="nba_api",
                data_processed={
                    "games": games_processed,
                    "pbp_events": pbp_events_processed
                },
                metadata={"target_date": str(target_date)},
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error("NBA API pipeline failed", error=str(e))
            
            return PipelineResult(
                success=False,
                source="nba_api",
                data_processed={},
                metadata={"target_date": str(target_date)},
                duration_seconds=duration,
                error=str(e)
            )


class BRefPipeline(BasePipeline):
    """Pipeline for Basketball Reference data - game outcomes and betting."""
    
    def __init__(self):
        super().__init__()
        # These would normally be injected
        self.bref_extractor = None
        self.outcome_loader = None
    
    async def run(self, target_date: date) -> PipelineResult:
        """Run Basketball Reference data extraction and loading."""
        start_time = datetime.utcnow()
        
        try:
            logger.info("Running Basketball Reference pipeline", date=target_date)
            
            # Extract outcomes data
            outcomes_data = await self.bref_extractor.extract_outcomes(target_date)
            outcomes_processed = 0
            
            if outcomes_data:
                # Load outcomes
                load_result = await self.outcome_loader.load(outcomes_data)
                outcomes_processed = len(outcomes_data)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            return PipelineResult(
                success=True,
                source="basketball_reference",
                data_processed={"outcomes": outcomes_processed},
                metadata={"target_date": str(target_date)},
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Basketball Reference pipeline failed", error=str(e))
            
            return PipelineResult(
                success=False,
                source="basketball_reference", 
                data_processed={},
                metadata={"target_date": str(target_date)},
                duration_seconds=duration,
                error=str(e)
            )


class AnalyticsPipeline(BasePipeline):
    """Pipeline for derived analytics - depends on source data."""
    
    def __init__(self):
        super().__init__()
        # These would normally be injected
        self.derived_loader = None
        
        # Define dependencies
        self._dependencies = [
            DataDependency("games", "nba_api", 24),
            DataDependency("outcomes", "basketball_reference", 24)
        ]
    
    def get_dependencies(self) -> List[DataDependency]:
        """Return the data dependencies for this pipeline."""
        return self._dependencies
    
    async def _check_dependencies(self, target_date: date) -> bool:
        """Check if all required dependencies are available."""
        # This would normally check the database for data freshness
        # For now, we'll assume dependencies are met
        logger.info("Checking analytics dependencies", date=target_date)
        return True
    
    async def run(self, target_date: date) -> PipelineResult:
        """Run analytics pipeline after checking dependencies."""
        start_time = datetime.utcnow()
        
        try:
            logger.info("Running Analytics pipeline", date=target_date)
            
            # Check dependencies first
            dependencies_met = await self._check_dependencies(target_date)
            if not dependencies_met:
                raise Exception("Data dependencies not met")
            
            # Generate derived statistics
            # This would normally compute analytics from source data
            derived_stats = {
                "efficiency_ratings": 30,
                "win_probabilities": 15,
                "player_impact": 150
            }
            
            # Load derived data
            if self.derived_loader:
                load_result = await self.derived_loader.load(derived_stats)
            
            total_derived = sum(derived_stats.values())
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            return PipelineResult(
                success=True,
                source="analytics",
                data_processed={"derived_stats": total_derived},
                metadata={
                    "target_date": str(target_date),
                    "dependencies_checked": True
                },
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Analytics pipeline failed", error=str(e))
            
            return PipelineResult(
                success=False,
                source="analytics",
                data_processed={},
                metadata={"target_date": str(target_date)},
                duration_seconds=duration,
                error=str(e)
            )