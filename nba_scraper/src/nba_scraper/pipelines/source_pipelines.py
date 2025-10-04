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
from ..loaders import GameLoader, RefLoader, LineupLoader, PbpLoader, AdvancedMetricsLoader
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
        # Add advanced metrics loader for Tranche 1
        self.advanced_metrics_loader = AdvancedMetricsLoader()
    
    def get_supported_data_types(self) -> Set[str]:
        """NBA Stats provides comprehensive game data."""
        return {
            'games', 'pbp_events', 'starting_lineups', 
            'boxscore_stats', 'player_stats', 'advanced_metrics'  # Added advanced metrics
        }
    
    def get_priority_data_types(self) -> Set[str]:
        """NBA Stats is the primary source for PBP and real-time data."""
        return {'pbp_events', 'boxscore_stats', 'player_stats', 'advanced_metrics'}  # Added advanced metrics
    
    async def process_game(
        self,
        game_id: str,
        force_refresh: bool = False,
        dry_run: bool = False
    ) -> SourcePipelineResult:
        """Process game data from NBA Stats API including advanced metrics (Tranche 1)."""
        start_time = datetime.utcnow()
        result = await self._create_result(game_id, start_time)
        
        try:
            logger.info("Processing NBA Stats data", game_id=game_id, dry_run=dry_run)
            
            # Import core extractors
            from ..extractors import (
                extract_games_from_scoreboard,
                extract_pbp_from_response,
                extract_boxscore_lineups,
                extract_advanced_player_stats,
                extract_advanced_team_stats,
                extract_misc_player_stats,
                extract_usage_player_stats,
                extract_shot_chart_detail
            )
            
            # Apply rate limiting
            await self.rate_limiter.acquire()

            # STEP 1: Fetch basic game data and PBP events (foundational data)
            # This must come first so advanced metrics have foreign keys to reference
            
            total_records = 0
            shot_chart_data = {}
            
            # Fetch PBP events first (needed for shot coordinate attachment)
            try:
                logger.info("Fetching PBP events", game_id=game_id)
                pbp_response = await self.client.fetch_pbp(game_id)
                
                if pbp_response:
                    source_url = f"https://stats.nba.com/stats/playbyplayv2?GameID={game_id}"
                    pbp_events = extract_pbp_from_response(pbp_response, game_id, source_url)
                    
                    if pbp_events and not dry_run:
                        # Transform and load PBP events
                        transformed_events = []
                        for event in pbp_events:
                            try:
                                transformed_event = await self.pbp_transformer.transform(event)
                                transformed_events.append(transformed_event)
                            except Exception as e:
                                logger.warning("Failed to transform PBP event", game_id=game_id, error=str(e))
                                continue
                        
                        if transformed_events:
                            records = await self.pbp_loader.upsert_events(transformed_events)
                            result.records_updated['pbp_events'] = records
                            total_records += records
                            logger.info("Loaded PBP events", game_id=game_id, records=records)
                    
            except Exception as e:
                logger.warning("Failed to process PBP events", game_id=game_id, error=str(e))
            
            # Fetch boxscore for game metadata and lineups
            try:
                logger.info("Fetching boxscore for game data", game_id=game_id)
                boxscore_response = await self.client.fetch_boxscore(game_id)
                
                if boxscore_response:
                    source_url = f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={game_id}"
                    
                    # FIRST: Extract and create game record (needed for foreign keys)
                    try:
                        # Extract game metadata from boxscore 
                        if 'resultSets' in boxscore_response:
                            for result_set in boxscore_response['resultSets']:
                                if result_set.get('name') == 'GameSummary':
                                    headers = result_set.get('headers', [])
                                    rows = result_set.get('rowSet', [])
                                    if rows:
                                        game_data = dict(zip(headers, rows[0]))
                                        
                                        # Create game record from boxscore data
                                        from ..extractors.nba_stats import GameRow
                                        game_row = GameRow.from_nba_stats(game_data, source_url)
                                        
                                        if not dry_run:
                                            # Transform and load game record
                                            transformed_game = await self.game_transformer.transform(game_row)
                                            records = await self.game_loader.upsert_games([transformed_game])
                                            result.records_updated['games'] = records
                                            total_records += records
                                            logger.info("Loaded game record", game_id=game_id, records=records)
                                        break
                    except Exception as e:
                        logger.warning("Failed to create game record from boxscore", game_id=game_id, error=str(e))
                    
                    # THEN: Extract starting lineups
                    lineups = extract_boxscore_lineups(boxscore_response, game_id, source_url)
                    if lineups and not dry_run:
                        transformed_lineups = []
                        for lineup in lineups:
                            try:
                                transformed_lineup = await self.lineup_transformer.transform(lineup)
                                transformed_lineups.append(transformed_lineup)
                            except Exception as e:
                                logger.warning("Failed to transform lineup", game_id=game_id, error=str(e))
                                continue
                        
                        if transformed_lineups:
                            records = await self.lineup_loader.upsert_lineups(transformed_lineups)
                            result.records_updated['starting_lineups'] = records
                            total_records += records
                            logger.info("Loaded starting lineups", game_id=game_id, records=records)
                    
            except Exception as e:
                logger.warning("Failed to process boxscore", game_id=game_id, error=str(e))
            
            # STEP 2: Fetch shot chart data for coordinate enhancement (Tranche 2)
            
            # First, try to fetch shot chart data for coordinate enhancement
            try:
                logger.info("Fetching shot chart data for coordinate enhancement", game_id=game_id)
                
                # Use the reliable nba_api method instead of problematic REST calls
                shot_response = await self.client.fetch_shotchart_reliable(game_id, '2024-25')
                
                if shot_response:
                    source_url = f"https://stats.nba.com/stats/shotchartdetail?GameID={game_id}"
                    shot_chart_data = extract_shot_chart_detail(shot_response, game_id, source_url)
                    logger.info("Fetched shot chart data", game_id=game_id, shots=len(shot_chart_data))
                
            except Exception as e:
                logger.warning("Failed to fetch shot chart data - continuing without coordinates", 
                             game_id=game_id, error=str(e))
                # Continue without shot chart data - this is not critical for Tranche 1
            
            # Process advanced boxscore data
            try:
                logger.info("Fetching advanced boxscore data", game_id=game_id)
                advanced_data = await self.client.fetch_boxscore_advanced(game_id)
                
                if advanced_data:
                    source_url = f"https://stats.nba.com/stats/boxscoreadvancedv2?GameID={game_id}"
                    
                    # Extract advanced player stats
                    advanced_player_stats = extract_advanced_player_stats(advanced_data, game_id, source_url)
                    if advanced_player_stats and not dry_run:
                        records = await self.advanced_metrics_loader.upsert_advanced_player_stats(advanced_player_stats)
                        result.records_updated['advanced_player_stats'] = records
                        total_records += records
                        logger.info("Loaded advanced player stats", game_id=game_id, records=records)
                    
                    # Extract advanced team stats
                    advanced_team_stats = extract_advanced_team_stats(advanced_data, game_id, source_url)
                    if advanced_team_stats and not dry_run:
                        records = await self.advanced_metrics_loader.upsert_advanced_team_stats(advanced_team_stats)
                        result.records_updated['advanced_team_stats'] = records
                        total_records += records
                        logger.info("Loaded advanced team stats", game_id=game_id, records=records)
                        
            except Exception as e:
                logger.warning("Failed to process advanced boxscore", game_id=game_id, error=str(e))
            
            # Process misc boxscore data
            try:
                logger.info("Fetching misc boxscore data", game_id=game_id)
                misc_data = await self.client.fetch_boxscore_misc(game_id)
                
                if misc_data:
                    source_url = f"https://stats.nba.com/stats/boxscoremiscv2?GameID={game_id}"
                    
                    # Extract misc player stats
                    misc_player_stats = extract_misc_player_stats(misc_data, game_id, source_url)
                    if misc_player_stats and not dry_run:
                        records = await self.advanced_metrics_loader.upsert_misc_player_stats(misc_player_stats)
                        result.records_updated['misc_player_stats'] = records
                        total_records += records
                        logger.info("Loaded misc player stats", game_id=game_id, records=records)
                        
            except Exception as e:
                logger.warning("Failed to process misc boxscore", game_id=game_id, error=str(e))
            
            # Process usage boxscore data
            try:
                logger.info("Fetching usage boxscore data", game_id=game_id)
                usage_data = await self.client.fetch_boxscore_usage(game_id)
                
                if usage_data:
                    source_url = f"https://stats.nba.com/stats/boxscoreusagev2?GameID={game_id}"
                    
                    # Extract usage player stats
                    usage_player_stats = extract_usage_player_stats(usage_data, game_id, source_url)
                    if usage_player_stats and not dry_run:
                        records = await self.advanced_metrics_loader.upsert_usage_player_stats(usage_player_stats)
                        result.records_updated['usage_player_stats'] = records
                        total_records += records
                        logger.info("Loaded usage player stats", game_id=game_id, records=records)
                        
            except Exception as e:
                logger.warning("Failed to process usage boxscore", game_id=game_id, error=str(e))
            
            # Update result
            result.success = True
            result.data_types_processed = ['advanced_metrics']
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("NBA Stats processing completed", 
                       game_id=game_id, 
                       total_records=total_records,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            logger.error("NBA Stats processing failed", game_id=game_id, error=str(e))
        
        return result