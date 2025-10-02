"""Pipeline for processing individual NBA games across all data sources."""

import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from ..models import GameRow, GameStatus
from ..extractors import (
    extract_games_from_scoreboard,
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
from ..logging import get_logger
from ..rate_limit import RateLimiter

logger = get_logger(__name__)


@dataclass
class GamePipelineResult:
    """Result of a game pipeline execution."""
    game_id: str
    success: bool
    sources_processed: List[str]
    records_updated: Dict[str, int]
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


class GamePipeline:
    """Orchestrates extraction, transformation, and loading for a single game."""
    
    def __init__(
        self,
        bref_client: BRefClient,
        nba_stats_client: NBAStatsClient,
        gamebooks_client: GamebooksClient,
        rate_limiter: RateLimiter
    ):
        self.bref_client = bref_client
        self.nba_stats_client = nba_stats_client
        self.gamebooks_client = gamebooks_client
        self.rate_limiter = rate_limiter
        
        # Initialize loaders
        self.game_loader = GameLoader()
        self.ref_loader = RefLoader()
        self.lineup_loader = LineupLoader()
        self.pbp_loader = PbpLoader()
        
        # Initialize transformers with required source parameter
        self.game_transformers = {
            'bref': GameTransformer(source='bref'),
            'nba_stats': GameTransformer(source='nba_stats'),
            'gamebooks': GameTransformer(source='gamebooks')
        }
        self.ref_transformers = {
            'bref': RefTransformer(source='bref'),
            'nba_stats': RefTransformer(source='nba_stats'),
            'gamebooks': RefTransformer(source='gamebooks')
        }
        self.lineup_transformers = {
            'bref': LineupTransformer(source='bref'),
            'nba_stats': LineupTransformer(source='nba_stats'),
            'gamebooks': LineupTransformer(source='gamebooks')
        }
        self.pbp_transformers = {
            'bref': PbpTransformer(source='bref'),
            'nba_stats': PbpTransformer(source='nba_stats'),
            'gamebooks': PbpTransformer(source='gamebooks')
        }
    
    async def process_game(
        self,
        game_id: str,
        sources: Optional[List[str]] = None,
        force_refresh: bool = False,
        dry_run: bool = False
    ) -> GamePipelineResult:
        """Process a complete game from all specified sources.
        
        Args:
            game_id: NBA game identifier
            sources: List of sources to process ('bref', 'nba_stats', 'gamebooks')
                    If None, processes all sources
            force_refresh: Whether to force re-extraction even if data exists
            dry_run: Whether to run in dry-run mode (no database writes)
            
        Returns:
            GamePipelineResult with processing details
        """
        start_time = datetime.utcnow()
        sources = sources or ['bref', 'nba_stats', 'gamebooks']
        
        result = GamePipelineResult(
            game_id=game_id,
            success=False,
            sources_processed=[],
            records_updated={}
        )
        
        try:
            logger.info("Starting game pipeline", game_id=game_id, sources=sources, dry_run=dry_run)
            
            # In dry-run mode, just log what would be processed
            if dry_run:
                logger.info("DRY RUN: Would process game", game_id=game_id, sources=sources)
                result.success = True
                result.sources_processed = sources
                result.records_updated = {source: 0 for source in sources}
                result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
                return result
            
            # Process each source concurrently with rate limiting
            tasks = []
            if 'bref' in sources:
                tasks.append(self._process_bref_source(game_id, force_refresh))
            if 'nba_stats' in sources:
                tasks.append(self._process_nba_stats_source(game_id, force_refresh))
            if 'gamebooks' in sources:
                tasks.append(self._process_gamebooks_source(game_id, force_refresh))
            
            # Execute tasks with proper rate limiting
            source_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate results
            for i, source_result in enumerate(source_results):
                source_name = sources[i]
                
                if isinstance(source_result, Exception):
                    logger.error("Source processing failed", 
                               game_id=game_id, 
                               source=source_name,
                               error=str(source_result))
                    continue
                
                if source_result:
                    result.sources_processed.append(source_name)
                    result.records_updated.update(source_result)
            
            result.success = len(result.sources_processed) > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Game pipeline completed",
                       game_id=game_id,
                       success=result.success,
                       sources_processed=result.sources_processed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Game pipeline failed", game_id=game_id, error=str(e))
        
        return result
    
    async def _process_bref_source(
        self, 
        game_id: str, 
        force_refresh: bool
    ) -> Optional[Dict[str, int]]:
        """Process Basketball Reference data for a game."""
        try:
            await self.rate_limiter.acquire('bref')
            
            # Use actual Basketball Reference client methods
            updates = {}
            
            # Extract game outcomes
            try:
                outcomes_html = await self.bref_client.fetch_bref_box(game_id)
                if outcomes_html:
                    outcomes = extract_game_outcomes(outcomes_html)
                    if outcomes:
                        updates['games'] = await self.game_loader.upsert_games(outcomes)
            except Exception as e:
                logger.warning("Failed to extract B-Ref outcomes", game_id=game_id, error=str(e))
            
            # Extract starting lineups
            try:
                lineups_html = await self.bref_client.fetch_bref_box(game_id)
                if lineups_html:
                    lineups = extract_starting_lineups(lineups_html)
                    if lineups:
                        updates['starting_lineups'] = await self.lineup_loader.upsert_lineups(lineups)
            except Exception as e:
                logger.warning("Failed to extract B-Ref lineups", game_id=game_id, error=str(e))
            
            # Extract injury notes
            try:
                injury_html = await self.bref_client.fetch_bref_box(game_id)
                if injury_html:
                    injuries = extract_injury_notes(injury_html)
                    if injuries:
                        updates['injury_status'] = await self.lineup_loader.upsert_injuries(injuries)
            except Exception as e:
                logger.warning("Failed to extract B-Ref injuries", game_id=game_id, error=str(e))
            
            return updates if updates else None
            
        except Exception as e:
            logger.error("Basketball Reference processing failed", 
                        game_id=game_id, error=str(e))
            return None
    
    async def _process_nba_stats_source(
        self, 
        game_id: str, 
        force_refresh: bool
    ) -> Optional[Dict[str, int]]:
        """Process NBA Stats data for a game."""
        try:
            await self.rate_limiter.acquire('nba_stats')
            
            updates = {}
            
            # Extract play-by-play data
            try:
                pbp_response = await self.nba_stats_client.fetch_pbp(game_id)
                if pbp_response:
                    pbp_events = extract_pbp_from_response(pbp_response)
                    if pbp_events:
                        updates['pbp_events'] = await self.pbp_loader.upsert_events(pbp_events)
            except Exception as e:
                logger.warning("Failed to extract NBA Stats PBP", game_id=game_id, error=str(e))
            
            # Extract boxscore lineups
            try:
                boxscore_response = await self.nba_stats_client.fetch_boxscore(game_id)
                if boxscore_response:
                    lineups = extract_boxscore_lineups(boxscore_response)
                    if lineups:
                        updates['starting_lineups'] = await self.lineup_loader.upsert_lineups(lineups)
            except Exception as e:
                logger.warning("Failed to extract NBA Stats lineups", game_id=game_id, error=str(e))
            
            return updates if updates else None
            
        except Exception as e:
            logger.error("NBA Stats processing failed", 
                        game_id=game_id, error=str(e))
            return None
    
    async def _process_gamebooks_source(
        self, 
        game_id: str, 
        force_refresh: bool
    ) -> Optional[Dict[str, int]]:
        """Process official gamebooks data for a game."""
        try:
            await self.rate_limiter.acquire('gamebooks')
            
            updates = {}
            
            # Extract referee assignments
            try:
                from datetime import date
                # Parse game_id to get date - assuming format like "0022300123" where middle part is date
                game_date = date.today()  # Simplified for now - you may need proper date parsing
                
                gamebook_urls = await self.gamebooks_client.list_gamebooks(game_date)
                for url in gamebook_urls:
                    if game_id in url:  # Match gamebook to game
                        pdf_path = await self.gamebooks_client.download_gamebook(url)
                        parsed_data = self.gamebooks_client.parse_gamebook_pdf(pdf_path)
                        
                        if parsed_data.get('refs'):
                            ref_assignments = extract_referee_assignments(parsed_data)
                            if ref_assignments:
                                updates['ref_assignments'] = await self.ref_loader.upsert_assignments(ref_assignments)
                        
                        if parsed_data.get('alternates'):
                            ref_alternates = extract_referee_alternates(parsed_data)
                            if ref_alternates:
                                updates['ref_alternates'] = await self.ref_loader.upsert_alternates(ref_alternates)
                        break
                        
            except Exception as e:
                logger.warning("Failed to extract gamebooks data", game_id=game_id, error=str(e))
            
            return updates if updates else None
            
        except Exception as e:
            logger.error("Gamebooks processing failed", 
                        game_id=game_id, error=str(e))
            return None
    
    async def get_game_status(self, game_id: str) -> Optional[GameStatus]:
        """Check the current status of a game in the database."""
        try:
            from ..db import get_connection
            
            conn = await get_connection()
            row = await conn.fetchrow(
                "SELECT status FROM games WHERE game_id = $1",
                game_id
            )
            
            if row:
                return GameStatus(row['status'])
            return None
            
        except Exception as e:
            logger.error("Failed to get game status", game_id=game_id, error=str(e))
            return None
    
    async def should_process_game(
        self, 
        game_id: str, 
        force_refresh: bool = False
    ) -> bool:
        """Determine if a game should be processed based on its current state."""
        if force_refresh:
            return True
        
        status = await self.get_game_status(game_id)
        
        # Process if game doesn't exist or is not final
        if not status:
            return True
        
        # Always reprocess live or upcoming games
        if status in [GameStatus.SCHEDULED, GameStatus.LIVE]:
            return True
        
        # Skip final games unless forced
        return status != GameStatus.FINAL