"""Refactored foundation pipeline with hardened preprocessing and clock handling."""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional
import asyncpg

from ..http import NBAStatsClient
from ..db import get_connection
from ..nba_logging import get_logger
from ..utils.db import maybe_transaction

# Extractors - shape raw API responses to dicts/lists
from ..extractors.boxscore import extract_game_from_boxscore
from ..extractors.pbp import extract_pbp_from_response
from ..extractors.lineups import extract_lineups_from_response

# Transformers - apply preprocessing and convert to validated models
from ..transformers.games import transform_game
from ..transformers.pbp import transform_pbp
from ..transformers.lineups import transform_lineups

# Loaders - persist to database with idempotent upserts
from ..loaders.games import upsert_game
from ..loaders.pbp import upsert_pbp
from ..loaders.lineups import upsert_lineups


class FoundationPipeline:
    """
    Refactored foundation pipeline that processes NBA Stats API data.
    
    Architecture:
    1. Extract: Raw API → dicts/lists (no preprocessing)
    2. Transform: Apply preprocessing → validated Pydantic models
    3. Load: Models → database with idempotent upserts
    
    Key features:
    - Hardened preprocessing prevents clock string coercion
    - Auto-derived clock_seconds field with fractional support
    - Deferrable FK constraints for transaction safety
    - Comprehensive error handling and logging
    """
    
    def __init__(self, client: Optional[NBAStatsClient] = None):
        self.client = client or NBAStatsClient()
        self.logger = get_logger(__name__)
    
    async def process_game(self, game_id: str, *, skip_pbp: bool = False, skip_lineups: bool = False) -> dict:
        """
        Process a complete game through the foundation pipeline.
        
        Args:
            game_id: NBA game identifier
            skip_pbp: Skip PBP processing (for testing)
            skip_lineups: Skip lineup processing (for testing)
            
        Returns:
            dict: Processing results with counts and any errors
        """
        results = {
            'game_id': game_id,
            'game_processed': False,
            'pbp_events_processed': 0,
            'lineups_processed': 0,
            'errors': []
        }
        
        conn = None
        try:
            # Get database connection
            conn = await get_connection()
            
            # Start transaction with deferred constraints
            async with maybe_transaction(conn):
                await conn.execute('SET CONSTRAINTS ALL DEFERRED')
                
                self.logger.info(f"🏀 Processing game {game_id} through foundation pipeline")
                
                # Step 1: Process game metadata from boxscore
                boxscore_resp = None
                try:
                    boxscore_resp = await self.client.get_boxscore(game_id)
                    
                    # Extract → Transform → Load game metadata
                    game_meta = extract_game_from_boxscore(boxscore_resp)
                    game_model = transform_game(game_meta)
                    await upsert_game(conn, game_model)
                    
                    # Set game_processed to True after successful upsert
                    results['game_processed'] = True
                    self.logger.info(f"✅ Game metadata processed: {game_model.season} {game_model.game_date}")
                    
                except Exception as e:
                    error = f"Game metadata processing failed: {e}"
                    results['errors'].append(error)
                    self.logger.error(error, game_id=game_id, exc_info=True)
                    # Keep game_processed=False but continue with PBP/lineups if we have boxscore data
                
                # Step 2: Process PBP events (with clock handling) - separate try/except
                # Continue even if game metadata processing failed, as long as we have the API response
                if not skip_pbp:
                    try:
                        pbp_resp = await self.client.get_pbp(game_id)
                        
                        # Extract → Transform → Load PBP events
                        raw_events = extract_pbp_from_response(pbp_resp)
                        pbp_models = transform_pbp(raw_events, game_id)
                        await upsert_pbp(conn, pbp_models)
                        
                        results['pbp_events_processed'] = len(pbp_models)
                        self.logger.info(f"✅ PBP events processed: {len(pbp_models)} events with clock_seconds")
                        
                        # Log a sample of clock parsing results
                        if pbp_models:
                            sample = pbp_models[0]
                            self.logger.debug(f"Clock sample: '{sample.clock}' → {sample.clock_seconds}s")
                        
                    except Exception as e:
                        error = f"PBP processing failed: {e}"
                        results['errors'].append(error)
                        self.logger.error(error, game_id=game_id, exc_info=True)
                        # Note: Do NOT reset game_processed to False
                
                # Step 3: Process lineup stints - separate try/except
                # Continue even if game metadata processing failed, as long as we have boxscore data
                if not skip_lineups and boxscore_resp is not None:
                    try:
                        # Reuse boxscore response for lineup extraction
                        raw_lineups = extract_lineups_from_response(boxscore_resp)
                        lineup_models = transform_lineups(raw_lineups, game_id)
                        await upsert_lineups(conn, lineup_models)
                        
                        results['lineups_processed'] = len(lineup_models)
                        self.logger.info(f"✅ Lineup stints processed: {len(lineup_models)} stints")
                        
                    except Exception as e:
                        error = f"Lineup processing failed: {e}"
                        results['errors'].append(error)
                        self.logger.error(error, game_id=game_id, exc_info=True)
                        # Note: Do NOT reset game_processed to False
                
                # Transaction will auto-commit if we reach here
                self.logger.info(f"🎯 Foundation pipeline completed for {game_id}")
        
        except Exception as e:
            error = f"Pipeline transaction failed: {e}"
            results['errors'].append(error)
            self.logger.error(error, game_id=game_id, exc_info=True)
        
        finally:
            if conn:
                await conn.close()
        
        return results
    
    async def process_multiple_games(self, game_ids: list[str], *, concurrency: int = 3) -> list[dict]:
        """
        Process multiple games with controlled concurrency.
        
        Args:
            game_ids: List of NBA game identifiers
            concurrency: Maximum concurrent processing
            
        Returns:
            list[dict]: Results for each game processed
        """
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_with_semaphore(game_id: str) -> dict:
            async with semaphore:
                return await self.process_game(game_id)
        
        tasks = [process_with_semaphore(game_id) for game_id in game_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to error results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    'game_id': game_ids[i],
                    'game_processed': False,
                    'pbp_events_processed': 0,
                    'lineups_processed': 0,
                    'errors': [f"Pipeline exception: {result}"]
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def health_check(self) -> dict:
        """
        Perform a health check of the pipeline components.
        
        Returns:
            dict: Health status of each component
        """
        health = {
            'api_client': False,
            'database': False,
            'preprocessing': False,
            'clock_parsing': False
        }
        
        try:
            # Test API client
            await self.client.get_today_scoreboard()
            health['api_client'] = True
        except Exception:
            pass
        
        try:
            # Test database connection
            conn = await get_connection()
            await conn.execute('SELECT 1')
            await conn.close()
            health['database'] = True
        except Exception:
            pass
        
        try:
            # Test preprocessing
            from ..utils.preprocess import preprocess_nba_stats_data
            test_data = {"PCTIMESTRING": "24:49", "VALUE": "123"}
            result = preprocess_nba_stats_data(test_data)
            # Verify clock string is preserved, numeric is converted
            if result["PCTIMESTRING"] == "24:49" and result["VALUE"] == 123:
                health['preprocessing'] = True
        except Exception:
            pass
        
        try:
            # Test clock parsing
            from ..utils.clock import parse_clock_to_seconds
            if parse_clock_to_seconds("24:49") == 1489.0:
                health['clock_parsing'] = True
        except Exception:
            pass
        
        return health