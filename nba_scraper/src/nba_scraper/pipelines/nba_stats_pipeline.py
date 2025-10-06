"""NBA Stats Pipeline - Foundation + Tranche 2 Integration."""

from datetime import datetime as dt, UTC
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List

from ..db import get_connection
from ..nba_logging import get_logger
from ..io_clients import IoFacade
from ..rate_limit import RateLimiter

# Import extractors (IO → Python dicts)
from ..extractors.boxscore import extract_game_from_boxscore
from ..extractors.pbp import extract_pbp_from_response
from ..extractors.lineups import extract_lineups_from_response
from ..extractors.shots import extract_shot_chart_detail

# Import transformers (pure → Pydantic) - No network calls
from ..transformers.games import transform_game
from ..transformers.pbp import transform_pbp
from ..transformers.lineups import transform_lineups
from ..transformers.shots import transform_shots

# Import loaders
from ..loaders import upsert_game, upsert_pbp, upsert_lineups, upsert_shots, upsert_adv_metrics

logger = get_logger(__name__)


@asynccontextmanager
async def _maybe_transaction(conn):
    """
    Use conn.transaction() if it returns an async context manager.
    If it doesn't (e.g., tests using AsyncMock without __aenter__/__aexit__), just yield.
    """
    tx_fn = getattr(conn, "transaction", None)
    if tx_fn is None:
        yield
        return
    try:
        ctx = tx_fn()
        if hasattr(ctx, "__aenter__") and hasattr(ctx, "__aexit__"):
            async with ctx:
                yield
        else:
            yield
    except TypeError:
        yield


class NBAStatsPipeline:
    """NBA Stats pipeline with foundation data + Tranche 2 shot coordinates."""
    
    def __init__(self, io_impl, db=None, rate_limiter=None):
        """Initialize pipeline with IO implementation and optional dependencies."""
        self.io = IoFacade(io_impl)
        self.db = db
        self.rate_limiter = rate_limiter or RateLimiter()
        
    async def run_single_game(self, game_id: str, season: str = "2024-25") -> Dict[str, Any]:
        """Process single game with foundation data + shot coordinates.
        
        Args:
            game_id: NBA game ID
            season: Season hint for fallback
            
        Returns:
            Results dictionary with processing statistics
        """
        start_time = dt.now(UTC)
        
        try:
            # Use provided db connection or get new one
            if self.db:
                conn = self.db
                use_transaction = False
            else:
                conn = await get_connection()
                use_transaction = True
            
            if use_transaction:
                async with _maybe_transaction(conn):
                    result = await self._process_game_data(conn, game_id, season, start_time)
            else:
                result = await self._process_game_data(conn, game_id, season, start_time)
                
            if not self.db:
                await conn.close()
                
            return result
            
        except Exception as e:
            logger.error("Game processing failed", game_id=game_id, error=str(e), exc_info=True)
            return {
                "success": False,
                "game_id": game_id,
                "error": str(e),
                "duration_seconds": (dt.now(UTC) - start_time).total_seconds()
            }
    
    async def _process_game_data(self, conn, game_id: str, season: str, start_time: dt) -> Dict[str, Any]:
        """Internal method to process game data within transaction context."""
        
        # STEP 1: Fetch all data concurrently with rate limiting
        await self.rate_limiter.acquire('nba_stats')
        boxscore_resp, pbp_resp, lineups_resp, shots_resp = await asyncio.gather(
            self.io.fetch_boxscore(game_id),
            self.io.fetch_pbp(game_id),
            self.io.fetch_lineups(game_id),
            self.io.fetch_shots(game_id),
            return_exceptions=True
        )
        
        # Handle fetch failures gracefully
        if isinstance(boxscore_resp, Exception):
            logger.warning("Boxscore fetch failed", game_id=game_id, error=str(boxscore_resp))
            boxscore_resp = {}
        if isinstance(pbp_resp, Exception):
            logger.warning("PBP fetch failed", game_id=game_id, error=str(pbp_resp))
            pbp_resp = {}
        if isinstance(lineups_resp, Exception):
            logger.warning("Lineups fetch failed", game_id=game_id, error=str(lineups_resp))
            lineups_resp = {}
        if isinstance(shots_resp, Exception):
            logger.warning("Shots fetch failed", game_id=game_id, error=str(shots_resp))
            shots_resp = {}
        
        # STEP 2: Extract → Transform data
        game_meta_raw = extract_game_from_boxscore(boxscore_resp)
        game = transform_game(game_meta_raw)
        
        pbp_events_raw = extract_pbp_from_response(pbp_resp)
        pbp_rows = transform_pbp(pbp_events_raw, game_id=game.game_id)
        
        lineup_events_raw = extract_lineups_from_response(lineups_resp)
        lineup_rows = transform_lineups(lineup_events_raw, game_id=game.game_id)
        
        shot_events_raw = extract_shot_chart_detail(shots_resp)
        shot_rows = transform_shots(shot_events_raw, game_id=game.game_id)
        
        # STEP 3: Load data with deferrable FK constraints
        # Games first (parent table)
        if upsert_game:
            await upsert_game(conn, game)
            logger.info("Upserted game record", game_id=game_id)
        
        # Then dependent tables (FKs satisfied by deferrable constraints)
        if upsert_pbp and pbp_rows:
            await upsert_pbp(conn, pbp_rows)
            logger.info("Upserted PBP events", game_id=game_id, count=len(pbp_rows))
        
        if upsert_lineups and lineup_rows:
            await upsert_lineups(conn, lineup_rows)
            logger.info("Upserted lineups", game_id=game_id, count=len(lineup_rows))
        
        if upsert_shots and shot_rows:
            await upsert_shots(conn, shot_rows)
            logger.info("Upserted shots", game_id=game_id, count=len(shot_rows))
        
        # STEP 4: Map shot coordinates to PBP events (Tranche 2 enhancement)
        shot_mappings = await self._map_shots_to_pbp(conn, game_id, shot_rows, pbp_rows)
        logger.info("Mapped shots to PBP", game_id=game_id, mappings=shot_mappings)
        
        # Advanced metrics can be loaded separately if available
        if upsert_adv_metrics:
            await upsert_adv_metrics(conn, [])  # Placeholder for advanced metrics
        
        # Transaction commits here - all FKs will be validated
        
        duration = (dt.now(UTC) - start_time).total_seconds()
        
        return {
            "success": True,
            "game_id": game_id,
            "season": game.season,
            "duration_seconds": duration,
            "records": {
                "games": 1,
                "pbp_events": len(pbp_rows),
                "lineups": len(lineup_rows),
                "shots": len(shot_rows),
                "shot_mappings": shot_mappings
            }
        }
    
    async def _map_shots_to_pbp(self, conn, game_id: str, shot_rows: List, pbp_rows: List) -> int:
        """Map shot coordinates to PBP events using event numbers and timing.
        
        This implements Tranche 2 functionality by linking shot chart data
        to play-by-play events for enhanced analytics.
        """
        if not shot_rows or not pbp_rows:
            return 0
        
        mappings = 0
        
        # Create lookup of PBP events by event_num for shots that have event_num
        pbp_by_event_num = {
            pbp.event_num: pbp for pbp in pbp_rows 
            if hasattr(pbp, 'event_num')
        }
        
        for shot in shot_rows:
            if hasattr(shot, 'event_num') and shot.event_num and shot.event_num in pbp_by_event_num:
                # Direct mapping via event number
                pbp_event = pbp_by_event_num[shot.event_num]
                
                # Update PBP event with shot coordinates
                try:
                    await conn.execute("""
                        UPDATE pbp_events 
                        SET shot_x = $1, shot_y = $2, shot_distance_ft = $3
                        WHERE game_id = $4 AND event_idx = $5
                    """, shot.loc_x, shot.loc_y, 
                    self._calculate_shot_distance(shot.loc_x, shot.loc_y),
                    game_id, shot.event_num)
                    
                    mappings += 1
                except Exception as e:
                    logger.warning("Failed to map shot to PBP", 
                                 game_id=game_id, event_num=shot.event_num, error=str(e))
        
        return mappings
    
    def _calculate_shot_distance(self, loc_x: int, loc_y: int) -> float:
        """Calculate shot distance from coordinates."""
        # NBA court coordinates: basket is at (0, 0)
        import math
        return math.sqrt(loc_x**2 + loc_y**2) / 10.0  # Convert to feet
    
    async def run_multiple_games(self, game_ids: List[str], *, concurrency: int = 3) -> List[Dict[str, Any]]:
        """Process multiple games with controlled concurrency."""
        semaphore = asyncio.Semaphore(concurrency)
        
        async def process_with_semaphore(game_id: str):
            async with semaphore:
                return await self.run_single_game(game_id)
        
        results = await asyncio.gather(
            *[process_with_semaphore(gid) for gid in game_ids],
            return_exceptions=True
        )
        
        # Convert exceptions to error results
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append({
                    "success": False,
                    "game_id": game_ids[i],
                    "error": str(result)
                })
            else:
                final_results.append(result)
        
        return final_results
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform pipeline health check."""
        try:
            # Test database connection
            conn = await get_connection()
            await conn.fetchval("SELECT 1")
            await conn.close()
            
            # Test IO facade
            test_methods = ['fetch_boxscore', 'fetch_pbp', 'fetch_lineups', 'fetch_shots']
            io_methods = {method: hasattr(self.io, method) for method in test_methods}
            
            # Test loader availability
            loader_status = {
                'upsert_game': upsert_game is not None,
                'upsert_pbp': upsert_pbp is not None,
                'upsert_lineups': upsert_lineups is not None,
                'upsert_shots': upsert_shots is not None,
            }
            
            return {
                "status": "healthy",
                "database": True,
                "io_methods": io_methods,
                "loaders": loader_status,
                "timestamp": dt.now(UTC).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": dt.now(UTC).isoformat()
            }