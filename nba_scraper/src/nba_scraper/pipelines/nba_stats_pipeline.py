"""NBA Stats Pipeline - Foundation + Tranche 2 Integration."""

from datetime import datetime as dt
import asyncio
from typing import Optional

from ..db import get_connection
from ..nba_logging import get_logger
from ..io_clients.nba_stats import NBAStatsClient
from ..rate_limit import RateLimiter

# Import extractors (IO → Python dicts)
from ..extractors.boxscore import extract_game_from_boxscore
from ..extractors.pbp import extract_pbp_from_response
from ..extractors.lineups import extract_lineups_from_response
from ..extractors.nba_stats import extract_shot_chart_detail

# Import transformers (pure → Pydantic) - No network calls
from ..transformers.games import transform_game
from ..transformers.pbp import transform_pbp
from ..transformers.lineups import transform_lineups
from ..transformers.shots import transform_shots

# Import loaders
from ..loaders.games import upsert_game
from ..loaders.pbp import upsert_pbp
from ..loaders.lineups import upsert_lineups
from ..loaders.shots import upsert_shots

logger = get_logger(__name__)


class NBAStatsPipeline:
    """
    Foundation pipeline implementing Tranche 0 + Tranche 2 integration.
    
    Pipeline is async, transformers are sync (pure). Only DB and network are awaited.
    Enforces load order inside ONE transaction with deferrable FKs.
    """
    
    def __init__(self, io_client: NBAStatsClient, rate_limiter: RateLimiter):
        self.io_client = io_client
        self.rate_limiter = rate_limiter
    
    async def run_single_game(self, game_id: str, season: str = "2024-25") -> dict:
        """
        Process a single game with foundation data + shot coordinates.
        
        Returns:
            dict with success status and metrics
        """
        start_time = dt.utcnow()
        
        try:
            logger.info("Starting NBA Stats pipeline", game_id=game_id, season=season)
            
            # 1) Fetch raw data (all network calls with rate limiting)
            await self.rate_limiter.acquire("nba_stats_pipeline")
            
            logger.info("Fetching raw API responses", game_id=game_id)
            
            # Fetch boxscore for game metadata + lineups
            bs_resp = await self.io_client.fetch_boxscore(game_id)
            
            # Fetch PBP events  
            pbp_resp = await self.io_client.fetch_pbp(game_id)
            
            # Fetch shot chart data (Tranche 2)
            shots_resp = await self.io_client.fetch_shotchart_reliable(game_id, season)
            
            logger.info("Completed API fetches", game_id=game_id)
            
            # 2) Extract raw data (pure functions, no await)
            logger.info("Extracting data structures", game_id=game_id)
            
            game_meta_raw = extract_game_from_boxscore(bs_resp)
            pbp_raw = extract_pbp_from_response(pbp_resp)
            lineups_raw = extract_lineups_from_response(bs_resp)  # Extract lineups from boxscore
            shots_raw = extract_shot_chart_detail(shots_resp, game_id, f"shotchart/{game_id}")
            
            logger.info("Extracted raw data", 
                       game_id=game_id,
                       pbp_events=len(pbp_raw),
                       lineups=len(lineups_raw),
                       shots=len(shots_raw))
            
            # 3) Transform to Pydantic models (pure functions, no await)
            logger.info("Transforming to validated models", game_id=game_id)
            
            game = transform_game(game_meta_raw)
            pbp_rows = transform_pbp(pbp_raw, game_id=game.game_id)
            lineup_rows = transform_lineups(lineups_raw, game_id=game.game_id)
            shot_rows = transform_shots(shots_raw, game_id=game.game_id)
            
            logger.info("Transformed to models", 
                       game_id=game_id,
                       pbp_count=len(pbp_rows),
                       lineup_count=len(lineup_rows),
                       shot_count=len(shot_rows))
            
            # 4) Load in one transaction with correct order (deferrable FKs)
            logger.info("Loading to database", game_id=game_id)
            
            conn = await get_connection()
            
            async with conn.transaction():
                # STEP 1: Load parent game record first
                await upsert_game(conn, game)
                logger.info("Upserted game record", game_id=game_id)
                
                # STEP 2: Load dependent tables (FKs satisfied by deferrable constraints)
                await upsert_pbp(conn, pbp_rows)
                logger.info("Upserted PBP events", game_id=game_id, count=len(pbp_rows))
                
                await upsert_lineups(conn, lineup_rows) 
                logger.info("Upserted lineups", game_id=game_id, count=len(lineup_rows))
                
                # STEP 3: Optionally store shots separately OR enrich PBP with coordinates
                # For now, store shots separately for Tranche 2
                await upsert_shots(conn, shot_rows)
                logger.info("Upserted shots", game_id=game_id, count=len(shot_rows))
                
                # STEP 4: Map shot coordinates to PBP events (Tranche 2 enhancement)
                shot_mappings = await self._map_shots_to_pbp(conn, game_id, shot_rows, pbp_rows)
                logger.info("Mapped shots to PBP", game_id=game_id, mappings=shot_mappings)
                
                # Transaction commits here - all FKs will be validated
            
            await conn.close()
            
            duration = (dt.utcnow() - start_time).total_seconds()
            
            result = {
                "success": True,
                "game_id": game_id,
                "duration_seconds": duration,
                "records": {
                    "games": 1,
                    "pbp_events": len(pbp_rows),
                    "lineups": len(lineup_rows),
                    "shots": len(shot_rows),
                    "shot_mappings": shot_mappings
                }
            }
            
            logger.info("Pipeline completed successfully", 
                       game_id=game_id, 
                       duration=duration,
                       records=result["records"])
            
            return result
            
        except Exception as e:
            duration = (dt.utcnow() - start_time).total_seconds()
            logger.error("Pipeline failed", game_id=game_id, error=str(e), duration=duration)
            
            return {
                "success": False,
                "game_id": game_id,
                "error": str(e),
                "duration_seconds": duration
            }
    
    async def _map_shots_to_pbp(self, conn, game_id: str, shot_rows, pbp_rows) -> int:
        """
        Map shot coordinates to PBP events by matching event numbers.
        Returns count of successful mappings.
        """
        if not shot_rows or not pbp_rows:
            return 0
        
        # Create mapping from shot event_num to coordinates
        shot_map = {}
        for shot in shot_rows:
            if shot.event_num is not None:
                shot_map[shot.event_num] = {
                    'loc_x': shot.loc_x,
                    'loc_y': shot.loc_y,
                    'shot_distance': self._calculate_shot_distance(shot.loc_x, shot.loc_y),
                    'shot_zone': self._classify_shot_zone(shot.loc_x, shot.loc_y)
                }
        
        # Update PBP events with shot coordinates
        mappings = 0
        for shot_event_num, coords in shot_map.items():
            result = await conn.execute("""
                UPDATE pbp_events 
                SET loc_x = $1, loc_y = $2, shot_distance = $3, shot_zone = $4
                WHERE game_id = $5 AND event_num = $6
                  AND action_type IN (1, 2)  -- Only shot events
            """, coords['loc_x'], coords['loc_y'], coords['shot_distance'], 
                coords['shot_zone'], game_id, shot_event_num)
            
            # Extract count from result like "UPDATE 1"
            if result and 'UPDATE' in result:
                count = int(result.split()[-1])
                mappings += count
        
        return mappings
    
    def _calculate_shot_distance(self, loc_x: int, loc_y: int) -> int:
        """Calculate shot distance in feet from coordinates."""
        import math
        # NBA court coordinates: center is (0,0), basket at (0, 0)
        # Convert to feet (approximate)
        distance_units = math.sqrt(loc_x**2 + loc_y**2)
        distance_feet = int(distance_units / 10)  # Rough conversion
        return max(0, distance_feet)
    
    def _classify_shot_zone(self, loc_x: int, loc_y: int) -> str:
        """Classify shot zone based on coordinates."""
        import math
        
        distance = math.sqrt(loc_x**2 + loc_y**2)
        
        if distance < 80:
            return "Paint"
        elif distance < 160:
            return "Mid-Range"
        elif distance < 240:
            return "Above the Break 3"
        else:
            return "Backcourt"