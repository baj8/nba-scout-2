"""Play-by-play data loader with batch upserts."""

from datetime import datetime
from typing import List

from ..models import PbpEventRow
from ..db import get_connection
from ..logging import get_logger

logger = get_logger(__name__)


class PbpLoader:
    """Loader for play-by-play events with batch processing."""
    
    async def upsert_events(self, events: List[PbpEventRow], batch_size: int = 1000) -> int:
        """Upsert PBP events in batches for performance.
        
        Args:
            events: List of PbpEventRow instances
            batch_size: Number of events to process in each batch
            
        Returns:
            Number of rows actually updated
        """
        if not events:
            return 0
        
        conn = await get_connection()
        total_updated = 0
        
        try:
            # Process events in batches to manage memory and transaction size
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                batch_updated = await self._upsert_batch(conn, batch)
                total_updated += batch_updated
                
                logger.debug("Processed PBP batch", 
                           batch_num=i//batch_size + 1,
                           batch_size=len(batch),
                           updated=batch_updated)
            
            logger.info("Upserted PBP events", total=len(events), updated=total_updated)
            
        except Exception as e:
            logger.error("Failed to upsert PBP events", error=str(e))
            raise
        
        return total_updated
    
    async def _upsert_batch(self, conn, batch: List[PbpEventRow]) -> int:
        """Upsert a single batch of PBP events."""
        updated_count = 0
        
        async with conn.transaction():
            for event in batch:
                query = """
                INSERT INTO pbp_events (
                    game_id, event_idx, period, game_time_seconds, 
                    event_type, description, home_score, away_score,
                    player1_name_slug, player1_team_tricode, player1_id,
                    player2_name_slug, player2_team_tricode, player2_id,
                    player3_name_slug, player3_team_tricode, player3_id,
                    shot_made, shot_type, shot_distance, rebound_type,
                    foul_type, turnover_type, source, source_url, ingested_at_utc
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27
                )
                ON CONFLICT (game_id, event_idx) DO UPDATE SET
                    period = CASE 
                        WHEN excluded.period IS DISTINCT FROM pbp_events.period 
                        THEN excluded.period ELSE pbp_events.period END,
                    game_time_seconds = CASE 
                        WHEN excluded.game_time_seconds IS DISTINCT FROM pbp_events.game_time_seconds 
                        THEN excluded.game_time_seconds ELSE pbp_events.game_time_seconds END,
                    event_type = CASE 
                        WHEN excluded.event_type IS DISTINCT FROM pbp_events.event_type 
                        THEN excluded.event_type ELSE pbp_events.event_type END,
                    description = CASE 
                        WHEN excluded.description IS DISTINCT FROM pbp_events.description 
                        THEN excluded.description ELSE pbp_events.description END,
                    home_score = CASE 
                        WHEN excluded.home_score IS DISTINCT FROM pbp_events.home_score 
                        THEN excluded.home_score ELSE pbp_events.home_score END,
                    away_score = CASE 
                        WHEN excluded.away_score IS DISTINCT FROM pbp_events.away_score 
                        THEN excluded.away_score ELSE pbp_events.away_score END,
                    player1_name_slug = CASE 
                        WHEN excluded.player1_name_slug IS DISTINCT FROM pbp_events.player1_name_slug 
                        THEN excluded.player1_name_slug ELSE pbp_events.player1_name_slug END,
                    player1_team_tricode = CASE 
                        WHEN excluded.player1_team_tricode IS DISTINCT FROM pbp_events.player1_team_tricode 
                        THEN excluded.player1_team_tricode ELSE pbp_events.player1_team_tricode END,
                    player1_id = CASE 
                        WHEN excluded.player1_id IS DISTINCT FROM pbp_events.player1_id 
                        THEN excluded.player1_id ELSE pbp_events.player1_id END,
                    player2_name_slug = CASE 
                        WHEN excluded.player2_name_slug IS DISTINCT FROM pbp_events.player2_name_slug 
                        THEN excluded.player2_name_slug ELSE pbp_events.player2_name_slug END,
                    player2_team_tricode = CASE 
                        WHEN excluded.player2_team_tricode IS DISTINCT FROM pbp_events.player2_team_tricode 
                        THEN excluded.player2_team_tricode ELSE pbp_events.player2_team_tricode END,
                    player2_id = CASE 
                        WHEN excluded.player2_id IS DISTINCT FROM pbp_events.player2_id 
                        THEN excluded.player2_id ELSE pbp_events.player2_id END,
                    player3_name_slug = CASE 
                        WHEN excluded.player3_name_slug IS DISTINCT FROM pbp_events.player3_name_slug 
                        THEN excluded.player3_name_slug ELSE pbp_events.player3_name_slug END,
                    player3_team_tricode = CASE 
                        WHEN excluded.player3_team_tricode IS DISTINCT FROM pbp_events.player3_team_tricode 
                        THEN excluded.player3_team_tricode ELSE pbp_events.player3_team_tricode END,
                    player3_id = CASE 
                        WHEN excluded.player3_id IS DISTINCT FROM pbp_events.player3_id 
                        THEN excluded.player3_id ELSE pbp_events.player3_id END,
                    shot_made = CASE 
                        WHEN excluded.shot_made IS DISTINCT FROM pbp_events.shot_made 
                        THEN excluded.shot_made ELSE pbp_events.shot_made END,
                    shot_type = CASE 
                        WHEN excluded.shot_type IS DISTINCT FROM pbp_events.shot_type 
                        THEN excluded.shot_type ELSE pbp_events.shot_type END,
                    shot_distance = CASE 
                        WHEN excluded.shot_distance IS DISTINCT FROM pbp_events.shot_distance 
                        THEN excluded.shot_distance ELSE pbp_events.shot_distance END,
                    rebound_type = CASE 
                        WHEN excluded.rebound_type IS DISTINCT FROM pbp_events.rebound_type 
                        THEN excluded.rebound_type ELSE pbp_events.rebound_type END,
                    foul_type = CASE 
                        WHEN excluded.foul_type IS DISTINCT FROM pbp_events.foul_type 
                        THEN excluded.foul_type ELSE pbp_events.foul_type END,
                    turnover_type = CASE 
                        WHEN excluded.turnover_type IS DISTINCT FROM pbp_events.turnover_type 
                        THEN excluded.turnover_type ELSE pbp_events.turnover_type END,
                    source = excluded.source,
                    source_url = excluded.source_url,
                    ingested_at_utc = excluded.ingested_at_utc
                WHERE (
                    excluded.period IS DISTINCT FROM pbp_events.period OR
                    excluded.game_time_seconds IS DISTINCT FROM pbp_events.game_time_seconds OR
                    excluded.event_type IS DISTINCT FROM pbp_events.event_type OR
                    excluded.description IS DISTINCT FROM pbp_events.description OR
                    excluded.home_score IS DISTINCT FROM pbp_events.home_score OR
                    excluded.away_score IS DISTINCT FROM pbp_events.away_score OR
                    excluded.player1_name_slug IS DISTINCT FROM pbp_events.player1_name_slug OR
                    excluded.player1_team_tricode IS DISTINCT FROM pbp_events.player1_team_tricode OR
                    excluded.player1_id IS DISTINCT FROM pbp_events.player1_id OR
                    excluded.player2_name_slug IS DISTINCT FROM pbp_events.player2_name_slug OR
                    excluded.player2_team_tricode IS DISTINCT FROM pbp_events.player2_team_tricode OR
                    excluded.player2_id IS DISTINCT FROM pbp_events.player2_id OR
                    excluded.player3_name_slug IS DISTINCT FROM pbp_events.player3_name_slug OR
                    excluded.player3_team_tricode IS DISTINCT FROM pbp_events.player3_team_tricode OR
                    excluded.player3_id IS DISTINCT FROM pbp_events.player3_id OR
                    excluded.shot_made IS DISTINCT FROM pbp_events.shot_made OR
                    excluded.shot_type IS DISTINCT FROM pbp_events.shot_type OR
                    excluded.shot_distance IS DISTINCT FROM pbp_events.shot_distance OR
                    excluded.rebound_type IS DISTINCT FROM pbp_events.rebound_type OR
                    excluded.foul_type IS DISTINCT FROM pbp_events.foul_type OR
                    excluded.turnover_type IS DISTINCT FROM pbp_events.turnover_type
                )
                """
                
                result = await conn.execute(
                    query,
                    event.game_id,
                    event.event_idx,
                    event.period,
                    event.game_time_seconds,
                    event.event_type.value,
                    event.description,
                    event.home_score,
                    event.away_score,
                    event.player1_name_slug,
                    event.player1_team_tricode,
                    event.player1_id,
                    event.player2_name_slug,
                    event.player2_team_tricode,
                    event.player2_id,
                    event.player3_name_slug,
                    event.player3_team_tricode,
                    event.player3_id,
                    event.shot_made,
                    event.shot_type.value if event.shot_type else None,
                    event.shot_distance,
                    event.rebound_type.value if event.rebound_type else None,
                    event.foul_type.value if event.foul_type else None,
                    event.turnover_type.value if event.turnover_type else None,
                    event.source,
                    event.source_url,
                    datetime.utcnow(),
                )
                
                if result.startswith('UPDATE'):
                    updated_count += 1
        
        return updated_count
    
    async def delete_game_events(self, game_id: str) -> int:
        """Delete all PBP events for a game (useful for reprocessing).
        
        Args:
            game_id: Game identifier
            
        Returns:
            Number of rows deleted
        """
        conn = await get_connection()
        
        try:
            result = await conn.execute(
                "DELETE FROM pbp_events WHERE game_id = $1",
                game_id
            )
            
            # Extract count from result string like "DELETE 1234"
            deleted_count = int(result.split()[-1]) if result.split()[-1].isdigit() else 0
            
            logger.info("Deleted PBP events for game", game_id=game_id, count=deleted_count)
            return deleted_count
            
        except Exception as e:
            logger.error("Failed to delete PBP events", game_id=game_id, error=str(e))
            raise