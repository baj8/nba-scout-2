"""Lineup and injury data loader with idempotent upserts."""

from datetime import datetime
from typing import List

from ..models import StartingLineupRow, InjuryStatusRow
from ..db import get_connection
from ..logging import get_logger

logger = get_logger(__name__)


class LineupLoader:
    """Loader for lineup and injury data with diff-aware upserts."""
    
    async def upsert_lineups(self, lineups: List[StartingLineupRow]) -> int:
        """Upsert starting lineups.
        
        Args:
            lineups: List of StartingLineupRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not lineups:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with conn.transaction():
                for lineup in lineups:
                    query = """
                    INSERT INTO starting_lineups (
                        game_id, team_tricode, player_name_slug, player_display_name,
                        position, jersey_number, source, source_url, ingested_at_utc
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (game_id, team_tricode, player_name_slug) DO UPDATE SET
                        player_display_name = CASE 
                            WHEN excluded.player_display_name IS DISTINCT FROM starting_lineups.player_display_name 
                            THEN excluded.player_display_name ELSE starting_lineups.player_display_name END,
                        position = CASE 
                            WHEN excluded.position IS DISTINCT FROM starting_lineups.position 
                            THEN excluded.position ELSE starting_lineups.position END,
                        jersey_number = CASE 
                            WHEN excluded.jersey_number IS DISTINCT FROM starting_lineups.jersey_number 
                            THEN excluded.jersey_number ELSE starting_lineups.jersey_number END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.player_display_name IS DISTINCT FROM starting_lineups.player_display_name OR
                        excluded.position IS DISTINCT FROM starting_lineups.position OR
                        excluded.jersey_number IS DISTINCT FROM starting_lineups.jersey_number
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        lineup.game_id,
                        lineup.team_tricode,
                        lineup.player_name_slug,
                        lineup.player_display_name,
                        lineup.position.value if lineup.position else None,
                        lineup.jersey_number,
                        lineup.source,
                        lineup.source_url,
                        datetime.utcnow(),
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted starting lineups", total=len(lineups), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert starting lineups", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_injuries(self, injuries: List[InjuryStatusRow]) -> int:
        """Upsert injury status records.
        
        Args:
            injuries: List of InjuryStatusRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not injuries:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with conn.transaction():
                for injury in injuries:
                    query = """
                    INSERT INTO injury_status (
                        game_id, team_tricode, player_name_slug, player_display_name,
                        status, injury_reason, notes, source, source_url, ingested_at_utc
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (game_id, team_tricode, player_name_slug) DO UPDATE SET
                        player_display_name = CASE 
                            WHEN excluded.player_display_name IS DISTINCT FROM injury_status.player_display_name 
                            THEN excluded.player_display_name ELSE injury_status.player_display_name END,
                        status = CASE 
                            WHEN excluded.status IS DISTINCT FROM injury_status.status 
                            THEN excluded.status ELSE injury_status.status END,
                        injury_reason = CASE 
                            WHEN excluded.injury_reason IS DISTINCT FROM injury_status.injury_reason 
                            THEN excluded.injury_reason ELSE injury_status.injury_reason END,
                        notes = CASE 
                            WHEN excluded.notes IS DISTINCT FROM injury_status.notes 
                            THEN excluded.notes ELSE injury_status.notes END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.player_display_name IS DISTINCT FROM injury_status.player_display_name OR
                        excluded.status IS DISTINCT FROM injury_status.status OR
                        excluded.injury_reason IS DISTINCT FROM injury_status.injury_reason OR
                        excluded.notes IS DISTINCT FROM injury_status.notes
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        injury.game_id,
                        injury.team_tricode,
                        injury.player_name_slug,
                        injury.player_display_name,
                        injury.status.value,
                        injury.injury_reason,
                        injury.notes,
                        injury.source,
                        injury.source_url,
                        datetime.utcnow(),
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted injury status", total=len(injuries), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert injury status", error=str(e))
            raise
        
        return updated_count