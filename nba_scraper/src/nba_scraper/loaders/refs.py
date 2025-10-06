"""Referee data loader with idempotent upserts."""

from datetime import datetime, UTC
from typing import List

from ..models import RefAssignmentRow, RefAlternateRow
from ..db import get_connection
from ..nba_logging import get_logger
from ..utils.db import maybe_transaction

logger = get_logger(__name__)


class RefLoader:
    """Loader for referee assignment data with diff-aware upserts."""
    
    async def upsert_assignments(self, assignments: List[RefAssignmentRow]) -> int:
        """Upsert referee assignments.
        
        Args:
            assignments: List of RefAssignmentRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not assignments:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with maybe_transaction(conn):
                for assignment in assignments:
                    query = """
                    INSERT INTO ref_assignments (
                        game_id, referee_name_slug, referee_display_name, role,
                        position, source, source_url, ingested_at_utc
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (game_id, referee_name_slug) DO UPDATE SET
                        referee_display_name = CASE 
                            WHEN excluded.referee_display_name IS DISTINCT FROM ref_assignments.referee_display_name 
                            THEN excluded.referee_display_name ELSE ref_assignments.referee_display_name END,
                        role = CASE 
                            WHEN excluded.role IS DISTINCT FROM ref_assignments.role 
                            THEN excluded.role ELSE ref_assignments.role END,
                        position = CASE 
                            WHEN excluded.position IS DISTINCT FROM ref_assignments.position 
                            THEN excluded.position ELSE ref_assignments.position END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.referee_display_name IS DISTINCT FROM ref_assignments.referee_display_name OR
                        excluded.role IS DISTINCT FROM ref_assignments.role OR
                        excluded.position IS DISTINCT FROM ref_assignments.position
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        assignment.game_id,
                        assignment.referee_name_slug,
                        assignment.referee_display_name,
                        assignment.role.value,
                        assignment.position,
                        assignment.source,
                        assignment.source_url,
                        datetime.now(UTC),
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted referee assignments", total=len(assignments), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert referee assignments", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_alternates(self, alternates: List[RefAlternateRow]) -> int:
        """Upsert referee alternates.
        
        Args:
            alternates: List of RefAlternateRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not alternates:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with maybe_transaction(conn):
                for alternate in alternates:
                    query = """
                    INSERT INTO ref_alternates (
                        game_id, referee_name_slug, referee_display_name,
                        source, source_url, ingested_at_utc
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (game_id, referee_name_slug) DO UPDATE SET
                        referee_display_name = CASE 
                            WHEN excluded.referee_display_name IS DISTINCT FROM ref_alternates.referee_display_name 
                            THEN excluded.referee_display_name ELSE ref_alternates.referee_display_name END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.referee_display_name IS DISTINCT FROM ref_alternates.referee_display_name
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        alternate.game_id,
                        alternate.referee_name_slug,
                        alternate.referee_display_name,
                        alternate.source,
                        alternate.source_url,
                        datetime.now(UTC),
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted referee alternates", total=len(alternates), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert referee alternates", error=str(e))
            raise
        
        return updated_count