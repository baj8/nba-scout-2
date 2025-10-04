"""PBP loaders with idempotent upserts and clock_seconds support."""

import asyncpg
from typing import List
from ..models.pbp import PbpEvent


async def upsert_pbp(conn: asyncpg.Connection, rows: List[PbpEvent]) -> None:
    """Upsert PBP events in batch with clock_seconds support."""
    if not rows:
        return
    
    # Prepare batch data
    values = [
        (
            row.game_id, row.event_num, row.period, row.clock,
            row.team_id, row.player1_id, row.action_type, 
            row.action_subtype, row.description, row.clock_seconds
        ) 
        for row in rows
    ]
    
    await conn.executemany("""
        INSERT INTO pbp_events (
            game_id, event_num, period, clock, team_id, 
            player1_id, action_type, action_subtype, description,
            clock_seconds, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        ON CONFLICT (game_id, event_num) 
        DO UPDATE SET
            period = EXCLUDED.period,
            clock = EXCLUDED.clock,
            team_id = EXCLUDED.team_id,
            player1_id = EXCLUDED.player1_id,
            action_type = EXCLUDED.action_type,
            action_subtype = EXCLUDED.action_subtype,
            description = EXCLUDED.description,
            clock_seconds = EXCLUDED.clock_seconds
    """, values)