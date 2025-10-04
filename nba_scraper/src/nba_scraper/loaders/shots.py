"""Shot loaders with idempotent upserts for coordinate data."""

import asyncpg
from typing import List
from ..models.shots import ShotEvent


async def upsert_shots(conn: asyncpg.Connection, rows: List[ShotEvent]) -> None:
    """Upsert shot events in batch with coordinate data."""
    if not rows:
        return
    
    # Prepare batch data
    values = [
        (
            row.game_id, row.player_id, row.team_id, row.period,
            row.shot_made_flag, row.loc_x, row.loc_y, row.event_num
        ) 
        for row in rows
    ]
    
    await conn.executemany("""
        INSERT INTO shot_events (
            game_id, player_id, team_id, period,
            shot_made_flag, loc_x, loc_y, event_num, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (game_id, player_id, period, loc_x, loc_y) 
        DO UPDATE SET
            team_id = EXCLUDED.team_id,
            shot_made_flag = EXCLUDED.shot_made_flag,
            event_num = EXCLUDED.event_num
    """, values)