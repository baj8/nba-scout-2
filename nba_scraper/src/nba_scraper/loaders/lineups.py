"""Lineup loaders with idempotent upserts using array primary keys."""

import asyncpg
from typing import List
from ..models.lineups import LineupStint


async def upsert_lineups(conn: asyncpg.Connection, rows: List[LineupStint]) -> None:
    """Upsert lineup stints in batch with array-based primary key."""
    if not rows:
        return
    
    # Prepare batch data
    values = [
        (
            row.game_id, row.team_id, row.period, 
            row.lineup, row.seconds_played
        ) 
        for row in rows
    ]
    
    await conn.executemany("""
        INSERT INTO lineup_stints (
            game_id, team_id, period, lineup_player_ids, 
            seconds_played, created_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (game_id, team_id, period, lineup_player_ids) 
        DO UPDATE SET
            seconds_played = EXCLUDED.seconds_played
    """, values)