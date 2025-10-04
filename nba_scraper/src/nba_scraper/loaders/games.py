"""Game loaders with idempotent upserts."""

import asyncpg
from ..models.games import Game


async def upsert_game(conn: asyncpg.Connection, game: Game) -> None:
    """Upsert a single game with idempotent behavior."""
    await conn.execute("""
        INSERT INTO games (
            game_id, season, game_date, home_team_id, 
            away_team_id, status, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        ON CONFLICT (game_id) 
        DO UPDATE SET
            season = EXCLUDED.season,
            game_date = EXCLUDED.game_date,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            status = EXCLUDED.status,
            updated_at = NOW()
    """, game.game_id, game.season, game.game_date, 
        game.home_team_id, game.away_team_id, game.status)