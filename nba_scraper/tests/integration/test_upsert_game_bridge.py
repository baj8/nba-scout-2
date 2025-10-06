"""Integration tests for GameRow to Game transformation with database upsert."""

import pytest
from datetime import datetime, date

from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.enums import GameStatus
from nba_scraper.transformers.games_bridge import to_db_game
from nba_scraper.utils.team_lookup import get_team_index
from nba_scraper.loaders.games import upsert_game
from nba_scraper.db import get_connection


@pytest.mark.asyncio
class TestUpsertGameBridge:
    """Integration tests for GameRow -> Game -> DB pipeline."""

    async def test_full_pipeline_game_upsert(self):
        """Test complete pipeline: GameRow -> Game -> upsert_game -> DB verification."""
        # Create a minimal GameRow
        game_row = GameRow(
            game_id="TEST_GAME_001",
            season="2024-25", 
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS", 
            status=GameStatus.FINAL,
            source="integration_test",
            source_url="https://test.com/integration"
        )

        # Transform to Game model
        db_game = to_db_game(game_row, team_index=get_team_index())
        
        # Verify transformation
        assert db_game.game_id == "TEST_GAME_001"
        assert db_game.season == "2024-25"
        assert db_game.game_date == "2024-01-15"
        assert db_game.home_team_id == 1610612747  # LAL
        assert db_game.away_team_id == 1610612738   # BOS
        assert db_game.status == "FINAL"

        # Upsert to database
        conn = await get_connection()
        
        # Clean up any existing test data
        await conn.execute("DELETE FROM games WHERE game_id = $1", "TEST_GAME_001")
        
        try:
            # Perform upsert
            await upsert_game(conn, db_game)
            
            # Verify data was written correctly
            row = await conn.fetchrow(
                "SELECT * FROM games WHERE game_id = $1", "TEST_GAME_001"
            )
            
            assert row is not None
            assert row['game_id'] == "TEST_GAME_001"
            assert row['season'] == "2024-25"
            assert str(row['game_date']) == "2024-01-15"
            assert row['home_team_id'] == 1610612747
            assert row['away_team_id'] == 1610612738
            assert row['status'] == "FINAL"
            
        finally:
            # Clean up test data
            await conn.execute("DELETE FROM games WHERE game_id = $1", "TEST_GAME_001")

    async def test_upsert_game_update_behavior(self):
        """Test that upsert properly updates existing games."""
        # Create initial GameRow
        initial_game_row = GameRow(
            game_id="TEST_GAME_002",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 19, 0),  # Different time
            game_date_local=date(2024, 1, 15),
            arena_tz="America/New_York",
            home_team_tricode="NYK", 
            away_team_tricode="BRK",
            status=GameStatus.SCHEDULED,
            source="integration_test",
            source_url="https://test.com/integration"
        )

        # Transform and insert initial game
        initial_db_game = to_db_game(initial_game_row)
        
        conn = await get_connection()
        
        # Clean up any existing test data
        await conn.execute("DELETE FROM games WHERE game_id = $1", "TEST_GAME_002")
        
        try:
            await upsert_game(conn, initial_db_game)
            
            # Verify initial insert
            row = await conn.fetchrow(
                "SELECT status FROM games WHERE game_id = $1", "TEST_GAME_002"
            )
            assert row['status'] == "SCHEDULED"
            
            # Create updated GameRow with different status
            updated_game_row = GameRow(
                game_id="TEST_GAME_002",  # Same ID
                season="2024-25",
                game_date_utc=datetime(2024, 1, 15, 19, 0),
                game_date_local=date(2024, 1, 15),
                arena_tz="America/New_York", 
                home_team_tricode="NYK",
                away_team_tricode="BRK",
                status=GameStatus.FINAL,  # Updated status
                source="integration_test",
                source_url="https://test.com/integration"
            )
            
            # Transform and upsert updated game
            updated_db_game = to_db_game(updated_game_row)
            await upsert_game(conn, updated_db_game)
            
            # Verify update occurred
            row = await conn.fetchrow(
                "SELECT status FROM games WHERE game_id = $1", "TEST_GAME_002"
            )
            assert row['status'] == "FINAL"
            
            # Verify only one row exists (upsert, not insert)
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM games WHERE game_id = $1", "TEST_GAME_002"
            )
            assert count == 1
            
        finally:
            # Clean up test data
            await conn.execute("DELETE FROM games WHERE game_id = $1", "TEST_GAME_002")