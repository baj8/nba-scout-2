"""Integration tests for games bridge with real loader interaction."""

import pytest
import asyncio
from datetime import datetime, date
from unittest.mock import patch

from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.enums import GameStatus
from nba_scraper.transformers.games_bridge import to_db_game
from nba_scraper.db import get_connection
from nba_scraper.utils.db import maybe_transaction


class TestGamesBridgeIntegration:
    """Integration tests for GameRow → Game → DB pipeline."""

    @pytest.mark.asyncio
    async def test_full_pipeline_with_rollback(self):
        """Test complete pipeline from GameRow to DB with transaction rollback."""
        # Import loader via facade to tolerate different file names
        try:
            from nba_scraper.loaders import upsert_game
        except ImportError:
            # Fallback import path if needed
            from nba_scraper.loaders.games import upsert_game
        
        game_row = GameRow(
            game_id="TEST_INTEGRATION_001",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            status=GameStatus.FINAL,
            source="integration_test",
            source_url="https://test-integration.com"
        )

        # Transform using bridge
        db_game = to_db_game(game_row)

        # Verify transformation
        assert db_game.game_id == "TEST_INTEGRATION_001"
        assert db_game.season == "2024-25"
        assert db_game.game_date == "2024-01-15"
        assert db_game.home_team_id == 1610612747  # LAL
        assert db_game.away_team_id == 1610612738   # BOS
        assert db_game.status == "Final"

        # Test DB insertion with transaction rollback
        conn = await get_connection()
        
        async with maybe_transaction(conn):
            # Insert the game
            result = await upsert_game(conn, db_game)
            
            # Verify it was inserted
            query = "SELECT * FROM games WHERE game_id = $1"
            stored_game = await conn.fetchrow(query, "TEST_INTEGRATION_001")
            
            assert stored_game is not None
            assert stored_game['game_id'] == "TEST_INTEGRATION_001"
            assert stored_game['season'] == "2024-25"
            assert stored_game['game_date'] == "2024-01-15"
            assert stored_game['home_team_id'] == 1610612747
            assert stored_game['away_team_id'] == 1610612738
            
            # Verify status casing matches DB expectations
            assert stored_game['status'] == "Final"
            
            # Explicitly rollback to keep CI idempotent
            raise Exception("Intentional rollback for test cleanup")

    @pytest.mark.asyncio 
    async def test_date_provenance_logging(self):
        """Test that date provenance is properly logged and can be asserted."""
        game_row_local = GameRow(
            game_id="TEST_PROVENANCE_LOCAL",
            season="2024-25", 
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),  # Local date present
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            source="test",
            source_url="https://test.com"
        )

        game_row_utc = GameRow(
            game_id="TEST_PROVENANCE_UTC",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=None,  # No local date - should use UTC
            arena_tz="America/Los_Angeles", 
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            source="test",
            source_url="https://test.com"
        )

        # Capture logging to assert provenance
        with patch('nba_scraper.utils.dates.logger') as mock_dates_logger:
            # Test local date provenance
            db_game_local = to_db_game(game_row_local)
            assert db_game_local.game_date == "2024-01-15"
            
            # Verify local provenance was logged
            mock_dates_logger.debug.assert_called()
            local_log_call = mock_dates_logger.debug.call_args
            assert local_log_call[1]['provenance'] == 'local'
            
            mock_dates_logger.reset_mock()
            
            # Test UTC date provenance
            db_game_utc = to_db_game(game_row_utc)
            assert db_game_utc.game_date == "2024-01-15"
            
            # Verify UTC provenance was logged
            mock_dates_logger.debug.assert_called()
            utc_log_call = mock_dates_logger.debug.call_args
            assert utc_log_call[1]['provenance'] == 'utc'

    @pytest.mark.asyncio
    async def test_alias_integration_with_db(self):
        """Test that team aliases work correctly in full DB integration."""
        try:
            from nba_scraper.loaders import upsert_game
        except ImportError:
            from nba_scraper.loaders.games import upsert_game

        # Use legacy Brooklyn tricode (BKN) which should map to BRK
        game_row = GameRow(
            game_id="TEST_ALIAS_BKN",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            arena_tz="America/New_York",
            home_team_tricode="BKN",  # Legacy Brooklyn
            away_team_tricode="BOS",
            status=GameStatus.FINAL,
            source="alias_test", 
            source_url="https://test-alias.com"
        )

        # Transform and verify alias resolution
        db_game = to_db_game(game_row)
        assert db_game.home_team_id == 1610612751  # Should be BRK's team ID

        # Test in database context
        conn = await get_connection()
        async with maybe_transaction(conn):
            await upsert_game(conn, db_game)
            
            # Verify stored with correct team ID
            stored = await conn.fetchrow(
                "SELECT home_team_id FROM games WHERE game_id = $1", 
                "TEST_ALIAS_BKN"
            )
            assert stored['home_team_id'] == 1610612751
            
            # Cleanup rollback
            raise Exception("Rollback for cleanup")

    @pytest.mark.asyncio
    async def test_error_handling_in_pipeline(self):
        """Test error propagation through the full pipeline."""
        try:
            from nba_scraper.loaders import upsert_game
        except ImportError:
            from nba_scraper.loaders.games import upsert_game

        # Create GameRow with invalid tricode
        game_row = GameRow(
            game_id="TEST_ERROR_HANDLING",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            arena_tz="America/New_York",
            home_team_tricode="INVALID_TEAM",  # Should cause error
            away_team_tricode="BOS", 
            source="error_test",
            source_url="https://test-error.com"
        )

        # Should fail at transformation stage with clear error
        with pytest.raises(ValueError, match=r"Unknown home team tricode 'INVALID_TEAM'"):
            to_db_game(game_row)

    def test_loader_facade_import(self):
        """Contract test: verify loader facade import works to prevent module path regressions."""
        # This test ensures the loader can be imported via facade
        try:
            from nba_scraper.loaders import upsert_game
            assert callable(upsert_game), "upsert_game should be callable"
        except ImportError as e:
            pytest.fail(f"Could not import upsert_game via loaders facade: {e}")

    @pytest.mark.asyncio
    async def test_runtime_type_assertion_when_enabled(self):
        """Test runtime type assertion when STRICT_TYPES environment flag is set."""
        try:
            from nba_scraper.loaders import upsert_game
        except ImportError:
            from nba_scraper.loaders.games import upsert_game
        
        game_row = GameRow(
            game_id="TEST_TYPE_CHECK",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            arena_tz="America/New_York",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            source="type_test",
            source_url="https://test-types.com"
        )

        db_game = to_db_game(game_row)

        # Mock environment to enable strict types
        with patch.dict('os.environ', {'STRICT_TYPES': 'true'}):
            conn = await get_connection()
            
            # This should pass since we're passing a proper Game object
            async with maybe_transaction(conn):
                result = await upsert_game(conn, db_game)
                
                # Test would fail if we passed wrong type (like GameRow instead of Game)
                with pytest.raises((TypeError, AssertionError)):
                    # This should trigger type assertion if implemented
                    await upsert_game(conn, game_row)  # Wrong type
                    
                raise Exception("Rollback for cleanup")