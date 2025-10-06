"""Unit tests for games bridge transformation logic."""

import pytest
from datetime import datetime, date, timezone
from unittest.mock import patch

from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.games import Game
from nba_scraper.models.enums import GameStatus
from nba_scraper.transformers.games_bridge import to_db_game
from nba_scraper.utils.team_crosswalk import get_team_index


class TestGamesBridge:
    """Test core GameRow → Game transformation logic."""

    def test_basic_transformation_success(self):
        """Test successful GameRow to Game transformation with all required fields."""
        # Create valid GameRow
        game_row = GameRow(
            game_id="0022400001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="LAL",
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        # Mock team index
        team_index = {"LAL": 1610612747, "GSW": 1610612744}
        
        result = to_db_game(game_row, team_index=team_index)
        
        assert isinstance(result, Game)
        assert result.game_id == "0022400001"
        assert result.season == "2024-25"
        assert result.game_date == "2024-10-15"
        assert result.home_team_id == 1610612747
        assert result.away_team_id == 1610612744
        assert result.status == "Final"

    def test_local_vs_utc_date_precedence(self):
        """Test that game_date_local is preferred over game_date_utc."""
        # Create GameRow with different local vs UTC dates
        game_row = GameRow(
            game_id="0022400002",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 16, 3, 30, tzinfo=timezone.utc),  # Next day UTC
            game_date_local=date(2024, 10, 15),  # Local date should win
            arena_tz="US/Pacific",
            home_team_tricode="BOS",
            away_team_tricode="MIA",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        team_index = {"BOS": 1610612738, "MIA": 1610612748}
        result = to_db_game(game_row, team_index=team_index)
        
        # Should use local date, not UTC
        assert result.game_date == "2024-10-15"

    def test_tricode_alias_resolution(self):
        """Test that team tricode aliases are resolved correctly."""
        game_row = GameRow(
            game_id="0022400003",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Eastern",
            home_team_tricode="BKN",  # Alias for BRK
            away_team_tricode="PHO",  # Alias for PHX
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        # Team index should include aliases
        team_index = {
            "BRK": 1610612751, "BKN": 1610612751,  # Brooklyn aliases
            "PHX": 1610612756, "PHO": 1610612756,  # Phoenix aliases
        }
        
        result = to_db_game(game_row, team_index=team_index)
        
        assert result.home_team_id == 1610612751  # Brooklyn
        assert result.away_team_id == 1610612756  # Phoenix

    def test_status_casing_normalization(self):
        """Test that various status inputs are normalized to proper casing."""
        test_cases = [
            (GameStatus.FINAL, "Final"),
            (GameStatus.LIVE, "Live"),
            (GameStatus.SCHEDULED, "Scheduled"),
            (GameStatus.POSTPONED, "Postponed"),
            (GameStatus.CANCELLED, "Cancelled"),
        ]
        
        for input_status, expected_output in test_cases:
            game_row = GameRow(
                game_id="0022400004",
                season="2024-25",
                game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
                game_date_local=date(2024, 10, 15),
                arena_tz="US/Eastern",
                home_team_tricode="BOS",
                away_team_tricode="NYK",
                status=input_status,
                source="test",
                source_url="https://test.com"
            )
            
            team_index = {"BOS": 1610612738, "NYK": 1610612752}
            result = to_db_game(game_row, team_index=team_index)
            
            assert result.status == expected_output

    def test_season_derivation_fallbacks(self):
        """Test season derivation with various fallback strategies."""
        # Test 1: Fallback to game ID derivation
        game_row = GameRow(
            game_id="0022400456",  # Should derive 2024-25
            season="",  # Empty season
            game_date_utc=datetime(2024, 1, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 1, 15),
            arena_tz="US/Eastern",
            home_team_tricode="BOS",
            away_team_tricode="MIA",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        team_index = {"BOS": 1610612738, "MIA": 1610612748}
        result = to_db_game(game_row, team_index=team_index)
        
        assert result.season == "2024-25"  # Derived from game ID

        # Test 2: Fallback to date derivation
        game_row = GameRow(
            game_id="INVALID123",  # Can't derive from this
            season=None,  # No explicit season
            game_date_utc=datetime(2024, 1, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 1, 15),  # January = 2023-24 season
            arena_tz="US/Eastern",
            home_team_tricode="BOS",
            away_team_tricode="MIA",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        result = to_db_game(game_row, team_index=team_index)
        
        assert result.season == "2023-24"  # Derived from date

    def test_missing_game_id_error(self):
        """Test that missing game_id raises ValueError."""
        game_row = GameRow(
            game_id="",  # Empty game_id
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="LAL",
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        team_index = {"LAL": 1610612747, "GSW": 1610612744}
        
        with pytest.raises(ValueError, match="game_id is required"):
            to_db_game(game_row, team_index=team_index)

    def test_unknown_tricode_error(self):
        """Test that unknown tricodes raise targeted errors."""
        game_row = GameRow(
            game_id="0022400001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="INVALID",  # Unknown tricode
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        team_index = {"GSW": 1610612744}  # Missing INVALID
        
        with pytest.raises(ValueError, match="Unknown tricode for game 0022400001"):
            to_db_game(game_row, team_index=team_index)

    def test_performance_with_team_index(self):
        """Test performance optimization with pre-built team index."""
        game_row = GameRow(
            game_id="0022400001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="LAL",
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        # Pass pre-built index
        team_index = {"LAL": 1610612747, "GSW": 1610612744}
        
        with patch('nba_scraper.transformers.games_bridge.get_team_index') as mock_get_index:
            result = to_db_game(game_row, team_index=team_index)
            
            # Should not call get_team_index since we provided one
            mock_get_index.assert_not_called()
            assert result.home_team_id == 1610612747
            assert result.away_team_id == 1610612744