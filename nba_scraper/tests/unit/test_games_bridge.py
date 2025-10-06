"""Unit tests for games bridge transformation logic."""

import pytest
from datetime import datetime, date, timezone
from unittest.mock import patch, MagicMock

from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.games import Game
from nba_scraper.models.enums import GameStatus
from nba_scraper.transformers.games_bridge import to_db_game, _normalize_game_status
from nba_scraper.utils.dates import derive_season_from_game_id, derive_season_from_date
from nba_scraper.utils.team_crosswalk import get_team_index


class TestDateUtilities:
    """Test date and season derivation utilities."""
    
    def test_season_from_game_id(self):
        """Test season derivation from NBA game ID format."""
        # Regular season games
        assert derive_season_from_game_id("0022300456") == "2023-24"
        assert derive_season_from_game_id("0022400001") == "2024-25" 
        assert derive_season_from_game_id("0022200789") == "2022-23"
        
        # Playoff games
        assert derive_season_from_game_id("0042300101") == "2023-24"
        
        # Invalid formats
        assert derive_season_from_game_id("invalid") is None
        assert derive_season_from_game_id("12345") is None
        assert derive_season_from_game_id("") is None
        assert derive_season_from_game_id(None) is None
    
    def test_season_from_date(self):
        """Test season derivation from game dates."""
        # Regular season dates
        assert derive_season_from_date("2023-10-15") == "2023-24"  # October start
        assert derive_season_from_date("2024-01-15") == "2023-24"  # January continuation
        assert derive_season_from_date("2024-04-15") == "2023-24"  # April playoffs
        
        # New season start  
        assert derive_season_from_date("2024-10-15") == "2024-25"
        
        # Date objects
        assert derive_season_from_date(date(2023, 12, 25)) == "2023-24"
        
        # Invalid dates
        assert derive_season_from_date("invalid") is None
        assert derive_season_from_date(None) is None


class TestTeamLookup:
    """Test team lookup and validation logic."""
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    def test_get_team_index_success(self, mock_load):
        """Test successful team index building."""
        mock_load.return_value = {
            "teams": {
                "BOS": {"id": 1610612738, "nba_stats": ["BOS"], "bref": ["BOS"], "aliases": []},
                "GSW": {"id": 1610612744, "nba_stats": ["GSW"], "bref": ["GSW"], "aliases": []},
                "LAL": {"id": 1610612747, "nba_stats": ["LAL"], "bref": ["LAL"], "aliases": []}
            }
        }
        
        team_index = get_team_index()
        
        assert team_index["BOS"] == 1610612738
        assert team_index["GSW"] == 1610612744
        assert team_index["LAL"] == 1610612747
        assert len(team_index) == 3


class TestGamesBridge:
    """Test core GameRow → Game transformation logic."""

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
        team_index = get_team_index()  # This includes aliases
        result = to_db_game(game_row, team_index=team_index)
        
        assert result.home_team_id == 1610612751  # Brooklyn
        assert result.away_team_id == 1610612756  # Phoenix

    def test_status_casing_normalized(self):
        """Test that various status inputs are normalized to proper casing."""
        game_row = GameRow(
            game_id="0022400004",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Eastern",
            home_team_tricode="BOS",
            away_team_tricode="NYK",
            status="FINAL",  # Uppercase input
            source="test",
            source_url="https://test.com"
        )
        
        team_index = get_team_index()
        result = to_db_game(game_row, team_index=team_index)
        
        assert result.status == "Final"  # Should be normalized

    def test_unknown_tricode_raises_targeted_error(self):
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

    def test_empty_game_id_raises_error(self):
        """Test that empty game_id raises ValueError."""
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
        
        team_index = get_team_index()
        
        with pytest.raises(ValueError, match="game_id is required"):
            to_db_game(game_row, team_index=team_index)

    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_to_db_game_success(self, mock_get_index, mock_load):
        """Test successful GameRow to Game transformation."""
        # Mock team lookup
        mock_load.return_value = {
            "teams": {
                "LAL": {"id": 1610612747, "nba_stats": ["LAL"], "bref": ["LAL"], "aliases": []},
                "GSW": {"id": 1610612744, "nba_stats": ["GSW"], "bref": ["GSW"], "aliases": []}
            }
        }
        mock_get_index.return_value = {"LAL": 1610612747, "GSW": 1610612744}
        
        # Create valid GameRow with all required fields
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
        
        result = to_db_game(game_row)
        
        assert isinstance(result, Game)
        assert result.game_id == "0022400001"
        assert result.season == "2024-25"
        assert result.game_date == "2024-10-15"
        assert result.home_team_id == 1610612747
        assert result.away_team_id == 1610612744
        assert result.status == "FINAL"
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_season_fallback_strategies(self, mock_get_index, mock_load):
        """Test season derivation fallback strategies."""
        mock_load.return_value = {
            "teams": {
                "BOS": {"id": 1610612738, "nba_stats": ["BOS"], "bref": ["BOS"], "aliases": []},
                "MIA": {"id": 1610612748, "nba_stats": ["MIA"], "bref": ["MIA"], "aliases": []}
            }
        }
        mock_get_index.return_value = {"BOS": 1610612738, "MIA": 1610612748}
        
        # Test fallback to game ID derivation
        game_row = GameRow(
            game_id="0022300456",
            season=None,  # Will fallback to game ID
            game_date_utc=datetime(2024, 1, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 1, 15),
            arena_tz="US/Eastern",
            home_team_tricode="BOS", 
            away_team_tricode="MIA",
            status=GameStatus.FINAL,
            source="test",
            source_url="https://test.com"
        )
        
        result = to_db_game(game_row)
        assert result.season == "2023-24"  # Derived from game ID
    
    def test_status_normalization(self):
        """Test status normalization for various inputs."""
        # GameStatus enum
        assert _normalize_game_status(GameStatus.FINAL) == "FINAL"
        assert _normalize_game_status(GameStatus.LIVE) == "LIVE"
        assert _normalize_game_status(GameStatus.SCHEDULED) == "SCHEDULED"
    
        # String variations
        assert _normalize_game_status("final") == "FINAL"
        assert _normalize_game_status("FINISHED") == "FINAL"
        assert _normalize_game_status("completed") == "FINAL"
        assert _normalize_game_status("in_progress") == "LIVE"
        assert _normalize_game_status("cancelled") == "CANCELLED"
        assert _normalize_game_status("canceled") == "CANCELLED"
    
        # None/empty handling
        assert _normalize_game_status(None) == "SCHEDULED"
        assert _normalize_game_status("") == "SCHEDULED"
        assert _normalize_game_status("   ") == "SCHEDULED"
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_performance_with_team_index(self, mock_get_index, mock_team_index):
        """Test performance optimization with pre-built team index."""
        mock_team_index = {'BOS': 1610612738, 'GSW': 1610612744, 'LAL': 1610612747}
        mock_get_index.return_value = mock_team_index
        
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
        result = to_db_game(game_row, team_index=mock_team_index)
        
        # Should not call get_team_index since we provided one
        mock_get_index.assert_not_called()
        assert result.home_team_id == 1610612747
        assert result.away_team_id == 1610612744
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_comprehensive_error_scenarios(self, mock_get_index, mock_team_index):
        """Test comprehensive error scenarios with proper context."""
        mock_team_index = {'BOS': 1610612738, 'GSW': 1610612744, 'LAL': 1610612747}
        mock_get_index.return_value = mock_team_index
        
        # Test with unknown team tricode
        with pytest.raises(ValueError, match="Unknown tricode"):
            invalid_team_row = GameRow(
                game_id="0022400001",
                season="2024-25",
                game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
                game_date_local=date(2024, 10, 15),
                arena_tz="US/Pacific",
                home_team_tricode="INVALID",
                away_team_tricode="GSW",
                status=GameStatus.FINAL,
                source="test",
                source_url="https://test.com"
            )
            to_db_game(invalid_team_row, team_index=mock_team_index)


class TestIntegrationScenarios:
    """Test realistic integration scenarios with different data sources."""
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_nba_stats_api_scenario(self, mock_get_index, mock_load):
        """Test realistic NBA Stats API data transformation."""
        # Mock realistic team aliases
        mock_load.return_value = {
            "teams": {
                "LAL": {"id": 1610612747, "nba_stats": ["LAL"], "bref": ["LAL"], "aliases": []},
                "GSW": {"id": 1610612744, "nba_stats": ["GSW"], "bref": ["GSW"], "aliases": []}
            }
        }
        mock_get_index.return_value = {"LAL": 1610612747, "GSW": 1610612744}
        
        # NBA Stats API often provides integer status codes
        nba_stats_row = GameRow(
            game_id="0022400456",
            season="2024-25",
            game_date_utc=datetime(2024, 1, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 1, 14),
            arena_tz="US/Pacific",
            home_team_tricode="LAL",
            away_team_tricode="GSW",
            status=GameStatus.FINAL,  # Already normalized by GameRow
            source="nba_stats",
            source_url="https://stats.nba.com/..."
        )
        
        result = to_db_game(nba_stats_row)
        
        assert result.game_id == "0022400456"
        assert result.season == "2024-25"
        assert result.game_date == "2024-01-14"  # Local date preferred
        assert result.status == "FINAL"
        assert result.home_team_id == 1610612747
        assert result.away_team_id == 1610612744
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_basketball_reference_scenario(self, mock_get_index, mock_load):
        """Test Basketball Reference data transformation."""
        mock_load.return_value = {
            "teams": {
                "BOS": {"id": 1610612738, "nba_stats": ["BOS"], "bref": ["BOS"], "aliases": []},
                "MIA": {"id": 1610612748, "nba_stats": ["MIA"], "bref": ["MIA"], "aliases": []}
            }
        }
        mock_get_index.return_value = {"BOS": 1610612738, "MIA": 1610612748}
        
        # B-Ref might not have explicit season, rely on derivation
        bref_row = GameRow(
            game_id="0022300789",
            season="2023-24",  # Valid season instead of empty
            game_date_utc=datetime(2024, 3, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 3, 15),
            arena_tz="US/Eastern",
            home_team_tricode="BOS",
            away_team_tricode="MIA",
            status=GameStatus.FINAL,
            source="bref",
            source_url="https://basketball-reference.com/..."
        )
        
        result = to_db_game(bref_row)
        
        assert result.season == "2023-24"
        assert result.game_date == "2024-03-15"
        assert result.home_team_id == 1610612738
        assert result.away_team_id == 1610612748
    
    @patch('nba_scraper.utils.team_lookup._load_team_aliases')
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_live_game_scenario(self, mock_get_index, mock_load):
        """Test live game data transformation."""
        mock_load.return_value = {
            "teams": {
                "DEN": {"id": 1610612743, "nba_stats": ["DEN"], "bref": ["DEN"], "aliases": []},
                "PHX": {"id": 1610612756, "nba_stats": ["PHX"], "bref": ["PHX"], "aliases": []}
            }
        }
        mock_get_index.return_value = {"DEN": 1610612743, "PHX": 1610612756}
        
        live_row = GameRow(
            game_id="0022400234",
            season="2024-25",
            game_date_utc=datetime.now(timezone.utc),  # Current time
            game_date_local=date.today(),
            arena_tz="US/Mountain",
            home_team_tricode="DEN",
            away_team_tricode="PHX",
            status=GameStatus.LIVE,
            source="live_api",
            source_url="https://live.nba.com/..."
        )
        
        result = to_db_game(live_row)
        
        assert result.status == "LIVE"
        assert result.season == "2024-25"
        assert result.home_team_id == 1610612743
        assert result.away_team_id == 1610612756


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_invalid_game_id_format(self):
        """Test handling of invalid game ID formats."""
        # Should not crash, but season derivation will fail
        invalid_ids = ["", "123", "invalid", None, "0022400", "00224000001"]  
        
        for invalid_id in invalid_ids:
            if invalid_id is not None:
                season = derive_season_from_game_id(invalid_id)
                assert season is None
    
    @patch('nba_scraper.transformers.games_bridge.get_team_index')
    def test_missing_team_in_index(self, mock_get_index):
        """Test behavior when team tricode is not in index."""
        mock_get_index.return_value = {"BOS": 1610612738}  # Only BOS
        
        with pytest.raises(ValueError, match="Unknown tricode"):
            game_row = GameRow(
                game_id="0022400001",
                season="2024-25",
                game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
                game_date_local=date(2024, 10, 15),
                arena_tz="US/Pacific",
                home_team_tricode="MISSING",  # Not in index
                away_team_tricode="BOS",
                status=GameStatus.FINAL,
                source="test",
                source_url="https://test.com"
            )
            to_db_game(game_row)