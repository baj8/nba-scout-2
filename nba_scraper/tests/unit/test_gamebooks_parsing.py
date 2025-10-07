"""Tests for gamebook parsing with header aliasing and robust numeric coercion."""

from unittest.mock import patch

from nba_scraper.extractors.gamebooks import (
    _coerce_number,
    _normalize_header,
    _validate_team_totals,
    extract_player_stats_tables,
)


class TestGamebooksHeaderNormalization:
    """Test header aliasing and normalization."""

    def test_header_aliases_coverage(self):
        """Test that all expected header variants are covered."""
        # Test +/- vs PLUS/MINUS
        assert _normalize_header("+/-") == "plus_minus"
        assert _normalize_header("PLUS/MINUS") == "plus_minus"

        # Test 3P vs 3PT variants
        assert _normalize_header("3P") == "tpm_tpa"
        assert _normalize_header("3PT") == "tpm_tpa"
        assert _normalize_header("3PM-A") == "tpm_tpa"
        assert _normalize_header("3PTM-A") == "tpm_tpa"

        # Test minute variants
        assert _normalize_header("MIN") == "min"
        assert _normalize_header("MINS") == "min"

        # Test field goal variants
        assert _normalize_header("FG") == "fgm_fga"
        assert _normalize_header("FGM-A") == "fgm_fga"

        # Test turnover variants
        assert _normalize_header("TO") == "tov"
        assert _normalize_header("TOV") == "tov"

    def test_unknown_headers_dropped(self):
        """Test that unknown headers are dropped with debug logging."""
        with patch("nba_scraper.extractors.gamebooks.logger") as mock_logger:
            result = _normalize_header("UNKNOWN_STAT")
            assert result is None
            mock_logger.debug.assert_called_once()

    def test_empty_header_handling(self):
        """Test handling of empty or None headers."""
        assert _normalize_header("") is None
        assert _normalize_header(None) is None
        assert _normalize_header("   ") is None  # Whitespace-only should also be None


class TestGamebooksNumericCoercion:
    """Test robust numeric coercion."""

    def test_dnp_variants_return_none(self):
        """Test that DNP variants return None."""
        dnp_variants = ["", "-", "—", "DNP*", "DNP", "DND"]

        for variant in dnp_variants:
            result = _coerce_number(variant)
            assert result is None, f"Failed for DNP variant: '{variant}'"

    def test_numeric_coercion_integers(self):
        """Test integer coercion."""
        assert _coerce_number("42") == 42
        assert _coerce_number("0") == 0
        assert _coerce_number("-5") == -5

    def test_numeric_coercion_floats(self):
        """Test float coercion."""
        assert _coerce_number("42.5") == 42.5
        assert _coerce_number("0.0") == 0.0
        assert _coerce_number("-5.25") == -5.25

    def test_comma_stripping(self):
        """Test that commas are stripped from numbers."""
        assert _coerce_number("1,234") == 1234
        assert _coerce_number("1,234.56") == 1234.56

    def test_whitespace_handling(self):
        """Test whitespace trimming."""
        assert _coerce_number("  42  ") == 42
        assert _coerce_number("\t42.5\n") == 42.5

    def test_invalid_values_return_none(self):
        """Test that invalid values return None."""
        invalid_values = ["abc", "12.34.56", "not_a_number", "12-34"]

        for value in invalid_values:
            result = _coerce_number(value)
            assert result is None, f"Failed for invalid value: '{value}'"


class TestGamebooksPlayerStatsExtraction:
    """Test player stats table extraction."""

    def test_header_variants_in_full_extraction(self):
        """Test full extraction with various header formats."""
        parsed_gamebook = {
            "game_id": "0022400123",
            "player_stats_tables": [
                {
                    "team": "LAL",
                    "headers": ["PLAYER", "+/-", "3PT", "MINS", "PTS"],
                    "rows": [
                        ["LeBron James", "5", "2-4", "35:24", "28"],
                        ["Anthony Davis", "-3", "0-1", "32:15", "22"],
                        ["Totals", "2", "2-5", "240:00", "50"],
                    ],
                }
            ],
        }

        result = extract_player_stats_tables(parsed_gamebook, "http://test.com")

        # Should have 2 players (excluding totals)
        assert len(result) == 2

        # Check header normalization worked
        lebron = result[0]
        assert lebron["plus_minus"] == 5
        assert lebron["tpm_tpa"] is None  # "2-4" can't be coerced to number
        assert lebron["min"] == "35:24"  # Keep as string for transformer
        assert lebron["pts"] == 28

    def test_dnp_players_handled_correctly(self):
        """Test that DNP players have None values for stats."""
        parsed_gamebook = {
            "game_id": "0022400123",
            "player_stats_tables": [
                {
                    "team": "LAL",
                    "headers": ["PLAYER", "MIN", "PTS", "REB"],
                    "rows": [
                        ["Active Player", "25:30", "15", "8"],
                        ["DNP Player", "DNP*", "-", "—"],
                        ["Totals", "240:00", "15", "8"],
                    ],
                }
            ],
        }

        result = extract_player_stats_tables(parsed_gamebook, "http://test.com")

        assert len(result) == 2

        active_player = result[0]
        assert active_player["pts"] == 15
        assert active_player["reb"] == 8

        dnp_player = result[1]
        assert dnp_player["pts"] is None
        assert dnp_player["reb"] is None
        assert dnp_player["min"] == "DNP*"  # Keep original for transformer

    @patch("nba_scraper.extractors.gamebooks.logger")
    def test_totals_mismatch_triggers_warning(self, mock_logger):
        """Test that totals mismatch triggers a single WARNING, not failure."""
        parsed_gamebook = {
            "game_id": "0022400123",
            "player_stats_tables": [
                {
                    "team": "LAL",
                    "headers": ["PLAYER", "PTS", "REB"],
                    "rows": [
                        ["Player 1", "20", "5"],
                        ["Player 2", "15", "3"],
                        ["Totals", "40", "10"],  # Mismatch: should be 35 pts, 8 reb
                    ],
                }
            ],
        }

        # Should not raise exception
        result = extract_player_stats_tables(parsed_gamebook, "http://test.com")

        # Should still return player data
        assert len(result) == 2

        # Should have logged warning about mismatch
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args
        assert "totals mismatch" in warning_call[0][0].lower()

    def test_multi_page_table_merging(self):
        """Test merging of multi-page tables with single totals row."""
        parsed_gamebook = {
            "game_id": "0022400123",
            "player_stats_tables": [
                {
                    "team": "LAL",
                    "headers": ["PLAYER", "PTS"],
                    "rows": [
                        ["Player 1", "20"],
                        ["Player 2", "15"],
                        ["Player 3", "10"],
                        ["Totals", "45"],  # Should be only one totals row
                    ],
                }
            ],
        }

        result = extract_player_stats_tables(parsed_gamebook, "http://test.com")

        # Should have 3 players, no duplicate totals
        assert len(result) == 3

        # All players should have valid data
        total_pts = sum(player["pts"] for player in result if player["pts"] is not None)
        assert total_pts == 45

    def test_minutes_kept_as_raw_strings(self):
        """Test that minutes are kept as raw MM:SS strings for transformer."""
        parsed_gamebook = {
            "game_id": "0022400123",
            "player_stats_tables": [
                {
                    "team": "LAL",
                    "headers": ["PLAYER", "MIN", "PTS"],
                    "rows": [
                        ["Player 1", "25:30", "20"],
                        ["Player 2", "32:45", "15"],
                        ["Totals", "240:00", "35"],
                    ],
                }
            ],
        }

        result = extract_player_stats_tables(parsed_gamebook, "http://test.com")

        assert len(result) == 2
        assert result[0]["min"] == "25:30"
        assert result[1]["min"] == "32:45"
        # Minutes should be strings, not converted to numbers
        assert isinstance(result[0]["min"], str)
        assert isinstance(result[1]["min"], str)


class TestGamebooksTotalsValidation:
    """Test team totals validation logic."""

    @patch("nba_scraper.extractors.gamebooks.logger")
    def test_totals_validation_with_tolerance(self, mock_logger):
        """Test totals validation with 1-point tolerance."""
        player_stats = [
            {"pts": 20, "reb": 5, "ast": 3},
            {"pts": 15, "reb": 8, "ast": 7},
            {"pts": 10, "reb": 2, "ast": 4},
        ]

        # Test within tolerance (should not warn)
        team_totals = {"pts": 45, "reb": 15, "ast": 14}  # 1 point difference in assists
        _validate_team_totals("test_game", "LAL", player_stats, team_totals)
        mock_logger.warning.assert_not_called()

        mock_logger.reset_mock()

        # Test outside tolerance (should warn)
        team_totals = {"pts": 47, "reb": 15, "ast": 14}  # 2 point difference in points
        _validate_team_totals("test_game", "LAL", player_stats, team_totals)
        mock_logger.warning.assert_called_once()

    @patch("nba_scraper.extractors.gamebooks.logger")
    def test_totals_validation_handles_none_values(self, mock_logger):
        """Test totals validation handles None values correctly."""
        player_stats = [
            {"pts": 20, "reb": None, "ast": 3},  # DNP player with None values
            {"pts": 15, "reb": 8, "ast": None},
            {"pts": None, "reb": 2, "ast": 4},  # Partial data
        ]

        team_totals = {"pts": 35, "reb": 10, "ast": 7}

        # Should not crash with None values
        _validate_team_totals("test_game", "LAL", player_stats, team_totals)

        # Should not warn since calculation handles None correctly
        mock_logger.warning.assert_not_called()


class TestGamebooksIntegration:
    """Integration tests for complete gamebook processing."""

    def test_complete_gamebook_extraction_flow(self):
        """Test complete flow from parsed gamebook to extracted stats."""
        parsed_gamebook = {
            "game_id": "0022400123",
            "player_stats_tables": [
                {
                    "team": "LAL",
                    "headers": [
                        "PLAYER",
                        "MIN",
                        "FG",
                        "3PT",
                        "FT",
                        "+/-",
                        "REB",
                        "AST",
                        "STL",
                        "BLK",
                        "TO",
                        "PF",
                        "PTS",
                    ],
                    "rows": [
                        [
                            "LeBron James",
                            "35:24",
                            "8-15",
                            "2-5",
                            "4-4",
                            "+5",
                            "8",
                            "7",
                            "1",
                            "1",
                            "3",
                            "2",
                            "22",
                        ],
                        [
                            "Anthony Davis",
                            "32:15",
                            "9-16",
                            "0-1",
                            "4-6",
                            "-3",
                            "12",
                            "2",
                            "0",
                            "3",
                            "2",
                            "4",
                            "22",
                        ],
                        [
                            "DNP Player",
                            "DNP",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                            "-",
                        ],
                        [
                            "Totals",
                            "240:00",
                            "17-31",
                            "2-6",
                            "8-10",
                            "+2",
                            "20",
                            "9",
                            "1",
                            "4",
                            "5",
                            "6",
                            "44",
                        ],
                    ],
                }
            ],
        }

        result = extract_player_stats_tables(parsed_gamebook, "http://test.com")

        # Should extract 3 players (including DNP)
        assert len(result) == 3

        # Check LeBron's data
        lebron = next(p for p in result if p["player"] == "LeBron James")
        assert lebron["min"] == "35:24"
        assert lebron["plus_minus"] == 5
        assert lebron["reb"] == 8
        assert lebron["ast"] == 7
        assert lebron["pts"] == 22

        # Check DNP player
        dnp_player = next(p for p in result if p["player"] == "DNP Player")
        assert dnp_player["pts"] is None
        assert dnp_player["reb"] is None
        assert dnp_player["min"] == "DNP"

        # Verify all players have required metadata
        for player in result:
            assert player["game_id"] == "0022400123"
            assert player["team_tricode"] == "LAL"
            assert player["source"] == "gamebook_pdf"
            assert player["source_url"] == "http://test.com"
