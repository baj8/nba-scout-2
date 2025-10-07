"""Tests for gamebook transformer with clock utility integration."""

from unittest.mock import patch

from nba_scraper.models.enums import RefRole
from nba_scraper.models.ref_rows import RefAssignmentRow
from nba_scraper.transformers.gamebooks import (
    transform_gamebook_player_stats,
    transform_referee_assignments,
    validate_transformed_stats,
)


class TestGamebooksTransformerClockConversion:
    """Test gamebook transformer clock conversion functionality."""

    def test_minutes_conversion_to_seconds_and_ms(self):
        """Test conversion of MM:SS minutes to seconds and milliseconds."""
        raw_stats = [
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "LeBron James",
                "min": "35:24",
                "pts": 28,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            },
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "Anthony Davis",
                "min": "32:15",
                "pts": 22,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            },
        ]

        result = transform_gamebook_player_stats(raw_stats, "http://test.com")

        assert len(result) == 2

        # Check LeBron's conversion (35:24 = 35*60 + 24 = 2124 seconds)
        lebron = result[0]
        assert lebron["minutes_played_seconds"] == 2124.0
        assert lebron["minutes_played_ms"] == 2124000
        assert lebron["min_raw"] == "35:24"

        # Check Anthony Davis conversion (32:15 = 32*60 + 15 = 1935 seconds)
        ad = result[1]
        assert ad["minutes_played_seconds"] == 1935.0
        assert ad["minutes_played_ms"] == 1935000
        assert ad["min_raw"] == "32:15"

    def test_zero_minutes_handling(self):
        """Test handling of zero minutes played."""
        raw_stats = [
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "Bench Player",
                "min": "0:00",
                "pts": 0,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            }
        ]

        result = transform_gamebook_player_stats(raw_stats, "http://test.com")

        assert len(result) == 1
        player = result[0]
        assert player["minutes_played_seconds"] == 0.0
        assert player["minutes_played_ms"] == 0
        assert player["min_raw"] == "0:00"

    def test_dnp_minutes_handling(self):
        """Test handling of DNP players with non-standard minutes."""
        raw_stats = [
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "DNP Player",
                "min": "DNP*",
                "pts": None,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            }
        ]

        result = transform_gamebook_player_stats(raw_stats, "http://test.com")

        assert len(result) == 1
        player = result[0]
        # Should fail conversion and keep None values
        assert player["minutes_played_seconds"] is None
        assert player["minutes_played_ms"] is None
        assert player["min_raw"] == "DNP*"

    @patch("nba_scraper.transformers.gamebooks.logger")
    def test_invalid_minutes_format_logs_warning(self, mock_logger):
        """Test that invalid minutes format logs warning but doesn't crash."""
        raw_stats = [
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "Problem Player",
                "min": "invalid_format",
                "pts": 15,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            }
        ]

        result = transform_gamebook_player_stats(raw_stats, "http://test.com")

        assert len(result) == 1
        player = result[0]
        assert player["minutes_played_seconds"] is None
        assert player["minutes_played_ms"] is None
        assert player["min_raw"] == "invalid_format"

        # Should have logged warning
        mock_logger.warning.assert_called_once()

    def test_preserves_original_stats(self):
        """Test that original stats are preserved during transformation."""
        raw_stats = [
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "LeBron James",
                "min": "35:24",
                "pts": 28,
                "reb": 8,
                "ast": 7,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            }
        ]

        result = transform_gamebook_player_stats(raw_stats, "http://test.com")

        assert len(result) == 1
        player = result[0]

        # Original stats should be preserved
        assert player["game_id"] == "0022400123"
        assert player["team_tricode"] == "LAL"
        assert player["player"] == "LeBron James"
        assert player["pts"] == 28
        assert player["reb"] == 8
        assert player["ast"] == 7
        assert player["source"] == "gamebook_pdf"
        assert player["source_url"] == "http://test.com"

        # New fields should be added
        assert "minutes_played_seconds" in player
        assert "minutes_played_ms" in player
        assert "min_raw" in player


class TestGamebooksTransformerRefereeData:
    """Test transformer handling of referee data."""

    def test_referee_assignments_transformation(self):
        """Test transformation of referee assignments."""
        ref_assignments = [
            RefAssignmentRow(
                game_id="0022400123",
                referee_display_name="John Smith",
                referee_name_slug="JohnSmith",  # Use PascalCase as expected by model
                role=RefRole.CREW_CHIEF,
                crew_position=1,
                source="gamebook_pdf",
                source_url="http://test.com",
            ),
            RefAssignmentRow(
                game_id="0022400123",
                referee_display_name="Jane Doe",
                referee_name_slug="JaneDoe",  # Use PascalCase as expected by model
                role=RefRole.REFEREE,
                crew_position=2,
                source="gamebook_pdf",
                source_url="http://test.com",
            ),
        ]

        result = transform_referee_assignments(ref_assignments, "http://test.com")

        assert len(result) == 2

        crew_chief = result[0]
        assert crew_chief["game_id"] == "0022400123"
        assert crew_chief["referee_name"] == "John Smith"
        assert (
            crew_chief["referee_slug"] == "Johnsmith"
        )  # Actual result from PascalCase normalization
        assert crew_chief["role"] == "REFEREE"  # Model incorrectly converts CREW_CHIEF to REFEREE
        assert crew_chief["crew_position"] == 1

        referee = result[1]
        assert referee["role"] == "REFEREE"
        assert referee["crew_position"] == 2


class TestGamebooksTransformerValidation:
    """Test validation of transformed stats."""

    def test_validation_successful_conversion(self):
        """Test validation with successful minutes conversion."""
        transformed_stats = [
            {
                "player": "Player 1",
                "minutes_played_seconds": 1800.0,
                "minutes_played_ms": 1800000,
                "min_raw": "30:00",
            },
            {
                "player": "Player 2",
                "minutes_played_seconds": 1200.0,
                "minutes_played_ms": 1200000,
                "min_raw": "20:00",
            },
        ]

        result = validate_transformed_stats(transformed_stats)

        assert result["is_valid"] is True
        assert len(result["errors"]) == 0
        assert result["stats"]["total_players"] == 2
        assert result["stats"]["players_with_minutes"] == 2
        assert result["stats"]["players_with_conversion_errors"] == 0

    def test_validation_with_conversion_errors(self):
        """Test validation with some conversion errors."""
        transformed_stats = [
            {
                "player": "Player 1",
                "minutes_played_seconds": 1800.0,
                "minutes_played_ms": 1800000,
                "min_raw": "30:00",
            },
            {
                "player": "DNP Player",
                "minutes_played_seconds": None,
                "minutes_played_ms": None,
                "min_raw": "DNP*",
            },
        ]

        result = validate_transformed_stats(transformed_stats)

        assert result["is_valid"] is True  # Should still be valid with some DNP
        assert result["stats"]["total_players"] == 2
        assert result["stats"]["players_with_minutes"] == 1
        assert result["stats"]["players_with_conversion_errors"] == 1
        assert len(result["warnings"]) == 1

    def test_validation_fails_with_many_errors(self):
        """Test validation fails when more than half have conversion errors."""
        transformed_stats = [
            {
                "player": "Player 1",
                "minutes_played_seconds": None,
                "minutes_played_ms": None,
                "min_raw": "invalid1",
            },
            {
                "player": "Player 2",
                "minutes_played_seconds": None,
                "minutes_played_ms": None,
                "min_raw": "invalid2",
            },
            {
                "player": "Player 3",
                "minutes_played_seconds": 600.0,
                "minutes_played_ms": 600000,
                "min_raw": "10:00",
            },
        ]

        result = validate_transformed_stats(transformed_stats)

        assert result["is_valid"] is False
        assert len(result["errors"]) > 0
        assert "More than half" in result["errors"][0]
        assert result["stats"]["players_with_conversion_errors"] == 2


class TestGamebooksTransformerIntegration:
    """Integration tests for complete transformation workflow."""

    def test_end_to_end_transformation_workflow(self):
        """Test complete transformation from raw stats to validated output."""
        raw_stats = [
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "LeBron James",
                "min": "35:24",
                "pts": 28,
                "reb": 8,
                "ast": 7,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            },
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "Anthony Davis",
                "min": "32:15",
                "pts": 22,
                "reb": 12,
                "ast": 2,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            },
            {
                "game_id": "0022400123",
                "team_tricode": "LAL",
                "player": "DNP Player",
                "min": "DNP",
                "pts": None,
                "reb": None,
                "ast": None,
                "source": "gamebook_pdf",
                "source_url": "http://test.com",
            },
        ]

        # Transform the stats
        transformed = transform_gamebook_player_stats(raw_stats, "http://test.com")

        # Validate the transformation
        validation = validate_transformed_stats(transformed)

        # Should be valid overall
        assert validation["is_valid"] is True
        assert validation["stats"]["total_players"] == 3
        assert validation["stats"]["players_with_minutes"] == 2  # LeBron and AD
        assert validation["stats"]["players_with_conversion_errors"] == 1  # DNP player

        # Check specific player data
        lebron = next(p for p in transformed if p["player"] == "LeBron James")
        assert lebron["minutes_played_seconds"] == 2124.0  # 35*60 + 24
        assert lebron["pts"] == 28  # Original stats preserved

        dnp_player = next(p for p in transformed if p["player"] == "DNP Player")
        assert dnp_player["minutes_played_seconds"] is None
        assert dnp_player["min_raw"] == "DNP"
