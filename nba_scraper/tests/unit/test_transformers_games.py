"""Unit tests for game transformation functions with strict validation."""

import re
import pytest
from nba_scraper.transformers.games import transform_game


def test_transform_game_valid_id_and_season(caplog):
    """Test that valid game_id and season pass validation unchanged."""
    raw = {
        "game_id": "0022301234", 
        "season": "2024-25",
        "game_date": "2024-01-15", 
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739, 
        "status": "Final"
    }
    game = transform_game(raw)
    assert game.game_id == "0022301234"
    assert game.season == "2024-25"
    assert game.game_date == "2024-01-15"
    assert game.home_team_id == 1610612744
    assert game.away_team_id == 1610612739
    assert game.status == "Final"
    
    # No warnings should be logged for valid data
    assert len(caplog.records) == 0


@pytest.mark.parametrize("bad_id", [
    "002301234",     # Too short (9 chars)
    "0012301234",    # Wrong prefix (starts with 001 - preseason)
    "0032301234",    # Wrong prefix (starts with 003 - playoffs)
    "0023012340",    # Wrong third digit (should be 2 for regular season)
    "00A2301234",    # Non-digit character
    "12345",         # Much too short
    "002230123456",  # Too long
    "",              # Empty string
])
def test_transform_game_rejects_bad_game_id(bad_id):
    """Test that invalid game_id formats raise ValueError with specific message."""
    raw = {
        "game_id": bad_id, 
        "season": "2024-25",
        "game_date": "2024-01-15", 
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739,
        "status": "Final"
    }
    
    with pytest.raises(ValueError) as exc_info:
        transform_game(raw)
    
    # Verify the error message format
    error_msg = str(exc_info.value)
    assert "invalid game_id:" in error_msg
    assert bad_id in error_msg
    assert "must be 10-char string matching ^002\\d{7}$" in error_msg


def test_transform_game_warns_and_derives_season_from_int(monkeypatch, caplog):
    """Test that integer season triggers warning and smart derivation."""
    import logging
    caplog.set_level(logging.WARNING)
    
    def fake_derive(game_id, game_date, fallback_season):
        return "2024-25"
    
    from nba_scraper.transformers import games as mod
    monkeypatch.setattr(mod, "derive_season_smart", fake_derive)
    
    raw = {
        "game_id": "0022301234", 
        "season": 2024,  # Integer instead of string
        "game_date": "2024-01-15",
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739,
        "status": "Final"
    }
    
    game = transform_game(raw)
    assert game.season == "2024-25"
    
    # Check that warning was logged
    warning_records = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert len(warning_records) >= 1
    # Check message content more flexibly
    warning_text = " ".join([rec.message for rec in warning_records])
    assert "season format invalid" in warning_text
    assert "2024" in warning_text


def test_transform_game_warns_and_derives_season_when_malformed(monkeypatch, caplog):
    """Test that malformed season string triggers warning and smart derivation."""
    import logging
    caplog.set_level(logging.WARNING)
    
    def fake_derive(game_id, game_date, fallback_season):
        return "2024-25"
    
    from nba_scraper.transformers import games as mod
    monkeypatch.setattr(mod, "derive_season_smart", fake_derive)
    
    raw = {
        "game_id": "0022301234", 
        "season": "2024/25",  # Wrong separator
        "game_date": "2024-01-15",
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739,
        "status": "Final"
    }
    
    game = transform_game(raw)
    assert game.season == "2024-25"
    
    # Check that warning was logged
    warning_records = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert len(warning_records) >= 1
    # Check message content more flexibly
    warning_text = " ".join([rec.message for rec in warning_records])
    assert "season format invalid" in warning_text
    assert "2024/25" in warning_text


def test_transform_game_missing_season_derives_without_warning(monkeypatch, caplog):
    """Test that missing season derives smart season without warning (existing behavior)."""
    def fake_derive(game_id, game_date, fallback_season):
        return "2024-25"
    
    from nba_scraper.transformers import games as mod
    monkeypatch.setattr(mod, "derive_season_smart", fake_derive)
    
    raw = {
        "game_id": "0022301234", 
        # No season field
        "game_date": "2024-01-15",
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739,
        "status": "Final"
    }
    
    game = transform_game(raw)
    assert game.season == "2024-25"
    
    # No warnings should be logged for missing season (existing behavior)
    warning_records = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert len(warning_records) == 0


@pytest.mark.parametrize("invalid_season", [
    "24-25",      # Wrong year format (2 digits)
    "2024-2025",  # Wrong format (4 digits for second year)
    "2024",       # Just year
    "2024-",      # Missing second part
    "-25",        # Missing first part
    "twenty24-25", # Non-numeric
    "2024_25",    # Wrong separator
])
def test_transform_game_various_invalid_season_formats(monkeypatch, caplog, invalid_season):
    """Test various invalid season formats all trigger warning and derivation."""
    def fake_derive(game_id, game_date, fallback_season):
        return "2024-25"
    
    from nba_scraper.transformers import games as mod
    monkeypatch.setattr(mod, "derive_season_smart", fake_derive)
    
    raw = {
        "game_id": "0022301234", 
        "season": invalid_season,
        "game_date": "2024-01-15",
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739,
        "status": "Final"
    }
    
    game = transform_game(raw)
    assert game.season == "2024-25"
    
    # Check that warning was logged
    warning_records = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert len(warning_records) == 1
    assert "season format invalid" in warning_records[0].message
    assert invalid_season in warning_records[0].message


def test_transform_game_valid_season_formats_no_warning(caplog):
    """Test that various valid season formats don't trigger warnings."""
    valid_seasons = ["2024-25", "2023-24", "2022-23", "1999-00", "2000-01"]
    
    for season in valid_seasons:
        caplog.clear()  # Clear previous records
        
        raw = {
            "game_id": "0022301234", 
            "season": season,
            "game_date": "2024-01-15",
            "home_team_id": 1610612744, 
            "away_team_id": 1610612739,
            "status": "Final"
        }
        
        game = transform_game(raw)
        assert game.season == season
        
        # No warnings should be logged for valid seasons
        warning_records = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert len(warning_records) == 0, f"Unexpected warning for valid season {season}"


def test_transform_game_preprocessing_preserves_game_id_leading_zeros():
    """Test that preprocessing preserves leading zeros in game_id (regression test)."""
    raw = {
        "game_id": "0022301234",  # String with leading zeros
        "season": "2024-25",
        "game_date": "2024-01-15",
        "home_team_id": 1610612744, 
        "away_team_id": 1610612739,
        "status": "Final"
    }
    
    game = transform_game(raw)
    
    # Verify leading zeros are preserved
    assert game.game_id == "0022301234"
    assert len(game.game_id) == 10
    assert game.game_id.startswith("00")


def test_transform_game_handles_string_numeric_fields():
    """Test that string numeric fields are properly converted."""
    raw = {
        "game_id": "0022301234",
        "season": "2024-25",
        "game_date": "2024-01-15",
        "home_team_id": "1610612744",  # String team ID
        "away_team_id": "1610612739",  # String team ID  
        "status": "Final"
    }
    
    game = transform_game(raw)
    
    # Verify conversion to int
    assert game.home_team_id == 1610612744
    assert game.away_team_id == 1610612739
    assert isinstance(game.home_team_id, int)
    assert isinstance(game.away_team_id, int)