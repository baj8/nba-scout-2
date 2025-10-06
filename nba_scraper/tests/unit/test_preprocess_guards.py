"""Unit tests to prevent preprocessing regressions."""

import pytest
from nba_scraper.utils.preprocess import preprocess_nba_stats_data


def test_game_id_preserves_leading_zeros():
    """Assert that game_id strings with leading zeros are preserved."""
    data = {"GAME_ID": "0022301234"}
    result = preprocess_nba_stats_data(data)
    
    # Game ID must remain a string with leading zeros
    assert result["GAME_ID"] == "0022301234"
    assert isinstance(result["GAME_ID"], str)
    assert len(result["GAME_ID"]) == 10


def test_numeric_strings_still_coerced_appropriately():
    """Assert that non-ID numeric strings are still converted where appropriate."""
    data = {
        "GAME_ID": "0022301234",  # Should stay string
        "TEAM_ID": "1610612744",  # Should be coerced to int
        "POINTS": "25",           # Should be coerced to int
        "PLAYER_NAME": "LeBron James",  # Should stay string
    }
    result = preprocess_nba_stats_data(data)
    
    # Game ID preserved as string
    assert result["GAME_ID"] == "0022301234"
    assert isinstance(result["GAME_ID"], str)
    
    # Other numeric fields coerced appropriately
    assert result["TEAM_ID"] == 1610612744
    assert isinstance(result["TEAM_ID"], int)
    
    assert result["POINTS"] == 25
    assert isinstance(result["POINTS"], int)
    
    # Non-numeric strings preserved
    assert result["PLAYER_NAME"] == "LeBron James"
    assert isinstance(result["PLAYER_NAME"], str)


def test_preprocessing_handles_mixed_data_types():
    """Test that preprocessing handles various data types correctly."""
    data = {
        "GAME_ID": "0022301234",
        "SEASON_ID": "22023",
        "VISITOR_TEAM_ID": 1610612744,  # Already int
        "HOME_TEAM_ID": "1610612739",   # String that should become int
        "GAME_STATUS_TEXT": "Final",    # Non-numeric string
        "PTS": 108.0,                   # Float that should become int
        "PCT": 0.456,                   # Float that should stay float
    }
    result = preprocess_nba_stats_data(data)
    
    # Game-like IDs preserved as strings
    assert result["GAME_ID"] == "0022301234"
    assert isinstance(result["GAME_ID"], str)
    
    # Season ID gets converted to int (doesn't have leading zeros pattern like game_id)
    assert result["SEASON_ID"] == 22023
    assert isinstance(result["SEASON_ID"], int)
    
    # Team IDs as integers
    assert result["VISITOR_TEAM_ID"] == 1610612744
    assert result["HOME_TEAM_ID"] == 1610612739
    assert isinstance(result["VISITOR_TEAM_ID"], int)
    assert isinstance(result["HOME_TEAM_ID"], int)
    
    # Text preserved
    assert result["GAME_STATUS_TEXT"] == "Final"
    assert isinstance(result["GAME_STATUS_TEXT"], str)
    
    # Numeric conversions
    assert result["PTS"] == 108.0  # Float input stays float
    assert isinstance(result["PTS"], float)
    assert result["PCT"] == 0.456
    assert isinstance(result["PCT"], float)