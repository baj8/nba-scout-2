"""Unit tests for season derivation utilities."""

import pytest
from nba_scraper.utils.season import (
    derive_season_from_game_id,
    derive_season_from_date,
    derive_season_smart,
    validate_season_format,
    get_current_nba_season
)


def test_derive_season_from_game_id_regex():
    """Test season derivation from NBA game ID using regex patterns."""
    # Regular season games
    assert derive_season_from_game_id("0022300001") == "2023-24"
    assert derive_season_from_game_id("0022400123") == "2024-25"
    assert derive_season_from_game_id("0022200456") == "2022-23"
    
    # Preseason games
    assert derive_season_from_game_id("0012300001") == "2023-24"
    
    # Playoff games
    assert derive_season_from_game_id("0032300001") == "2023-24"
    
    # Play-in games
    assert derive_season_from_game_id("0042300001") == "2023-24"
    
    # Invalid formats
    assert derive_season_from_game_id("invalid") is None
    assert derive_season_from_game_id("123456") is None  # Too short
    assert derive_season_from_game_id("0000000000") is None  # Invalid type code
    assert derive_season_from_game_id("") is None
    assert derive_season_from_game_id(None) is None


def test_derive_season_from_date():
    """Test season derivation from game date using NBA season boundaries."""
    # Regular season dates
    assert derive_season_from_date("2023-10-15") == "2023-24"  # October start
    assert derive_season_from_date("2023-12-25") == "2023-24"  # Christmas
    assert derive_season_from_date("2024-01-15") == "2023-24"  # January
    assert derive_season_from_date("2024-04-10") == "2023-24"  # April end
    assert derive_season_from_date("2024-06-15") == "2023-24"  # Finals
    
    # Offseason dates (assume previous season)
    assert derive_season_from_date("2024-07-15") == "2023-24"  # July offseason
    assert derive_season_from_date("2024-08-15") == "2023-24"  # August offseason
    assert derive_season_from_date("2024-09-15") == "2023-24"  # September offseason
    
    # Next season start
    assert derive_season_from_date("2024-10-15") == "2024-25"
    
    # Invalid dates
    assert derive_season_from_date("invalid-date") is None
    assert derive_season_from_date("2023-13-01") is None  # Invalid month
    assert derive_season_from_date("") is None
    assert derive_season_from_date(None) is None


def test_derive_season_smart():
    """Test smart season derivation with multiple fallback strategies."""
    # Game ID takes priority
    result = derive_season_smart("0022300001", "2024-01-15", "2022-23")
    assert result == "2023-24"  # From game ID, not date or fallback
    
    # Date fallback when game ID invalid
    result = derive_season_smart("invalid_id", "2024-01-15", "2022-23")
    assert result == "2023-24"  # From date
    
    # Explicit fallback when both invalid
    result = derive_season_smart("invalid_id", "invalid_date", "2022-23")
    assert result == "2022-23"  # From fallback
    
    # UNKNOWN when all invalid
    result = derive_season_smart("invalid_id", "invalid_date", "invalid_season")
    assert result == "UNKNOWN"
    
    # Handle None inputs
    result = derive_season_smart(None, None, None)
    assert result == "UNKNOWN"


def test_validate_season_format():
    """Test season format validation."""
    # Valid formats
    assert validate_season_format("2023-24")
    assert validate_season_format("2024-25")
    assert validate_season_format("1999-00")
    assert validate_season_format("2000-01")
    
    # Invalid formats
    assert not validate_season_format("2023-2024")  # Wrong format
    assert not validate_season_format("23-24")      # Missing century
    assert not validate_season_format("2023")       # Missing end year
    assert not validate_season_format("UNKNOWN")    # Not numeric
    assert not validate_season_format("")
    assert not validate_season_format(None)
    assert not validate_season_format(123)          # Not string


def test_coalesce_season():
    """Test season coalescing from multiple sources."""
    from nba_scraper.utils.season import coalesce_season
    
    # First valid season
    assert coalesce_season("2023-24", "2022-23") == "2023-24"
    
    # Skip invalid, return first valid
    assert coalesce_season("invalid", "2023-24", "2022-23") == "2023-24"
    
    # All invalid
    assert coalesce_season("invalid", "", None) is None
    
    # Empty input
    assert coalesce_season() is None


def test_get_current_nba_season():
    """Test getting current NBA season based on date logic."""
    # This test is date-dependent, so we'll just verify the format
    current_season = get_current_nba_season()
    assert validate_season_format(current_season)
    assert current_season.startswith("20")  # Should be 21st century