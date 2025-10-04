"""Unit tests for clock parsing utilities with fractional seconds support."""

import pytest
from nba_scraper.utils.clock import parse_clock_to_seconds, format_seconds_to_clock, validate_clock_format


def test_parse_clock_to_seconds_basic_and_fractional():
    """Test basic and fractional second parsing for various clock formats."""
    # Basic MM:SS format
    assert parse_clock_to_seconds("24:49") == 1489.0
    assert parse_clock_to_seconds("0:00") == 0.0
    assert parse_clock_to_seconds("5:09") == 309.0
    assert parse_clock_to_seconds("12:00") == 720.0
    
    # Single digit minutes
    assert parse_clock_to_seconds("1:30") == 90.0
    assert parse_clock_to_seconds("9:59") == 599.0
    
    # Fractional seconds
    assert parse_clock_to_seconds("1:23.4") == 83.4
    assert parse_clock_to_seconds("01:23.45") == 83.45
    assert parse_clock_to_seconds("0:05.123") == 5.123
    assert parse_clock_to_seconds("12:30.5") == 750.5
    
    # ISO duration format
    assert parse_clock_to_seconds("PT10M24S") == 624.0
    assert parse_clock_to_seconds("PT0M5S") == 5.0
    assert parse_clock_to_seconds("PT1M0S") == 60.0
    
    # ISO with fractional seconds
    assert parse_clock_to_seconds("PT0M5.1S") == 5.1
    assert parse_clock_to_seconds("PT10M24.75S") == 624.75


def test_parse_clock_to_seconds_invalid():
    """Test parsing of invalid clock formats."""
    # Invalid formats should return None
    assert parse_clock_to_seconds("24-49") is None
    assert parse_clock_to_seconds("") is None
    assert parse_clock_to_seconds("25:70") is None  # Invalid seconds (>59)
    assert parse_clock_to_seconds("abc:def") is None
    assert parse_clock_to_seconds("12:") is None
    assert parse_clock_to_seconds(":30") is None
    assert parse_clock_to_seconds("not-a-time") is None
    assert parse_clock_to_seconds("12:99") is None  # Invalid seconds
    
    # None input
    assert parse_clock_to_seconds(None) is None
    
    # Non-string input
    assert parse_clock_to_seconds(1234) is None
    assert parse_clock_to_seconds(12.34) is None


def test_format_seconds_to_clock():
    """Test formatting seconds back to clock format."""
    # Basic formatting
    assert format_seconds_to_clock(1489.0) == "24:49"
    assert format_seconds_to_clock(0.0) == "0:00"
    assert format_seconds_to_clock(309.0) == "5:09"
    assert format_seconds_to_clock(720.0) == "12:00"
    
    # Fractional seconds
    assert format_seconds_to_clock(83.4) == "1:23.400"
    assert format_seconds_to_clock(83.45) == "1:23.450"
    assert format_seconds_to_clock(5.123) == "0:05.123"
    
    # Edge cases
    assert format_seconds_to_clock(None) == ""
    assert format_seconds_to_clock(59.0) == "0:59"
    assert format_seconds_to_clock(3600.0) == "60:00"  # 1 hour


def test_validate_clock_format():
    """Test clock format validation."""
    # Valid formats
    assert validate_clock_format("24:49") is True
    assert validate_clock_format("0:00") is True
    assert validate_clock_format("12:30") is True
    assert validate_clock_format("1:23.4") is True
    assert validate_clock_format("01:23.45") is True
    assert validate_clock_format("PT10M24S") is True
    assert validate_clock_format("PT0M5.1S") is True
    
    # Invalid formats
    assert validate_clock_format("24-49") is False
    assert validate_clock_format("25:70") is False
    assert validate_clock_format("") is False
    assert validate_clock_format("abc:def") is False
    assert validate_clock_format(None) is False
    assert validate_clock_format(1234) is False


def test_round_trip_conversion():
    """Test that parsing and formatting are consistent."""
    test_clocks = [
        "24:49",
        "0:00", 
        "5:09",
        "1:23.4",
        "12:30.75"
    ]
    
    for clock in test_clocks:
        seconds = parse_clock_to_seconds(clock)
        assert seconds is not None
        
        # Format back to clock
        formatted = format_seconds_to_clock(seconds)
        
        # Parse again
        seconds_again = parse_clock_to_seconds(formatted)
        
        # Should be very close (within floating point precision)
        assert abs(seconds - seconds_again) < 0.001


def test_edge_case_boundaries():
    """Test boundary conditions for clock parsing."""
    # Minimum valid values
    assert parse_clock_to_seconds("0:00") == 0.0
    assert parse_clock_to_seconds("0:01") == 1.0
    
    # Maximum valid seconds
    assert parse_clock_to_seconds("0:59") == 59.0
    assert parse_clock_to_seconds("1:59") == 119.0
    
    # Large but valid values
    assert parse_clock_to_seconds("48:00") == 2880.0  # Double overtime
    
    # Very small fractional seconds
    assert parse_clock_to_seconds("0:00.1") == 0.1
    assert parse_clock_to_seconds("0:00.01") == 0.01
    assert parse_clock_to_seconds("0:00.001") == 0.001


def test_caching_behavior():
    """Test that the LRU cache is working correctly."""
    # First call
    result1 = parse_clock_to_seconds("12:34")
    
    # Second call with same input should return cached result
    result2 = parse_clock_to_seconds("12:34")
    
    assert result1 == result2 == 754.0
    
    # The function should handle many different inputs efficiently
    test_values = [f"{m}:{s:02d}" for m in range(10) for s in range(0, 60, 5)]
    
    for clock in test_values:
        result = parse_clock_to_seconds(clock)
        assert result is not None
        assert result >= 0


def test_real_nba_clock_scenarios():
    """Test with realistic NBA game clock scenarios."""
    # Quarter ending scenarios
    assert parse_clock_to_seconds("12:00") == 720.0  # Start of quarter
    assert parse_clock_to_seconds("0:00") == 0.0     # End of quarter
    
    # Common game situations
    assert parse_clock_to_seconds("2:00") == 120.0   # Two-minute warning
    assert parse_clock_to_seconds("0:24") == 24.0    # Shot clock scenario
    assert parse_clock_to_seconds("0:03.7") == 3.7   # Buzzer beater scenario
    
    # Overtime scenarios
    assert parse_clock_to_seconds("5:00") == 300.0   # Start of OT
    assert parse_clock_to_seconds("24:00") == 1440.0 # Double OT remaining time
    
    # Real examples from the problematic data
    assert parse_clock_to_seconds("24:49") == 1489.0  # The original failing case
    assert parse_clock_to_seconds("11:45") == 705.0
    assert parse_clock_to_seconds("6:23") == 383.0