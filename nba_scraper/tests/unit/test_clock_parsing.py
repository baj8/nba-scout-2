"""Unit tests for clock parsing utilities."""

import pytest
from nba_scraper.utils.clock import (
    parse_clock_to_seconds, 
    compute_seconds_elapsed,
    validate_clock_format,
    format_seconds_as_clock
)


def test_parse_clock_to_seconds_basic_and_fractional():
    """Test clock parsing with basic and fractional seconds."""
    # Basic formats
    assert parse_clock_to_seconds("24:49") == 1489
    assert parse_clock_to_seconds("0:00") == 0
    assert parse_clock_to_seconds("5:09") == 309
    assert parse_clock_to_seconds("12:00") == 720
    
    # Fractional seconds
    assert parse_clock_to_seconds("1:23.4") == 83.4
    assert parse_clock_to_seconds("01:23.45") == 83.45
    assert parse_clock_to_seconds("0:05.123") == 5.123
    
    # ISO duration formats
    assert parse_clock_to_seconds("PT10M24S") == 624
    assert parse_clock_to_seconds("PT0M5.1S") == 5.1
    assert parse_clock_to_seconds("PT12M0S") == 720
    
    # Invalid formats
    assert parse_clock_to_seconds("25:00") is None  # Invalid minutes
    assert parse_clock_to_seconds("12:60") is None  # Invalid seconds
    assert parse_clock_to_seconds("invalid") is None
    assert parse_clock_to_seconds("") is None
    assert parse_clock_to_seconds(None) is None


def test_elapsed_regulation_and_ot():
    """Test seconds elapsed calculation for regulation and overtime periods."""
    # Period 1 - regulation
    cs = parse_clock_to_seconds("12:00")
    assert compute_seconds_elapsed(1, cs) == 0  # Start of game
    
    cs = parse_clock_to_seconds("0:00") 
    assert compute_seconds_elapsed(1, cs) == 12*60  # End of Q1
    
    cs = parse_clock_to_seconds("6:00")
    assert compute_seconds_elapsed(1, cs) == 6*60  # 6 minutes into Q1
    
    # Period 2 - regulation
    cs = parse_clock_to_seconds("6:00")
    assert compute_seconds_elapsed(2, cs) == 12*60 + 6*60  # Q1 + 6min into Q2
    
    # Period 4 - end of regulation
    cs = parse_clock_to_seconds("0:00")
    assert compute_seconds_elapsed(4, cs) == 48*60  # End of regulation
    
    # Period 5 - first overtime
    cs = parse_clock_to_seconds("5:00")
    assert compute_seconds_elapsed(5, cs) == 48*60  # Start of OT
    
    cs = parse_clock_to_seconds("0:00")
    assert compute_seconds_elapsed(5, cs) == 48*60 + 5*60  # End of 1st OT
    
    # Period 6 - second overtime
    cs = parse_clock_to_seconds("2:30")
    assert compute_seconds_elapsed(6, cs) == 48*60 + 5*60 + 2.5*60  # Into 2nd OT


def test_elapsed_mode_auto_correction():
    """Test auto-correction when mode assumption is wrong."""
    # Test case where we get negative result and auto-correct
    period = 1
    clock_seconds = 600  # 10 minutes
    
    # If this was actually elapsed time, not remaining time
    result = compute_seconds_elapsed(period, clock_seconds, mode="remaining")
    assert result >= 0  # Should auto-correct and clamp to 0 if needed
    
    # Test explicit elapsed mode
    result = compute_seconds_elapsed(period, clock_seconds, mode="elapsed")
    assert result == 600  # 10 minutes elapsed


def test_fractional_seconds_elapsed():
    """Test elapsed calculation with fractional seconds."""
    cs = parse_clock_to_seconds("11:30.5")  # 11:30.5 remaining in Q1
    result = compute_seconds_elapsed(1, cs)
    expected = 12*60 - (11*60 + 30.5)  # Should be 29.5 seconds elapsed
    assert abs(result - expected) < 0.01


def test_validate_clock_format():
    """Test clock format validation."""
    # Valid formats
    assert validate_clock_format("12:34")
    assert validate_clock_format("0:00")
    assert validate_clock_format("1:23.4")
    assert validate_clock_format("PT12M34S")
    assert validate_clock_format("PT1M23.4S")
    
    # Invalid formats
    assert not validate_clock_format("25:00")  # Invalid minutes
    assert not validate_clock_format("12:60")  # Invalid seconds
    assert not validate_clock_format("12:3")   # Missing zero pad
    assert not validate_clock_format("invalid")
    assert not validate_clock_format("")
    assert not validate_clock_format(None)


def test_format_seconds_as_clock():
    """Test formatting seconds back to clock string."""
    assert format_seconds_as_clock(0) == "0:00"
    assert format_seconds_as_clock(30) == "0:30"
    assert format_seconds_as_clock(90) == "1:30"
    assert format_seconds_as_clock(723.5) == "12:03.500"
    assert format_seconds_as_clock(-10) == "0:00"  # Negative clamped
    assert format_seconds_as_clock(None) == "0:00"  # None handled