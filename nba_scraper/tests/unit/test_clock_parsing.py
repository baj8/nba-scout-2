"""Unit tests for clock parsing and seconds elapsed derivation."""

import pytest
from nba_scraper.utils.clock import (
    parse_clock_to_seconds, 
    compute_seconds_elapsed,
    validate_clock_format,
    format_seconds_as_clock
)


class TestClockParsing:
    """Test clock parsing functionality."""

    def test_basic_mm_ss_formats(self):
        """Test parsing of basic MM:SS formats."""
        test_cases = [
            ("12:00", 720.0),  # Start of period
            ("11:45", 705.0),  # Mid period
            ("5:30", 330.0),   # Later in period
            ("0:45", 45.0),    # End of period
            ("0:00", 0.0),     # Period end
        ]
        
        for time_str, expected_seconds in test_cases:
            result = parse_clock_to_seconds(time_str)
            assert result == expected_seconds, f"Failed for {time_str}"

    def test_fractional_seconds_parsing(self):
        """Test parsing clock formats with fractional seconds."""
        test_cases = [
            ("11:45.5", 705.5),     # Half second
            ("11:45.500", 705.5),   # 500 milliseconds
            ("5:30.123", 330.123),  # 123 milliseconds
            ("0:00.001", 0.001),    # 1 millisecond
            ("2:30.75", 150.75),    # 750 milliseconds
        ]
        
        for time_str, expected_seconds in test_cases:
            result = parse_clock_to_seconds(time_str)
            assert abs(result - expected_seconds) < 0.001, f"Failed for {time_str}"

    def test_iso_duration_parsing(self):
        """Test parsing ISO 8601 duration format."""
        test_cases = [
            ("PT11M45S", 705.0),      # 11 minutes 45 seconds
            ("PT5M30S", 330.0),       # 5 minutes 30 seconds
            ("PT0M45S", 45.0),        # 0 minutes 45 seconds
            ("PT11M45.5S", 705.5),    # With fractional seconds
            ("PT12M0S", 720.0),       # 12 minutes exactly
        ]
        
        for time_str, expected_seconds in test_cases:
            result = parse_clock_to_seconds(time_str)
            assert result == expected_seconds, f"Failed for {time_str}"

    def test_invalid_formats_return_none(self):
        """Test that invalid formats return None."""
        invalid_formats = [
            "invalid",
            "25:99",      # Invalid minutes/seconds
            "abc:def",
            "",
            "12",         # Missing colon
            None,
            "12:60",      # Invalid seconds
        ]
        
        for invalid_time in invalid_formats:
            result = parse_clock_to_seconds(invalid_time)
            assert result is None, f"Should return None for {invalid_time}"


class TestSecondsElapsed:
    """Test seconds elapsed computation."""

    def test_regulation_periods_q1_start_end(self):
        """Test Q1 start and end scenarios."""
        # Q1 start (12:00 remaining)
        result = compute_seconds_elapsed("12:00", 1)
        assert result == 0.0  # Start of period
        
        # Q1 end (0:00 remaining)
        result = compute_seconds_elapsed("0:00", 1)
        assert result == 720.0  # Full 12 minutes elapsed

    def test_regulation_periods_mid_game(self):
        """Test mid-game regulation period scenarios."""
        # Q1 mid-period (8:30 remaining)
        result = compute_seconds_elapsed("8:30", 1)
        assert result == 210.0  # 3:30 elapsed (12:00 - 8:30)
        
        # Q2 various times
        result = compute_seconds_elapsed("6:15", 2)
        assert result == 345.0  # 5:45 elapsed
        
        # Q4 clutch time
        result = compute_seconds_elapsed("2:00", 4)
        assert result == 600.0  # 10:00 elapsed

    def test_overtime_periods_start_end(self):
        """Test OT start and end scenarios."""
        # OT start (5:00 remaining)
        result = compute_seconds_elapsed("5:00", 5)
        assert result == 0.0  # Start of OT
        
        # OT end (0:00 remaining)
        result = compute_seconds_elapsed("0:00", 5)
        assert result == 300.0  # Full 5 minutes elapsed

    def test_overtime_periods_mid_period(self):
        """Test mid-period overtime scenarios."""
        # First OT mid-period (2:30 remaining)
        result = compute_seconds_elapsed("2:30", 5)
        assert result == 150.0  # 2:30 elapsed (5:00 - 2:30)
        
        # Second OT (1:15 remaining)
        result = compute_seconds_elapsed("1:15", 6)
        assert result == 225.0  # 3:45 elapsed
        
        # Multiple OT periods
        for period in range(5, 10):  # OT 1-5
            result = compute_seconds_elapsed("3:00", period)
            assert result == 120.0  # 2:00 elapsed in any OT

    def test_fractional_seconds_in_elapsed(self):
        """Test fractional seconds handling in elapsed computation."""
        # Regulation with fractional
        result = compute_seconds_elapsed("8:30.5", 1)
        assert result == 209.5  # 3:29.5 elapsed
        
        # OT with fractional
        result = compute_seconds_elapsed("2:30.123", 5)
        assert result == 149.877  # 2:29.877 elapsed

    def test_negative_elapsed_auto_flip(self):
        """Test that negative elapsed times are auto-flipped."""
        # This could happen with data inconsistencies
        result = compute_seconds_elapsed("13:00", 1)  # More than period length
        assert result >= 0  # Should be positive (flipped from negative)
        assert result == 60.0  # abs(720 - 780) = 60

    def test_invalid_time_returns_none(self):
        """Test handling when time_remaining is invalid."""
        invalid_times = [None, "", "invalid", "25:99", "abc:def"]
        
        for invalid_time in invalid_times:
            result = compute_seconds_elapsed(invalid_time, 1)
            assert result is None

    def test_edge_case_periods(self):
        """Test edge case period numbers."""
        # Period 1-4 should all use 12 minutes (720 seconds)
        for period in range(1, 5):
            result = compute_seconds_elapsed("0:00", period)
            assert result == 720.0
        
        # Period 5+ should all use 5 minutes (300 seconds)
        for period in range(5, 15):  # Test up to 10 OT periods
            result = compute_seconds_elapsed("0:00", period)
            assert result == 300.0


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