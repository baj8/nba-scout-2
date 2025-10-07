"""Tests for clock parsing utility."""

import pytest
from nba_scraper.utils.clock import parse_clock_to_ms, period_length_ms


class TestClockParsing:
    """Test the centralized clock parsing utility."""

    def test_happy_paths_standard_format(self):
        """Test happy path cases with standard MM:SS format."""
        # Basic formats
        assert parse_clock_to_ms("12:00", 1) == 720000  # 12 * 60 * 1000
        assert parse_clock_to_ms("12:00.0", 1) == 720000
        assert parse_clock_to_ms("0:00.5", 1) == 500
        assert parse_clock_to_ms("1:23.456", 1) == 83456  # (1*60 + 23)*1000 + 456

        # Edge cases
        assert parse_clock_to_ms("0:00", 1) == 0
        assert parse_clock_to_ms("0:01", 1) == 1000
        assert parse_clock_to_ms("4:59.999", 5) == 299999  # OT period - within 5 minute limit

    def test_happy_paths_iso_format(self):
        """Test happy path cases with ISO PT format."""
        assert parse_clock_to_ms("PT11M22.3S", 1) == 682300  # (11*60 + 22.3)*1000
        assert parse_clock_to_ms("PT12M0S", 1) == 720000
        assert parse_clock_to_ms("PT0M30S", 1) == 30000
        assert parse_clock_to_ms("PT5M", 5) == 300000  # 5 minutes in OT

    def test_boundaries_regulation(self):
        """Test boundary conditions for regulation periods."""
        # Q1-Q4: 12 minutes = 720000ms
        for period in range(1, 5):
            assert parse_clock_to_ms("12:00.000", period) == 720000
            assert parse_clock_to_ms("00:00.0", period) == 0

    def test_boundaries_overtime(self):
        """Test boundary conditions for overtime periods."""
        # OT: 5 minutes = 300000ms
        for period in range(5, 8):
            assert parse_clock_to_ms("5:00.000", period) == 300000
            assert parse_clock_to_ms("00:00.0", period) == 0

    def test_invalid_formats_raise_errors(self):
        """Test that invalid formats raise ValueError."""
        invalid_formats = [
            "",  # empty
            "invalid",  # no colon
            "abc:def",  # non-numeric
            "12",  # missing seconds
            ":30",  # missing minutes
        ]

        for invalid in invalid_formats:
            with pytest.raises(ValueError, match="empty clock|unsupported clock format"):
                parse_clock_to_ms(invalid, 1)

        # Test invalid seconds separately since they have different validation
        with pytest.raises(ValueError, match="unsupported clock format"):
            parse_clock_to_ms("25:99", 1)  # invalid seconds

        with pytest.raises(ValueError, match="unsupported clock format"):
            parse_clock_to_ms("12:60", 1)  # seconds >= 60

    def test_exceeds_period_bounds(self):
        """Test that clocks exceeding period bounds raise errors."""
        # Regulation period bounds
        with pytest.raises(ValueError, match="exceeds period bounds"):
            parse_clock_to_ms("13:00", 1)  # > 12 minutes

        # Overtime period bounds
        with pytest.raises(ValueError, match="exceeds period bounds"):
            parse_clock_to_ms("6:00", 5)  # > 5 minutes for OT

    def test_negative_values_raise_errors(self):
        """Test that negative values raise errors."""
        # This would be caught by the regex, but test explicit negative
        with pytest.raises(ValueError, match="unsupported clock format"):
            parse_clock_to_ms("-1:00", 1)

    def test_period_length_calculation(self):
        """Test period length calculations."""
        # Regulation periods (1-4): 12 minutes
        for period in range(1, 5):
            assert period_length_ms(period) == 720000

        # Overtime periods (5+): 5 minutes
        for period in range(5, 10):
            assert period_length_ms(period) == 300000
