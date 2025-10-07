"""Unit tests for PBP clock parsing and seconds_elapsed derivation."""

from nba_scraper.models.enums import EventType
from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.utils.clock import parse_clock_to_ms, period_length_ms


class TestPbpClockHandling:
    """Test PBP clock parsing and seconds_elapsed derivation."""

    def test_clock_parsing_basic_formats(self):
        """Test parsing of basic clock formats."""
        test_cases = [
            ("12:00", 720000),  # Start of period - milliseconds
            ("11:45", 705000),  # Mid period
            ("5:30", 330000),  # Later in period
            ("0:45", 45000),  # End of period
            ("0:00", 0),  # Period end
        ]

        for time_str, expected_ms in test_cases:
            result = parse_clock_to_ms(time_str, 1)  # Use centralized function
            assert result == expected_ms, f"Failed for {time_str}"

    def test_clock_parsing_fractional_seconds(self):
        """Test parsing clock formats with fractional seconds."""
        test_cases = [
            ("11:45.5", 705500),  # Half second
            ("11:45.500", 705500),  # 500 milliseconds
            ("5:30.123", 330123),  # 123 milliseconds
            ("0:00.001", 1),  # 1 millisecond
        ]

        for time_str, expected_ms in test_cases:
            result = parse_clock_to_ms(time_str, 1)  # Use centralized function
            assert result == expected_ms, f"Failed for {time_str}"

    def test_clock_parsing_iso_duration(self):
        """Test parsing ISO 8601 duration format."""
        test_cases = [
            ("PT11M45S", 705000),  # 11 minutes 45 seconds
            ("PT5M30S", 330000),  # 5 minutes 30 seconds
            ("PT0M45S", 45000),  # 0 minutes 45 seconds
            ("PT11M45.5S", 705500),  # With fractional seconds
        ]

        for time_str, expected_ms in test_cases:
            result = parse_clock_to_ms(time_str, 1)  # Use centralized function
            assert result == expected_ms, f"Failed for {time_str}"

    def test_seconds_elapsed_regulation_periods(self):
        """Test seconds_elapsed calculation for regulation periods."""
        # Q1 start (12:00 remaining)
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=1,
            event_idx=1,
            time_remaining="12:00",
            event_type=EventType.PERIOD_BEGIN,
            source="test",
            source_url="https://test.com",
        )

        assert pbp_event.seconds_elapsed == 0.0  # Start of period

        # Q1 mid-period (8:30 remaining)
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=1,
            event_idx=50,
            time_remaining="8:30",
            event_type=EventType.SHOT_MADE,
            source="test",
            source_url="https://test.com",
        )

        assert pbp_event.seconds_elapsed == 210.0  # 3:30 elapsed (12:00 - 8:30)

        # Q1 end (0:00 remaining)
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=1,
            event_idx=100,
            time_remaining="0:00",
            event_type=EventType.PERIOD_END,
            source="test",
            source_url="https://test.com",
        )

        assert pbp_event.seconds_elapsed == 720.0  # Full 12 minutes elapsed

    def test_seconds_elapsed_overtime_periods(self):
        """Test seconds_elapsed calculation for overtime periods."""
        # OT start (5:00 remaining)
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=5,  # First OT
            event_idx=1,
            time_remaining="5:00",
            event_type=EventType.PERIOD_BEGIN,
            source="test",
            source_url="https://test.com",
        )

        assert pbp_event.seconds_elapsed == 0.0  # Start of OT

        # OT mid-period (2:30 remaining)
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=5,
            event_idx=50,
            time_remaining="2:30",
            event_type=EventType.SHOT_MADE,
            source="test",
            source_url="https://test.com",
        )

        assert pbp_event.seconds_elapsed == 150.0  # 2:30 elapsed (5:00 - 2:30)

    def test_negative_elapsed_time_safety(self):
        """Test that invalid clock times don't crash the model."""
        # This could happen with data inconsistencies
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=1,
            event_idx=1,
            time_remaining="13:00",  # More than period length (invalid)
            event_type=EventType.SHOT_MADE,
            source="test",
            source_url="https://test.com",
        )

        # The clock parsing should fail gracefully and seconds_elapsed should be None
        # since 13:00 exceeds the 12-minute period bounds
        assert pbp_event.seconds_elapsed is None

    def test_missing_time_remaining_handling(self):
        """Test handling when time_remaining is missing."""
        pbp_event = PbpEventRow(
            game_id="0022400001",
            period=1,
            event_idx=1,
            time_remaining=None,  # Missing time
            event_type=EventType.SHOT_MADE,
            source="test",
            source_url="https://test.com",
        )

        # Should not crash, seconds_elapsed should remain None
        assert pbp_event.seconds_elapsed is None

    def test_invalid_time_format_handling(self):
        """Test handling of invalid time formats."""
        invalid_formats = [
            "invalid",
            "25:99",  # Invalid minutes/seconds
            "abc:def",
            "",
            "12",  # Missing colon
        ]

        for invalid_time in invalid_formats:
            pbp_event = PbpEventRow(
                game_id="0022400001",
                period=1,
                event_idx=1,
                time_remaining=invalid_time,
                event_type=EventType.SHOT_MADE,
                source="test",
                source_url="https://test.com",
            )

            # Should not crash, but seconds_elapsed should be None for invalid formats
            assert isinstance(pbp_event, PbpEventRow)
            assert pbp_event.seconds_elapsed is None

    def test_period_length_calculation(self):
        """Test period length calculation for different periods."""
        # Regulation periods (1-4)
        for period in range(1, 5):
            length_ms = period_length_ms(period)  # Use centralized function
            assert length_ms == 720000  # 12 minutes = 720000 milliseconds

        # Overtime periods (5+)
        for period in range(5, 10):
            length_ms = period_length_ms(period)  # Use centralized function
            assert length_ms == 300000  # 5 minutes = 300000 milliseconds
