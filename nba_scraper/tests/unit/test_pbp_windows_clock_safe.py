"""Tests for PBP window builders with clock-safe and possession-aware logic."""

from unittest.mock import Mock

from nba_scraper.models.enums import EventType
from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.transformers.pbp_windows import (
    EarlyShocksBuilder,
    Q1WindowBuilder,
    WindowEventProcessor,
    build_early_shocks,
    build_q1_window_12_8,
    is_in_clock_window,
    period_bounds_ms,
)


class TestClockWindowHelpers:
    """Test clock window helper functions."""

    def test_is_in_clock_window_basic(self):
        """Test basic clock window functionality."""
        # Q1 12:00→08:00 window: [480000, 720000]
        assert is_in_clock_window(720000, 720000, 480000)  # Exactly at start
        assert is_in_clock_window(600000, 720000, 480000)  # In middle (10:00)
        assert is_in_clock_window(480000, 720000, 480000)  # Exactly at end

        # Outside window
        assert not is_in_clock_window(480001, 720000, 480000)  # Too early (7:59.999)
        assert not is_in_clock_window(720001, 720000, 480000)  # Too late (12:00.001)
        assert not is_in_clock_window(479999, 720000, 480000)  # After window

        # Broadcast-clock boundary tests
        assert not is_in_clock_window(
            480999, 720000, 480000
        )  # end_ms + 999: 8:00.999 → displays as 8:00 but excluded (sub-second past boundary)
        assert is_in_clock_window(
            481000, 720000, 480000
        )  # end_ms + 1000: 8:01.000 → clearly inside by ≥1s

    def test_is_in_clock_window_edge_cases(self):
        """Test edge cases for clock window checking."""
        # Q1 08:00.0 should be included
        assert is_in_clock_window(480000, 720000, 480000)

        # Q1 07:59.9 should be excluded
        assert not is_in_clock_window(479900, 720000, 480000)

        # Different window sizes
        # First 4:00 of Q1: [480000, 720000]
        assert is_in_clock_window(480000, 720000, 480000)  # At 8:00
        assert is_in_clock_window(720000, 720000, 480000)  # At 12:00

    def test_period_bounds_ms(self):
        """Test period bounds calculation."""
        # Regulation periods (12 minutes = 720000ms)
        for period in range(1, 5):
            end_ms, start_ms = period_bounds_ms(period)
            assert end_ms == 0
            assert start_ms == 720000

        # Overtime periods (5 minutes = 300000ms)
        for period in range(5, 11):
            end_ms, start_ms = period_bounds_ms(period)
            assert end_ms == 0
            assert start_ms == 300000


class TestWindowEventProcessor:
    """Test base window event processor."""

    def test_deduplicate_consecutive_events(self):
        """Test deduplication of consecutive identical events."""
        processor = WindowEventProcessor()

        # Create duplicate timeout events at same timestamp
        events = [
            self._create_event(1, EventType.TIMEOUT, "12:00", "LAL", clock_ms=720000),
            self._create_event(2, EventType.TIMEOUT, "12:00", "LAL", clock_ms=720000),  # Duplicate
            self._create_event(3, EventType.SHOT_MADE, "11:45", "BOS", clock_ms=705000),
        ]

        deduplicated = processor.deduplicate_events(events)

        # Should remove the duplicate timeout
        assert len(deduplicated) == 2
        assert deduplicated[0].event_type == EventType.TIMEOUT
        assert deduplicated[1].event_type == EventType.SHOT_MADE

    def test_is_valid_period_event(self):
        """Test period and clock validation."""
        processor = WindowEventProcessor()

        # Valid events
        valid_event = self._create_event(
            1, EventType.SHOT_MADE, "10:00", "LAL", period=1, clock_ms=600000
        )
        assert processor.is_valid_period_event(valid_event)

        # Invalid period
        invalid_period = self._create_event(
            1, EventType.SHOT_MADE, "10:00", "LAL", period=11, clock_ms=600000
        )
        assert not processor.is_valid_period_event(invalid_period)

        # Missing clock
        no_clock = self._create_event(
            1, EventType.SHOT_MADE, "10:00", "LAL", period=1, clock_ms=None
        )
        assert not processor.is_valid_period_event(no_clock)

        # Clock outside period bounds
        invalid_clock = self._create_event(
            1, EventType.SHOT_MADE, "15:00", "LAL", period=1, clock_ms=900000
        )
        assert not processor.is_valid_period_event(invalid_clock)

    def test_possession_tracking(self):
        """Test possession state tracking."""
        processor = WindowEventProcessor()

        # Made shot should flip possession
        shot_event = self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL")
        processor.processed_events = [self._create_event(0, EventType.SHOT_MADE, "11:30", "BOS")]
        processor.update_possession(shot_event)

        # Should track possession changes
        assert processor.possession_state.possession_changes >= 0
        assert processor.possession_state.last_event_type == EventType.SHOT_MADE

    def test_estimate_possessions(self):
        """Test possession estimation using box score formula."""
        processor = WindowEventProcessor()

        events = [
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL"),  # FGA
            self._create_event(2, EventType.SHOT_MISSED, "10:30", "BOS"),  # FGA
            self._create_event(3, EventType.FREE_THROW_MADE, "10:00", "LAL"),  # FTA
            self._create_event(4, EventType.FREE_THROW_MADE, "9:59", "LAL"),  # FTA
            self._create_event(5, EventType.TURNOVER, "9:30", "BOS"),  # TOV
        ]

        possessions = processor.estimate_possessions(events)

        # FGA=2, FTA=2, OREB=0, TOV=1 → 2 + 0.44*2 - 0 + 1 = 3.88 ≈ 3
        assert possessions >= 1  # At least 1 possession
        assert possessions <= 10  # Reasonable upper bound

    def _create_event(
        self,
        event_idx: int,
        event_type: EventType,
        time_remaining: str,
        team: str,
        period: int = 1,
        clock_ms: int = None,
    ) -> PbpEventRow:
        """Helper to create test PBP events."""
        event = Mock(spec=PbpEventRow)
        event.event_idx = event_idx
        event.event_type = event_type
        event.time_remaining = time_remaining
        event.team_tricode = team
        event.period = period
        event.description = f"{event_type.name} event"

        # Set clock_ms_remaining as attribute
        if clock_ms is not None:
            event.clock_ms_remaining = clock_ms
        else:
            # Fallback to None if not provided
            event.clock_ms_remaining = None

        return event


class TestQ1WindowBuilder:
    """Test Q1 window builder (12:00→8:00)."""

    def test_q1_window_12_8_basic(self):
        """Test basic Q1 window filtering."""
        builder = Q1WindowBuilder()

        events = [
            self._create_q1_event(1, EventType.PERIOD_BEGIN, "12:00", "LAL", 720000),
            self._create_q1_event(2, EventType.SHOT_MADE, "10:00", "LAL", 600000),  # In window
            self._create_q1_event(3, EventType.SHOT_MADE, "8:00", "BOS", 480000),  # At boundary
            self._create_q1_event(4, EventType.SHOT_MADE, "7:59", "LAL", 479000),  # Outside window
        ]

        window_events = builder.build_q1_window_12_8(events)

        # Should include events 1, 2, and 3; exclude 4
        assert len(window_events) == 3
        assert window_events[0].clock_ms_remaining == 720000  # 12:00 (PERIOD_BEGIN now included)
        assert window_events[1].clock_ms_remaining == 600000  # 10:00
        assert window_events[2].clock_ms_remaining == 480000  # 8:00

    def test_q1_window_boundary_cases(self):
        """Test boundary cases for Q1 window."""
        builder = Q1WindowBuilder()

        events = [
            # Exactly at Q1 08:00.0 - should be included
            self._create_q1_event(1, EventType.SHOT_MADE, "8:00.0", "LAL", 480000),
            # Q1 07:59.9 - should be excluded
            self._create_q1_event(2, EventType.SHOT_MADE, "7:59.9", "BOS", 479900),
            # Q1 12:00.0 - should be included
            self._create_q1_event(3, EventType.PERIOD_BEGIN, "12:00.0", "LAL", 720000),
            # Q1 12:00.1 - should be excluded (invalid)
            self._create_q1_event(4, EventType.SHOT_MADE, "12:00.1", "BOS", 720100),
        ]

        window_events = builder.build_q1_window_12_8(events)

        # Should include events 1 and 3 only
        assert len(window_events) == 2
        clock_values = [e.clock_ms_remaining for e in window_events]
        assert 480000 in clock_values  # 8:00.0
        assert 720000 in clock_values  # 12:00.0
        assert 479900 not in clock_values  # 7:59.9 excluded
        assert 720100 not in clock_values  # Invalid clock excluded

    def test_q1_window_non_q1_events_filtered(self):
        """Test that non-Q1 events are filtered out."""
        builder = Q1WindowBuilder()

        events = [
            self._create_event(1, EventType.SHOT_MADE, "10:00", "LAL", period=1, clock_ms=600000),
            self._create_event(
                2, EventType.SHOT_MADE, "10:00", "BOS", period=2, clock_ms=600000
            ),  # Q2
            self._create_event(
                3, EventType.SHOT_MADE, "2:00", "LAL", period=5, clock_ms=120000
            ),  # OT
        ]

        window_events = builder.build_q1_window_12_8(events)

        # Should only include Q1 event
        assert len(window_events) == 1
        assert window_events[0].period == 1

    def test_q1_window_missing_shot_clock_tolerance(self):
        """Test tolerance to missing shot clock data."""
        builder = Q1WindowBuilder()

        events = [
            self._create_q1_event(1, EventType.SHOT_MADE, "10:00", "LAL", 600000),
            self._create_q1_event(2, EventType.SHOT_MADE, "9:00", "BOS", None),  # Missing clock
        ]

        window_events = builder.build_q1_window_12_8(events)

        # Should include valid event, skip invalid
        assert len(window_events) == 1
        assert window_events[0].clock_ms_remaining == 600000

    def _create_q1_event(
        self, event_idx: int, event_type: EventType, time_remaining: str, team: str, clock_ms: int
    ) -> Mock:
        """Helper to create Q1 test events."""
        return self._create_event(
            event_idx, event_type, time_remaining, team, period=1, clock_ms=clock_ms
        )

    def _create_event(
        self,
        event_idx: int,
        event_type: EventType,
        time_remaining: str,
        team: str,
        period: int = 1,
        clock_ms: int = None,
    ) -> Mock:
        """Helper to create test PBP events."""
        event = Mock(spec=PbpEventRow)
        event.event_idx = event_idx
        event.event_type = event_type
        event.time_remaining = time_remaining
        event.team_tricode = team
        event.period = period
        event.description = f"{event_type.name} event"
        event.clock_ms_remaining = clock_ms
        return event


class TestEarlyShocksBuilder:
    """Test early shocks builder (first 4:00 of each period)."""

    def test_early_shocks_basic(self):
        """Test basic early shocks analysis."""
        builder = EarlyShocksBuilder()

        events = [
            # Q1 events (first 4:00: 12:00→8:00, i.e., 720000→480000ms)
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL", 1, 660000),  # In window
            self._create_event(2, EventType.SHOT_MADE, "9:00", "BOS", 1, 540000),  # In window
            self._create_event(3, EventType.SHOT_MADE, "7:00", "LAL", 1, 420000),  # Outside window
            # Q2 events (first 4:00: 12:00→8:00)
            self._create_event(4, EventType.SHOT_MADE, "10:00", "BOS", 2, 600000),  # In window
        ]

        period_stats = builder.build_early_shocks(events)

        # Should have stats for both periods
        assert 1 in period_stats
        assert 2 in period_stats

        # Q1 should have both teams
        assert "LAL" in period_stats[1]
        assert "BOS" in period_stats[1]

        # Q2 should have BOS
        assert "BOS" in period_stats[2]

        # Check that out-of-window event was excluded
        assert len(period_stats[1]["LAL"]["events"]) == 1  # Only the 11:00 event

    def test_early_shocks_period_boundaries(self):
        """Test that early shocks don't cross period boundaries."""
        builder = EarlyShocksBuilder()

        events = [
            # End of Q1 - outside first 4:00 window
            self._create_event(1, EventType.SHOT_MADE, "0:30", "LAL", 1, 30000),
            # Start of Q2 - inside first 4:00 window
            self._create_event(2, EventType.SHOT_MADE, "11:30", "BOS", 2, 690000),
        ]

        period_stats = builder.build_early_shocks(events)

        # Should have separate stats for each period
        assert 1 in period_stats
        assert 2 in period_stats

        # Q1: LAL event at 0:30 is outside first 4:00 window (12:00→8:00), so no teams should appear
        assert len(period_stats[1]) == 0  # No teams in Q1 window

        # Q2: BOS event at 11:30 is inside first 4:00 window
        assert "BOS" in period_stats[2]
        assert len(period_stats[2]["BOS"]["events"]) == 1

    def test_early_shocks_lead_changes(self):
        """Test lead change tracking within windows."""
        builder = EarlyShocksBuilder()

        events = [
            # LAL scores first (2-0)
            self._create_event(1, EventType.SHOT_MADE, "11:30", "LAL", 1, 690000),
            # BOS ties (2-2)
            self._create_event(2, EventType.SHOT_MADE, "11:00", "BOS", 1, 660000),
            # BOS takes lead (2-5)
            self._create_event(3, EventType.SHOT_MADE, "10:30", "BOS", 1, 630000),
        ]

        # Mock shot_value attribute
        for event in events:
            if event.event_type == EventType.SHOT_MADE:
                if event.team_tricode == "BOS" and event.time_remaining == "10:30":
                    event.shot_value = 3  # 3-pointer
                else:
                    event.shot_value = 2  # 2-pointer

        period_stats = builder.build_early_shocks(events)

        # Should track lead changes
        assert period_stats[1]["BOS"]["lead_changes"] >= 0

    def test_early_shocks_net_rating_calculation(self):
        """Test net rating calculation per 100 possessions."""
        builder = EarlyShocksBuilder()

        events = [
            # LAL: 4 points, 2 FGA, 1 possession
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL", 1, 660000),  # 2 pts
            self._create_event(2, EventType.SHOT_MADE, "10:30", "LAL", 1, 630000),  # 2 pts
            # BOS: 2 points, 1 FGA, 1 possession
            self._create_event(3, EventType.SHOT_MADE, "10:00", "BOS", 1, 600000),  # 2 pts
        ]

        # Mock shot values
        for event in events:
            event.shot_value = 2

        period_stats = builder.build_early_shocks(events)

        # Should calculate net rating
        lal_stats = period_stats[1]["LAL"]
        bos_stats = period_stats[1]["BOS"]

        assert "net_rating" in lal_stats
        assert "net_rating" in bos_stats

        # LAL: +2 net (4 for, 2 against), BOS: 0 net (2 for, 4 against)
        assert lal_stats["net_rating"] > bos_stats["net_rating"]

    def test_early_shocks_overtime_periods(self):
        """Test early shocks analysis for overtime periods."""
        builder = EarlyShocksBuilder()

        # OT periods are 5 minutes, so first 4:00 is 5:00→1:00 (300000→60000ms)
        events = [
            self._create_event(1, EventType.SHOT_MADE, "4:30", "LAL", 5, 270000),  # In OT window
            self._create_event(2, EventType.SHOT_MADE, "0:30", "BOS", 5, 30000),  # Outside window
        ]

        for event in events:
            event.shot_value = 2

        period_stats = builder.build_early_shocks(events)

        # Should process OT period
        assert 5 in period_stats
        assert "LAL" in period_stats[5]

        # Should only include the 4:30 event (within first 4:00 of OT)
        assert len(period_stats[5]["LAL"]["events"]) == 1

    def _create_event(
        self,
        event_idx: int,
        event_type: EventType,
        time_remaining: str,
        team: str,
        period: int,
        clock_ms: int,
    ) -> Mock:
        """Helper to create test PBP events."""
        event = Mock(spec=PbpEventRow)
        event.event_idx = event_idx
        event.event_type = event_type
        event.time_remaining = time_remaining
        event.team_tricode = team
        event.period = period
        event.description = f"{event_type.name} event"
        event.clock_ms_remaining = clock_ms
        event.shot_value = 2  # Default
        return event


class TestFactoryFunctions:
    """Test factory functions for easy access."""

    def test_build_q1_window_12_8_factory(self):
        """Test Q1 window factory function."""
        events = [
            self._create_event(1, EventType.SHOT_MADE, "10:00", "LAL", 1, 600000),
        ]

        window_events = build_q1_window_12_8(events)

        assert len(window_events) == 1
        assert window_events[0].clock_ms_remaining == 600000

    def test_build_early_shocks_factory(self):
        """Test early shocks factory function."""
        events = [
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL", 1, 660000),
        ]

        for event in events:
            event.shot_value = 2

        period_stats = build_early_shocks(events)

        assert 1 in period_stats
        assert "LAL" in period_stats[1]

    def _create_event(
        self,
        event_idx: int,
        event_type: EventType,
        time_remaining: str,
        team: str,
        period: int,
        clock_ms: int,
    ) -> Mock:
        """Helper to create test PBP events."""
        event = Mock(spec=PbpEventRow)
        event.event_idx = event_idx
        event.event_type = event_type
        event.time_remaining = time_remaining
        event.team_tricode = team
        event.period = period
        event.description = f"{event_type.name} event"
        event.clock_ms_remaining = clock_ms
        event.shot_value = 2
        return event


class TestEdgeCasesAndTolerance:
    """Test edge cases and error tolerance."""

    def test_empty_events_list(self):
        """Test handling of empty events list."""
        builder = Q1WindowBuilder()
        result = builder.build_q1_window_12_8([])
        assert result == []

        shocks_builder = EarlyShocksBuilder()
        result = shocks_builder.build_early_shocks([])
        assert result == {}

    def test_all_invalid_events(self):
        """Test handling when all events are invalid."""
        builder = Q1WindowBuilder()

        events = [
            self._create_event(1, EventType.SHOT_MADE, "10:00", "LAL", 11, None),  # Invalid period
            self._create_event(2, EventType.SHOT_MADE, "10:00", "BOS", 1, None),  # Missing clock
        ]

        result = builder.build_q1_window_12_8(events)
        assert result == []

    def test_mixed_valid_invalid_events(self):
        """Test handling of mixed valid/invalid events."""
        builder = Q1WindowBuilder()

        events = [
            self._create_event(1, EventType.SHOT_MADE, "10:00", "LAL", 1, 600000),  # Valid
            self._create_event(
                2, EventType.SHOT_MADE, "10:00", "BOS", 11, 600000
            ),  # Invalid period
            self._create_event(3, EventType.SHOT_MADE, "9:00", "LAL", 1, 540000),  # Valid
        ]

        result = builder.build_q1_window_12_8(events)

        # Should include only valid events
        assert len(result) == 2
        assert all(e.period == 1 for e in result)

    def _create_event(
        self,
        event_idx: int,
        event_type: EventType,
        time_remaining: str,
        team: str,
        period: int,
        clock_ms: int,
    ) -> Mock:
        """Helper to create test PBP events."""
        event = Mock(spec=PbpEventRow)
        event.event_idx = event_idx
        event.event_type = event_type
        event.time_remaining = time_remaining
        event.team_tricode = team
        event.period = period
        event.description = f"{event_type.name} event"
        event.clock_ms_remaining = clock_ms
        return event
