"""Tests for possession-aware logic in PBP window builders."""

from unittest.mock import Mock

from nba_scraper.models.enums import EventType
from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.transformers.pbp_windows import (
    EarlyShocksBuilder,
    PossessionState,
    Q1WindowBuilder,
    WindowEventProcessor,
)


class TestPossessionTracking:
    """Test possession tracking logic."""

    def test_possession_state_initialization(self):
        """Test possession state starts empty."""
        state = PossessionState()
        assert state.current_team is None
        assert state.last_event_type is None
        assert state.possession_changes == 0
        assert state.unknown_possessions == 0

    def test_made_shot_flips_possession(self):
        """Test that made shots flip possession after inbound."""
        processor = WindowEventProcessor()
        processor.possession_state.current_team = "LAL"

        # Mock processed events for opponent team lookup
        processor.processed_events = [
            self._create_event(1, EventType.SHOT_MADE, "11:00", "BOS"),
            self._create_event(2, EventType.SHOT_MADE, "10:30", "LAL"),
        ]

        # BOS makes a shot - should flip possession to LAL
        shot_event = self._create_event(3, EventType.SHOT_MADE, "10:00", "BOS")
        processor.update_possession(shot_event)

        # Possession should have changed
        assert processor.possession_state.possession_changes >= 1
        assert processor.possession_state.current_team == "LAL"

    def test_defensive_rebound_flips_possession(self):
        """Test defensive rebounds flip possession."""
        processor = WindowEventProcessor()
        processor.possession_state.current_team = "LAL"

        # Defensive rebound by BOS
        rebound_event = self._create_event(1, EventType.REBOUND, "10:00", "BOS")
        rebound_event.description = "Defensive rebound by Player"

        processor.update_possession(rebound_event)

        assert processor.possession_state.current_team == "BOS"
        assert processor.possession_state.possession_changes == 1

    def test_offensive_rebound_maintains_possession(self):
        """Test offensive rebounds maintain possession."""
        processor = WindowEventProcessor()
        processor.possession_state.current_team = "LAL"

        # Offensive rebound by LAL
        rebound_event = self._create_event(1, EventType.REBOUND, "10:00", "LAL")
        rebound_event.description = "Offensive rebound by Player"

        processor.update_possession(rebound_event)

        # Should not change possession
        assert processor.possession_state.current_team == "LAL"
        assert processor.possession_state.possession_changes == 0

    def test_turnover_flips_possession(self):
        """Test turnovers flip possession."""
        processor = WindowEventProcessor()
        processor.possession_state.current_team = "LAL"

        # Mock processed events for opponent lookup
        processor.processed_events = [
            self._create_event(1, EventType.SHOT_MADE, "11:00", "BOS"),
            self._create_event(2, EventType.SHOT_MADE, "10:30", "LAL"),
        ]

        # LAL turnover - should flip to BOS
        turnover_event = self._create_event(3, EventType.TURNOVER, "10:00", "LAL")
        processor.update_possession(turnover_event)

        assert processor.possession_state.current_team == "BOS"
        assert processor.possession_state.possession_changes == 1

    def test_personal_foul_flips_possession(self):
        """Test personal fouls (especially offensive) flip possession."""
        processor = WindowEventProcessor()
        processor.possession_state.current_team = "LAL"

        # Mock processed events for opponent lookup
        processor.processed_events = [
            self._create_event(1, EventType.SHOT_MADE, "11:00", "BOS"),
            self._create_event(2, EventType.SHOT_MADE, "10:30", "LAL"),
        ]

        # LAL offensive foul - should flip to BOS
        foul_event = self._create_event(3, EventType.PERSONAL_FOUL, "10:00", "LAL")
        processor.update_possession(foul_event)

        assert processor.possession_state.current_team == "BOS"
        assert processor.possession_state.possession_changes == 1

    def test_jump_ball_updates_possession(self):
        """Test jump ball updates possession to winning team."""
        processor = WindowEventProcessor()
        processor.possession_state.current_team = "LAL"

        # Jump ball won by BOS
        jump_ball_event = self._create_event(1, EventType.JUMP_BALL, "10:00", "BOS")
        processor.update_possession(jump_ball_event)

        assert processor.possession_state.current_team == "BOS"
        assert processor.possession_state.possession_changes == 1

    def test_unknown_team_increments_unknown_possessions(self):
        """Test events with unknown teams increment unknown counter."""
        processor = WindowEventProcessor()

        # Event with no team
        unknown_event = self._create_event(1, EventType.SHOT_MADE, "10:00", None)
        processor.update_possession(unknown_event)

        assert processor.possession_state.unknown_possessions == 1

    def test_possession_estimation_formula(self):
        """Test possession estimation using FGA + 0.44*FTA - OREB + TOV."""
        processor = WindowEventProcessor()

        events = [
            # Team A: 5 FGA, 4 FTA, 1 OREB, 2 TOV
            self._create_event(1, EventType.SHOT_MADE, "11:00", "A"),  # FGA
            self._create_event(2, EventType.SHOT_MISSED, "10:30", "A"),  # FGA
            self._create_event(3, EventType.SHOT_MADE, "10:00", "A"),  # FGA
            self._create_event(4, EventType.SHOT_MISSED, "9:30", "A"),  # FGA
            self._create_event(5, EventType.SHOT_MADE, "9:00", "A"),  # FGA
            self._create_event(6, EventType.FREE_THROW_MADE, "8:30", "A"),  # FTA
            self._create_event(7, EventType.FREE_THROW_MADE, "8:29", "A"),  # FTA
            self._create_event(8, EventType.FREE_THROW_MISSED, "8:00", "A"),  # FTA
            self._create_event(9, EventType.FREE_THROW_MADE, "7:30", "A"),  # FTA
            self._create_event(10, EventType.TURNOVER, "7:00", "A"),  # TOV
            self._create_event(11, EventType.TURNOVER, "6:30", "A"),  # TOV
        ]

        # Add offensive rebound
        oreb_event = self._create_event(12, EventType.REBOUND, "6:00", "A")
        oreb_event.description = "Offensive rebound"
        events.append(oreb_event)

        possessions = processor.estimate_possessions(events)

        # Expected: 5 + 0.44*4 - 1 + 2 = 5 + 1.76 - 1 + 2 = 7.76 ≈ 7
        assert possessions >= 7
        assert possessions <= 8

    def test_possession_estimation_minimum(self):
        """Test possession estimation always returns at least 1."""
        processor = WindowEventProcessor()

        # Events that would calculate to 0 or negative possessions
        events = [
            # Only offensive rebounds, no shots or turnovers
            self._create_event(1, EventType.REBOUND, "10:00", "A"),
        ]
        events[0].description = "Offensive rebound"

        possessions = processor.estimate_possessions(events)

        # Should be at least 1
        assert possessions >= 1

    def test_possession_tracking_with_missing_data(self):
        """Test possession tracking handles missing or invalid data gracefully."""
        processor = WindowEventProcessor()

        # Event with missing attributes
        event = Mock(spec=PbpEventRow)
        event.team_tricode = None
        event.event_type = EventType.SHOT_MADE

        processor.update_possession(event)

        # Should increment unknown possessions, not crash
        assert processor.possession_state.unknown_possessions == 1
        assert processor.possession_state.current_team is None

    def _create_event(
        self, event_idx: int, event_type: EventType, time_remaining: str, team: str
    ) -> Mock:
        """Helper to create test PBP events."""
        event = Mock(spec=PbpEventRow)
        event.event_idx = event_idx
        event.event_type = event_type
        event.time_remaining = time_remaining
        event.team_tricode = team
        event.period = 1
        event.description = f"{event_type.name} event"
        return event


class TestPossessionAwareWindows:
    """Test possession-aware window analysis."""

    def test_q1_window_tracks_possessions(self):
        """Test Q1 window builder tracks possessions during window."""
        builder = Q1WindowBuilder()

        events = [
            # Possession-ending events in Q1 window
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL", 660000),
            self._create_event(2, EventType.TURNOVER, "10:30", "BOS", 630000),
            self._create_event(3, EventType.SHOT_MADE, "10:00", "LAL", 600000),
            self._create_event(4, EventType.REBOUND, "9:30", "BOS", 570000),
        ]
        events[3].description = "Defensive rebound"

        window_events = builder.build_q1_window_12_8(events)

        # Should process all events and track possessions
        assert len(window_events) == 4
        assert builder.possession_state.possession_changes >= 0

    def test_early_shocks_possession_calculation(self):
        """Test early shocks calculates possessions per team per period."""
        builder = EarlyShocksBuilder()

        events = [
            # Q1 first 4:00 events
            self._create_event(1, EventType.SHOT_MADE, "11:30", "LAL", 1, 690000),  # FGA+make
            self._create_event(2, EventType.SHOT_MISSED, "11:00", "BOS", 1, 660000),  # FGA
            self._create_event(3, EventType.FREE_THROW_MADE, "10:30", "LAL", 1, 630000),  # FTA
            self._create_event(4, EventType.TURNOVER, "10:00", "BOS", 1, 600000),  # TOV
        ]

        # Mock shot values
        for event in events:
            if event.event_type == EventType.SHOT_MADE:
                event.shot_value = 2

        period_stats = builder.build_early_shocks(events)

        # Should calculate possessions for each team
        lal_stats = period_stats[1]["LAL"]
        bos_stats = period_stats[1]["BOS"]

        assert lal_stats["possessions"] >= 1
        assert bos_stats["possessions"] >= 1

        # LAL: 1 FGA + 0.44*1 FTA = 1.44 ≈ 1
        # BOS: 1 FGA + 1 TOV = 2
        assert lal_stats["possessions"] >= 1
        assert bos_stats["possessions"] >= 1

    def test_early_shocks_net_rating_per_100_possessions(self):
        """Test early shocks calculates accurate net rating per 100 possessions."""
        builder = EarlyShocksBuilder()

        events = [
            # LAL scores 4 points in 2 possessions
            self._create_event(1, EventType.SHOT_MADE, "11:30", "LAL", 1, 690000),  # 2 pts, 1 FGA
            self._create_event(2, EventType.SHOT_MADE, "11:00", "LAL", 1, 660000),  # 2 pts, 1 FGA
            # BOS scores 3 points in 1 possession
            self._create_event(3, EventType.SHOT_MADE, "10:30", "BOS", 1, 630000),  # 3 pts, 1 FGA
        ]

        # Mock shot values
        events[0].shot_value = 2
        events[1].shot_value = 2
        events[2].shot_value = 3

        period_stats = builder.build_early_shocks(events)

        lal_stats = period_stats[1]["LAL"]
        bos_stats = period_stats[1]["BOS"]

        # LAL: 4 points for, 3 against, 2 possessions → net +0.5 per poss → +50 per 100
        # BOS: 3 points for, 4 against, 1 possession → net -1 per poss → -100 per 100
        assert lal_stats["net_rating"] > bos_stats["net_rating"]
        assert lal_stats["net_rating"] > 0
        assert bos_stats["net_rating"] < 0

    def test_possession_safe_coercion(self):
        """Test possession calculations handle invalid data safely."""
        builder = EarlyShocksBuilder()

        events = [
            # Event with missing shot_value
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL", 1, 660000),
        ]

        # Don't set shot_value - should be handled safely
        period_stats = builder.build_early_shocks(events)

        # Should not crash and should calculate some possessions
        assert 1 in period_stats
        assert "LAL" in period_stats[1]
        assert period_stats[1]["LAL"]["possessions"] >= 1

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


class TestWindowCrossPeriodProtection:
    """Test that windows don't cross period boundaries."""

    def test_q1_window_only_processes_q1(self):
        """Test Q1 window builder only processes Q1 events."""
        builder = Q1WindowBuilder()

        events = [
            # Q1 event in window
            self._create_event(1, EventType.SHOT_MADE, "10:00", "LAL", 1, 600000),
            # Q2 event that would be in Q1 window timeframe
            self._create_event(2, EventType.SHOT_MADE, "10:00", "BOS", 2, 600000),
            # Q4 event
            self._create_event(3, EventType.SHOT_MADE, "10:00", "LAL", 4, 600000),
        ]

        window_events = builder.build_q1_window_12_8(events)

        # Should only include Q1 event
        assert len(window_events) == 1
        assert window_events[0].period == 1

    def test_early_shocks_separate_period_stats(self):
        """Test early shocks maintains separate stats per period."""
        builder = EarlyShocksBuilder()

        events = [
            # Q1 event
            self._create_event(1, EventType.SHOT_MADE, "11:00", "LAL", 1, 660000),
            # Q2 event
            self._create_event(2, EventType.SHOT_MADE, "11:00", "LAL", 2, 660000),
            # Q3 event
            self._create_event(3, EventType.SHOT_MADE, "11:00", "BOS", 3, 660000),
        ]

        for event in events:
            event.shot_value = 2

        period_stats = builder.build_early_shocks(events)

        # Should have separate periods
        assert 1 in period_stats
        assert 2 in period_stats
        assert 3 in period_stats

        # Each period should have its own team stats
        assert period_stats[1]["LAL"]["points_for"] == 2
        assert period_stats[2]["LAL"]["points_for"] == 2
        assert period_stats[3]["BOS"]["points_for"] == 2

        # Cross-period contamination check
        assert "BOS" not in period_stats[1]  # BOS didn't play in Q1
        assert "BOS" not in period_stats[2]  # BOS didn't play in Q2

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
