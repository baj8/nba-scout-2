"""PBP window builders with clock-safe and possession-aware logic."""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from ..models.enums import EventType
from ..models.pbp_rows import PbpEventRow
from ..nba_logging import get_logger
from ..utils.clock import period_length_ms
from ..utils.coerce import to_int_or_none

logger = get_logger(__name__)


def is_in_clock_window(clock_ms_remaining: int, start_ms: int, end_ms: int) -> bool:
    """
    Return True if `clock_ms_remaining` is within the descending window defined by
    [start_ms .. end_ms], where `start_ms` > `end_ms`.

    Semantics follow the broadcast clock (seconds resolution):
    - Start is inclusive (<= start_ms)
    - End is inclusive ONLY at the exact second tick (== end_ms)
      Any sub-second beyond the end (e.g., end_ms+1) is OUT.

    This intentionally applies broadcast-visible semantics at the lower edge:
    the clock displays whole seconds, so 8:00.000 is visible as "8:00" (included),
    but 7:59.999 displays as "7:59" (excluded from an 8:00 boundary).

    Args:
        clock_ms_remaining: Clock time in milliseconds remaining
        start_ms: Window start time (higher value, earlier in game)
        end_ms: Window end time (lower value, later in game)

    Returns:
        True if clock_ms_remaining is within the window

    Examples:
        # Q1 12:00→08:00 window: start_ms=720000, end_ms=480000
        is_in_clock_window(720000, 720000, 480000) → True  (12:00 included)
        is_in_clock_window(600000, 720000, 480000) → True  (10:00 included)
        is_in_clock_window(480000, 720000, 480000) → True  (8:00.000 included)
        is_in_clock_window(480001, 720000, 480000) → False (7:59.999 excluded)
        is_in_clock_window(479000, 720000, 480000) → False (7:59 excluded)
    """
    hi = max(start_ms, end_ms)
    lo = min(start_ms, end_ms)

    # Always must be <= start (inclusive)
    if clock_ms_remaining > hi:
        return False

    if clock_ms_remaining == lo:
        # exactly at the lower bound second (e.g., 8:00.000)
        return True

    # For anything above the lower bound, require at least one full second clearance.
    # This excludes sub-second values just "past" 8:00 (e.g., 7:59.999),
    # which appear as < 8:00 on the broadcast clock and should be OUT.
    return clock_ms_remaining >= lo + 1000


def period_bounds_ms(period: int) -> Tuple[int, int]:
    """Get period bounds in milliseconds.

    Args:
        period: Period number (1-4 for regulation, 5+ for overtime)

    Returns:
        Tuple of (end_ms, start_ms) for that period
    """
    start = period_length_ms(period)
    return (0, start)


@dataclass
class PossessionState:
    """Track possession state for window analysis."""

    current_team: Optional[str] = None
    last_event_type: Optional[EventType] = None
    possession_changes: int = 0
    unknown_possessions: int = 0


class WindowEventProcessor:
    """Base class for processing events within time windows."""

    def __init__(self):
        self.possession_state = PossessionState()
        self.processed_events: List[PbpEventRow] = []

    def deduplicate_events(self, events: Iterable[PbpEventRow]) -> List[PbpEventRow]:
        """Remove consecutive duplicate events.

        Deduplicates events with identical (period, clock_ms_remaining, event_type, team_id)
        by keeping the first occurrence.

        Args:
            events: Input events to deduplicate

        Returns:
            List of deduplicated events
        """
        seen_keys: Set[Tuple] = set()
        deduplicated = []

        for event in events:
            # Create deduplication key
            key = (
                event.period,
                getattr(event, "clock_ms_remaining", None),
                event.event_type,
                getattr(event, "team_id", event.team_tricode),
            )

            if key not in seen_keys:
                seen_keys.add(key)
                deduplicated.append(event)
            else:
                logger.debug(f"Deduplicated event: {event.event_type} at {event.time_remaining}")

        return deduplicated

    def is_valid_period_event(self, event: PbpEventRow) -> bool:
        """Check if event has valid period and clock bounds.

        Args:
            event: Event to validate

        Returns:
            True if event is valid for processing
        """
        # Check period bounds
        if event.period < 1 or event.period > 10:
            return False

        # Check if we have clock information
        clock_ms = getattr(event, "clock_ms_remaining", None)
        if clock_ms is None:
            return False

        # Validate clock is within period bounds
        period_end_ms, period_start_ms = period_bounds_ms(event.period)
        return period_end_ms <= clock_ms <= period_start_ms

    def update_possession(self, event: PbpEventRow) -> None:
        """Update possession tracking based on event.

        Args:
            event: Event to process for possession logic
        """
        team = event.team_tricode
        if not team:
            self.possession_state.unknown_possessions += 1
            return

        # Track possession changes based on event type
        if event.event_type in [EventType.SHOT_MADE, EventType.FREE_THROW_MADE]:
            # Made shots generally flip possession after the inbound
            if self.possession_state.current_team and self.possession_state.current_team != team:
                self.possession_state.possession_changes += 1
            self.possession_state.current_team = self._get_opponent_team(team)

        elif event.event_type == EventType.REBOUND:
            # Defensive rebounds flip possession
            if "defensive" in (event.description or "").lower():
                if self.possession_state.current_team != team:
                    self.possession_state.possession_changes += 1
                self.possession_state.current_team = team
            # Offensive rebounds maintain possession

        elif event.event_type in [EventType.TURNOVER, EventType.PERSONAL_FOUL]:
            # Turnovers and offensive fouls flip possession
            opponent = self._get_opponent_team(team)
            if opponent and self.possession_state.current_team != opponent:
                self.possession_state.possession_changes += 1
            self.possession_state.current_team = opponent

        elif event.event_type == EventType.JUMP_BALL:
            # Jump ball - try to determine who gains control
            # This is a simplified heuristic
            if self.possession_state.current_team != team:
                self.possession_state.possession_changes += 1
            self.possession_state.current_team = team

        self.possession_state.last_event_type = event.event_type

    def _get_opponent_team(self, team: str) -> Optional[str]:
        """Get opponent team code (simplified heuristic)."""
        # This would ideally use game metadata, but for now use a simple approach
        # In practice, this should be enhanced with actual team mappings
        all_teams = set(e.team_tricode for e in self.processed_events if e.team_tricode)
        all_teams.add(team)
        opponents = all_teams - {team}
        return opponents.pop() if opponents else None

    def estimate_possessions(self, events: List[PbpEventRow]) -> int:
        """Estimate possessions using box score formula.

        Possessions ≈ FGA + 0.44*FTA - OREB + TOV

        Args:
            events: Events to analyze

        Returns:
            Estimated possession count
        """
        fga = sum(1 for e in events if e.event_type in [EventType.SHOT_MADE, EventType.SHOT_MISSED])
        fta = sum(
            1
            for e in events
            if e.event_type in [EventType.FREE_THROW_MADE, EventType.FREE_THROW_MISSED]
        )
        oreb = sum(
            1
            for e in events
            if e.event_type == EventType.REBOUND and "offensive" in (e.description or "").lower()
        )
        tov = sum(1 for e in events if e.event_type == EventType.TURNOVER)

        # Safe coercion for the calculation
        fga_safe = to_int_or_none(fga) or 0
        fta_safe = to_int_or_none(fta) or 0
        oreb_safe = to_int_or_none(oreb) or 0
        tov_safe = to_int_or_none(tov) or 0

        possessions = fga_safe + int(0.44 * fta_safe) - oreb_safe + tov_safe
        return max(1, possessions)  # At least 1 possession


class Q1WindowBuilder(WindowEventProcessor):
    """Builder for Q1 12:00→8:00 window analysis."""

    def __init__(self):
        super().__init__()
        # Q1 window: 12:00 (720000ms) to 8:00 (480000ms)
        self.window_start_ms = 720000  # 12:00 remaining
        self.window_end_ms = 480000  # 8:00 remaining

    def build_q1_window_12_8(self, events: Iterable[PbpEventRow]) -> List[PbpEventRow]:
        """Build Q1 window events for 12:00→8:00 timeframe.

        Args:
            events: All PBP events

        Returns:
            Filtered and processed events within the Q1 window
        """
        # Filter to Q1 events only
        q1_events = [e for e in events if e.period == 1]

        # Deduplicate events
        q1_events = self.deduplicate_events(q1_events)

        # Filter to valid events within window
        window_events = []
        for event in q1_events:
            if not self.is_valid_period_event(event):
                continue

            clock_ms = getattr(event, "clock_ms_remaining", None)
            if clock_ms is None:
                continue

            # Check if event is in the 12:00→8:00 window
            if is_in_clock_window(clock_ms, self.window_start_ms, self.window_end_ms):
                window_events.append(event)
                self.update_possession(event)

        self.processed_events = window_events
        return window_events


class EarlyShocksBuilder(WindowEventProcessor):
    """Builder for early shocks analysis (first 4:00 of each period)."""

    def build_early_shocks(self, events: Iterable[PbpEventRow]) -> Dict[int, Dict[str, any]]:
        """Build early shocks analysis for first 4:00 of each period.

        Args:
            events: All PBP events

        Returns:
            Dictionary with period -> team stats for early period windows
        """
        period_stats = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "points_for": 0,
                    "points_against": 0,
                    "lead_changes": 0,
                    "possessions": 0,
                    "fga": 0,
                    "fta": 0,
                    "oreb": 0,
                    "tov": 0,
                    "events": [],
                }
            )
        )

        # Process events by period
        events_by_period = defaultdict(list)
        for event in events:
            if 1 <= event.period <= 10:  # Valid periods only
                events_by_period[event.period].append(event)

        # Process each period separately
        for period, period_events in events_by_period.items():
            # Get period length and calculate window end (first 4:00)
            period_start_ms = period_length_ms(period)
            window_end_ms = period_start_ms - (4 * 60 * 1000)  # 4 minutes = 240000ms

            # Filter to window events
            window_events = []
            for event in period_events:
                if not self.is_valid_period_event(event):
                    continue

                clock_ms = getattr(event, "clock_ms_remaining", None)
                if clock_ms is None:
                    continue

                # Check if in first 4:00 of period
                if is_in_clock_window(clock_ms, period_start_ms, window_end_ms):
                    window_events.append(event)

            # Deduplicate events
            window_events = self.deduplicate_events(window_events)

            # Process events for team stats
            self._process_early_period_events(period, window_events, period_stats)

        return dict(period_stats)

    def _process_early_period_events(
        self, period: int, events: List[PbpEventRow], period_stats: Dict
    ) -> None:
        """Process events for a single period's early window."""
        team_scores = defaultdict(int)
        last_leader = None

        # First pass: identify all teams in the period to ensure proper points_against tracking
        all_teams = set()
        for event in events:
            if event.team_tricode:
                all_teams.add(event.team_tricode)

        # Initialize stats for all teams
        for team in all_teams:
            if team not in period_stats[period]:
                period_stats[period][team] = {
                    "points_for": 0,
                    "points_against": 0,
                    "lead_changes": 0,
                    "possessions": 0,
                    "fga": 0,
                    "fta": 0,
                    "oreb": 0,
                    "tov": 0,
                    "events": [],
                }

        for event in events:
            team = event.team_tricode
            if not team:
                continue

            stats = period_stats[period][team]
            stats["events"].append(event)

            # Track scoring events
            points = 0
            if event.event_type == EventType.SHOT_MADE:
                points = getattr(event, "shot_value", 2) or 2
            elif event.event_type == EventType.FREE_THROW_MADE:
                points = 1

            if points > 0:
                stats["points_for"] += points
                team_scores[team] += points

                # Update points_against for all other teams
                for other_team in all_teams:
                    if other_team != team:
                        period_stats[period][other_team]["points_against"] += points

                # Check for lead changes
                current_leader = max(team_scores.keys(), key=lambda t: team_scores[t], default=None)
                if last_leader and current_leader != last_leader:
                    stats["lead_changes"] += 1
                last_leader = current_leader

            # Track possession estimation components
            if event.event_type in [EventType.SHOT_MADE, EventType.SHOT_MISSED]:
                stats["fga"] += 1
            elif event.event_type in [EventType.FREE_THROW_MADE, EventType.FREE_THROW_MISSED]:
                stats["fta"] += 1
            elif (
                event.event_type == EventType.REBOUND
                and "offensive" in (event.description or "").lower()
            ):
                stats["oreb"] += 1
            elif event.event_type == EventType.TURNOVER:
                stats["tov"] += 1

        # Calculate possessions and net rating for each team
        for team in all_teams:
            stats = period_stats[period][team]

            # Estimate possessions: FGA + 0.44*FTA - OREB + TOV
            possessions = stats["fga"] + int(0.44 * stats["fta"]) - stats["oreb"] + stats["tov"]
            possessions = max(1, possessions)  # At least 1 possession
            stats["possessions"] = possessions

            # Calculate net rating per 100 possessions
            points_for = stats["points_for"]
            points_against = stats["points_against"]
            stats["net_rating"] = ((points_for - points_against) / possessions) * 100


# Factory functions for easy access
def build_q1_window_12_8(events: Iterable[PbpEventRow]) -> List[PbpEventRow]:
    """Build Q1 window events for 12:00→8:00 timeframe."""
    builder = Q1WindowBuilder()
    return builder.build_q1_window_12_8(events)


def build_early_shocks(events: Iterable[PbpEventRow]) -> Dict[int, Dict[str, any]]:
    """Build early shocks analysis for first 4:00 of each period."""
    builder = EarlyShocksBuilder()
    return builder.build_early_shocks(events)
