"""Q1 window (12:00 to 8:00) analytics transformer."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from ..models.derived_rows import Q1WindowRow
from ..models.enums import EventType
from ..models.pbp_rows import PbpEventRow
from ..nba_logging import get_logger
from ..utils.coerce import to_float_or_none
from .pbp_windows import is_in_clock_window, period_bounds_ms

logger = get_logger(__name__)


@dataclass
class TeamStats:
    """Track team statistics during Q1 window."""

    team_tricode: str
    field_goals_made: int = 0
    field_goals_attempted: int = 0
    three_pointers_made: int = 0
    three_pointers_attempted: int = 0
    free_throws_made: int = 0
    free_throws_attempted: int = 0
    offensive_rebounds: int = 0
    defensive_rebounds: int = 0
    turnovers: int = 0
    assists: int = 0
    personal_fouls: int = 0
    points: int = 0
    possessions: int = 0

    # Bonus tracking
    team_fouls_in_quarter: int = 0
    bonus_start_time: Optional[float] = None  # seconds elapsed when bonus started

    @property
    def total_rebounds(self) -> int:
        return self.offensive_rebounds + self.defensive_rebounds

    @property
    def effective_fg_pct(self) -> Optional[float]:
        """Calculate effective field goal percentage."""
        if self.field_goals_attempted == 0:
            return None
        # Fix the calculation - it was adding instead of calculating correctly
        return (self.field_goals_made + 0.5 * self.three_pointers_made) / self.field_goals_attempted

    @property
    def turnover_rate(self) -> Optional[float]:
        """Calculate turnover rate per possession."""
        if self.possessions == 0:
            return None
        return self.turnovers / self.possessions

    @property
    def free_throw_rate(self) -> Optional[float]:
        """Calculate free throw rate (FTA/FGA)."""
        if self.field_goals_attempted == 0:
            return None
        return self.free_throws_attempted / self.field_goals_attempted

    @property
    def offensive_rebound_pct(self) -> Optional[float]:
        """Calculate offensive rebound percentage (requires opponent's defensive rebounds)."""
        # This will be calculated at the game level with opponent data
        return None

    @property
    def defensive_rebound_pct(self) -> Optional[float]:
        """Calculate defensive rebound percentage (requires opponent's offensive rebounds)."""
        # This will be calculated at the game level with opponent data
        return None


class Q1WindowTransformer:
    """Transformer for Q1 window analytics (12:00 to 8:00)."""

    def __init__(
        self,
        source: str = "pbp_q1_window",
        window_start_sec: float = 0.0,
        window_end_sec: float = 240.0,  # 4:00 mark (8:00 remaining)
        expected_pace: float = 100.0,
    ):
        """Initialize transformer.

        Args:
            source: Data source identifier
            window_start_sec: Start of window in seconds elapsed (0 = 12:00)
            window_end_sec: End of window in seconds elapsed (240 = 8:00)
            expected_pace: Expected possessions per 48 minutes for pace comparison
        """
        self.source = source
        self.window_start_sec = window_start_sec
        self.window_end_sec = window_end_sec
        self.expected_pace = expected_pace

    def transform(self, pbp_events: List[PbpEventRow], source_url: str) -> Optional[Q1WindowRow]:
        """Transform PBP events into Q1 window analytics.

        Args:
            pbp_events: List of play-by-play events
            source_url: Source URL for provenance

        Returns:
            Q1WindowRow instance or None if insufficient data
        """
        if not pbp_events:
            return None

        game_id = pbp_events[0].game_id
        logger.debug(
            "Processing Q1 window analytics", game_id=game_id, total_events=len(pbp_events)
        )

        # Filter to Q1 events within our window using clock-safe logic
        window_events = []
        for e in pbp_events:
            # Ignore events outside Q1 or invalid periods
            if e.period != 1 or e.period < 1 or e.period > 10:
                continue

            if e.clock_ms_remaining is not None:
                # Use clock-safe window checking
                end_ms, start_ms = period_bounds_ms(1)
                window_start_ms = start_ms - (self.window_start_sec * 1000)  # 12:00 -> 720000ms
                window_end_ms = start_ms - (self.window_end_sec * 1000)  # 08:00 -> 480000ms

                if is_in_clock_window(e.clock_ms_remaining, window_start_ms, window_end_ms):
                    window_events.append(e)
            elif e.seconds_elapsed is not None:
                # Fallback to seconds_elapsed for backwards compatibility
                seconds_elapsed = to_float_or_none(e.seconds_elapsed)
                if seconds_elapsed is None:
                    logger.warning(
                        f"Invalid seconds_elapsed value: {e.seconds_elapsed} for game {game_id}"
                    )
                    continue

                if self.window_start_sec <= seconds_elapsed <= self.window_end_sec:
                    window_events.append(e)

        if not window_events:
            logger.debug("No Q1 window events found", game_id=game_id)
            return None

        # Deduplicate consecutive events with identical (period, clock_ms_remaining, event_type, team_id)
        window_events = self._deduplicate_events(window_events)

        # Sort by event index to ensure chronological order
        window_events.sort(key=lambda x: x.event_idx)

        # Identify teams
        teams = set()
        for event in window_events:
            if event.team_tricode:
                teams.add(event.team_tricode)

        if len(teams) != 2:
            logger.warning("Expected 2 teams, found", game_id=game_id, teams=list(teams))
            return None

        home_team, away_team = self._identify_home_away_teams(window_events)

        # Track statistics for both teams
        team_stats = {home_team: TeamStats(home_team), away_team: TeamStats(away_team)}

        # Process all events to build statistics
        possessions_elapsed = self._process_window_events(window_events, team_stats)

        # Calculate pace metrics
        window_duration_min = (self.window_end_sec - self.window_start_sec) / 60.0
        pace48_actual = (
            (possessions_elapsed / window_duration_min) * 48.0 if window_duration_min > 0 else None
        )

        # Calculate rebound percentages (requires both teams' data)
        home_stats = team_stats[home_team]
        away_stats = team_stats[away_team]

        home_orb_pct = self._calculate_orb_pct(
            home_stats.offensive_rebounds, away_stats.defensive_rebounds
        )
        home_drb_pct = self._calculate_drb_pct(
            home_stats.defensive_rebounds, away_stats.offensive_rebounds
        )
        away_orb_pct = self._calculate_orb_pct(
            away_stats.offensive_rebounds, home_stats.defensive_rebounds
        )
        away_drb_pct = self._calculate_drb_pct(
            away_stats.defensive_rebounds, home_stats.offensive_rebounds
        )

        # Calculate bonus time
        bonus_time_home = self._calculate_bonus_time(home_stats, self.window_end_sec)
        bonus_time_away = self._calculate_bonus_time(away_stats, self.window_end_sec)

        # Calculate transition and early clock rates
        transition_rate = self._calculate_transition_rate(window_events)
        early_clock_rate = self._calculate_early_clock_rate(window_events)

        return Q1WindowRow(
            game_id=game_id,
            home_team_tricode=home_team,
            away_team_tricode=away_team,
            possessions_elapsed=possessions_elapsed,
            pace48_actual=pace48_actual,
            pace48_expected=self.expected_pace,
            home_efg_actual=home_stats.effective_fg_pct,
            home_efg_expected=0.52,  # League average benchmark
            away_efg_actual=away_stats.effective_fg_pct,
            away_efg_expected=0.52,
            home_to_rate=home_stats.turnover_rate,
            away_to_rate=away_stats.turnover_rate,
            home_ft_rate=home_stats.free_throw_rate,
            away_ft_rate=away_stats.free_throw_rate,
            home_orb_pct=home_orb_pct,
            home_drb_pct=home_drb_pct,
            away_orb_pct=away_orb_pct,
            away_drb_pct=away_drb_pct,
            bonus_time_home_sec=bonus_time_home,
            bonus_time_away_sec=bonus_time_away,
            transition_rate=transition_rate,
            early_clock_rate=early_clock_rate,
            source=self.source,
            source_url=source_url,
        )

    def _deduplicate_events(self, events: List[PbpEventRow]) -> List[PbpEventRow]:
        """Deduplicate consecutive events with identical (period, clock_ms_remaining, event_type, team_id)."""
        if not events:
            return events

        deduplicated = [events[0]]  # Keep first event

        for event in events[1:]:
            prev_event = deduplicated[-1]

            # Check if this event is identical to the previous one
            if (
                event.period == prev_event.period
                and event.clock_ms_remaining == prev_event.clock_ms_remaining
                and event.event_type == prev_event.event_type
                and event.team_tricode == prev_event.team_tricode
            ):
                # Skip duplicate, keep the first occurrence
                logger.debug(
                    f"Skipping duplicate event: {event.event_idx} (keeping {prev_event.event_idx})"
                )
                continue

            deduplicated.append(event)

        return deduplicated

    def _identify_home_away_teams(self, events: List[PbpEventRow]) -> Tuple[str, str]:
        """Identify home and away teams from events.

        Heuristic: Look for score updates or use alphabetical ordering as fallback.
        """
        teams = set(e.team_tricode for e in events if e.team_tricode)
        teams_list = sorted(list(teams))

        if len(teams_list) == 2:
            # Simple alphabetical assignment as fallback
            # In production, this would use game metadata
            return teams_list[0], teams_list[1]  # home, away

        # Fallback if we can't determine
        return "HOME", "AWAY"

    def _process_window_events(
        self, events: List[PbpEventRow], team_stats: Dict[str, TeamStats]
    ) -> int:
        """Process events to build team statistics and count possessions."""
        possessions = 0

        for event in events:
            if not event.team_tricode or event.team_tricode not in team_stats:
                continue

            stats = team_stats[event.team_tricode]

            # Track shooting statistics
            if event.event_type in [EventType.SHOT_MADE, EventType.SHOT_MISSED]:
                stats.field_goals_attempted += 1
                if event.shot_made:
                    stats.field_goals_made += 1
                    stats.points += event.shot_value or 2

                if event.shot_value == 3:
                    stats.three_pointers_attempted += 1
                    if event.shot_made:
                        stats.three_pointers_made += 1

            # Track free throws - fix the logic to properly assign to the fouled team
            elif event.event_type in [EventType.FREE_THROW_MADE, EventType.FREE_THROW_MISSED]:
                stats.free_throws_attempted += 1
                if event.shot_made:
                    stats.free_throws_made += 1
                    stats.points += 1

            # Track rebounds
            elif event.event_type == EventType.REBOUND:
                # Determine if it's offensive or defensive based on context
                # If the rebounding team is different from the shooting team, it's defensive
                if "offensive" in (event.description or "").lower():
                    stats.offensive_rebounds += 1
                else:
                    stats.defensive_rebounds += 1

            # Track turnovers and assists
            elif event.event_type == EventType.TURNOVER:
                stats.turnovers += 1
            elif event.event_type == EventType.ASSIST:
                stats.assists += 1

            # Track fouls and bonus situations - assign to the fouling team
            elif event.event_type in [EventType.FOUL, EventType.PERSONAL_FOUL]:
                stats.personal_fouls += 1
                stats.team_fouls_in_quarter += 1

                # Check for bonus threshold (4 team fouls = bonus for opponent)
                if stats.team_fouls_in_quarter >= 4 and stats.bonus_start_time is None:
                    # Use robust coercion for bonus time calculation
                    stats.bonus_start_time = to_float_or_none(event.seconds_elapsed)

            # Count possessions on certain events
            if self._is_possession_ending_event(event):
                possessions += 1

        # Update possession counts for rate calculations
        for stats in team_stats.values():
            stats.possessions = max(1, possessions // 2)  # Rough estimate per team

        return possessions

    def _is_possession_ending_event(self, event: PbpEventRow) -> bool:
        """Determine if event ends a possession."""
        return event.event_type in [
            EventType.SHOT_MADE,
            EventType.TURNOVER,
            EventType.REBOUND,  # Defensive rebound ends possession
        ]

    def _calculate_orb_pct(self, team_orb: int, opponent_drb: int) -> Optional[float]:
        """Calculate offensive rebound percentage."""
        total_orb_opportunities = team_orb + opponent_drb
        if total_orb_opportunities == 0:
            return None
        return team_orb / total_orb_opportunities

    def _calculate_drb_pct(self, team_drb: int, opponent_orb: int) -> Optional[float]:
        """Calculate defensive rebound percentage."""
        total_drb_opportunities = team_drb + opponent_orb
        if total_drb_opportunities == 0:
            return None
        return team_drb / total_drb_opportunities

    def _calculate_bonus_time(self, stats: TeamStats, window_end_sec: float) -> float:
        """Calculate seconds spent in bonus during the window."""
        if stats.bonus_start_time is None:
            return 0.0

        # Time in bonus = window_end - bonus_start_time
        bonus_seconds = window_end_sec - stats.bonus_start_time
        return max(0.0, bonus_seconds)

    def _calculate_transition_rate(self, events: List[PbpEventRow]) -> Optional[float]:
        """Calculate rate of transition possessions (events within 6 seconds of possession change)."""
        if not events:
            return None

        transition_events = sum(1 for e in events if e.is_transition)
        total_events = len(events)

        return transition_events / total_events if total_events > 0 else None

    def _calculate_early_clock_rate(self, events: List[PbpEventRow]) -> Optional[float]:
        """Calculate rate of early shot clock events (within first 8 seconds)."""
        if not events:
            return None

        early_clock_events = sum(1 for e in events if e.is_early_clock)
        total_events = len(events)

        return early_clock_events / total_events if total_events > 0 else None
