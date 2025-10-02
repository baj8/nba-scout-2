"""Early shocks transformer for detecting Q1 disruption events."""

from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict, Counter
from dataclasses import dataclass

from ..models.pbp_rows import PbpEventRow
from ..models.derived_rows import EarlyShockRow
from ..models.enums import EarlyShockType, EventType
from ..logging import get_logger

logger = get_logger(__name__)


@dataclass
class PlayerFoulTracker:
    """Track fouls for a player in Q1."""
    fouls: List[Tuple[float, int]]  # (seconds_elapsed, event_idx)
    team_tricode: str
    player_slug: str


@dataclass
class InjuryTracker:
    """Track potential injury events."""
    event_idx: int
    seconds_elapsed: float
    team_tricode: str
    player_slug: str
    last_seen_event_idx: Optional[int] = None


class EarlyShocksTransformer:
    """Transformer for detecting early shock events in Q1."""
    
    def __init__(self, source: str = "pbp_derived", early_foul_threshold_sec: float = 360.0):
        """Initialize transformer.
        
        Args:
            source: Data source identifier
            early_foul_threshold_sec: Time threshold for "early" fouls (default 6:00 = 360 seconds)
        """
        self.source = source
        self.early_foul_threshold_sec = early_foul_threshold_sec
    
    def transform(self, pbp_events: List[PbpEventRow], source_url: str) -> List[EarlyShockRow]:
        """Transform PBP events into early shock rows.
        
        Args:
            pbp_events: List of play-by-play events
            source_url: Source URL for provenance
            
        Returns:
            List of early shock events
        """
        if not pbp_events:
            return []
        
        game_id = pbp_events[0].game_id
        logger.debug("Processing early shocks", game_id=game_id, total_events=len(pbp_events))
        
        # Filter to Q1 events only
        q1_events = [e for e in pbp_events if e.period == 1]
        if not q1_events:
            logger.debug("No Q1 events found", game_id=game_id)
            return []
        
        # Sort by event index to ensure chronological order
        q1_events.sort(key=lambda x: x.event_idx)
        
        early_shocks = []
        
        # Track various shock types
        early_shocks.extend(self._detect_early_foul_trouble(q1_events, source_url))
        early_shocks.extend(self._detect_technical_fouls(q1_events, source_url))
        early_shocks.extend(self._detect_flagrant_fouls(q1_events, source_url))
        early_shocks.extend(self._detect_injury_leaves(q1_events, source_url))
        
        logger.debug("Early shocks detected", 
                    game_id=game_id, 
                    total_shocks=len(early_shocks),
                    shock_types=Counter(s.shock_type for s in early_shocks))
        
        return early_shocks
    
    def _detect_early_foul_trouble(self, q1_events: List[PbpEventRow], source_url: str) -> List[EarlyShockRow]:
        """Detect players with two personal fouls early in Q1."""
        foul_trackers = defaultdict(lambda: PlayerFoulTracker([], "", ""))
        early_shocks = []
        
        for event in q1_events:
            # Convert seconds_elapsed to float for comparison
            try:
                if event.seconds_elapsed is None:
                    continue
                    
                if isinstance(event.seconds_elapsed, str):
                    seconds_elapsed = float(event.seconds_elapsed)
                else:
                    seconds_elapsed = event.seconds_elapsed
            except (ValueError, TypeError):
                # Skip events with invalid time data
                logger.warning(f"Invalid seconds_elapsed value: {event.seconds_elapsed} (type: {type(event.seconds_elapsed)}) for game {event.game_id}")
                continue
                
            if seconds_elapsed > self.early_foul_threshold_sec:
                continue
                
            # Track personal fouls (not technical/flagrant)
            if (event.event_type == EventType.FOUL and 
                event.player1_name_slug and 
                event.team_tricode and
                not self._is_technical_or_flagrant_foul(event)):
                
                player_key = (event.team_tricode, event.player1_name_slug)
                tracker = foul_trackers[player_key]
                tracker.team_tricode = event.team_tricode
                tracker.player_slug = event.player1_name_slug
                tracker.fouls.append((seconds_elapsed, event.event_idx))
                
                # Check if this is the second foul in the early window
                if len(tracker.fouls) == 2:
                    # Verify both fouls are within threshold
                    if all(foul_time <= self.early_foul_threshold_sec for foul_time, _ in tracker.fouls):
                        # Check for immediate substitution
                        immediate_sub = self._check_immediate_substitution(
                            q1_events, event.event_idx, event.player1_name_slug
                        )
                        
                        early_shock = EarlyShockRow(
                            game_id=event.game_id,
                            team_tricode=event.team_tricode,
                            player_slug=event.player1_name_slug,
                            shock_type=EarlyShockType.TWO_PF_EARLY,
                            shock_seq=1,  # Only one TWO_PF_EARLY per player
                            period=1,
                            clock_hhmmss=self._format_clock(seconds_elapsed),
                            event_idx_start=tracker.fouls[0][1],
                            event_idx_end=tracker.fouls[1][1],
                            immediate_sub=immediate_sub,
                            poss_since_event=self._count_possessions_since(q1_events, event.event_idx),
                            notes=f"2 PF in {seconds_elapsed:.1f}s",
                            source=self.source,
                            source_url=source_url
                        )
                        early_shocks.append(early_shock)
        
        return early_shocks
    
    def _detect_technical_fouls(self, q1_events: List[PbpEventRow], source_url: str) -> List[EarlyShockRow]:
        """Detect technical fouls in Q1."""
        early_shocks = []
        tech_sequences = defaultdict(int)
        
        for event in q1_events:
            if (event.event_type == EventType.TECHNICAL_FOUL or
                (event.event_type == EventType.FOUL and self._is_technical_foul(event))):
                
                player_slug = event.player1_name_slug or "TEAM"
                team_tricode = event.team_tricode or "UNK"
                
                # Convert seconds_elapsed to float for consistency
                try:
                    if event.seconds_elapsed is None:
                        seconds_elapsed = 0.0
                    elif isinstance(event.seconds_elapsed, str):
                        seconds_elapsed = float(event.seconds_elapsed)
                    else:
                        seconds_elapsed = event.seconds_elapsed
                except (ValueError, TypeError):
                    logger.warning(f"Invalid seconds_elapsed value: {event.seconds_elapsed} for game {event.game_id}")
                    seconds_elapsed = 0.0
                
                # Increment sequence for this player/team
                shock_key = (team_tricode, player_slug)
                tech_sequences[shock_key] += 1
                
                immediate_sub = False
                if player_slug != "TEAM" and event.player1_name_slug:
                    immediate_sub = self._check_immediate_substitution(
                        q1_events, event.event_idx, event.player1_name_slug
                    )
                
                early_shock = EarlyShockRow(
                    game_id=event.game_id,
                    team_tricode=team_tricode,
                    player_slug=player_slug,
                    shock_type=EarlyShockType.TECH,
                    shock_seq=tech_sequences[shock_key],
                    period=1,
                    clock_hhmmss=self._format_clock(seconds_elapsed),
                    event_idx_start=event.event_idx,
                    immediate_sub=immediate_sub,
                    poss_since_event=self._count_possessions_since(q1_events, event.event_idx),
                    notes="Technical foul",
                    source=self.source,
                    source_url=source_url
                )
                early_shocks.append(early_shock)
        
        return early_shocks
    
    def _detect_flagrant_fouls(self, q1_events: List[PbpEventRow], source_url: str) -> List[EarlyShockRow]:
        """Detect flagrant fouls in Q1."""
        early_shocks = []
        flagrant_sequences = defaultdict(int)
        
        for event in q1_events:
            if (event.event_type == EventType.FLAGRANT_FOUL or
                (event.event_type == EventType.FOUL and self._is_flagrant_foul(event))):
                
                if not event.player1_name_slug or not event.team_tricode:
                    continue
                
                # Convert seconds_elapsed to float for consistency
                try:
                    if event.seconds_elapsed is None:
                        seconds_elapsed = 0.0
                    elif isinstance(event.seconds_elapsed, str):
                        seconds_elapsed = float(event.seconds_elapsed)
                    else:
                        seconds_elapsed = event.seconds_elapsed
                except (ValueError, TypeError):
                    logger.warning(f"Invalid seconds_elapsed value: {event.seconds_elapsed} for game {event.game_id}")
                    seconds_elapsed = 0.0
                
                shock_key = (event.team_tricode, event.player1_name_slug)
                flagrant_sequences[shock_key] += 1
                
                immediate_sub = self._check_immediate_substitution(
                    q1_events, event.event_idx, event.player1_name_slug
                )
                
                early_shock = EarlyShockRow(
                    game_id=event.game_id,
                    team_tricode=event.team_tricode,
                    player_slug=event.player1_name_slug,
                    shock_type=EarlyShockType.FLAGRANT,
                    shock_seq=flagrant_sequences[shock_key],
                    period=1,
                    clock_hhmmss=self._format_clock(seconds_elapsed),
                    event_idx_start=event.event_idx,
                    immediate_sub=immediate_sub,
                    poss_since_event=self._count_possessions_since(q1_events, event.event_idx),
                    notes=self._extract_flagrant_type(event),
                    source=self.source,
                    source_url=source_url
                )
                early_shocks.append(early_shock)
        
        return early_shocks
    
    def _detect_injury_leaves(self, q1_events: List[PbpEventRow], source_url: str, 
                            min_absent_possessions: int = 6) -> List[EarlyShockRow]:
        """Detect players who leave due to injury and don't return for several possessions."""
        early_shocks = []
        
        # Find potential injury events (look for injury-related keywords in descriptions)
        injury_events = []
        for event in q1_events:
            if (event.description and 
                any(keyword in event.description.lower() for keyword in 
                    ['injury', 'hurt', 'twisted', 'sprain', 'strain', 'collision'])):
                
                if event.player1_name_slug and event.team_tricode:
                    # Convert seconds_elapsed to float for consistency
                    try:
                        if event.seconds_elapsed is None:
                            seconds_elapsed = 0.0
                        elif isinstance(event.seconds_elapsed, str):
                            seconds_elapsed = float(event.seconds_elapsed)
                        else:
                            seconds_elapsed = event.seconds_elapsed
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid seconds_elapsed value: {event.seconds_elapsed} for game {event.game_id}")
                        seconds_elapsed = 0.0
                    
                    injury_events.append(InjuryTracker(
                        event_idx=event.event_idx,
                        seconds_elapsed=seconds_elapsed,
                        team_tricode=event.team_tricode,
                        player_slug=event.player1_name_slug
                    ))
        
        # Track when players are last seen after injury events
        for injury in injury_events:
            last_seen = self._find_last_appearance(q1_events, injury.event_idx, injury.player_slug)
            possessions_absent = self._count_possessions_since(q1_events, last_seen or injury.event_idx)
            
            if possessions_absent >= min_absent_possessions:
                immediate_sub = self._check_immediate_substitution(
                    q1_events, injury.event_idx, injury.player_slug
                )
                
                early_shock = EarlyShockRow(
                    game_id=q1_events[0].game_id,
                    team_tricode=injury.team_tricode,
                    player_slug=injury.player_slug,
                    shock_type=EarlyShockType.INJURY_LEAVE,
                    shock_seq=1,
                    period=1,
                    clock_hhmmss=self._format_clock(injury.seconds_elapsed),
                    event_idx_start=injury.event_idx,
                    event_idx_end=last_seen,
                    immediate_sub=immediate_sub,
                    poss_since_event=possessions_absent,
                    notes=f"Absent {possessions_absent} possessions",
                    source=self.source,
                    source_url=source_url
                )
                early_shocks.append(early_shock)
        
        return early_shocks
    
    def _is_technical_or_flagrant_foul(self, event: PbpEventRow) -> bool:
        """Check if foul is technical or flagrant (not personal)."""
        if event.event_type in [EventType.TECHNICAL_FOUL, EventType.FLAGRANT_FOUL]:
            return True
        
        if event.description:
            desc = event.description.lower()
            return any(keyword in desc for keyword in 
                      ['technical', 'flagrant', 'unsportsmanlike'])
        
        return False
    
    def _is_technical_foul(self, event: PbpEventRow) -> bool:
        """Check if event is a technical foul."""
        if event.description:
            return 'technical' in event.description.lower()
        return False
    
    def _is_flagrant_foul(self, event: PbpEventRow) -> bool:
        """Check if event is a flagrant foul."""
        if event.description:
            return 'flagrant' in event.description.lower()
        return False
    
    def _extract_flagrant_type(self, event: PbpEventRow) -> str:
        """Extract flagrant foul type from description."""
        if event.description:
            desc = event.description.lower()
            if 'flagrant 2' in desc:
                return "Flagrant 2"
            elif 'flagrant 1' in desc:
                return "Flagrant 1"
            elif 'flagrant' in desc:
                return "Flagrant"
        return "Flagrant foul"
    
    def _check_immediate_substitution(self, events: List[PbpEventRow], 
                                    event_idx: int, player_slug: str) -> bool:
        """Check if player was substituted within next possession."""
        # Find next few events to look for substitution
        current_event_pos = next((i for i, e in enumerate(events) if e.event_idx == event_idx), -1)
        if current_event_pos == -1:
            return False
        
        # Look ahead for substitution in next few events (within same possession)
        possessions_seen = 0
        for i in range(current_event_pos + 1, len(events)):
            event = events[i]
            
            # Count possession changes
            if self._is_possession_change(event):
                possessions_seen += 1
                if possessions_seen > 1:  # More than one possession has passed
                    break
            
            # Check for substitution removing this player
            if (event.event_type == EventType.SUBSTITUTION and
                event.player1_name_slug == player_slug):  # player being subbed out
                return True
        
        return False
    
    def _find_last_appearance(self, events: List[PbpEventRow], 
                            after_event_idx: int, player_slug: str) -> Optional[int]:
        """Find the last event where player appears after given event."""
        last_appearance = None
        
        for event in events:
            if (event.event_idx > after_event_idx and
                (event.player1_name_slug == player_slug or 
                 event.player2_name_slug == player_slug or
                 event.player3_name_slug == player_slug)):
                last_appearance = event.event_idx
        
        return last_appearance
    
    def _count_possessions_since(self, events: List[PbpEventRow], event_idx: int) -> int:
        """Count possessions that have occurred since given event."""
        possessions = 0
        current_event_pos = next((i for i, e in enumerate(events) if e.event_idx == event_idx), -1)
        
        if current_event_pos == -1:
            return 0
        
        for i in range(current_event_pos + 1, len(events)):
            if self._is_possession_change(events[i]):
                possessions += 1
        
        return possessions
    
    def _is_possession_change(self, event: PbpEventRow) -> bool:
        """Determine if event represents a possession change."""
        # Possession changes on:
        # - Made shots (except and-1 situations)
        # - Defensive rebounds
        # - Live ball turnovers
        # - Start of period
        
        if event.event_type in [EventType.PERIOD_BEGIN, EventType.JUMP_BALL]:
            return True
        
        if event.event_type == EventType.SHOT_MADE:
            # Check if it's an and-1 (free throw follows)
            # For simplicity, assume most made shots change possession
            return True
        
        if event.event_type == EventType.REBOUND:
            # Defensive rebounds change possession, offensive don't
            # Simple heuristic: if rebounder's team differs from shooter's team
            return True  # Simplified - would need more context in production
        
        if event.event_type == EventType.TURNOVER:
            return True
        
        return False
    
    def _format_clock(self, seconds_elapsed: Optional[float]) -> str:
        """Format seconds elapsed into HH:MM:SS clock format."""
        if seconds_elapsed is None:
            return "00:00:00"
        
        # Convert to float if it's a string
        try:
            if isinstance(seconds_elapsed, str):
                seconds_elapsed = float(seconds_elapsed)
        except (ValueError, TypeError):
            logger.warning(f"Invalid seconds_elapsed value for clock formatting: {seconds_elapsed}")
            return "00:00:00"
        
        # Q1 is 12 minutes, so remaining = 720 - elapsed
        remaining_seconds = max(0, 720 - seconds_elapsed)
        
        hours = 0
        minutes = int(remaining_seconds // 60)
        seconds = int(remaining_seconds % 60)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"