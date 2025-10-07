"""Play-by-play event row Pydantic model."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from ..utils.clock import parse_clock_to_ms, period_length_ms
from .enums import EventSubtype, EventType, ShotType, ShotZone
from .ref_rows import normalize_name_slug


class PbpEventRow(BaseModel):
    """Play-by-play event row model."""

    game_id: str = Field(..., description="Game identifier")
    period: int = Field(..., description="Period number")
    event_idx: int = Field(..., description="Event index within period")
    event_id: Optional[str] = Field(None, description="Source event ID")
    time_remaining: Optional[str] = Field(None, description="Time remaining in period")
    clock_ms_remaining: Optional[int] = Field(
        None, description="Clock time in milliseconds remaining"
    )
    seconds_elapsed: Optional[float] = Field(None, description="Seconds elapsed in period")
    score_home: Optional[int] = Field(None, description="Home team score after event")
    score_away: Optional[int] = Field(None, description="Away team score after event")
    event_type: EventType = Field(..., description="Event type")
    event_subtype: Optional[EventSubtype] = Field(None, description="Event subtype")
    description: Optional[str] = Field(None, description="Event description")
    team_tricode: Optional[str] = Field(None, description="Team associated with event")

    # Player fields
    player1_name_slug: Optional[str] = Field(None, description="Primary player name slug")
    player1_display_name: Optional[str] = Field(None, description="Primary player display name")
    player1_id: Optional[str] = Field(None, description="Primary player ID")
    player2_name_slug: Optional[str] = Field(None, description="Secondary player name slug")
    player2_display_name: Optional[str] = Field(None, description="Secondary player display name")
    player2_id: Optional[str] = Field(None, description="Secondary player ID")
    player3_name_slug: Optional[str] = Field(None, description="Tertiary player name slug")
    player3_display_name: Optional[str] = Field(None, description="Tertiary player display name")
    player3_id: Optional[str] = Field(None, description="Tertiary player ID")

    # Shot-specific fields
    shot_made: Optional[bool] = Field(None, description="Whether shot was made")
    shot_value: Optional[int] = Field(None, description="Point value of shot (2 or 3)")
    shot_type: Optional[ShotType] = Field(None, description="Shot type classification")
    shot_zone: Optional[ShotZone] = Field(None, description="Shot zone classification")
    shot_distance_ft: Optional[float] = Field(None, description="Shot distance in feet")
    shot_x: Optional[float] = Field(None, description="Shot X coordinate")
    shot_y: Optional[float] = Field(None, description="Shot Y coordinate")

    # Game situation
    shot_clock_seconds: Optional[float] = Field(None, description="Shot clock seconds remaining")
    possession_team: Optional[str] = Field(None, description="Team with possession")

    # Enrichment flags
    is_transition: bool = Field(default=False, description="Event in transition")
    is_early_clock: bool = Field(default=False, description="Event in early shot clock")

    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")

    @model_validator(mode="after")
    def derive_clock_fields(self) -> "PbpEventRow":
        """Derive clock_ms_remaining and seconds_elapsed from time_remaining."""
        if self.time_remaining and self.clock_ms_remaining is None:
            try:
                clock_ms = parse_clock_to_ms(self.time_remaining, self.period)
                object.__setattr__(self, "clock_ms_remaining", clock_ms)

                # Also update seconds_elapsed if not already set
                if self.seconds_elapsed is None:
                    period_ms = period_length_ms(self.period)
                    elapsed_ms = period_ms - clock_ms
                    seconds_elapsed = elapsed_ms / 1000.0
                    object.__setattr__(self, "seconds_elapsed", seconds_elapsed)
            except Exception:
                # Don't fail the whole model for clock parsing issues
                pass
        return self

    @model_validator(mode="before")
    @classmethod
    def preprocess_data(cls, data: Any) -> Any:
        """Apply comprehensive preprocessing before field validation to prevent int/str comparison errors."""
        if isinstance(data, dict):
            # Import here to avoid circular imports
            from .nba_stats_enums import preprocess_pbp_event_data
            from .utils import preprocess_nba_stats_data

            # Apply NBA Stats preprocessing if this looks like NBA Stats data
            processed_data = preprocess_nba_stats_data(data)

            # Apply comprehensive PBP preprocessing to handle all enum conversions
            processed_data = preprocess_pbp_event_data(processed_data)

            return processed_data
        return data

    @field_validator("player1_name_slug", "player2_name_slug", "player3_name_slug", mode="before")
    @classmethod
    def normalize_player_name_slugs(cls, v: Optional[str]) -> Optional[str]:
        """Normalize player name slugs."""
        return normalize_name_slug(v) if v else None

    @field_validator("team_tricode", "possession_team")
    @classmethod
    def normalize_team_tricode(cls, v: Optional[str]) -> Optional[str]:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else None

    @field_validator("event_type", mode="before")
    @classmethod
    def normalize_event_type(cls, v) -> EventType:
        """Normalize event type from various sources."""
        # FIXED: If already a valid EventType enum, return it directly
        if isinstance(v, EventType):
            return v

        # CRITICAL: Convert any input to string first to prevent int/str comparison errors
        if v is None:
            event_str = "1"  # Default to SHOT_MADE
        else:
            # Handle EventType enum instances that might be converted to string representation
            if hasattr(v, "value"):  # This is likely an enum
                event_str = v.value
            elif str(v).startswith("EventType."):
                # Handle "EventType.FOUL" format by extracting the enum value
                event_str = str(v).replace("EventType.", "")
            else:
                # Ensure we always work with a string, regardless of input type
                event_str = str(v).strip()

        # Map NBA Stats event types to our enum - handle both string and integer inputs
        event_map = {
            # Integer string mappings (from NBA Stats API)
            "1": EventType.SHOT_MADE,
            "2": EventType.SHOT_MISSED,
            "3": EventType.FREE_THROW_MADE,
            "4": EventType.FREE_THROW_MISSED,
            "5": EventType.REBOUND,
            "6": EventType.TURNOVER,
            "7": EventType.FOUL,
            "8": EventType.SUBSTITUTION,
            "9": EventType.TIMEOUT,
            "10": EventType.JUMP_BALL,
            "11": EventType.EJECTION,
            "12": EventType.PERIOD_BEGIN,
            "13": EventType.PERIOD_END,
            "18": EventType.INSTANT_REPLAY,
            # Canonical string mappings (from preprocessing or B-Ref)
            "shot": EventType.SHOT_MADE,
            "free_throw": EventType.FREE_THROW_MADE,
            "rebound": EventType.REBOUND,
            "turnover": EventType.TURNOVER,
            "foul": EventType.FOUL,
            "violation": EventType.VIOLATION,
            "substitution": EventType.SUBSTITUTION,
            "timeout": EventType.TIMEOUT,
            "jump_ball": EventType.JUMP_BALL,
            "ejection": EventType.EJECTION,
            "period_start": EventType.PERIOD_BEGIN,
            "period_end": EventType.PERIOD_END,
            "technical": EventType.TECHNICAL_FOUL,
            "flagrant": EventType.FLAGRANT_FOUL,
            "instant_replay": EventType.INSTANT_REPLAY,
            # Legacy string mappings
            "SHOT_MADE": EventType.SHOT_MADE,
            "SHOT_MISSED": EventType.SHOT_MISSED,
            "FREE_THROW_MADE": EventType.FREE_THROW_MADE,
            "FREE_THROW_MISSED": EventType.FREE_THROW_MISSED,
            "REBOUND": EventType.REBOUND,
            "ASSIST": EventType.ASSIST,
            "TURNOVER": EventType.TURNOVER,
            "STEAL": EventType.STEAL,
            "BLOCK": EventType.BLOCK,
            "FOUL": EventType.FOUL,
            "PERSONAL_FOUL": EventType.PERSONAL_FOUL,
            "TIMEOUT": EventType.TIMEOUT,
            "SUBSTITUTION": EventType.SUBSTITUTION,
            "JUMP_BALL": EventType.JUMP_BALL,
            "EJECTION": EventType.EJECTION,
            "TECHNICAL_FOUL": EventType.TECHNICAL_FOUL,
            "FLAGRANT_FOUL": EventType.FLAGRANT_FOUL,
            "PERIOD_BEGIN": EventType.PERIOD_BEGIN,
            "PERIOD_END": EventType.PERIOD_END,
            "GAME_END": EventType.GAME_END,
            "INSTANT_REPLAY": EventType.INSTANT_REPLAY,
            "VIOLATION": EventType.VIOLATION,
        }

        # Return mapped value or default to SHOT_MADE if not found
        return event_map.get(event_str, EventType.SHOT_MADE)

    @classmethod
    def from_nba_stats(
        cls, game_id: str, pbp_data: Dict[str, Any], source_url: str
    ) -> "PbpEventRow":
        """Create from NBA Stats play-by-play data."""
        # Preprocess data to handle mixed data types before validation
        from .utils import preprocess_nba_stats_data

        pbp_data = preprocess_nba_stats_data(pbp_data)

        # Parse NBA Stats PBP format - CRITICAL FIX: Always convert to string first
        raw_event_type = pbp_data.get("EVENTMSGTYPE", "")
        event_type_id = str(raw_event_type).strip() if raw_event_type is not None else "1"

        # Map NBA Stats event types to our enum (using the mapping from normalize_event_type)
        event_map = {
            "1": EventType.SHOT_MADE,
            "2": EventType.SHOT_MISSED,
            "3": EventType.FREE_THROW_MADE,
            "4": EventType.FREE_THROW_MISSED,
            "5": EventType.REBOUND,
            "6": EventType.TURNOVER,
            "7": EventType.FOUL,
            "8": EventType.SUBSTITUTION,
            "9": EventType.TIMEOUT,
            "10": EventType.JUMP_BALL,
            "11": EventType.EJECTION,
            "12": EventType.PERIOD_BEGIN,
            "13": EventType.PERIOD_END,
            "18": EventType.INSTANT_REPLAY,
        }

        # CRITICAL FIX: Ensure we always get an EventType enum, never an integer
        event_type = event_map.get(event_type_id, EventType.SHOT_MADE)

        # Verify event_type is actually an EventType enum (additional safety check)
        if not isinstance(event_type, EventType):
            event_type = EventType.SHOT_MADE

        # Determine shot information - now safe to use enum comparisons
        shot_made = None
        shot_value = None
        shot_type = None

        if event_type in [EventType.SHOT_MADE, EventType.SHOT_MISSED]:
            shot_made = event_type == EventType.SHOT_MADE
            # Parse shot value from description or action type
            description = pbp_data.get("HOMEDESCRIPTION", "") or pbp_data.get(
                "VISITORDESCRIPTION", ""
            )
            if "3PT" in description:
                shot_value = 3
                shot_type = ShotType.THREE_POINT
            else:
                shot_value = 2
                shot_type = ShotType.TWO_POINT
        elif event_type in [EventType.FREE_THROW_MADE, EventType.FREE_THROW_MISSED]:
            shot_made = event_type == EventType.FREE_THROW_MADE
            shot_value = 1
            shot_type = ShotType.FREE_THROW

        # Parse time
        time_str = pbp_data.get("PCTIMESTRING", "")
        seconds_elapsed = None
        clock_ms_remaining = None

        if time_str:
            try:
                # Parse using the new clock utility
                clock_ms_remaining = parse_clock_to_ms(time_str, int(pbp_data.get("PERIOD", 1)))

                # Calculate seconds elapsed
                period_ms = period_length_ms(int(pbp_data.get("PERIOD", 1)))
                elapsed_ms = period_ms - clock_ms_remaining
                seconds_elapsed = elapsed_ms / 1000.0
            except Exception:
                # Fallback to old parsing method
                try:
                    minutes, seconds = map(int, time_str.split(":"))
                    # NBA periods are 12 minutes, so elapsed = 12*60 - (minutes*60 + seconds)
                    seconds_elapsed = (12 * 60) - (minutes * 60 + seconds)
                except (ValueError, AttributeError):
                    pass

        # Parse scores from combined score string like "27 - 46"
        score_home = None
        score_away = None
        score_str = pbp_data.get("SCORE", "")
        if score_str and isinstance(score_str, str) and " - " in score_str:
            try:
                home_str, away_str = score_str.strip().split(" - ", 1)
                score_home = int(home_str.strip())
                score_away = int(away_str.strip())
            except (ValueError, AttributeError):
                # If parsing fails, leave scores as None
                pass

        return cls(
            game_id=game_id,
            period=int(pbp_data.get("PERIOD", 1)),
            event_idx=int(pbp_data.get("EVENTNUM", 0)),
            event_id=str(pbp_data.get("EVENTMSGACTIONTYPE", "")),
            time_remaining=time_str,
            clock_ms_remaining=clock_ms_remaining,
            seconds_elapsed=seconds_elapsed,
            score_home=score_home,
            score_away=score_away,
            event_type=event_type,
            description=pbp_data.get("HOMEDESCRIPTION") or pbp_data.get("VISITORDESCRIPTION"),
            player1_name_slug=normalize_name_slug(pbp_data.get("PLAYER1_NAME", "")),
            player1_display_name=pbp_data.get("PLAYER1_NAME"),
            player1_id=str(pbp_data.get("PLAYER1_ID", "")) if pbp_data.get("PLAYER1_ID") else None,
            player2_name_slug=normalize_name_slug(pbp_data.get("PLAYER2_NAME", "")),
            player2_display_name=pbp_data.get("PLAYER2_NAME"),
            player2_id=str(pbp_data.get("PLAYER2_ID", "")) if pbp_data.get("PLAYER2_ID") else None,
            shot_made=shot_made,
            shot_value=shot_value,
            shot_type=shot_type,
            source="nba_stats",
            source_url=source_url,
        )
