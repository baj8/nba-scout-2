"""Enumerations for NBA scraper data validation."""

from enum import Enum
from typing import Any


class GameStatus(str, Enum):
    """Game status enumeration."""

    SCHEDULED = "SCHEDULED"
    LIVE = "LIVE"
    IN_PROGRESS = "IN_PROGRESS"
    FINAL = "FINAL"
    POSTPONED = "POSTPONED"
    CANCELLED = "CANCELLED"
    SUSPENDED = "SUSPENDED"
    RESCHEDULED = "RESCHEDULED"

    @classmethod
    def _missing_(cls, value: Any) -> "GameStatus":
        """Handle missing values by converting NBA Stats integer codes to enum values."""
        from ..alerts import notify_schema_drift
        
        # Handle integer inputs from NBA Stats API
        if isinstance(value, int):
            # Common NBA Stats status codes
            status_map = {
                1: cls.SCHEDULED,
                2: cls.LIVE,
                3: cls.FINAL,
            }
            if value in status_map:
                return status_map[value]
            # Unknown integer code - log drift
            notify_schema_drift(
                field="game_status",
                value=value,
                source="nba_stats",
                expected_values=list(status_map.keys())
            )
            return cls.SCHEDULED

        # Handle string representations of integers
        if isinstance(value, str) and value.isdigit():
            return cls._missing_(int(value))

        # Handle common string variations
        if isinstance(value, str):
            status_str = value.upper().strip()
            status_map = {
                "FINAL": cls.FINAL,
                "FINISHED": cls.FINAL,
                "COMPLETED": cls.FINAL,
                "LIVE": cls.LIVE,
                "IN_PROGRESS": cls.IN_PROGRESS,
                "ACTIVE": cls.LIVE,
                "SCHEDULED": cls.SCHEDULED,
                "UPCOMING": cls.SCHEDULED,
                "POSTPONED": cls.POSTPONED,
                "CANCELLED": cls.CANCELLED,
                "CANCELED": cls.CANCELLED,
                "SUSPENDED": cls.SUSPENDED,
                "RESCHEDULED": cls.RESCHEDULED,
            }
            if status_str in status_map:
                return status_map[status_str]
            # Unknown string value - log drift
            notify_schema_drift(
                field="game_status",
                value=value,
                source="vendor_api",
                expected_values=list(status_map.keys())
            )

        # Default fallback
        return cls.SCHEDULED


class RefRole(str, Enum):
    """Referee role enumeration."""

    CREW_CHIEF = "CREW_CHIEF"
    REFEREE = "REFEREE"
    UMPIRE = "UMPIRE"
    OFFICIAL = "OFFICIAL"

    @classmethod
    def _missing_(cls, value: Any) -> "RefRole":
        """Handle missing values for referee roles."""
        # Handle integer inputs
        if isinstance(value, int):
            role_map = {
                1: cls.CREW_CHIEF,
                2: cls.REFEREE,
                3: cls.UMPIRE,
                4: cls.OFFICIAL,
            }
            return role_map.get(value, cls.REFEREE)

        # Handle string variations
        if isinstance(value, str):
            role_str = value.upper().strip()
            role_map = {
                "CREW_CHIEF": cls.CREW_CHIEF,
                "CREW CHIEF": cls.CREW_CHIEF,
                "CHIEF": cls.CREW_CHIEF,
                "REFEREE": cls.REFEREE,
                "REF": cls.REFEREE,
                "UMPIRE": cls.UMPIRE,
                "UMP": cls.UMPIRE,
                "OFFICIAL": cls.OFFICIAL,
            }
            return role_map.get(role_str, cls.REFEREE)

        return cls.REFEREE


class Position(str, Enum):
    """Player position enumeration."""

    PG = "PG"
    SG = "SG"
    SF = "SF"
    PF = "PF"
    C = "C"
    G = "G"  # Generic guard
    F = "F"  # Generic forward

    @classmethod
    def _missing_(cls, value: Any) -> "Position":
        """Handle missing values for player positions."""
        # Handle integer inputs
        if isinstance(value, int):
            position_map = {
                1: cls.PG,
                2: cls.SG,
                3: cls.SF,
                4: cls.PF,
                5: cls.C,
            }
            return position_map.get(value, cls.G)

        # Handle string variations
        if isinstance(value, str):
            pos_str = value.upper().strip()
            position_map = {
                "PG": cls.PG,
                "SG": cls.SG,
                "SF": cls.SF,
                "PF": cls.PF,
                "C": cls.C,
                "G": cls.G,
                "F": cls.F,
                "POINT_GUARD": cls.PG,
                "SHOOTING_GUARD": cls.SG,
                "SMALL_FORWARD": cls.SF,
                "POWER_FORWARD": cls.PF,
                "CENTER": cls.C,
                "GUARD": cls.G,
                "FORWARD": cls.F,
            }
            return position_map.get(pos_str, cls.G)

        return cls.G


class InjuryStatus(str, Enum):
    """Player injury/availability status."""

    OUT = "OUT"
    QUESTIONABLE = "QUESTIONABLE"
    PROBABLE = "PROBABLE"
    ACTIVE = "ACTIVE"
    DNP = "DNP"  # Did not play
    INACTIVE = "INACTIVE"

    @classmethod
    def _missing_(cls, value: Any) -> "InjuryStatus":
        """Handle missing values for injury status."""
        # Handle integer inputs
        if isinstance(value, int):
            status_map = {
                0: cls.ACTIVE,
                1: cls.QUESTIONABLE,
                2: cls.PROBABLE,
                3: cls.OUT,
                4: cls.DNP,
                5: cls.INACTIVE,
            }
            return status_map.get(value, cls.ACTIVE)

        # Handle string variations
        if isinstance(value, str):
            status_str = value.upper().strip()
            status_map = {
                "OUT": cls.OUT,
                "QUESTIONABLE": cls.QUESTIONABLE,
                "PROBABLE": cls.PROBABLE,
                "ACTIVE": cls.ACTIVE,
                "DNP": cls.DNP,
                "INACTIVE": cls.INACTIVE,
                "AVAILABLE": cls.ACTIVE,
                "HEALTHY": cls.ACTIVE,
                "DOUBTFUL": cls.QUESTIONABLE,
                "DAY_TO_DAY": cls.QUESTIONABLE,
                "INJURED": cls.OUT,
            }
            return status_map.get(status_str, cls.ACTIVE)

        return cls.ACTIVE


class EventType(str, Enum):
    """Play-by-play event types with support for NBA Stats integer codes."""

    SHOT_MADE = "SHOT_MADE"
    SHOT_MISSED = "SHOT_MISSED"
    FREE_THROW_MADE = "FREE_THROW_MADE"
    FREE_THROW_MISSED = "FREE_THROW_MISSED"
    REBOUND = "REBOUND"
    ASSIST = "ASSIST"
    TURNOVER = "TURNOVER"
    STEAL = "STEAL"
    BLOCK = "BLOCK"
    FOUL = "FOUL"
    PERSONAL_FOUL = "PERSONAL_FOUL"  # Added missing enum value
    TIMEOUT = "TIMEOUT"
    SUBSTITUTION = "SUBSTITUTION"
    JUMP_BALL = "JUMP_BALL"
    EJECTION = "EJECTION"
    TECHNICAL_FOUL = "TECHNICAL_FOUL"
    FLAGRANT_FOUL = "FLAGRANT_FOUL"
    CLEAR_PATH_FOUL = "CLEAR_PATH_FOUL"
    AWAY_FROM_PLAY_FOUL = "AWAY_FROM_PLAY_FOUL"
    DOUBLE_FOUL = "DOUBLE_FOUL"
    DOUBLE_TECHNICAL = "DOUBLE_TECHNICAL"
    PERIOD_BEGIN = "PERIOD_BEGIN"
    PERIOD_END = "PERIOD_END"
    GAME_END = "GAME_END"
    REPLAY_REVIEW = "REPLAY_REVIEW"
    INSTANT_REPLAY = "INSTANT_REPLAY"
    VIOLATION = "VIOLATION"

    @classmethod
    def _missing_(cls, value: Any) -> "EventType":
        """Handle missing values by converting NBA Stats integer codes to enum values.

        This prevents the '<' not supported between instances of 'int' and 'str' error
        by handling integer inputs from the NBA API.
        """
        from ..alerts import notify_schema_drift
        
        # NBA Stats integer code mappings
        nba_stats_map = {
            1: cls.SHOT_MADE,
            2: cls.SHOT_MISSED,
            3: cls.FREE_THROW_MADE,
            4: cls.FREE_THROW_MISSED,
            5: cls.REBOUND,
            6: cls.FOUL,
            7: cls.FOUL,
            8: cls.SUBSTITUTION,
            9: cls.TIMEOUT,
            10: cls.JUMP_BALL,
            11: cls.EJECTION,
            12: cls.PERIOD_BEGIN,
            13: cls.PERIOD_END,
            18: cls.INSTANT_REPLAY,
        }

        # Handle integer inputs from NBA Stats API
        if isinstance(value, int):
            if value in nba_stats_map:
                return nba_stats_map[value]
            # Unknown event type code - log drift and return safe default
            notify_schema_drift(
                field="event_type",
                value=value,
                source="nba_stats",
                expected_values=list(nba_stats_map.keys())
            )
            return cls.SHOT_MADE

        # Handle string representations of integers
        if isinstance(value, str) and value.isdigit():
            return cls._missing_(int(value))

        # Unknown string value - log drift
        if value is not None:
            notify_schema_drift(
                field="event_type",
                value=value,
                source="vendor_api"
            )
        
        # Default fallback
        return cls.SHOT_MADE


class EventSubtype(str, Enum):
    """Play-by-play event subtypes."""

    # Shot subtypes
    LAYUP = "LAYUP"
    DUNK = "DUNK"
    HOOK_SHOT = "HOOK_SHOT"
    JUMP_SHOT = "JUMP_SHOT"
    FADEAWAY = "FADEAWAY"
    PULLUP = "PULLUP"
    STEP_BACK = "STEP_BACK"
    TURNAROUND = "TURNAROUND"
    DRIVING = "DRIVING"
    CUTTING = "CUTTING"
    ALLEY_OOP = "ALLEY_OOP"
    TIP_SHOT = "TIP_SHOT"

    # Free throw subtypes
    CLEAR_PATH = "CLEAR_PATH"
    FLAGRANT_1 = "FLAGRANT_1"
    FLAGRANT_2 = "FLAGRANT_2"
    TECHNICAL = "TECHNICAL"

    # Timeout subtypes
    FULL_TIMEOUT = "FULL_TIMEOUT"
    SHORT_TIMEOUT = "SHORT_TIMEOUT"
    OFFICIAL_TIMEOUT = "OFFICIAL_TIMEOUT"

    # Violation subtypes
    TRAVELING = "TRAVELING"
    DOUBLE_DRIBBLE = "DOUBLE_DRIBBLE"
    SHOT_CLOCK = "SHOT_CLOCK"
    BACKCOURT = "BACKCOURT"
    GOALTENDING = "GOALTENDING"
    BASKET_INTERFERENCE = "BASKET_INTERFERENCE"
    LANE_VIOLATION = "LANE_VIOLATION"
    ILLEGAL_SCREEN = "ILLEGAL_SCREEN"
    DELAY_OF_GAME = "DELAY_OF_GAME"


class ShotResult(str, Enum):
    """Shot attempt result."""

    MADE = "MADE"
    MISSED = "MISSED"
    BLOCKED = "BLOCKED"


class ShotType(str, Enum):
    """Shot type classification."""

    TWO_POINT = "2PT"
    THREE_POINT = "3PT"
    FREE_THROW = "FT"

    @classmethod
    def _missing_(cls, value: Any) -> "ShotType":
        """Handle missing values for shot types."""
        # Handle enum objects that were incorrectly passed as values
        if hasattr(value, "name"):
            if "THREE_POINT" in str(value) or "3PT" in str(value):
                return cls.THREE_POINT
            elif "TWO_POINT" in str(value) or "2PT" in str(value):
                return cls.TWO_POINT
            elif "FREE_THROW" in str(value) or "FT" in str(value):
                return cls.FREE_THROW

        # Handle string variations
        if isinstance(value, str):
            shot_str = value.upper().strip()
            shot_map = {
                "2PT": cls.TWO_POINT,
                "3PT": cls.THREE_POINT,
                "FT": cls.FREE_THROW,
                "TWO_POINT": cls.TWO_POINT,
                "THREE_POINT": cls.THREE_POINT,
                "FREE_THROW": cls.FREE_THROW,
                "SHOTTYPE.TWO_POINT": cls.TWO_POINT,
                "SHOTTYPE.THREE_POINT": cls.THREE_POINT,
                "SHOTTYPE.FREE_THROW": cls.FREE_THROW,
                "2": cls.TWO_POINT,
                "3": cls.THREE_POINT,
                "FREE": cls.FREE_THROW,
            }
            return shot_map.get(shot_str, cls.TWO_POINT)

        # Handle integer inputs
        if isinstance(value, int):
            shot_map = {
                2: cls.TWO_POINT,
                3: cls.THREE_POINT,
                1: cls.FREE_THROW,  # Free throws might be coded as 1
            }
            return shot_map.get(value, cls.TWO_POINT)

        return cls.TWO_POINT


class ShotZone(str, Enum):
    """Shot zone classification."""

    RESTRICTED_AREA = "RESTRICTED_AREA"
    PAINT = "PAINT"
    MID_RANGE = "MID_RANGE"
    LEFT_CORNER_3 = "LEFT_CORNER_3"
    RIGHT_CORNER_3 = "RIGHT_CORNER_3"
    ABOVE_BREAK_3 = "ABOVE_BREAK_3"
    BACKCOURT = "BACKCOURT"


class FoulType(str, Enum):
    """Foul type classification."""

    PERSONAL = "PERSONAL"
    SHOOTING = "SHOOTING"
    OFFENSIVE = "OFFENSIVE"
    LOOSE_BALL = "LOOSE_BALL"
    TECHNICAL = "TECHNICAL"
    FLAGRANT_1 = "FLAGRANT_1"
    FLAGRANT_2 = "FLAGRANT_2"
    CLEAR_PATH = "CLEAR_PATH"
    AWAY_FROM_PLAY = "AWAY_FROM_PLAY"
    DOUBLE_FOUL = "DOUBLE_FOUL"
    DOUBLE_TECHNICAL = "DOUBLE_TECHNICAL"


class ReboundType(str, Enum):
    """Rebound type classification."""

    OFFENSIVE = "OFFENSIVE"
    DEFENSIVE = "DEFENSIVE"
    TEAM = "TEAM"


class TurnoverType(str, Enum):
    """Turnover type classification."""

    BAD_PASS = "BAD_PASS"
    LOST_BALL = "LOST_BALL"
    TRAVELING = "TRAVELING"
    DOUBLE_DRIBBLE = "DOUBLE_DRIBBLE"
    SHOT_CLOCK = "SHOT_CLOCK"
    OFFENSIVE_FOUL = "OFFENSIVE_FOUL"
    OUT_OF_BOUNDS = "OUT_OF_BOUNDS"
    BACKCOURT = "BACKCOURT"
    PALMING = "PALMING"
    DISCONTINUE_DRIBBLE = "DISCONTINUE_DRIBBLE"
    KICKED_BALL = "KICKED_BALL"
    LANE_VIOLATION = "LANE_VIOLATION"
    ILLEGAL_SCREEN = "ILLEGAL_SCREEN"
    NO_TURNOVER = "NO_TURNOVER"


class TimeoutType(str, Enum):
    """Timeout type classification."""

    FULL = "FULL"
    SHORT = "SHORT"
    OFFICIAL = "OFFICIAL"
    MANDATORY = "MANDATORY"


class EarlyShockType(str, Enum):
    """Early shock event types."""

    TWO_PF_EARLY = "TWO_PF_EARLY"
    EARLY_FOUL_TROUBLE = "EARLY_FOUL_TROUBLE"
    TECH = "TECH"
    FLAGRANT = "FLAGRANT"
    INJURY_LEAVE = "INJURY_LEAVE"
    EARLY_LEAD_CHANGES = "EARLY_LEAD_CHANGES"
    POOR_START = "POOR_START"


class Severity(str, Enum):
    """Event severity classification."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
