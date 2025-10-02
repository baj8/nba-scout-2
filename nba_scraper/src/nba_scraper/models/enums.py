"""Enumerations for NBA scraper data validation."""

from enum import Enum


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


class RefRole(str, Enum):
    """Referee role enumeration."""
    CREW_CHIEF = "CREW_CHIEF"
    REFEREE = "REFEREE"
    UMPIRE = "UMPIRE"
    OFFICIAL = "OFFICIAL"


class Position(str, Enum):
    """Player position enumeration."""
    PG = "PG"
    SG = "SG"
    SF = "SF"
    PF = "PF"
    C = "C"
    G = "G"  # Generic guard
    F = "F"  # Generic forward


class InjuryStatus(str, Enum):
    """Player injury/availability status."""
    OUT = "OUT"
    QUESTIONABLE = "QUESTIONABLE"
    PROBABLE = "PROBABLE"
    ACTIVE = "ACTIVE"
    DNP = "DNP"  # Did not play
    INACTIVE = "INACTIVE"


class EventType(str, Enum):
    """Play-by-play event types."""
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


class Severity(str, Enum):
    """Event severity classification."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"