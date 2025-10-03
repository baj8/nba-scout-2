"""NBA Stats API enum mappings for converting integer codes to canonical string values.

This module provides comprehensive mappings from NBA Stats API integer codes
to the canonical string enum values used in the database schema.
"""

from typing import Dict, Optional, Any


# Event Type Mappings (EVENT_MSG_TYPE)
NBA_STATS_EVENT_TYPE_MAP: Dict[int, str] = {
    1: "shot",           # Field Goal Made/Missed
    2: "shot",           # Field Goal Missed  
    3: "free_throw",     # Free Throw
    4: "rebound",        # Rebound
    5: "turnover",       # Turnover
    6: "foul",           # Personal Foul
    7: "violation",      # Violation
    8: "substitution",   # Substitution
    9: "timeout",        # Timeout
    10: "jump_ball",     # Jump Ball
    11: "ejection",      # Ejection
    12: "period_start",  # Start of Period
    13: "period_end",    # End of Period
    18: "technical",     # Technical Foul
    19: "flagrant",      # Flagrant Foul
    20: "instant_replay", # Instant Replay
}

# Shot Result Mappings (based on EVENT_MSG_TYPE and ACTION_TYPE)
NBA_STATS_SHOT_RESULT_MAP: Dict[int, str] = {
    1: "make",    # Field Goal Made
    2: "miss",    # Field Goal Missed
    3: "make",    # Free Throw Made (contextual)
}

# Rebound Type Mappings (ACTION_TYPE for rebounds)
NBA_STATS_REBOUND_TYPE_MAP: Dict[int, str] = {
    0: "off",     # Offensive Rebound
    1: "def",     # Defensive Rebound
}

# Foul Type Mappings (ACTION_TYPE for fouls)
NBA_STATS_FOUL_TYPE_MAP: Dict[int, str] = {
    1: "personal",      # Personal Foul
    2: "shooting",      # Shooting Foul
    3: "loose_ball",    # Loose Ball Foul
    4: "offensive",     # Offensive Foul
    5: "inbound",       # Inbound Foul
    6: "away_from_play", # Away from Play Foul
    7: "punch",         # Punch Foul
    8: "clear_path",    # Clear Path Foul
    9: "double",        # Double Foul
    10: "technical",    # Technical Foul
    11: "flagrant1",    # Flagrant 1
    12: "flagrant2",    # Flagrant 2
    13: "defensive",    # Defensive Foul
    14: "take",         # Take Foul
    15: "transition_take", # Transition Take Foul
}

# Turnover Type Mappings (ACTION_TYPE for turnovers)
NBA_STATS_TURNOVER_TYPE_MAP: Dict[int, str] = {
    1: "bad_pass",           # Bad Pass
    2: "lost_ball",          # Lost Ball
    3: "traveling",          # Traveling
    4: "double_dribble",     # Double Dribble
    5: "shot_clock",         # Shot Clock Violation
    6: "kicked_ball",        # Kicked Ball
    7: "lane_violation",     # Lane Violation
    8: "offensive_foul",     # Offensive Foul Turnover
    9: "discontinued_dribble", # Discontinued Dribble
    10: "backcourt",         # Backcourt Violation
    11: "palming",           # Palming
    40: "step_out_of_bounds", # Step Out of Bounds
    41: "out_of_bounds",     # Out of Bounds Lost Ball
    42: "steal",             # Steal
}

# Shot Zone Mappings (derived from shot coordinates and distance)
NBA_STATS_SHOT_ZONE_MAP: Dict[str, str] = {
    "Restricted Area": "rim",
    "In The Paint (Non-RA)": "short_mid",
    "Mid-Range": "long2",
    "Left Corner 3": "corner3",
    "Right Corner 3": "corner3",
    "Above the Break 3": "abovebreak3",
}

# Position Mappings (START_POSITION or POSITION)
NBA_STATS_POSITION_MAP: Dict[str, str] = {
    "G": "G",      # Guard
    "F": "F",      # Forward
    "C": "C",      # Center
    "PG": "G",     # Point Guard -> Generic Guard
    "SG": "G",     # Shooting Guard -> Generic Guard
    "SF": "F",     # Small Forward -> Generic Forward
    "PF": "F",     # Power Forward -> Generic Forward
}

# Team Foul Type Mappings
NBA_STATS_TEAM_FOUL_TYPE_MAP: Dict[int, str] = {
    1: "personal",
    2: "technical",
    3: "flagrant",
}


def convert_event_type(event_msg_type: Optional[Any]) -> Optional[str]:
    """Convert NBA Stats EVENT_MSG_TYPE to canonical event_type string."""
    if event_msg_type is None:
        return None
    # CRITICAL FIX: Safely convert to int, handling both string and int inputs
    try:
        event_msg_type_int = int(event_msg_type)
    except (ValueError, TypeError):
        # If conversion fails, return None instead of causing comparison errors
        return None
    return NBA_STATS_EVENT_TYPE_MAP.get(event_msg_type_int)


def convert_shot_result(event_msg_type: Optional[Any], action_type: Optional[Any] = None) -> Optional[str]:
    """Convert NBA Stats shot event to shot_result string."""
    if event_msg_type is None:
        return None
    
    # CRITICAL FIX: Safely convert to int, handling both string and int inputs
    try:
        event_msg_type_int = int(event_msg_type)
    except (ValueError, TypeError):
        return None
    
    # For shots, EVENT_MSG_TYPE determines make/miss
    if event_msg_type_int == 1:
        return "make"
    elif event_msg_type_int == 2:
        return "miss"
    
    # For free throws, need to check ACTION_TYPE or other context
    elif event_msg_type_int == 3:
        # This requires more context - often determined by description parsing
        return None
    
    return None


def convert_rebound_type(action_type: Optional[Any]) -> Optional[str]:
    """Convert NBA Stats rebound ACTION_TYPE to rebound_type string."""
    if action_type is None:
        return None
    # CRITICAL FIX: Safely convert to int, handling both string and int inputs
    try:
        action_type_int = int(action_type)
    except (ValueError, TypeError):
        return None
    return NBA_STATS_REBOUND_TYPE_MAP.get(action_type_int)


def convert_foul_type(action_type: Optional[Any]) -> Optional[str]:
    """Convert NBA Stats foul ACTION_TYPE to foul_type string."""
    if action_type is None:
        return None
    # CRITICAL FIX: Safely convert to int, handling both string and int inputs
    try:
        action_type_int = int(action_type)
    except (ValueError, TypeError):
        return None
    return NBA_STATS_FOUL_TYPE_MAP.get(action_type_int)


def convert_turnover_type(action_type: Optional[Any]) -> Optional[str]:
    """Convert NBA Stats turnover ACTION_TYPE to turnover_type string."""
    if action_type is None:
        return None
    # CRITICAL FIX: Safely convert to int, handling both string and int inputs
    try:
        action_type_int = int(action_type)
    except (ValueError, TypeError):
        return None
    return NBA_STATS_TURNOVER_TYPE_MAP.get(action_type_int)


def convert_shot_zone(zone_name: Optional[str]) -> Optional[str]:
    """Convert NBA Stats shot zone name to canonical shot_zone string."""
    if not zone_name:
        return None
    return NBA_STATS_SHOT_ZONE_MAP.get(str(zone_name))


def convert_position(position: Optional[str]) -> Optional[str]:
    """Convert NBA Stats position to canonical position string."""
    if not position:
        return None
    return NBA_STATS_POSITION_MAP.get(str(position).upper(), str(position))


def preprocess_pbp_event_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Comprehensive preprocessing for NBA Stats PBP event data.
    
    Converts all integer enum codes to canonical string values before
    Pydantic validation to prevent int/str comparison errors.
    """
    processed_data = data.copy()
    
    # Extract key fields for enum conversion
    event_msg_type = data.get('EVENTMSGTYPE') or data.get('EVENT_MSG_TYPE')
    action_type = data.get('EVENTMSGACTIONTYPE') or data.get('EVENT_MSG_ACTION_TYPE') or data.get('ACTION_TYPE')
    
    # Convert event type
    if event_msg_type is not None:
        processed_data['event_type'] = convert_event_type(event_msg_type)
    
    # CRITICAL FIX: Safe integer conversion for comparisons
    try:
        event_msg_type_int = int(event_msg_type) if event_msg_type is not None else None
    except (ValueError, TypeError):
        event_msg_type_int = None
    
    # Convert shot result - only if we have a valid event type
    if event_msg_type_int is not None and event_msg_type_int in [1, 2, 3]:  # Shot-related events
        processed_data['shot_result'] = convert_shot_result(event_msg_type, action_type)
    
    # Convert rebound type
    if event_msg_type_int is not None and event_msg_type_int == 4:  # Rebound events
        processed_data['rebound_type'] = convert_rebound_type(action_type)
    
    # Convert foul type  
    if event_msg_type_int is not None and event_msg_type_int in [6, 18, 19]:  # Foul events
        processed_data['foul_type'] = convert_foul_type(action_type)
    
    # Convert turnover type
    if event_msg_type_int is not None and event_msg_type_int == 5:  # Turnover events
        processed_data['turnover_type'] = convert_turnover_type(action_type)
    
    # Convert shot zone (safe string conversion)
    shot_zone = data.get('SHOT_ZONE_BASIC') or data.get('shot_zone_basic')
    if shot_zone:
        processed_data['shot_zone'] = convert_shot_zone(shot_zone)
    
    # Convert position (safe string conversion)
    position = data.get('START_POSITION') or data.get('POSITION') or data.get('position')
    if position:
        processed_data['position'] = convert_position(position)
    
    return processed_data