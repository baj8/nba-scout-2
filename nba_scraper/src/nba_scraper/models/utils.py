"""Utility functions for data preprocessing and normalization."""

from typing import Any, Dict, List

from .nba_stats_enums import preprocess_pbp_event_data


def preprocess_nba_stats_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preprocess NBA Stats API data to ensure consistent data types before Pydantic validation.

    This function addresses int/str comparison issues by ensuring all enum-related fields
    are converted to strings before they reach Pydantic validators.

    Args:
        data: Raw data dictionary from NBA Stats API

    Returns:
        Preprocessed data dictionary with normalized types
    """
    if not isinstance(data, dict):
        return data

    # First, apply comprehensive PBP event preprocessing if this looks like PBP data
    if _is_pbp_event_data(data):
        data = preprocess_pbp_event_data(data)

    # Create a copy to avoid mutating the original data
    processed_data = data.copy()

    # Import GameStatus here to avoid circular imports
    try:
        from .enums import GameStatus
    except ImportError:
        GameStatus = None

    # Comprehensive list of fields that should always be strings for enum validation
    # This includes ALL possible field name variations from NBA Stats API
    enum_fields = {
        # Game status fields
        "GAME_STATUS_TEXT",
        "STATUS",
        "game_status",
        "GAME_STATUS",
        # Event type fields (now handled by preprocess_pbp_event_data, but keeping for safety)
        "EVENT_TYPE",
        "event_type",
        "EVENTMSGTYPE",
        "EVENTMSGACTIONTYPE",
        "event_msg_type",
        "EVENT_MSG_TYPE",
        "eventmsgtype",
        "eventType",
        # Injury/player status fields
        "INJURY_STATUS",
        "injury_status",
        "player_status",
        "PLAYER_STATUS",
        "availability_status",
        "AVAILABILITY_STATUS",
        # Referee role fields
        "REFEREE_ROLE",
        "referee_role",
        "role",
        "ROLE",
        "ref_role",
        "REF_ROLE",
        "official_role",
        "OFFICIAL_ROLE",
        # Position fields (now handled by preprocess_pbp_event_data, but keeping for safety)
        "POSITION",
        "position",
        "POS",
        "pos",
        "player_position",
        "PLAYER_POSITION",
        "START_POSITION",
        "start_position",
        # Shot/event related enums (now handled by preprocess_pbp_event_data, but keeping for safety)
        "SHOT_TYPE",
        "shot_type",
        "shotType",
        "SHOT_RESULT",
        "shot_result",
        "SHOT_ZONE",
        "shot_zone",
        "shotZone",
        "SHOT_AREA",
        "shot_area",
        "SHOT_ZONE_BASIC",
        "shot_zone_basic",
        "SHOT_ZONE_AREA",
        "shot_zone_area",
        "FOUL_TYPE",
        "foul_type",
        "foulType",
        "FOUL_KIND",
        "foul_kind",
        "REBOUND_TYPE",
        "rebound_type",
        "reboundType",
        "REB_TYPE",
        "reb_type",
        "TURNOVER_TYPE",
        "turnover_type",
        "turnoverType",
        "TO_TYPE",
        "to_type",
        "TIMEOUT_TYPE",
        "timeout_type",
        "timeoutType",
        # Team/game identifiers that might be used in enum comparisons
        "TEAM_TRICODE",
        "team_tricode",
        "TEAM_CODE",
        "team_code",
        "HOME_TEAM_TRICODE",
        "home_team_tricode",
        "AWAY_TEAM_TRICODE",
        "away_team_tricode",
        "HOME_TEAM_ABBREVIATION",
        "VISITOR_TEAM_ABBREVIATION",
        "AWAY_TEAM_ABBREVIATION",
        # Any field ending with these suffixes (common enum patterns)
        "_TYPE",
        "_STATUS",
        "_ROLE",
        "_RESULT",
        "_ZONE",
        "_KIND",
        "_CODE",
    }

    # Convert all enum fields to strings, but preserve GameStatus enum objects
    for key, value in processed_data.items():
        if value is not None:
            # SPECIAL CASE: Don't convert GameStatus enum objects to strings
            # This preserves the enum for proper field validation
            if key == "status" and GameStatus and isinstance(value, GameStatus):
                # Keep GameStatus enum as-is
                continue
            # Direct field name match
            elif key in enum_fields:
                processed_data[key] = str(value)
            # Pattern matching for suffixes
            elif any(
                key.endswith(suffix)
                for suffix in ["_TYPE", "_STATUS", "_ROLE", "_RESULT", "_ZONE", "_KIND", "_CODE"]
            ):
                # Don't convert status field with GameStatus enum
                if key == "status" and GameStatus and isinstance(value, GameStatus):
                    continue
                processed_data[key] = str(value)
            # Handle nested dictionaries recursively
            elif isinstance(value, dict):
                processed_data[key] = preprocess_nba_stats_data(value)
            # Handle lists of dictionaries
            elif isinstance(value, list):
                processed_data[key] = [
                    preprocess_nba_stats_data(item) if isinstance(item, dict) else item
                    for item in value
                ]

    # CRITICAL: Also ensure any numeric values that might be enum identifiers are converted to strings
    # This catches cases where NBA Stats API sends numeric enum IDs (1, 2, 3, etc.)
    numeric_enum_fields = {
        "EVENTMSGTYPE",
        "EVENTMSGACTIONTYPE",
        "GAME_STATUS_ID",
        "STATUS_ID",
        "PERIOD_TYPE",
        "SHOT_TYPE_ID",
        "ACTION_TYPE",
        "EVENT_MSG_TYPE",
        "EVENT_MSG_ACTION_TYPE",
        "ACTIONTYPE",
    }

    for field in numeric_enum_fields:
        if field in processed_data and processed_data[field] is not None:
            processed_data[field] = str(processed_data[field])

    return processed_data


def _is_pbp_event_data(data: Dict[str, Any]) -> bool:
    """Check if data appears to be PBP event data that needs comprehensive enum conversion."""
    pbp_indicators = [
        "EVENTMSGTYPE",
        "EVENT_MSG_TYPE",
        "EVENTMSGACTIONTYPE",
        "EVENT_MSG_ACTION_TYPE",
        "GAME_EVENT_ID",
        "EVENTNUM",
        "event_idx",
        "period",
        "PERIOD",
    ]
    return any(indicator in data for indicator in pbp_indicators)


def preprocess_nba_stats_list(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Preprocess a list of NBA Stats API data dictionaries.

    Args:
        data_list: List of raw data dictionaries from NBA Stats API

    Returns:
        List of preprocessed data dictionaries
    """
    return [preprocess_nba_stats_data(item) for item in data_list]
