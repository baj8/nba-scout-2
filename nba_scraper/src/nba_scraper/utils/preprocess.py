"""Data preprocessing utilities to prevent dtype coercion errors."""

import re
from typing import Any

# Regex patterns for type detection
_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)$")
_CLOCK_LIKE = re.compile(r"^\d{1,2}:[0-5]\d(?:\.\d{1,3})?$")
_ISO_DURATION_RE = re.compile(r"^PT\d+M\d+(?:\.\d{1,3})?S$")


def _looks_like_clock(s: str) -> bool:
    """Check if string looks like a clock time or duration."""
    return bool(_CLOCK_LIKE.match(s)) or bool(_ISO_DURATION_RE.match(s))


def _coerce_scalar(v: Any) -> Any:
    """Safely coerce scalar values without breaking clock strings."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip()
        # CRITICAL: Don't coerce clock-like strings to floats
        if _looks_like_clock(s):
            return s
        # Only coerce pure numeric strings
        if _NUMERIC_RE.match(s):
            try:
                f = float(s)
                return int(f) if f.is_integer() else f
            except Exception:
                return s
        return s
    return v


def preprocess_nba_stats_data(obj: Any) -> Any:
    """Recursively preprocess NBA Stats API data to handle mixed types safely."""
    if isinstance(obj, list):
        return [preprocess_nba_stats_data(x) for x in obj]
    if isinstance(obj, dict):
        # Special handling for dictionaries to preserve game IDs as strings
        result = {}
        for k, v in obj.items():
            # CRITICAL FIX: Game ID fields should NEVER be converted to integers
            # They need to preserve leading zeros (e.g., "0022301234" must stay a string)
            if k.upper() in ('GAME_ID', 'GAMEID', 'ID') and isinstance(v, str) and v.isdigit() and len(v) >= 8:
                result[k] = v  # Keep as string to preserve leading zeros
            else:
                result[k] = preprocess_nba_stats_data(v)
        return result
    return _coerce_scalar(obj)


def normalize_team_id(team_id: Any) -> int | None:
    """Normalize team ID to integer, handling various input formats."""
    if team_id is None or team_id == '' or team_id == 'null':
        return None
    
    try:
        return int(team_id)
    except (ValueError, TypeError):
        return None


def normalize_player_id(player_id: Any) -> int | None:
    """Normalize player ID to integer, handling various input formats."""
    if player_id is None or player_id == '' or player_id == 'null':
        return None
    
    try:
        return int(player_id)
    except (ValueError, TypeError):
        return None


def normalize_clock_time(clock: Any) -> str:
    """Normalize clock time to consistent string format."""
    if clock is None:
        return ""
    
    clock_str = str(clock).strip()
    
    # Handle common formats: "PT11M45S", "11:45", "11:45.0", etc.
    if clock_str.startswith('PT') and 'M' in clock_str and 'S' in clock_str:
        # ISO 8601 duration format: PT11M45S
        return clock_str
    elif ':' in clock_str:
        # MM:SS format
        return clock_str
    else:
        return clock_str