"""Season derivation utilities with regex guarding and safe fallbacks."""

from __future__ import annotations

import re
from datetime import date, datetime

# NBA game ID pattern: 001=Pre, 002=Reg, 003=Playoffs, 004=Play-In
# Format: 00[1-9]YYxxxxx where YY is last 2 digits of season start year
_GAME_ID_RE = re.compile(r"^00[1-9](\d{2})\d{5}$")


def derive_season_from_game_id(game_id: str) -> str | None:
    """Derive season from NBA game ID using regex pattern.

    Args:
        game_id: NBA game ID (e.g., "0022300123")

    Returns:
        Season string (e.g., "2023-24") or None if invalid format
    """
    if not isinstance(game_id, str):
        return None

    m = _GAME_ID_RE.match(game_id.strip())
    if not m:
        return None

    yy = int(m.group(1))
    start_year = 2000 + yy
    end_yy = (yy + 1) % 100
    return f"{start_year}-{end_yy:02d}"


def derive_season_from_date(game_date: str | date) -> str | None:
    """Derive season from game date using NBA season boundaries.

    NBA seasons run from October to June of the following year.

    Args:
        game_date: Date string in YYYY-MM-DD format or date object

    Returns:
        Season string (e.g., "2023-24") or None if invalid date
    """
    try:
        if isinstance(game_date, str):
            d = datetime.strptime(game_date, "%Y-%m-%d")
        elif hasattr(game_date, "year") and hasattr(game_date, "month"):
            # Handle date objects (both date and datetime)
            d = (
                game_date
                if hasattr(game_date, "hour")
                else datetime.combine(game_date, datetime.min.time())
            )
        else:
            return None

        # NBA season logic:
        # Oct-Dec: Current year is season start
        # Jan-Jun: Previous year is season start
        # Jul-Sep: Assume previous year (offseason)
        if d.month >= 10:
            start = d.year
        elif d.month <= 6:
            start = d.year - 1
        else:
            # July-September is offseason, assume previous season
            start = d.year - 1

        return f"{start}-{(start + 1) % 100:02d}"
    except (ValueError, TypeError):
        return None


def coalesce_season(*sources: str | None) -> str | None:
    """Coalesce season from multiple sources, preferring game ID derivation.

    Args:
        *sources: Season sources in order of preference

    Returns:
        First valid season found, or None if all invalid
    """
    for source in sources:
        if source and isinstance(source, str) and source.strip():
            # Check if it's already in proper format
            if re.match(r"^\d{4}-\d{2}$", source.strip()):
                return source.strip()
    return None


def derive_season_smart(
    game_id: str | None, game_date: str | None, fallback_season: str | None = None
) -> str:
    """Smart season derivation with multiple fallback strategies.

    Args:
        game_id: NBA game ID for regex-based derivation
        game_date: Game date for date-based derivation
        fallback_season: Explicit fallback season

    Returns:
        Best available season string or "UNKNOWN" as last resort
    """
    # Try game ID first (most reliable for NBA data)
    if game_id:
        season = derive_season_from_game_id(game_id)
        if season:
            return season

    # Try date-based derivation
    if game_date:
        season = derive_season_from_date(game_date)
        if season:
            return season

    # Try explicit fallback
    if fallback_season:
        season = coalesce_season(fallback_season)
        if season:
            return season

    # Last resort - always return a string
    return "UNKNOWN"


def validate_season_format(season: str) -> bool:
    """Validate season string format.

    Args:
        season: Season string to validate

    Returns:
        True if valid format (YYYY-YY), False otherwise
    """
    if not isinstance(season, str):
        return False

    return bool(re.match(r"^\d{4}-\d{2}$", season.strip()))


def get_current_nba_season() -> str:
    """Get the current NBA season based on today's date.

    Returns:
        Current season string (e.g., "2024-25")
    """
    today = datetime.now()

    # Use the same logic as derive_season_from_date
    if today.month >= 10:
        start = today.year
    elif today.month <= 6:
        start = today.year - 1
    else:
        start = today.year - 1

    return f"{start}-{(start + 1) % 100:02d}"
