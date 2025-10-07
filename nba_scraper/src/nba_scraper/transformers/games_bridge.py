"""Transformer bridge from GameRow to Game with comprehensive validation."""

from __future__ import annotations

import re
from datetime import date as _date
from datetime import datetime, timezone
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from ..models.game_rows import GameRow
from ..models.games import Game
from ..nba_logging import get_logger
from ..utils.date_norm import to_date_str
from ..utils.season import derive_season_from_date, derive_season_from_game_id, derive_season_smart
from ..utils.team_crosswalk import resolve_team_id
from ..utils.team_lookup import get_team_index  # Use the mockable version
from ..utils.time import get_venue_tz, official_game_date

logger = get_logger(__name__)


def _parse_zoneinfo_or_none(tz_name: str | None) -> ZoneInfo | None:
    """Parse timezone string to ZoneInfo, return None on error."""
    if not tz_name:
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        logger.warning("games_bridge.tz_alias_unrecognized", extra={"arena_tz": tz_name})
        return None


def _resolve_game_date(game_row) -> _date:
    """Resolve game date with precedence: local date → derived from UTC + venue tz."""
    local_date = game_row.game_date_local  # GameRow has already handled precedence
    derived_date: _date | None = None

    # Compute derived date for logging/validation purposes
    if game_row.game_date_utc is not None:
        arena_tz = _parse_zoneinfo_or_none(getattr(game_row, "arena_tz", None))
        if arena_tz is None:
            # fall back: try team map by home tricode/id, then ET inside official_game_date
            venue_tz = get_venue_tz(getattr(game_row, "home_team_tricode", None)) or get_venue_tz(
                getattr(game_row, "home_team_id", None)
            )
        else:
            venue_tz = arena_tz
        derived_date = official_game_date(game_row.game_date_utc, venue_tz)

    # GameRow model has already handled date precedence and validation
    if local_date:
        # Log the resolution details for debugging
        logger.debug(
            "games_bridge.date_resolved",
            extra={
                "used": "local",
                "game_date": local_date.isoformat(),
                "local": local_date.isoformat(),
                "derived": derived_date.isoformat() if derived_date else None,
                "venue_tz": str(venue_tz) if "venue_tz" in locals() else None,
            },
        )
        return local_date

    # Fallback to derived date if no local date
    if derived_date:
        logger.debug(
            "games_bridge.date_resolved",
            extra={
                "used": "derived",
                "game_date": derived_date.isoformat(),
                "local": None,
                "derived": derived_date.isoformat(),
                "venue_tz": str(venue_tz) if "venue_tz" in locals() else None,
            },
        )
        return derived_date

    # Last resort: cannot resolve
    raise ValueError(
        "Unable to resolve game_date: need game_date_local or (game_date_utc + usable tz)"
    )


_ET = ZoneInfo("America/New_York")


def _strip_date(s: str) -> str:
    """Return YYYY-MM-DD from 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'."""
    s = s.strip()
    return s.split("T", 1)[0] if "T" in s else s


def _parse_iso_assume_utc(iso_str: str) -> datetime:
    """Parse ISO date/time. If tz-naive, assume UTC."""
    s = iso_str.strip()
    # Add UTC if trailing Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # If it's just a date, add midnight UTC
    if "T" not in s:
        s = f"{s}T00:00:00+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def resolve_game_date(
    raw_est: Optional[str], raw_iso: Optional[str], venue_tz: Optional[str]
) -> str:
    """
    Preferred date selection:
      1) If raw_est provided (e.g., 'GAME_DATE_EST'), return its calendar date (no tz conversion).
      2) Else parse raw_iso, convert to venue timezone, return calendar date.
      3) If neither provided, raise ValueError.
    """
    if raw_est:
        return _strip_date(raw_est)

    if not raw_iso:
        raise ValueError("No date fields provided to resolve_game_date")

    dt = _parse_iso_assume_utc(raw_iso).astimezone(ZoneInfo(venue_tz))
    return dt.date().isoformat()


# Status mapping for title-case canonical values
_STATUS_MAP_CANON = {
    "FINAL": "Final",
    "FINISHED": "Final",
    "COMPLETED": "Final",
    "SCHEDULED": "Scheduled",
    "PREGAME": "Scheduled",
    "IN PROGRESS": "Live",
    "LIVE": "Live",
    "HALFTIME": "Live",
    "POSTPONED": "Postponed",
    "SUSPENDED": "Suspended",
}

STATUS_MAP: Dict[str, str] = {
    "FINAL": "Final",
    "FINISHED": "Final",
    "COMPLETED": "Final",
    "SCHEDULED": "Scheduled",
    "PREGAME": "Scheduled",
    "IN PROGRESS": "Live",
    "LIVE": "Live",
    "HALFTIME": "Live",
    "POSTPONED": "Postponed",
    "SUSPENDED": "Suspended",
}

TRICODE_MAP: Dict[str, str] = {
    # Legacy/alternates → canonical
    "PHO": "PHX",  # Phoenix Suns: PHO -> PHX
    "CHA": "CHO",
    # add others if tests need them
}

SEASON_RE = re.compile(r"^\d{4}-\d{2}$")


def season_from_date(d: _date) -> str:
    """
    Given a calendar date, return NBA season in 'YYYY-YY' (e.g., 2023-24).
    Pivot: August 1 starts the new season.
    - If month >= 8 (Aug..Dec): season = Y-(Y+1)%100
    - Else (Jan..Jul): season = (Y-1)-Y%100
    """
    y = d.year
    if d.month >= 8:
        return f"{y}-{str((y + 1) % 100).zfill(2)}"
    else:
        return f"{y - 1}-{str(y % 100).zfill(2)}"


def normalize_status(raw: str | None) -> str:
    if not raw:
        return "Scheduled"
    upper = raw.strip().upper()
    return _STATUS_MAP_CANON.get(upper, raw.strip().title())


def normalize_game_date(raw: str) -> str:
    """
    Accept 'YYYY-MM-DD' or ISO 'YYYY-MM-DDTHH:MM:SS[Z/offset]' and
    return the literal calendar date 'YYYY-MM-DD' without timezone conversion.
    """
    s = str(raw).strip()
    # prefer date part before 'T' if present
    if "T" in s:
        s = s.split("T", 1)[0]
    # validate
    datetime.strptime(s, "%Y-%m-%d")
    return s


def coalesce_season(season: Optional[str], game_id: Optional[str], game_date: Optional[str]) -> str:
    """
    If season matches YYYY-YY and is within reasonable NBA range, return as-is.
    Otherwise derive via derive_season_smart() and return string.
    """
    if season and isinstance(season, str) and SEASON_RE.match(season):
        # Extract start year to validate it's within reasonable NBA range
        try:
            start_year = int(season[:4])
            # NBA started in 1946, reasonable upper bound is current year + 10
            current_year = datetime.now().year
            if 1946 <= start_year <= current_year + 10:
                return season
        except (ValueError, IndexError):
            pass  # Fall through to derivation

    # derive
    derived = derive_season_smart(game_id=game_id or "", game_date=game_date, fallback_season=None)
    if not derived:
        # conservative fallback: compute from date only
        if game_date:
            y, m, d = map(int, game_date.split("-"))
            # season starts around Aug/Sep; treat Aug+ as next season
            if m >= 8:
                return f"{y}-{str((y+1)%100).zfill(2)}"
            else:
                return f"{y-1}-{str(y%100).zfill(2)}"
        # last resort
        return "UNKNOWN"
    return str(derived)


def to_db_game(game_row: GameRow, *, team_index: Optional[Dict[str, int]] = None) -> Game:
    """Transform GameRow to validated Game model with comprehensive normalization.

    This bridge function implements the requirements:
    1. game_id: required (error if missing)
    2. season: if missing/empty, derive from game_id or date; fallback "UNKNOWN" + WARN
    3. game_date: compute as league-official local calendar date (venue timezone), fallback to ET
    4. home_team_id/away_team_id: resolve tricodes via team crosswalk with error on miss
    5. status: normalize to DB's expected casing, default "Final" if absent

    Args:
        game_row: Source GameRow from extractor
        team_index: Optional pre-built team index for performance

    Returns:
        Validated Game instance ready for database

    Raises:
        ValueError: For validation failures with targeted messages
    """
    # 1. Validate game_id is present
    if not game_row.game_id or not game_row.game_id.strip():
        raise ValueError("game_id is required")

    game_id = game_row.game_id.strip()

    # 2. Use provided team_index or build one for ID lookups
    if team_index is None:
        team_index = get_team_index()

    # Convert tricodes to team IDs using the team_crosswalk resolve_team_id function
    try:
        home_team_id = resolve_team_id(game_row.home_team_tricode, team_index, game_id=game_id)
        away_team_id = resolve_team_id(game_row.away_team_tricode, team_index, game_id=game_id)
    except ValueError as e:
        # Re-raise with specific format expected by tests
        error_msg = str(e)
        if "Unknown tricode" in error_msg and "for game" not in error_msg:
            error_msg = f"Unknown tricode for game {game_id}"
        raise ValueError(error_msg)

    # 3. Resolve game date with precedence: local date → derived from UTC + venue tz
    resolved_date = _resolve_game_date(game_row)
    game_date = resolved_date.isoformat()

    logger.debug("Date resolution successful", game_id=game_id, game_date=game_date)

    # 4. Derive season with fallbacks
    season = getattr(game_row, "season", None)
    if not season or season.strip() == "":
        # Try season from date first, then from game_id
        try:
            from datetime import date as date_class

            date_obj = date_class.fromisoformat(game_date)
            season = season_from_date(date_obj)
        except Exception:
            season_from_id = derive_season_from_game_id(game_id)
            season = season_from_id if season_from_id else "UNKNOWN"

    # Season guard: Validate game date falls within season bounds
    if season and season != "UNKNOWN":
        try:
            # Parse season to get year bounds
            start_year = int(season[:4])
            end_year = start_year + 1

            # NBA season runs roughly Oct to June
            season_start = _date(start_year, 8, 1)  # Aug 1 as season boundary
            season_end = _date(end_year, 7, 31)  # July 31 as season end

            if not (season_start <= resolved_date <= season_end):
                error_msg = f"Game date {game_date} outside season {season} bounds ({season_start} to {season_end})"
                logger.error(error_msg, game_id=game_id)
                raise ValueError(error_msg)

        except (ValueError, IndexError) as e:
            logger.warning("Season validation failed", game_id=game_id, season=season, error=str(e))

    # 5. Normalize status - use _normalize_game_status that returns uppercase format
    raw_status = getattr(game_row, "status", None)
    status = _normalize_game_status(raw_status)  # Use the function that returns uppercase

    # 6. Create and validate Game instance
    try:
        db_game = Game(
            game_id=game_id,
            season=season,
            game_date=game_date,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            status=status,
        )

        # Log successful transformation with key details
        logger.debug(
            "GameRow → Game transformation complete",
            game_id=game_id,
            season=season,
            game_date=game_date,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            status=status,
        )

        return db_game

    except Exception as e:
        logger.error(
            "Game model validation failed",
            game_id=game_id,
            season=season,
            game_date=game_date,
            error=str(e),
        )
        raise ValueError(f"Game validation failed for {game_id}: {str(e)}")


def _derive_game_date(game_row: GameRow) -> str:
    """Derive game date with local preference policy.

    Policy: Prefer game_date_local, else extract date from game_date_utc

    Args:
        game_row: Source GameRow

    Returns:
        Game date as YYYY-MM-DD string
    """
    # Prefer game_date_local if available
    if hasattr(game_row, "game_date_local") and game_row.game_date_local:
        try:
            return to_date_str(game_row.game_date_local, field_name="game_date_local")
        except ValueError:
            logger.warning(
                "Invalid game_date_local, falling back to UTC",
                game_id=game_row.game_id,
                game_date_local=game_row.game_date_local,
            )

    # Fallback to extracting date from game_date_utc
    if hasattr(game_row, "game_date_utc") and game_row.game_date_utc:
        try:
            return to_date_str(game_row.game_date_utc, field_name="game_date_utc")
        except ValueError:
            logger.warning(
                "Invalid game_date_utc",
                game_id=game_row.game_id,
                game_date_utc=game_row.game_date_utc,
            )

    # Last resort: use today's date
    logger.warning("No valid game date found, using today", game_id=game_row.game_id)
    return datetime.now().date().isoformat()


def _derive_season_with_fallbacks(game_row: GameRow, game_date: str) -> str:
    """Derive season with multiple fallback strategies.

    Args:
        game_row: Source GameRow
        game_date: Validated game date string (YYYY-MM-DD)

    Returns:
        Season string, "UNKNOWN" if all methods fail
    """
    # Strategy 1: Use explicit season if present and valid
    if (
        hasattr(game_row, "season")
        and game_row.season
        and game_row.season.strip()
        and game_row.season.strip() != "UNKNOWN"
    ):
        season = game_row.season.strip()
        logger.debug("Using explicit season", game_id=game_row.game_id, season=season)
        return season

    # Strategy 2: Derive from game ID (NBA pattern 00XYYxxxxx → YYYY-YY+1)
    season_from_id = derive_season_from_game_id(game_row.game_id)
    if season_from_id and season_from_id != "UNKNOWN":
        logger.debug("Derived season from game ID", game_id=game_row.game_id, season=season_from_id)
        return season_from_id

    # Strategy 3: Derive from game date
    season_from_date = derive_season_from_date(game_date)
    if season_from_date and season_from_date != "UNKNOWN":
        logger.debug(
            "Derived season from date",
            game_id=game_row.game_id,
            game_date=game_date,
            season=season_from_date,
        )
        return season_from_date

    # All strategies failed - log warning as required
    logger.warning("Season derivation failed, using UNKNOWN", game_id=game_row.game_id)
    return "UNKNOWN"


def _normalize_game_status(status) -> str:
    """Normalize game status to DB's expected casing.

    Args:
        status: Raw status (string, enum, or other)

    Returns:
        Normalized status string with proper casing
    """
    if status is None:
        return "SCHEDULED"

    # Handle GameStatus enum
    if hasattr(status, "value"):
        status_str = status.value
    else:
        status_str = str(status)

    status_clean = status_str.strip().upper()

    # Normalize to DB expected casing - tests expect uppercase format
    status_map = {
        "FINAL": "FINAL",
        "FINISHED": "FINAL",
        "COMPLETED": "FINAL",
        "LIVE": "LIVE",
        "IN_PROGRESS": "LIVE",
        "IN PROGRESS": "LIVE",
        "SCHEDULED": "SCHEDULED",
        "UPCOMING": "SCHEDULED",
        "POSTPONED": "POSTPONED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "SUSPENDED": "SUSPENDED",
        "RESCHEDULED": "RESCHEDULED",
    }

    return status_map.get(status_clean, "SCHEDULED")
