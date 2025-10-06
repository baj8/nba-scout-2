"""Date handling utilities with explicit UTC vs local semantics."""

import re
from datetime import datetime, date, timezone
from typing import Tuple, Optional, Union
from dateutil import tz

from ..nba_logging import get_logger

logger = get_logger(__name__)


def derive_game_date_with_provenance(
    game_date_local: Optional[Union[str, date]] = None,
    game_date_utc: Optional[Union[str, datetime]] = None,
    arena_timezone: Optional[str] = None,
    game_id: Optional[str] = None
) -> Tuple[str, str]:
    """Derive game date with explicit provenance tracking.
    
    Policy: Prefer game_date_local if present; else derive from game_date_utc.
    
    Args:
        game_date_local: Local arena date (preferred)
        game_date_utc: UTC datetime to convert if local not available
        arena_timezone: IANA timezone for UTC conversion
        game_id: Game ID for error context
        
    Returns:
        Tuple of (date_str, provenance) where provenance is "local" or "utc"
        
    Raises:
        ValueError: If neither date is usable or format is invalid
    """
    context = f" for game {game_id}" if game_id else ""
    
    # Policy 1: Prefer local date if available
    if game_date_local is not None:
        if isinstance(game_date_local, date):
            date_str = game_date_local.isoformat()
        elif isinstance(game_date_local, str):
            # Validate format
            if not _is_valid_date_format(game_date_local):
                raise ValueError(f"Invalid game_date_local format: {game_date_local}. Expected YYYY-MM-DD{context}")
            date_str = game_date_local.strip()
        else:
            raise ValueError(f"Invalid game_date_local type: {type(game_date_local)}{context}")
        
        logger.debug("Using local game date", game_date=date_str, provenance="local", game_id=game_id)
        return date_str, "local"
    
    # Policy 2: Derive from UTC if available
    if game_date_utc is not None:
        if isinstance(game_date_utc, datetime):
            # If timezone-aware and we have trustworthy arena timezone
            if game_date_utc.tzinfo is not None and arena_timezone:
                try:
                    arena_tz = tz.gettz(arena_timezone)
                    if arena_tz:
                        local_dt = game_date_utc.astimezone(arena_tz)
                        date_str = local_dt.date().isoformat()
                        logger.debug("Converted UTC to local using timezone", 
                                   game_date=date_str, provenance="utc", timezone=arena_timezone, game_id=game_id)
                        return date_str, "utc"
                except Exception as e:
                    logger.warning("Failed to convert UTC to local timezone", 
                                 error=str(e), timezone=arena_timezone, game_id=game_id)
            
            # Fallback: use UTC calendar date
            date_str = game_date_utc.date().isoformat()
            logger.debug("Using UTC calendar date", 
                       game_date=date_str, provenance="utc", note="no_timezone_conversion", game_id=game_id)
            return date_str, "utc"
        
        elif isinstance(game_date_utc, str):
            # Parse string and try again
            try:
                dt = datetime.fromisoformat(game_date_utc.replace('Z', '+00:00'))
                return derive_game_date_with_provenance(
                    game_date_local=None,
                    game_date_utc=dt,
                    arena_timezone=arena_timezone,
                    game_id=game_id
                )
            except ValueError as e:
                raise ValueError(f"Invalid game_date_utc format: {game_date_utc}{context}")
        else:
            raise ValueError(f"Invalid game_date_utc type: {type(game_date_utc)}{context}")
    
    # No usable date provided
    raise ValueError(f"Neither game_date_local nor game_date_utc provided{context}")


def _is_valid_date_format(date_str: str) -> bool:
    """Check if date string is in YYYY-MM-DD format."""
    if not isinstance(date_str, str):
        return False
    
    # Strict regex for YYYY-MM-DD
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if not re.match(pattern, date_str):
        return False
    
    # Validate it parses correctly
    try:
        date.fromisoformat(date_str)
        return True
    except ValueError:
        return False


def validate_date_format(date_str: str, field_name: str) -> str:
    """Validate date format with descriptive error including field name.
    
    Args:
        date_str: Date string to validate
        field_name: Name of the field for error context
        
    Returns:
        Validated date string
        
    Raises:
        ValueError: If date format is invalid
    """
    if not _is_valid_date_format(date_str):
        raise ValueError(f"Invalid {field_name} format: {date_str}. Expected YYYY-MM-DD")
    return date_str.strip()


def derive_season_from_game_id(game_id: str) -> Optional[str]:
    """Derive season from NBA game ID format.
    
    NBA game IDs follow format: TSSSEEEEE where
    - T: Game type (0=preseason, 2=regular, 4=playoffs)
    - SSS: Season year (e.g., 023 for 2023-24 season)
    - EEEEE: Game number within season
    
    Args:
        game_id: NBA game ID
        
    Returns:
        Season string like "2023-24" or None if parsing fails
    """
    if not game_id or len(game_id) != 10:
        return None
        
    try:
        # Extract season digits (positions 1-3)
        season_digits = game_id[1:4]
        
        # Convert to season year (add 2000 to get full year)
        season_year = 2000 + int(season_digits)
        
        # NBA season format: "YYYY-YY" where second year is +1
        next_year = (season_year + 1) % 100
        
        return f"{season_year}-{next_year:02d}"
        
    except (ValueError, IndexError):
        return None


def derive_season_from_date(game_date: Union[str, date]) -> Optional[str]:
    """Derive NBA season from game date.
    
    NBA seasons run from October to June of the following year.
    
    Args:
        game_date: Game date as string (YYYY-MM-DD) or date object
        
    Returns:
        Season string like "2023-24" or None if date is invalid
    """
    try:
        if isinstance(game_date, str):
            date_obj = date.fromisoformat(game_date)
        elif isinstance(game_date, date):
            date_obj = game_date
        else:
            return None
        
        # NBA season logic: Oct-Dec belongs to the season starting that year
        # Jan-Sep belongs to the season that started the previous year
        if date_obj.month >= 10:  # Oct, Nov, Dec
            start_year = date_obj.year
        else:  # Jan-Sep
            start_year = date_obj.year - 1
        
        return f"{start_year}-{str(start_year + 1)[2:]}"
    except (ValueError, AttributeError):
        return None