"""Season derivation utilities for NBA games."""

from __future__ import annotations
from datetime import datetime
import re
from typing import Optional

# NBA game ID pattern: 00[1-9]YYxxxxx where YY is season start year's last 2 digits
# 001=Pre, 002=Reg, 003=Playoffs, 004=Play-In (recent seasons)
_GAME_ID_RE = re.compile(r"^00[1-9](\d{2})\d{5}$")

def derive_season_from_game_id(game_id: str) -> Optional[str]:
    """
    Derive season from NBA game ID using regex pattern matching.
    
    Args:
        game_id: NBA game ID (e.g., "0022300001")
        
    Returns:
        Season string like "2023-24" or None if invalid format
    """
    if not isinstance(game_id, str):
        return None
    
    m = _GAME_ID_RE.match(game_id)
    if not m:
        return None
    
    yy = int(m.group(1))
    start_year = 2000 + yy
    end_yy = (yy + 1) % 100
    
    return f"{start_year}-{end_yy:02d}"

def derive_season_from_date(game_date: str) -> Optional[str]:
    """
    Derive season from game date string.
    
    NBA seasons typically run October-June, so:
    - October-December -> current year season  
    - January-June -> previous year season
    - July-September -> ambiguous, assume previous year
    
    Args:
        game_date: Date string in "YYYY-MM-DD" format
        
    Returns:
        Season string like "2023-24" or None if invalid date
    """
    try:
        d = datetime.strptime(game_date, "%Y-%m-%d")
        
        if d.month >= 10:  # October, November, December
            start = d.year
        elif d.month <= 6:  # January through June
            start = d.year - 1
        else:  # July, August, September - off-season
            start = d.year - 1
            
        end_year = start + 1
        return f"{start}-{end_year % 100:02d}"
        
    except (ValueError, TypeError):
        return None

def coalesce_season(game_id: Optional[str], game_date: Optional[str]) -> Optional[str]:
    """
    Derive season from game ID or date with fallback priority.
    
    Args:
        game_id: NBA game ID to try first
        game_date: Game date to try as fallback
        
    Returns:
        Season string or None if both methods fail
    """
    return (
        (derive_season_from_game_id(game_id) if game_id else None)
        or (derive_season_from_date(game_date) if game_date else None)
    )

def validate_season_format(season: str) -> bool:
    """
    Validate season string format.
    
    Args:
        season: Season string to validate
        
    Returns:
        True if valid format like "2023-24"
    """
    if not isinstance(season, str):
        return False
    
    pattern = r"^\d{4}-\d{2}$"
    return bool(re.match(pattern, season))

def get_season_years(season: str) -> tuple[int, int] | None:
    """
    Extract start and end years from season string.
    
    Args:
        season: Season string like "2023-24"
        
    Returns:
        Tuple of (start_year, end_year) or None if invalid
    """
    if not validate_season_format(season):
        return None
    
    try:
        start_str, end_str = season.split('-')
        start_year = int(start_str)
        end_year = 2000 + int(end_str) if int(end_str) < 50 else 1900 + int(end_str)
        return (start_year, end_year)
    except (ValueError, IndexError):
        return None