"""Date normalization utilities for consistent date handling."""

from datetime import datetime, date
from typing import Union, Optional


def to_date_str(date_input: Union[str, date, datetime], *, field_name: str = "date") -> str:
    """Convert various date inputs to YYYY-MM-DD string format.
    
    Args:
        date_input: Date in various formats (string, date, datetime)
        field_name: Field name for error context
        
    Returns:
        Date string in YYYY-MM-DD format
        
    Raises:
        ValueError: If date cannot be parsed or converted
    """
    if isinstance(date_input, str):
        # Handle string dates
        date_clean = date_input.strip()
        
        # Try common formats
        formats_to_try = [
            "%Y-%m-%d",        # 2024-10-15
            "%m/%d/%Y",        # 10/15/2024
            "%m/%d/%y",        # 10/15/24
            "%Y-%m-%d %H:%M:%S",  # 2024-10-15 15:30:00 (extract date part)
        ]
        
        for fmt in formats_to_try:
            try:
                parsed_date = datetime.strptime(date_clean, fmt).date()
                return parsed_date.isoformat()
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse {field_name}: {date_input}")
    
    elif isinstance(date_input, datetime):
        return date_input.date().isoformat()
    
    elif isinstance(date_input, date):
        return date_input.isoformat()
    
    else:
        raise ValueError(f"Invalid {field_name} type: {type(date_input)}")


def derive_season_from_game_id(game_id: str) -> Optional[str]:
    """Extract season from NBA game ID format.
    
    NBA game IDs follow pattern: 00XYYxxxxx where YY is the season start year
    Example: 0022400001 -> 24 -> 2024-25 season
    
    Args:
        game_id: NBA game ID
        
    Returns:
        Season string like "2024-25" or None if parsing fails
    """
    try:
        if not game_id or len(game_id) < 5:
            return None
        
        # Extract the YY part (positions 3-4 in 00XYYxxxxx)
        year_part = game_id[3:5]
        if not year_part.isdigit():
            return None
        
        # Convert to full year (assuming 21st century)
        year = 2000 + int(year_part)
        
        # NBA season format: start_year-end_year_suffix
        next_year_suffix = str(year + 1)[-2:]
        season = f"{year}-{next_year_suffix}"
        
        return season
        
    except (ValueError, IndexError):
        return None


def derive_season_from_date(date_input: Union[str, date, datetime]) -> Optional[str]:
    """Derive NBA season from game date.
    
    NBA seasons run from October to June of the following year.
    Example: 2024-01-15 -> 2023-24 season
    
    Args:
        date_input: Date in various formats
        
    Returns:
        Season string like "2023-24" or None if parsing fails
    """
    try:
        if isinstance(date_input, str):
            # Parse string date
            if len(date_input) >= 10:  # YYYY-MM-DD
                year, month, day = map(int, date_input[:10].split('-'))
            else:
                return None
        elif isinstance(date_input, datetime):
            year, month = date_input.year, date_input.month
        elif isinstance(date_input, date):
            year, month = date_input.year, date_input.month
        else:
            return None
        
        # NBA season logic: October-December = current season, January-September = previous season
        if month >= 10:  # October, November, December
            season_start = year
        else:  # January through September
            season_start = year - 1
        
        season_end_suffix = str(season_start + 1)[-2:]
        return f"{season_start}-{season_end_suffix}"
        
    except (ValueError, IndexError, TypeError):
        return None