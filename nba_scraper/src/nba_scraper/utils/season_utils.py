"""Season utilities for scheduler."""
from __future__ import annotations
from datetime import date
from typing import Tuple


def season_bounds(season: str) -> Tuple[date, date]:
    """
    Get start and end dates for a season.
    
    Args:
        season: Season string (e.g., '2024-25')
        
    Returns:
        Tuple of (start_date, end_date) for the season
        
    Example:
        >>> season_bounds('2024-25')
        (date(2024, 10, 1), date(2025, 9, 30))
    """
    # Parse season string like '2024-25'
    parts = season.split('-')
    if len(parts) != 2:
        raise ValueError(f"Invalid season format: {season}. Expected format: 'YYYY-YY'")
    
    start_year = int(parts[0])
    end_year = int('20' + parts[1]) if len(parts[1]) == 2 else int(parts[1])
    
    # NBA season runs from October 1 to September 30
    start = date(start_year, 10, 1)
    end = date(end_year, 9, 30)
    
    return start, end
