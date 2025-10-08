"""Game discovery utilities for scheduler."""
from __future__ import annotations
from datetime import date
from typing import List
import logging

logger = logging.getLogger(__name__)


def discover_game_ids_for_date(d: date) -> List[str]:
    """
    Discover game IDs for a specific date.
    
    This is a stub that should be wired to your existing discovery logic
    (e.g., NBA schedule endpoint, cached table, etc.).
    
    Args:
        d: Date to discover games for (in ET timezone)
        
    Returns:
        Sorted list of game_id strings for the date
    """
    # TODO: Wire to existing discovery logic
    # Example: query games table or call NBA API schedule endpoint
    logger.debug("discover_game_ids_for_date", extra={"date": d.isoformat()})
    return []


def discover_game_ids_for_date_range(start: date, end: date) -> List[str]:
    """
    Discover game IDs for a date range.
    
    Args:
        start: Start date (inclusive, in ET timezone)
        end: End date (inclusive, in ET timezone)
        
    Returns:
        Sorted list of game_id strings for the date range
    """
    # TODO: Wire to existing discovery logic
    logger.debug("discover_game_ids_for_date_range", extra={
        "start": start.isoformat(),
        "end": end.isoformat()
    })
    return []
