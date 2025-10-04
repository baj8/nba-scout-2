"""Clock parsing utilities for NBA game time formats."""

import re
from typing import Optional, Tuple, Union
from .preprocessing import safe_str_strip, safe_float_parse


def parse_game_clock(clock_str: Union[str, None]) -> Optional[float]:
    """Parse NBA game clock string to total seconds remaining.
    
    Handles formats like:
    - "12:00" -> 720.0
    - "2:30.5" -> 150.5 
    - "0:45" -> 45.0
    - "PT12M00.00S" -> 720.0
    
    Args:
        clock_str: Clock string in various NBA formats
        
    Returns:
        Total seconds as float, or None if invalid
    """
    if not clock_str:
        return None
    
    cleaned = safe_str_strip(clock_str)
    if not cleaned:
        return None
    
    # Handle PT format (e.g., "PT12M00.00S")
    pt_match = re.match(r'PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', cleaned)
    if pt_match:
        minutes_str, seconds_str = pt_match.groups()
        minutes = float(minutes_str) if minutes_str else 0.0
        seconds = float(seconds_str) if seconds_str else 0.0
        return minutes * 60 + seconds
    
    # Handle standard MM:SS or MM:SS.f format
    time_match = re.match(r'(\d+):(\d+(?:\.\d+)?)', cleaned)
    if time_match:
        minutes_str, seconds_str = time_match.groups()
        minutes = float(minutes_str)
        seconds = float(seconds_str)
        
        # Validate seconds don't exceed 59.999
        if seconds >= 60:
            return None
            
        return minutes * 60 + seconds
    
    # Handle seconds only (e.g., "45.5")
    seconds_only = safe_float_parse(cleaned)
    if seconds_only is not None and seconds_only >= 0:
        return seconds_only
    
    return None


def calculate_seconds_elapsed(period: int, clock_remaining: float, 
                            period_length: int = 12) -> Optional[float]:
    """Calculate total seconds elapsed in game from period and clock.
    
    Args:
        period: Current period (1-4 for regulation, 5+ for overtime)
        clock_remaining: Seconds remaining in current period
        period_length: Minutes per period (12 for NBA)
        
    Returns:
        Total seconds elapsed since game start, or None if invalid
    """
    if period is None or clock_remaining is None:
        return None
    
    if period < 1 or clock_remaining < 0:
        return None
    
    period_seconds = period_length * 60
    
    # Regulation periods (1-4)
    if period <= 4:
        completed_periods = period - 1
        elapsed_in_current = period_seconds - clock_remaining
    else:
        # Overtime periods (5+)
        completed_regulation = 4 * period_seconds
        completed_ot_periods = (period - 5) * (5 * 60)  # 5 min OT periods
        elapsed_in_current = (5 * 60) - clock_remaining
        return completed_regulation + completed_ot_periods + elapsed_in_current
    
    return completed_periods * period_seconds + elapsed_in_current


def normalize_clock_format(clock_str: Union[str, None]) -> Optional[str]:
    """Normalize clock string to standard MM:SS.f format.
    
    Args:
        clock_str: Clock string in various formats
        
    Returns:
        Normalized clock string or None if invalid
    """
    seconds = parse_game_clock(clock_str)
    if seconds is None:
        return None
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    
    # Format with fractional seconds if present
    if remaining_seconds == int(remaining_seconds):
        return f"{minutes}:{int(remaining_seconds):02d}"
    else:
        return f"{minutes}:{remaining_seconds:05.1f}"


def parse_fractional_seconds(time_str: Union[str, None]) -> Optional[Tuple[int, float]]:
    """Parse time string into minutes and fractional seconds.
    
    Args:
        time_str: Time string like "2:30.5"
        
    Returns:
        Tuple of (minutes, seconds) or None if invalid
    """
    if not time_str:
        return None
    
    cleaned = safe_str_strip(time_str)
    if not cleaned:
        return None
    
    # Match MM:SS.f pattern
    match = re.match(r'(\d+):(\d+(?:\.\d+)?)', cleaned)
    if not match:
        return None
    
    minutes_str, seconds_str = match.groups()
    minutes = int(minutes_str)
    seconds = float(seconds_str)
    
    # Validate bounds
    if minutes < 0 or seconds < 0 or seconds >= 60:
        return None
    
    return (minutes, seconds)


def validate_clock_bounds(period: int, clock_remaining: float, 
                         period_length: int = 12) -> bool:
    """Validate that clock time is within valid bounds for the period.
    
    Args:
        period: Current period
        clock_remaining: Seconds remaining in period
        period_length: Minutes per period
        
    Returns:
        True if clock time is valid for the period
    """
    if period is None or clock_remaining is None:
        return False
    
    if period < 1 or clock_remaining < 0:
        return False
    
    # Regulation periods
    if 1 <= period <= 4:
        max_seconds = period_length * 60
        return 0 <= clock_remaining <= max_seconds
    
    # Overtime periods (5 minutes each)
    elif period >= 5:
        return 0 <= clock_remaining <= 300  # 5 minutes
    
    return False