"""Clock parsing utilities for NBA time formats."""

import re
from typing import Optional

# Compiled regex for validation
_CLOCK_FIELD_RE = re.compile(r"^\d{1,2}:[0-5]\d(?:\.\d{1,3})?$|^PT\d+M\d+(?:\.\d{1,3})?S$")


def validate_clock_format(value: str) -> str:
    """Back-compat helper for older imports. Raises ValueError on bad format."""
    if value and _CLOCK_FIELD_RE.match(value.strip()):
        return value
    raise ValueError(f"Invalid clock format: {value!r}")


def format_seconds_as_clock(seconds: Optional[float]) -> str:
    """Format seconds back to clock string MM:SS format.
    
    Args:
        seconds: Seconds to format (can be None or negative)
        
    Returns:
        Formatted clock string like "5:30" or "0:00"
    """
    if seconds is None or seconds < 0:
        return "0:00"
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    
    # Handle fractional seconds
    if remaining_seconds != int(remaining_seconds):
        return f"{minutes}:{int(remaining_seconds):02d}.{int((remaining_seconds % 1) * 1000):03d}"
    else:
        return f"{minutes}:{int(remaining_seconds):02d}"


def parse_clock_to_seconds(time_str: str) -> Optional[float]:
    """Parse various clock formats to seconds remaining.
    
    Supports:
    - M:SS (e.g., "5:30")
    - MM:SS (e.g., "12:00")
    - MM:SS.fff (e.g., "11:45.500")
    - PTxMxS[.fff] (e.g., "PT11M45S" or "PT11M45.500S")
    
    Args:
        time_str: Time string in various formats
        
    Returns:
        Seconds remaining as float, or None if parsing fails
    """
    if not time_str or not isinstance(time_str, str):
        return None
        
    time_clean = time_str.strip()
    
    # Handle ISO 8601 duration format: PT11M45S or PT11M45.500S
    iso_match = re.match(r'^PT(\d+)M(\d+(?:\.\d+)?)S$', time_clean)
    if iso_match:
        minutes = int(iso_match.group(1))
        seconds = float(iso_match.group(2))
        return minutes * 60 + seconds
    
    # Handle MM:SS or MM:SS.fff format
    clock_match = re.match(r'^(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?$', time_clean)
    if clock_match:
        minutes = int(clock_match.group(1))
        seconds = int(clock_match.group(2))
        fractional = int(clock_match.group(3) or 0)
        
        # Convert fractional part to decimal (e.g., 500 milliseconds = 0.5 seconds)
        if fractional > 0:
            fractional_seconds = fractional / (10 ** len(str(fractional)))
        else:
            fractional_seconds = 0.0
            
        return minutes * 60 + seconds + fractional_seconds
    
    return None


def compute_seconds_elapsed(time_remaining: str, period: int) -> Optional[float]:
    """Compute seconds elapsed using NBA time remaining logic.
    
    Args:
        time_remaining: Time remaining in period (various formats)
        period: Period number (1-4 for regulation, 5+ for OT)
        
    Returns:
        Seconds elapsed in period, or None if parsing fails
    """
    clock_seconds = parse_clock_to_seconds(time_remaining)
    if clock_seconds is None:
        return None
    
    # Get period length in seconds
    if period <= 4:
        period_length = 12 * 60  # Regulation: 12 minutes = 720 seconds
    else:
        period_length = 5 * 60   # Overtime: 5 minutes = 300 seconds
    
    # Calculate elapsed time
    seconds_elapsed = period_length - clock_seconds
    
    # Safety: auto-flip once if negative (data inconsistency)
    if seconds_elapsed < 0:
        seconds_elapsed = abs(seconds_elapsed)
    
    return float(seconds_elapsed)


def period_length_seconds(period: int) -> int:
    """Get period length in seconds for NBA games.
    
    Args:
        period: Period number (1-4 for regulation, 5+ for OT)
        
    Returns:
        Period length in seconds (720 for regulation, 300 for OT)
    """
    if period <= 4:
        return 12 * 60  # Regulation: 12 minutes = 720 seconds
    else:
        return 5 * 60   # Overtime: 5 minutes = 300 seconds