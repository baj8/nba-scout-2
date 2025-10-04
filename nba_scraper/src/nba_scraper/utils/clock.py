"""Clock parsing and elapsed time calculation utilities with fractional seconds support."""

import re
from functools import lru_cache

# Regex patterns for clock parsing - now with proper bounds validation
_CLOCK_RE = re.compile(r"^(\d{1,2}):([0-5]\d)(?:\.(\d{1,3}))?$")
_ISO_DURATION_RE = re.compile(r"^PT(\d+)M(\d+)(?:\.(\d{1,3}))?S$")


@lru_cache(maxsize=4096)
def parse_clock_to_seconds(clock: str) -> float | None:
    """Parse clock string to seconds, supporting fractional seconds.
    
    Supports formats:
    - MM:SS (e.g., "12:34")
    - MM:SS.fff (e.g., "12:34.567")
    - PTMMS (e.g., "PT12M34S")
    - PTMM.fffS (e.g., "PT12M34.567S")
    
    Args:
        clock: Clock string to parse
        
    Returns:
        Total seconds as float, or None if parsing fails
    """
    if not isinstance(clock, str):
        return None
    
    s = clock.strip()
    if not s:
        return None

    # Try MM:SS.fff format first
    m = _CLOCK_RE.match(s)
    if m:
        mins, secs = int(m.group(1)), int(m.group(2))
        # Validate realistic bounds for basketball (max 24 minutes for regulation periods)
        if mins > 24:  # Reject unreasonable minutes for basketball
            return None
        frac = m.group(3)
        frac_val = 0.0 if not frac else float(f"0.{frac}")
        return mins * 60 + secs + frac_val

    # Try ISO duration format PT11M45.123S
    m = _ISO_DURATION_RE.match(s)
    if m:
        mins, secs = int(m.group(1)), int(m.group(2))
        # Validate realistic bounds for basketball
        if mins > 24 or secs > 59:  # Basketball periods are max 24 minutes
            return None
        frac = m.group(3)
        frac_val = 0.0 if not frac else float(f"0.{frac}")
        return mins * 60 + secs + frac_val

    return None


def period_length_seconds(period: int) -> int:
    """Get the length of a period in seconds.
    
    Args:
        period: Period number (1-4 for regulation, 5+ for overtime)
        
    Returns:
        Period length in seconds (720 for regulation, 300 for OT)
    """
    return 12 * 60 if period <= 4 else 5 * 60


def compute_seconds_elapsed(
    period: int, 
    clock_seconds: float | None, 
    *, 
    mode: str = "remaining"
) -> float | None:
    """Compute total seconds elapsed in game from period and clock time.
    
    Args:
        period: Period number (1-based)
        clock_seconds: Time value in seconds
        mode: "remaining" (NBA default) or "elapsed" interpretation
        
    Returns:
        Total seconds elapsed from start of game, or None if invalid
        
    Notes:
        - Automatically detects and corrects mode mismatches
        - Clamps negative results to 0.0
        - Uses regulation (12min) and overtime (5min) period lengths
    """
    if clock_seconds is None or period is None:
        return None
    
    # Calculate base seconds from completed periods
    if period <= 4:
        base = (period - 1) * 12 * 60  # Regulation periods
    else:
        base = 4 * 12 * 60 + (period - 5) * 5 * 60  # 4 reg + OT periods

    period_len = period_length_seconds(period)
    
    # Calculate elapsed time in current period
    if mode == "elapsed":
        elapsed_in_period = float(clock_seconds)
    else:  # "remaining" mode (NBA default)
        elapsed_in_period = period_len - float(clock_seconds)

    total = base + elapsed_in_period
    
    # Auto-correct if result is negative (wrong mode assumption)
    if total < 0:
        if mode == "remaining":
            elapsed_in_period = float(clock_seconds)
        else:
            elapsed_in_period = period_len - float(clock_seconds)
        
        total = base + elapsed_in_period
        if total < 0:
            total = 0.0
    
    return total


def format_seconds_as_clock(seconds: float) -> str:
    """Format seconds as MM:SS.fff clock string.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted clock string (e.g., "12:34.567")
    """
    if seconds is None or seconds < 0:
        return "0:00"
    
    mins = int(seconds // 60)
    secs = seconds % 60
    
    if secs == int(secs):
        return f"{mins}:{int(secs):02d}"
    else:
        return f"{mins}:{secs:06.3f}"


def validate_clock_format(clock_str: str) -> bool:
    """Validate if a string is in valid clock format.
    
    Args:
        clock_str: String to validate
        
    Returns:
        True if valid clock format, False otherwise
    """
    if not isinstance(clock_str, str):
        return False
    
    # Use parse_clock_to_seconds to validate - if it parses successfully, it's valid
    return parse_clock_to_seconds(clock_str) is not None