"""Clock parsing and time calculation utilities for NBA games."""

import re
from functools import lru_cache
from typing import Optional

_CLOCK_RE = re.compile(r"^(\d{1,2}):([0-5]\d)(?:\.(\d{1,3}))?$")
_ISO_DURATION_RE = re.compile(r"^PT(\d+)M(\d+)(?:\.(\d{1,3}))?S$")

@lru_cache(maxsize=4096)
def parse_clock_to_seconds(clock: str) -> Optional[float]:
    """
    Parse clock string to seconds, supporting fractional seconds.
    
    Supports formats:
    - "MM:SS" -> minutes:seconds 
    - "M:SS" -> minutes:seconds
    - "MM:SS.fff" -> minutes:seconds.fractional
    - "PTMM:SSS" -> ISO 8601 duration format
    
    Args:
        clock: Clock string to parse
        
    Returns:
        Total seconds as float, or None if unparseable
    """
    if not isinstance(clock, str):
        return None
    
    s = clock.strip()
    
    # Try MM:SS[.fff] format first
    m = _CLOCK_RE.match(s)
    if m:
        mins, secs = int(m.group(1)), int(m.group(2))
        frac = m.group(3)
        frac_val = 0.0 if not frac else float(f"0.{frac}")
        return mins * 60 + secs + frac_val
    
    # Try ISO duration format PT(M+)M(S+)S[.fff]
    m = _ISO_DURATION_RE.match(s)
    if m:
        mins, secs = int(m.group(1)), int(m.group(2))
        frac = m.group(3)
        frac_val = 0.0 if not frac else float(f"0.{frac}")
        return mins * 60 + secs + frac_val
    
    return None

def period_length_seconds(period: int) -> int:
    """Get the length of a period in seconds."""
    return 12 * 60 if period <= 4 else 5 * 60

def compute_seconds_elapsed(period: int, clock_seconds: Optional[float], *, mode: str = "remaining") -> Optional[float]:
    """
    Compute total seconds elapsed in game from period and clock.
    
    Args:
        period: Period number (1-4 regulation, 5+ overtime)
        clock_seconds: Clock time in seconds
        mode: "remaining" (NBA default) treats clock as time-left-in-period,
              "elapsed" treats clock as time-spent-in-period
              
    Returns:
        Total seconds elapsed since game start, or None if invalid input
        
    Notes:
        If result would be negative (feed mismatch), auto-corrects by flipping
        mode once and clamps result >= 0.
    """
    if clock_seconds is None or period is None:
        return None
    
    # Calculate base seconds from completed periods
    if period <= 4:
        base = (period - 1) * 12 * 60
    else:
        base = 4 * 12 * 60 + (period - 5) * 5 * 60
    
    period_len = period_length_seconds(period)
    
    # Calculate elapsed time in current period
    if mode == "elapsed":
        elapsed_in_period = float(clock_seconds)
    else:  # mode == "remaining"
        elapsed_in_period = period_len - float(clock_seconds)
    
    total = base + elapsed_in_period
    
    # Auto-correct if negative (feed format mismatch)
    if total < 0:
        # Flip mode once and retry
        if mode == "remaining":
            elapsed_in_period = float(clock_seconds)
        else:
            elapsed_in_period = period_len - float(clock_seconds)
        
        total = base + elapsed_in_period
        
        # Clamp to non-negative
        if total < 0:
            total = 0.0
    
    return total

def format_clock_display(seconds: float) -> str:
    """Format seconds back to MM:SS display format."""
    if seconds < 0:
        seconds = 0
    
    mins = int(seconds // 60)
    secs = seconds % 60
    
    if secs == int(secs):
        return f"{mins}:{int(secs):02d}"
    else:
        return f"{mins}:{secs:06.3f}"

def validate_clock_format(clock: str) -> bool:
    """Validate that clock string is in expected format."""
    if not isinstance(clock, str):
        return False
    
    return parse_clock_to_seconds(clock) is not None