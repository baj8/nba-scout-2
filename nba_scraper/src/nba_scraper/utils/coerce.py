"""Centralized coercion utilities for robust data type conversion."""

from __future__ import annotations

from typing import Any, Optional

# Common null/empty representations found in NBA data sources
_NULLS = {None, "", "-", "—", "N/A", "NA", "null", "NULL", "None", "NONE", "--"}


def _is_null(x: Any) -> bool:
    """Check if a value represents null/empty data.

    Args:
        x: Value to check

    Returns:
        True if value represents null/empty data
    """
    if x is None:
        return True

    # Convert to string and check common null representations
    s = str(x).strip()
    return s in _NULLS or s == ""


def to_int_or_none(x: Any) -> Optional[int]:
    """Convert value to integer or None if null/invalid.

    Handles common NBA data formats:
    - Empty/null values: None, "", "-", "—", "N/A" → None
    - Comma-separated numbers: "1,234" → 1234
    - Float strings: "12.0" → 12
    - Already integers: 42 → 42

    Args:
        x: Value to convert

    Returns:
        Integer value or None if conversion fails or value is null
    """
    if _is_null(x):
        return None

    try:
        # Remove commas and whitespace
        s = str(x).replace(",", "").strip()

        # Handle float strings by converting to float first
        return int(float(s))
    except (ValueError, TypeError, OverflowError):
        return None


def to_float_or_none(x: Any) -> Optional[float]:
    """Convert value to float or None if null/invalid.

    Handles common NBA data formats:
    - Empty/null values: None, "", "-", "—", "N/A" → None
    - Comma-separated numbers: "1,234.56" → 1234.56
    - Percentage strings: "45.6%" → 45.6 (percentage sign stripped)
    - Already floats: 42.5 → 42.5

    Args:
        x: Value to convert

    Returns:
        Float value or None if conversion fails or value is null
    """
    if _is_null(x):
        return None

    try:
        # Remove commas, percentage signs, and whitespace
        s = str(x).replace(",", "").replace("%", "").strip()
        result = float(s)

        # Check for NaN or infinity values and treat as None
        if not (result == result):  # NaN check (NaN != NaN)
            return None
        if result == float("inf") or result == float("-inf"):
            return None

        return result
    except (ValueError, TypeError, OverflowError):
        return None


def to_str_or_none(x: Any) -> Optional[str]:
    """Convert value to string or None if null/empty.

    Args:
        x: Value to convert

    Returns:
        String value or None if value represents null/empty data
    """
    if _is_null(x):
        return None

    # Convert to string and strip whitespace
    result = str(x).strip()

    # Double-check in case str(x) produced a null representation
    return result if result and result not in _NULLS else None


def to_bool_or_none(x: Any) -> Optional[bool]:
    """Convert value to boolean or None if null/invalid.

    Handles common boolean representations:
    - True values: True, "true", "True", "TRUE", "1", 1, "yes", "YES"
    - False values: False, "false", "False", "FALSE", "0", 0, "no", "NO"
    - Null values: None, "", "-" → None

    Args:
        x: Value to convert

    Returns:
        Boolean value or None if conversion fails or value is null
    """
    if _is_null(x):
        return None

    # Handle boolean values directly
    if isinstance(x, bool):
        return x

    # Convert to string for text-based boolean checks
    s = str(x).lower().strip()

    # True representations
    if s in {"true", "1", "yes", "y", "on", "enabled"}:
        return True

    # False representations
    if s in {"false", "0", "no", "n", "off", "disabled"}:
        return False

    # Invalid boolean representation
    return None


def safe_divide(numerator: Any, denominator: Any) -> Optional[float]:
    """Safely divide two values, returning None for invalid operations.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Division result or None if either value is null or division by zero
    """
    num = to_float_or_none(numerator)
    den = to_float_or_none(denominator)

    if num is None or den is None or den == 0:
        return None

    return num / den


def safe_percentage(
    numerator: Any, denominator: Any, multiply_by_100: bool = True
) -> Optional[float]:
    """Safely calculate percentage, returning None for invalid operations.

    Args:
        numerator: Numerator value
        denominator: Denominator value
        multiply_by_100: Whether to multiply result by 100 (default True)

    Returns:
        Percentage value or None if calculation is invalid
    """
    result = safe_divide(numerator, denominator)

    if result is None:
        return None

    return result * 100 if multiply_by_100 else result
