"""Utilities for NBA data processing with robust preprocessing and parsing."""

from .clock import (
    calculate_seconds_elapsed,
    ms_to_seconds,
    normalize_clock_format,
    parse_clock_to_ms,
    parse_game_clock,
    period_length_ms,
    seconds_to_ms,
    validate_clock_bounds,
)
from .preprocess import (
    normalize_clock_time,
    normalize_player_id,
    normalize_team_id,
    preprocess_nba_stats_data,
)
from .season import (
    coalesce_season,
    derive_season_from_date,
    derive_season_from_game_id,
    derive_season_smart,
    get_current_nba_season,
    validate_season_format,
)


# Legacy imports for backwards compatibility
def safe_str_strip(x):
    return str(x).strip() if x is not None else ""


safe_int_parse = normalize_team_id  # Alias for backwards compatibility


def safe_float_parse(x):
    return float(x) if x is not None else None


def safe_bool_parse(x):
    return bool(x) if x is not None else False


def normalize_team_tricode(x):
    return str(x).upper().strip() if x else ""


def clean_player_name(x):
    return str(x).strip() if x else ""


def normalize_description(x):
    return str(x).strip() if x else ""


def extract_numeric_from_text(x):
    return "".join(c for c in str(x) if c.isdigit()) if x else ""


def validate_game_id_format(x):
    return len(str(x)) == 10 and str(x).startswith("00") if x else False


# Legacy clock parsing aliases
parse_clock_to_seconds = parse_game_clock
compute_seconds_elapsed = calculate_seconds_elapsed
validate_clock_format = validate_clock_bounds
format_seconds_as_clock = normalize_clock_format


def period_length_seconds(period):
    return period_length_ms(period) // 1000


parse_fractional_seconds = parse_game_clock


# Legacy season aliases
def get_season_boundaries(season):
    return (f"{season}-10-01", f"{int(season)+1}-06-30") if season else None


def is_playoff_game(game_id):
    return str(game_id)[2:3] in ["3", "4"] if game_id else False


__all__ = [
    # New utilities
    "preprocess_nba_stats_data",
    "normalize_team_id",
    "normalize_player_id",
    "normalize_clock_time",
    # Clock utilities
    "parse_clock_to_ms",
    "period_length_ms",
    "parse_clock_to_seconds",
    "compute_seconds_elapsed",
    "validate_clock_format",
    "format_seconds_as_clock",
    "period_length_seconds",
    # Season utilities
    "derive_season_from_game_id",
    "derive_season_from_date",
    "derive_season_smart",
    "validate_season_format",
    "get_current_nba_season",
    "coalesce_season",
    # Legacy compatibility
    "safe_str_strip",
    "safe_int_parse",
    "safe_float_parse",
    "safe_bool_parse",
    "normalize_team_tricode",
    "clean_player_name",
    "normalize_description",
    "extract_numeric_from_text",
    "validate_game_id_format",
    "parse_game_clock",
    "calculate_seconds_elapsed",
    "normalize_clock_format",
    "parse_fractional_seconds",
    "validate_clock_bounds",
    "get_season_boundaries",
    "is_playoff_game",
]
