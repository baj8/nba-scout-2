"""Utilities for NBA data processing with robust preprocessing and parsing."""

from .preprocess import (
    preprocess_nba_stats_data,
    normalize_team_id,
    normalize_player_id,
    normalize_clock_time,
)
from .clock import (
    parse_clock_to_seconds,
    compute_seconds_elapsed,
    validate_clock_format,
    format_seconds_as_clock,
    period_length_seconds,
)
from .season import (
    derive_season_from_game_id,
    derive_season_from_date,
    derive_season_smart,
    validate_season_format,
    get_current_nba_season,
    coalesce_season,
)

# Legacy imports for backwards compatibility
safe_str_strip = lambda x: str(x).strip() if x is not None else ""
safe_int_parse = normalize_team_id  # Alias for backwards compatibility
safe_float_parse = lambda x: float(x) if x is not None else None
safe_bool_parse = lambda x: bool(x) if x is not None else False
normalize_team_tricode = lambda x: str(x).upper().strip() if x else ""
clean_player_name = lambda x: str(x).strip() if x else ""
normalize_description = lambda x: str(x).strip() if x else ""
extract_numeric_from_text = lambda x: "".join(c for c in str(x) if c.isdigit()) if x else ""
validate_game_id_format = lambda x: len(str(x)) == 10 and str(x).startswith('00') if x else False

# Legacy clock parsing aliases
parse_game_clock = parse_clock_to_seconds
calculate_seconds_elapsed = compute_seconds_elapsed
normalize_clock_format = normalize_clock_time
parse_fractional_seconds = parse_clock_to_seconds
validate_clock_bounds = validate_clock_format

# Legacy season aliases
get_season_boundaries = lambda season: (f"{season}-10-01", f"{int(season)+1}-06-30") if season else None
is_playoff_game = lambda game_id: str(game_id)[2:3] in ['3', '4'] if game_id else False

__all__ = [
    # New utilities
    "preprocess_nba_stats_data",
    "normalize_team_id",
    "normalize_player_id", 
    "normalize_clock_time",
    "parse_clock_to_seconds",
    "compute_seconds_elapsed",
    "validate_clock_format", 
    "format_seconds_as_clock",
    "period_length_seconds",
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