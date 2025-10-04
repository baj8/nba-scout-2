"""Utilities for NBA data processing with robust preprocessing and parsing."""

from .preprocessing import (
    safe_str_strip,
    safe_int_parse,
    safe_float_parse,
    safe_bool_parse,
    normalize_team_tricode,
    clean_player_name,
    normalize_description,
    extract_numeric_from_text,
    validate_game_id_format,
)
from .clock_parsing import (
    parse_game_clock,
    calculate_seconds_elapsed,
    normalize_clock_format,
    parse_fractional_seconds,
    validate_clock_bounds,
)
from .season_derivation import (
    derive_season_from_game_id,
    derive_season_from_date,
    validate_season_format,
    get_season_boundaries,
    is_playoff_game,
)

__all__ = [
    # Preprocessing utilities
    "safe_str_strip",
    "safe_int_parse", 
    "safe_float_parse",
    "safe_bool_parse",
    "normalize_team_tricode",
    "clean_player_name",
    "normalize_description",
    "extract_numeric_from_text",
    "validate_game_id_format",
    # Clock parsing utilities
    "parse_game_clock",
    "calculate_seconds_elapsed", 
    "normalize_clock_format",
    "parse_fractional_seconds",
    "validate_clock_bounds",
    # Season derivation utilities
    "derive_season_from_game_id",
    "derive_season_from_date",
    "validate_season_format",
    "get_season_boundaries", 
    "is_playoff_game",
]