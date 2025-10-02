"""Pure extraction functions for NBA data sources."""

from .nba_stats import (
    extract_games_from_scoreboard,
    extract_pbp_from_response,
    extract_boxscore_lineups,
)
from .bref import (
    extract_game_outcomes,
    extract_starting_lineups,
    extract_injury_notes,
)
from .gamebooks import (
    extract_referee_assignments,
    extract_referee_alternates,
)

__all__ = [
    # NBA Stats extractors
    "extract_games_from_scoreboard",
    "extract_pbp_from_response", 
    "extract_boxscore_lineups",
    # B-Ref extractors
    "extract_game_outcomes",
    "extract_starting_lineups",
    "extract_injury_notes",
    # Gamebooks extractors
    "extract_referee_assignments",
    "extract_referee_alternates",
]