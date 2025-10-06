"""Pure extraction functions for NBA data sources."""

from .nba_stats import (
    extract_games_from_scoreboard,
    extract_pbp_from_response,
    extract_boxscore_lineups,
    extract_advanced_player_stats,
    extract_misc_player_stats,
    extract_usage_player_stats,
    extract_advanced_team_stats,
    extract_shot_chart_detail,
)
from .boxscore import (
    extract_game_meta,
    extract_game_from_boxscore,
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
    "extract_advanced_player_stats",
    "extract_misc_player_stats", 
    "extract_usage_player_stats",
    "extract_advanced_team_stats",
    "extract_shot_chart_detail",
    # Boxscore extractors
    "extract_game_meta",
    "extract_game_from_boxscore",
    # B-Ref extractors
    "extract_game_outcomes",
    "extract_starting_lineups",
    "extract_injury_notes",
    # Gamebooks extractors
    "extract_referee_assignments",
    "extract_referee_alternates",
]