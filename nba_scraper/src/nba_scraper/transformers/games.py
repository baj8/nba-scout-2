"""Game transformers - pure functions with preprocessing."""

from ..models.games import Game
from ..utils.preprocess import preprocess_nba_stats_data


def transform_game(meta: dict) -> Game:
    """Transform raw game metadata to validated Game model."""
    m = preprocess_nba_stats_data(meta)
    return Game(
        game_id=m["game_id"],
        season=m["season"],
        game_date=m["game_date"],
        home_team_id=int(m["home_team_id"]),
        away_team_id=int(m["away_team_id"]),
        status=m.get("status", "Final")
    )