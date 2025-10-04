"""Lineup transformers - pure functions with preprocessing."""

from typing import List, Dict, Any
from ..models.lineups import LineupStint
from ..utils.preprocess import preprocess_nba_stats_data


def transform_lineups(raw: list[dict], game_id: str) -> list[LineupStint]:
    """Transform raw lineup data to validated LineupStint models."""
    out = []
    for r in raw:
        r = preprocess_nba_stats_data(r)
        out.append(LineupStint(
            game_id=game_id,
            team_id=int(r["TEAM_ID"]),
            period=int(r["PERIOD"]),
            lineup=[int(p) for p in r["PLAYER_IDS"]],
            seconds_played=int(r["SECS"])
        ))
    return out