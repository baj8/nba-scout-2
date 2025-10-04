"""Shot transformers - pure functions with preprocessing."""

from typing import List, Dict, Any
from ..models.shots import ShotEvent
from ..utils.preprocess import preprocess_nba_stats_data


def transform_shots(raw: list[dict], game_id: str) -> list[ShotEvent]:
    """Transform raw shot data to validated ShotEvent models."""
    out = []
    for s in raw:
        s = preprocess_nba_stats_data(s)
        
        # Extract team_id with fallback handling
        team_id = None
        if s.get("TEAM_ID") not in (None, "", 0):
            team_id = int(s["TEAM_ID"])
        
        # Extract event_num for PBP mapping
        event_num = None
        if s.get("GAME_EVENT_ID") not in (None, ""):
            event_num = int(s["GAME_EVENT_ID"])
        
        out.append(ShotEvent(
            game_id=game_id,
            player_id=int(s["PLAYER_ID"]),
            team_id=team_id,
            period=int(s["PERIOD"]),
            shot_made_flag=int(s["SHOT_MADE_FLAG"]),
            loc_x=int(s["LOC_X"]),
            loc_y=int(s["LOC_Y"]),
            event_num=event_num
        ))
    return out