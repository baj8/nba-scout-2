"""Shot transformation functions - pure, synchronous."""

from typing import Any, Dict, List

from ..models.shots import ShotEvent
from ..utils.coerce import to_int_or_none
from ..utils.preprocess import normalize_player_id, normalize_team_id, preprocess_nba_stats_data


def transform_shots(raw: List[Dict[str, Any]], game_id: str) -> List[ShotEvent]:
    """Transform extracted shot chart data to validated ShotEvent models.

    Args:
        raw: List of dictionaries from extract_shot_chart_detail
        game_id: Game identifier for linking

    Returns:
        List of validated ShotEvent instances with coordinate data
    """
    out = []

    for s in raw:
        # Apply preprocessing to handle mixed data types
        s = preprocess_nba_stats_data(s)

        try:
            # Extract required fields
            player_id = normalize_player_id(s.get("PLAYER_ID"))
            if player_id is None:
                continue  # Skip shots without valid player ID

            team_id = normalize_team_id(s.get("TEAM_ID"))
            period = to_int_or_none(s.get("PERIOD")) or 1

            # Extract shot result using robust coercion
            shot_made_flag = to_int_or_none(s.get("SHOT_MADE_FLAG")) or 0

            # Extract coordinates using robust coercion
            loc_x = to_int_or_none(s.get("LOC_X")) or 0
            loc_y = to_int_or_none(s.get("LOC_Y")) or 0

            # Optional event number for linking to PBP using robust coercion
            event_num = to_int_or_none(s.get("EVENT_NUM"))

            shot_event = ShotEvent(
                game_id=game_id,
                player_id=player_id,
                team_id=team_id,
                period=period,
                shot_made_flag=shot_made_flag,
                loc_x=loc_x,
                loc_y=loc_y,
                event_num=event_num,
            )
            out.append(shot_event)

        except Exception:
            # Skip invalid shot records
            continue

    return out
