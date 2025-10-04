"""Shot transformation functions - pure, synchronous."""

from typing import List, Dict, Any
from ..models.shots import ShotEvent
from ..utils.preprocess import preprocess_nba_stats_data, normalize_team_id, normalize_player_id


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
            period = int(s.get("PERIOD", 1))
            
            # Extract shot result
            shot_made_flag = int(s.get("SHOT_MADE_FLAG", 0))
            
            # Extract coordinates
            loc_x = int(s.get("LOC_X", 0))
            loc_y = int(s.get("LOC_Y", 0))
            
            # Optional event number for linking to PBP
            event_num = None
            if s.get("EVENT_NUM") not in (None, "", 0):
                try:
                    event_num = int(s["EVENT_NUM"])
                except (ValueError, TypeError):
                    event_num = None
            
            shot_event = ShotEvent(
                game_id=game_id,
                player_id=player_id,
                team_id=team_id,
                period=period,
                shot_made_flag=shot_made_flag,
                loc_x=loc_x,
                loc_y=loc_y,
                event_num=event_num
            )
            out.append(shot_event)
            
        except Exception:
            # Skip invalid shot records
            continue
    
    return out