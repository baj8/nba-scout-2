"""PBP transformers - pure functions with preprocessing and clock handling."""

from typing import Iterable, Dict, Any, List
from ..models.pbp import PbpEvent
from ..utils.preprocess import preprocess_nba_stats_data


def transform_pbp(events: Iterable[Dict[str, Any]], game_id: str) -> List[PbpEvent]:
    """Transform raw PBP events to validated PbpEvent models with clock handling."""
    rows: List[PbpEvent] = []
    
    for e in events:
        # Apply preprocessing to handle mixed data types
        e = preprocess_nba_stats_data(e)
        
        # Extract clock string (preserve as string, don't convert to float)
        clock_raw = e.get("PCTIMESTRING") or e.get("CLOCK") or ""
        
        # Extract team ID with proper null handling
        team_id = None
        if e.get("TEAM_ID") not in (None, "", 0):
            team_id = int(e["TEAM_ID"])
        elif e.get("PLAYER1_TEAM_ID") not in (None, "", 0):
            team_id = int(e["PLAYER1_TEAM_ID"])
        
        # Extract player ID with proper null handling
        player1_id = None
        if e.get("PLAYER1_ID") not in (None, "", 0):
            player1_id = int(e["PLAYER1_ID"])
        
        # Extract action types with proper null handling
        action_type = None
        if e.get("EVENTMSGTYPE") not in (None, ""):
            action_type = int(e["EVENTMSGTYPE"])
            
        action_subtype = None
        if e.get("EVENTMSGACTIONTYPE") not in (None, ""):
            action_subtype = int(e["EVENTMSGACTIONTYPE"])
        
        # Extract description from various possible fields
        description = (
            e.get("HOMEDESCRIPTION") or 
            e.get("NEUTRALDESCRIPTION") or 
            e.get("VISITORDESCRIPTION") or
            e.get("DESCRIPTION")
        )
        
        # Create PbpEvent - clock_seconds will be auto-derived by the model
        rows.append(PbpEvent(
            game_id=game_id,
            event_num=int(e["EVENTNUM"]),
            period=int(e["PERIOD"]),
            clock=clock_raw,  # Keep as string - no float conversion!
            team_id=team_id,
            player1_id=player1_id,
            action_type=action_type,
            action_subtype=action_subtype,
            description=description
        ))
    
    return rows