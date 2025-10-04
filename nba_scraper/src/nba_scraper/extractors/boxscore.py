"""Boxscore extraction functions - shape-only, null-safe."""

from typing import Dict, Any
from ..utils.season import derive_season_smart


def extract_game_from_boxscore(bs: Dict[str, Any]) -> Dict[str, Any]:
    """Extract game metadata from boxscore response - shape only, no preprocessing.
    
    Args:
        bs: Raw boxscore response dictionary
        
    Returns:
        Dictionary with extracted game fields
    """
    # Extract game ID from multiple possible locations
    game_id = (
        bs.get("gameId") or
        bs.get("GAME_ID") or 
        (bs.get("meta") or {}).get("gameId") or
        (bs.get("game") or {}).get("gameId")
    )
    
    # Extract game date
    game_date = (
        bs.get("gameDate") or
        bs.get("GAME_DATE") or
        bs.get("gameTimeUTC") or
        (bs.get("meta") or {}).get("gameDate") or
        (bs.get("game") or {}).get("gameTimeUTC")
    )
    
    # Extract team IDs - handle nested structures
    home_team_obj = bs.get("homeTeam") or bs.get("HOME") or {}
    away_team_obj = bs.get("awayTeam") or bs.get("AWAY") or bs.get("visitorTeam") or {}
    
    home_team_id = (
        home_team_obj.get("teamId") or
        home_team_obj.get("TEAM_ID") or
        bs.get("HOME_TEAM_ID")
    )
    
    away_team_id = (
        away_team_obj.get("teamId") or
        away_team_obj.get("TEAM_ID") or
        bs.get("VISITOR_TEAM_ID") or
        bs.get("AWAY_TEAM_ID")
    )
    
    # Extract game status
    status = (
        bs.get("gameStatusText") or
        bs.get("GAME_STATUS_TEXT") or
        bs.get("gameStatus") or
        (bs.get("game") or {}).get("gameStatusText") or
        "Final"
    )
    
    # Derive season with fallbacks
    explicit_season = bs.get("season") or bs.get("SEASON")
    season = (
        explicit_season or
        derive_season_smart(
            str(game_id) if game_id is not None else None,
            str(game_date) if game_date is not None else None
        ) or
        "UNKNOWN"
    )
    
    return {
        "game_id": str(game_id) if game_id is not None else "",
        "season": season,
        "game_date": str(game_date) if game_date is not None else "",
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "status": str(status) if status is not None else "Final",
    }