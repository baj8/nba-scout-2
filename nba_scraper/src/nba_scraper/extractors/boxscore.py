"""Boxscore extractors for NBA game metadata."""

from ..utils.season import coalesce_season

def extract_game_from_boxscore(bs: dict) -> dict:
    """Extract game metadata from boxscore response with null-safe field access."""
    
    # Try multiple possible field names for game_id
    game_id = (
        bs.get("gameId")
        or bs.get("GAME_ID")
        or (bs.get("meta") or {}).get("gameId")
        or (bs.get("game") or {}).get("gameId")
    )
    
    # Try multiple possible field names for game_date
    game_date = (
        bs.get("gameDate")
        or bs.get("GAME_DATE")
        or (bs.get("meta") or {}).get("gameDate")
        or (bs.get("game") or {}).get("gameDate")
    )
    
    # Try multiple possible field names for home team
    home_team_id = (
        (bs.get("homeTeam") or {}).get("teamId")
        or (bs.get("HOME") or {}).get("TEAM_ID")
        or (bs.get("game") or {}).get("homeTeam", {}).get("teamId")
    )
    
    # Try multiple possible field names for away team
    away_team_id = (
        (bs.get("awayTeam") or {}).get("teamId")
        or (bs.get("AWAY") or {}).get("TEAM_ID")
        or (bs.get("game") or {}).get("awayTeam", {}).get("teamId")
    )
    
    # Try multiple possible field names for status
    status = (
        bs.get("gameStatusText")
        or bs.get("GAME_STATUS_TEXT")
        or bs.get("status")
        or "Final"
    )
    
    # Derive season with fallback chain
    season = (
        bs.get("season")
        or bs.get("SEASON")
        or coalesce_season(str(game_id) if game_id else None, game_date)
        or "UNKNOWN"
    )
    
    return {
        "game_id": str(game_id) if game_id is not None else "",
        "season": season,
        "game_date": game_date or "",
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "status": status,
    }