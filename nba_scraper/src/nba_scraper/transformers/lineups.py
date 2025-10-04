"""Lineup transformation functions - pure, synchronous."""

from typing import List, Dict, Any
from ..models.lineups import LineupStint
from ..utils.preprocess import preprocess_nba_stats_data, normalize_team_id


def transform_lineups(raw: List[Dict[str, Any]], game_id: str) -> List[LineupStint]:
    """Transform extracted lineup data to validated LineupStint models.
    
    Args:
        raw: List of dictionaries from extract_lineups_from_response
        game_id: Game identifier for linking
        
    Returns:
        List of validated LineupStint instances
    """
    out = []
    
    for r in raw:
        # Apply preprocessing to handle mixed data types
        r = preprocess_nba_stats_data(r)
        
        try:
            # Extract team ID
            team_id = normalize_team_id(r.get("TEAM_ID"))
            if team_id is None:
                continue  # Skip if no valid team ID
            
            # Extract period
            period = int(r.get("PERIOD", 1))
            
            # Extract player IDs - handle various formats
            player_ids_raw = r.get("PLAYER_IDS") or r.get("LINEUP") or []
            
            # Handle different player ID formats
            if isinstance(player_ids_raw, str):
                # Comma-separated string
                player_ids = [int(pid.strip()) for pid in player_ids_raw.split(",") if pid.strip()]
            elif isinstance(player_ids_raw, list):
                # Already a list
                player_ids = [int(pid) for pid in player_ids_raw if pid is not None]
            else:
                # Try individual player fields (PLAYER1_ID, PLAYER2_ID, etc.)
                player_ids = []
                for i in range(1, 6):  # NBA lineups have 5 players
                    pid = r.get(f"PLAYER{i}_ID")
                    if pid is not None:
                        try:
                            player_ids.append(int(pid))
                        except (ValueError, TypeError):
                            pass
            
            # Validate lineup size
            if len(player_ids) != 5:
                continue  # Skip invalid lineups
            
            # Extract seconds played
            seconds_played = int(r.get("SECS", 0) or r.get("SECONDS_PLAYED", 0) or 0)
            
            lineup_stint = LineupStint(
                game_id=game_id,
                team_id=team_id,
                period=period,
                lineup=player_ids,
                seconds_played=seconds_played
            )
            out.append(lineup_stint)
            
        except Exception:
            # Skip invalid lineup records
            continue
    
    return out