"""Lineup extraction functions - no preprocessing here."""

from typing import List, Dict, Any


def extract_lineups_from_response(resp: dict) -> list[dict]:
    """
    Extract lineup data from boxscore response.
    Returns list of dicts with TEAM_ID, PERIOD, PLAYER_IDS (list[int]), SECS (int).
    """
    lineups = []
    
    if not isinstance(resp, dict) or 'resultSets' not in resp:
        return lineups
    
    # Look for starting lineups in PlayerStats result sets
    for result_set in resp.get('resultSets', []):
        name = result_set.get('name', '')
        
        if 'PlayerStats' in name:
            headers = result_set.get('headers', [])
            rows = result_set.get('rowSet', [])
            
            # Group starters by team
            team_starters = {}
            
            for row in rows:
                if len(row) == len(headers):
                    player_dict = dict(zip(headers, row))
                    
                    # Check if player was a starter (has START_POSITION)
                    start_position = player_dict.get('START_POSITION')
                    if start_position and str(start_position).strip():
                        team_id = player_dict.get('TEAM_ID')
                        player_id = player_dict.get('PLAYER_ID')
                        minutes = player_dict.get('MIN', 0)
                        
                        if team_id not in team_starters:
                            team_starters[team_id] = []
                        
                        team_starters[team_id].append({
                            'player_id': player_id,
                            'minutes': minutes
                        })
            
            # Convert to lineup format
            for team_id, starters in team_starters.items():
                if len(starters) == 5:  # Valid starting lineup
                    player_ids = [s['player_id'] for s in starters]
                    
                    # Estimate seconds played for starting lineup
                    # Convert minutes to seconds, use default if no minutes data
                    try:
                        avg_minutes = sum(float(s['minutes'] or 0) for s in starters) / len(starters)
                        seconds = int(avg_minutes * 60) if avg_minutes > 0 else 720  # Default 12 min
                    except (ValueError, TypeError):
                        seconds = 720  # Default 12 minutes in seconds
                    
                    lineups.append({
                        'TEAM_ID': team_id,
                        'PERIOD': 1,  # Starting lineup is period 1
                        'PLAYER_IDS': player_ids,
                        'SECS': seconds
                    })
    
    return lineups