"""Lineup extraction functions - shape-only, returns list of dicts."""

from typing import Dict, Any, List


def extract_lineups_from_response(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract lineup stints from NBA Stats response - shape only.
    
    Args:
        resp: Raw NBA Stats lineups response
        
    Returns:
        List of dictionaries with lineup data: TEAM_ID, PERIOD, PLAYER_IDS, SECS
    """
    lineups = []
    
    # Handle NBA Stats resultSets structure
    result_sets = resp.get("resultSets", [])
    if not result_sets:
        return lineups
    
    # Find lineup-related result set
    lineup_set = None
    for rs in result_sets:
        name = rs.get("name", "")
        if "lineup" in name.lower() or "stint" in name.lower():
            lineup_set = rs
            break
    
    if not lineup_set:
        return lineups
    
    headers = lineup_set.get("headers", [])
    rows = lineup_set.get("rowSet", [])
    
    for row in rows:
        if len(row) != len(headers):
            continue
            
        # Convert to dict
        lineup_dict = dict(zip(headers, row))
        
        # Return as-is - transformer will handle player ID parsing
        lineups.append(lineup_dict)
    
    return lineups