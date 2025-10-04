"""Shot chart extraction functions - shape-only, returns list of dicts."""

from typing import Dict, Any, List


def extract_shot_chart_detail(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract shot chart details from NBA Stats response - shape only.
    
    Args:
        resp: Raw NBA Stats shot chart response
        
    Returns:
        List of dictionaries with shot data: LOC_X, LOC_Y, PLAYER_ID, TEAM_ID, etc.
    """
    shots = []
    
    # Handle NBA Stats resultSets structure
    result_sets = resp.get("resultSets", [])
    if not result_sets:
        return shots
    
    # Find shot chart result set
    shot_set = None
    for rs in result_sets:
        name = rs.get("name", "")
        if "shot" in name.lower() or "chart" in name.lower():
            shot_set = rs
            break
    
    if not shot_set:
        # Try first result set as fallback
        shot_set = result_sets[0] if result_sets else None
    
    if not shot_set:
        return shots
    
    headers = shot_set.get("headers", [])
    rows = shot_set.get("rowSet", [])
    
    for row in rows:
        if len(row) != len(headers):
            continue
            
        # Convert to dict - transformer will handle preprocessing
        shot_dict = dict(zip(headers, row))
        shots.append(shot_dict)
    
    return shots