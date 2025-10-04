"""PBP extraction functions - shape-only, returns list of dicts."""

from typing import Dict, Any, List


def extract_pbp_from_response(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract PBP events from NBA Stats response - shape only.
    
    Args:
        resp: Raw NBA Stats PBP response
        
    Returns:
        List of dictionaries, each containing PBP event fields
    """
    events = []
    
    # Handle NBA Stats resultSets structure
    result_sets = resp.get("resultSets", [])
    if not result_sets:
        return events
    
    # Find the PlayByPlay result set
    pbp_set = None
    for rs in result_sets:
        if rs.get("name") == "PlayByPlay":
            pbp_set = rs
            break
    
    if not pbp_set:
        return events
    
    headers = pbp_set.get("headers", [])
    rows = pbp_set.get("rowSet", [])
    
    for row in rows:
        if len(row) != len(headers):
            continue  # Skip malformed rows
            
        # Convert row to dictionary using headers
        event_dict = dict(zip(headers, row))
        
        # Return as-is without preprocessing - transformers will handle it
        events.append(event_dict)
    
    return events