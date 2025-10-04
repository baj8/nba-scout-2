"""PBP extraction functions - no preprocessing here."""

from typing import List, Dict, Any


def extract_pbp_from_response(resp: dict) -> list[dict]:
    """
    Extract raw PBP events from NBA Stats API response.
    Returns a flat list of raw PBP event dicts without any preprocessing.
    """
    events = []
    
    if not isinstance(resp, dict) or 'resultSets' not in resp:
        return events
    
    # Find PlayByPlay result set
    for result_set in resp.get('resultSets', []):
        if result_set.get('name') == 'PlayByPlay':
            headers = result_set.get('headers', [])
            rows = result_set.get('rowSet', [])
            
            for row in rows:
                if len(row) == len(headers):
                    # Create raw dict without any preprocessing
                    event_dict = dict(zip(headers, row))
                    events.append(event_dict)
            break
    
    return events