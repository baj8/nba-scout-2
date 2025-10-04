def extract_pbp_from_response(resp: dict) -> list[dict]:
    """
    Extract play-by-play events from NBA Stats API response.
    Returns list of dicts with EVENTNUM, PERIOD, PCTIMESTRING/CLOCK, etc.
    """
    if not resp:
        return []
    
    # Try different possible response structures
    result_sets = (
        resp.get("resultSets", []) or
        resp.get("ResultSets", []) or 
        []
    )
    
    for rs in result_sets:
        if rs.get("name") == "PlayByPlay" or "PlayByPlay" in str(rs.get("name", "")):
            headers = rs.get("headers", [])
            rows = rs.get("rowSet", [])
            
            events = []
            for row in rows:
                if len(row) != len(headers):
                    continue
                    
                event = dict(zip(headers, row))
                # Ensure we have the required fields
                if event.get("EVENTNUM") is not None and event.get("PERIOD") is not None:
                    events.append(event)
            
            return events
    
    # Fallback: check if response is already flattened
    if isinstance(resp, list):
        return [event for event in resp if event.get("EVENTNUM") is not None]
    
    return []