"""Transform raw play-by-play data to event records."""

from typing import Dict, Any, List


def transform_pbp(pbp_json: Dict[str, Any], *, game_id: str) -> List[Dict[str, Any]]:
    """Transform raw play-by-play data to event records.
    
    Args:
        pbp_json: Raw NBA Stats API play-by-play response
        game_id: Game ID to associate with events
        
    Returns:
        List of play-by-play event records
    """
    if not pbp_json or 'resultSets' not in pbp_json:
        return []
    
    events = []
    
    try:
        # Find PlayByPlay result set
        for result_set in pbp_json['resultSets']:
            if result_set.get('name') == 'PlayByPlay':
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                if not headers:
                    continue
                
                # Create header-to-index mapping
                header_map = {header: i for i, header in enumerate(headers)}
                
                def get_value(row: List, field_name: str, default=None):
                    """Safely get value from row by header name."""
                    idx = header_map.get(field_name)
                    if idx is not None and idx < len(row):
                        value = row[idx]
                        return value if value is not None else default
                    return default
                
                # Process each play-by-play event
                for row in rows:
                    if not row:
                        continue
                    
                    event = {
                        'game_id': game_id,
                        'event_num': get_value(row, 'EVENTNUM'),
                        'period': get_value(row, 'PERIOD'),
                        'clock': get_value(row, 'PCTIMESTRING'),
                        'event_type': get_value(row, 'EVENTMSGTYPE'),
                        'event_subtype': get_value(row, 'EVENTMSGACTIONTYPE'),
                        'player1_id': get_value(row, 'PLAYER1_ID'),
                        'player1_name': get_value(row, 'PLAYER1_NAME'),
                        'player2_id': get_value(row, 'PLAYER2_ID'),
                        'player2_name': get_value(row, 'PLAYER2_NAME'),
                        'player3_id': get_value(row, 'PLAYER3_ID'),
                        'player3_name': get_value(row, 'PLAYER3_NAME'),
                        'team_id': get_value(row, 'PLAYER1_TEAM_ID'),
                        'home_description': get_value(row, 'HOMEDESCRIPTION'),
                        'away_description': get_value(row, 'VISITORDESCRIPTION'),
                        'neutral_description': get_value(row, 'NEUTRALDESCRIPTION'),
                        'score': get_value(row, 'SCORE'),
                        'score_margin': get_value(row, 'SCOREMARGIN')
                    }
                    
                    # Remove None values but keep the essential fields
                    cleaned_event = {k: v for k, v in event.items() if v is not None}
                    
                    # Ensure we have minimum required fields
                    if 'game_id' in cleaned_event and 'event_num' in cleaned_event:
                        events.append(cleaned_event)
                
                break  # Found PlayByPlay result set, no need to continue
        
    except (KeyError, IndexError, ValueError, TypeError):
        pass  # Return empty list on any parsing error
    
    return events