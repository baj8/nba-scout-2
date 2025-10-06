"""Transform raw shot chart data to shot records."""

from typing import Dict, Any, List


def transform_shots(shot_json: Dict[str, Any], *, game_id: str) -> List[Dict[str, Any]]:
    """Transform raw shot chart data to shot records.
    
    Args:
        shot_json: Raw NBA Stats API shot chart response
        game_id: Game ID to associate with shots
        
    Returns:
        List of shot records
    """
    if not shot_json or 'resultSets' not in shot_json:
        return []
    
    shots = []
    
    try:
        # Find Shot_Chart_Detail result set
        for result_set in shot_json['resultSets']:
            if result_set.get('name') == 'Shot_Chart_Detail':
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
                
                # Process each shot
                for row in rows:
                    if not row:
                        continue
                    
                    shot = {
                        'game_id': game_id,
                        'player_id': get_value(row, 'PLAYER_ID'),
                        'player_name': get_value(row, 'PLAYER_NAME'),
                        'team_id': get_value(row, 'TEAM_ID'),
                        'team_name': get_value(row, 'TEAM_NAME'),
                        'period': get_value(row, 'PERIOD'),
                        'minutes_remaining': get_value(row, 'MINUTES_REMAINING'),
                        'seconds_remaining': get_value(row, 'SECONDS_REMAINING'),
                        'event_type': get_value(row, 'EVENT_TYPE'),
                        'action_type': get_value(row, 'ACTION_TYPE'),
                        'shot_type': get_value(row, 'SHOT_TYPE'),
                        'shot_zone_basic': get_value(row, 'SHOT_ZONE_BASIC'),
                        'shot_zone_area': get_value(row, 'SHOT_ZONE_AREA'),
                        'shot_zone_range': get_value(row, 'SHOT_ZONE_RANGE'),
                        'shot_distance': get_value(row, 'SHOT_DISTANCE'),
                        'loc_x': get_value(row, 'LOC_X'),
                        'loc_y': get_value(row, 'LOC_Y'),
                        'shot_made_flag': get_value(row, 'SHOT_MADE_FLAG'),
                        'shot_attempted_flag': get_value(row, 'SHOT_ATTEMPTED_FLAG', 1),
                        'htm': get_value(row, 'HTM'),  # Home team margin
                        'vtm': get_value(row, 'VTM')   # Visitor team margin
                    }
                    
                    # Remove None values but keep essential fields
                    cleaned_shot = {k: v for k, v in shot.items() if v is not None}
                    
                    # Ensure we have minimum required fields
                    if 'game_id' in cleaned_shot and 'player_id' in cleaned_shot:
                        shots.append(cleaned_shot)
                
                break  # Found Shot_Chart_Detail result set, no need to continue
        
    except (KeyError, IndexError, ValueError, TypeError):
        pass  # Return empty list on any parsing error
    
    return shots