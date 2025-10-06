"""Transform raw boxscore summary data to starting lineups records."""

from typing import Dict, Any, List


def transform_starters(boxscore_summary_json: Dict[str, Any], *, game_id: str) -> List[Dict[str, Any]]:
    """Transform raw boxscore summary to starting lineups records.
    
    Args:
        boxscore_summary_json: Raw NBA Stats API boxscore summary response
        game_id: Game ID to associate with starting lineups
        
    Returns:
        List of starting lineup records
    """
    if not boxscore_summary_json or 'resultSets' not in boxscore_summary_json:
        return []
    
    starters = []
    
    try:
        # Find StartingLineup result set
        for result_set in boxscore_summary_json['resultSets']:
            if result_set.get('name') == 'StartingLineup':
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
                
                # Process each starter
                for row in rows:
                    if not row:
                        continue
                    
                    starter = {
                        'game_id': game_id,
                        'team_id': get_value(row, 'TEAM_ID'),
                        'team_abbreviation': get_value(row, 'TEAM_ABBREVIATION'),
                        'team_city': get_value(row, 'TEAM_CITY'),
                        'player_id': get_value(row, 'PLAYER_ID'),
                        'player_name': get_value(row, 'PLAYER_NAME'),
                        'jersey_num': get_value(row, 'JERSEY_NUM'),
                        'position': get_value(row, 'POSITION'),
                        'height': get_value(row, 'HEIGHT'),
                        'weight': get_value(row, 'WEIGHT'),
                        'birth_date': get_value(row, 'BIRTH_DATE'),
                        'age': get_value(row, 'AGE'),
                        'experience': get_value(row, 'EXP'),
                        'school': get_value(row, 'SCHOOL')
                    }
                    
                    # Remove None values but keep essential fields
                    cleaned_starter = {k: v for k, v in starter.items() if v is not None}
                    
                    # Ensure we have minimum required fields
                    if 'game_id' in cleaned_starter and 'player_id' in cleaned_starter and 'team_id' in cleaned_starter:
                        starters.append(cleaned_starter)
                
                break  # Found StartingLineup result set, no need to continue
        
    except (KeyError, IndexError, ValueError, TypeError):
        pass  # Return empty list on any parsing error
    
    return starters