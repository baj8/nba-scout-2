"""Transform raw boxscore summary data to officials records."""

from typing import Dict, Any, List


def transform_officials(boxscore_summary_json: Dict[str, Any], *, game_id: str) -> List[Dict[str, Any]]:
    """Transform raw boxscore summary to officials records.
    
    Args:
        boxscore_summary_json: Raw NBA Stats API boxscore summary response
        game_id: Game ID to associate with officials
        
    Returns:
        List of officials records
    """
    if not boxscore_summary_json or 'resultSets' not in boxscore_summary_json:
        return []
    
    officials = []
    
    try:
        # Find Officials result set
        for result_set in boxscore_summary_json['resultSets']:
            if result_set.get('name') == 'Officials':
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
                
                # Process each official
                for row in rows:
                    if not row:
                        continue
                    
                    official = {
                        'game_id': game_id,
                        'official_id': get_value(row, 'OFFICIAL_ID'),
                        'first_name': get_value(row, 'FIRST_NAME'),
                        'last_name': get_value(row, 'LAST_NAME'),
                        'jersey_num': get_value(row, 'JERSEY_NUM')
                    }
                    
                    # Remove None values but keep essential fields
                    cleaned_official = {k: v for k, v in official.items() if v is not None}
                    
                    # Ensure we have minimum required fields
                    if 'game_id' in cleaned_official and ('official_id' in cleaned_official or 'last_name' in cleaned_official):
                        officials.append(cleaned_official)
                
                break  # Found Officials result set, no need to continue
        
    except (KeyError, IndexError, ValueError, TypeError):
        pass  # Return empty list on any parsing error
    
    return officials