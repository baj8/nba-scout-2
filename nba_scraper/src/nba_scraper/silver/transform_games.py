"""Transform raw boxscore summary data to game records."""

from typing import Dict, Any, Optional
from datetime import datetime


def transform_game(boxscore_summary_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Transform raw boxscore summary to game record.
    
    Args:
        boxscore_summary_json: Raw NBA Stats API boxscore summary response
        
    Returns:
        Game record dict or None if transformation fails
    """
    if not boxscore_summary_json or 'resultSets' not in boxscore_summary_json:
        return None
    
    try:
        # Find GameSummary result set
        for result_set in boxscore_summary_json['resultSets']:
            if result_set.get('name') == 'GameSummary':
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                if not rows or not headers:
                    continue
                
                row = rows[0]  # First row contains game info
                
                # Create header-to-index mapping
                header_map = {header: i for i, header in enumerate(headers)}
                
                def get_value(field_name: str, default=None):
                    """Safely get value from row by header name."""
                    idx = header_map.get(field_name)
                    if idx is not None and idx < len(row):
                        value = row[idx]
                        return value if value is not None else default
                    return default
                
                # Extract game data
                game_data = {
                    'game_id': get_value('GAME_ID'),
                    'season': get_value('SEASON'),
                    'game_date_est': get_value('GAME_DATE_EST'),
                    'home_team_id': get_value('HOME_TEAM_ID'),
                    'away_team_id': get_value('VISITOR_TEAM_ID'),
                    'status': get_value('GAME_STATUS_TEXT', 'Unknown'),
                    'home_team_wins': get_value('HOME_TEAM_WINS', 0),
                    'home_team_losses': get_value('HOME_TEAM_LOSSES', 0),
                    'away_team_wins': get_value('VISITOR_TEAM_WINS', 0),
                    'away_team_losses': get_value('VISITOR_TEAM_LOSSES', 0),
                    'arena_name': get_value('ARENA_NAME'),
                    'attendance': get_value('ATTENDANCE'),
                    'game_time': get_value('GAMETIME')
                }
                
                # Remove None values and ensure game_id is present
                game_data = {k: v for k, v in game_data.items() if v is not None}
                
                if 'game_id' in game_data:
                    return game_data
        
        return None
        
    except (KeyError, IndexError, ValueError, TypeError):
        return None