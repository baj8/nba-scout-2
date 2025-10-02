"""NBA Stats API extraction functions."""

from datetime import datetime
from typing import Any, Dict, List

from ..models import GameRow, PbpEventRow, StartingLineupRow
from ..logging import get_logger

logger = get_logger(__name__)


def extract_games_from_scoreboard(
    scoreboard_data: Dict[str, Any], 
    source_url: str
) -> List[GameRow]:
    """Extract games from NBA Stats scoreboard response.
    
    Args:
        scoreboard_data: Raw scoreboard JSON response
        source_url: Source URL for provenance
        
    Returns:
        List of validated GameRow instances
    """
    games = []
    
    try:
        if 'resultSets' not in scoreboard_data:
            logger.warning("No resultSets in scoreboard data")
            return games
        
        # Find GameHeader result set
        game_header_set = None
        for result_set in scoreboard_data['resultSets']:
            if result_set.get('name') == 'GameHeader':
                game_header_set = result_set
                break
        
        if not game_header_set:
            logger.warning("No GameHeader found in scoreboard")
            return games
        
        headers = game_header_set.get('headers', [])
        rows = game_header_set.get('rowSet', [])
        
        for row in rows:
            try:
                # Convert row to dictionary
                game_dict = dict(zip(headers, row))
                
                # Create GameRow using the from_nba_stats class method
                game_row = GameRow.from_nba_stats(game_dict, source_url)
                games.append(game_row)
                
            except Exception as e:
                logger.warning("Failed to extract game from row", 
                              row=row[:3] if row else None, error=str(e))
                continue
        
        logger.info("Extracted games from scoreboard", count=len(games))
        
    except Exception as e:
        logger.error("Failed to extract games from scoreboard", error=str(e))
    
    return games


def extract_pbp_from_response(
    pbp_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> List[PbpEventRow]:
    """Extract play-by-play events from NBA Stats response.
    
    Args:
        pbp_data: Raw PBP JSON response  
        game_id: Game identifier
        source_url: Source URL for provenance
        
    Returns:
        List of validated PbpEventRow instances
    """
    events = []
    
    try:
        if 'resultSets' not in pbp_data:
            logger.warning("No resultSets in PBP data", game_id=game_id)
            return events
        
        # Find PlayByPlay result set
        pbp_set = None
        for result_set in pbp_data['resultSets']:
            if result_set.get('name') == 'PlayByPlay':
                pbp_set = result_set
                break
        
        if not pbp_set:
            logger.warning("No PlayByPlay found in response", game_id=game_id)
            return events
        
        headers = pbp_set.get('headers', [])
        rows = pbp_set.get('rowSet', [])
        
        for idx, row in enumerate(rows):
            try:
                # Convert row to dictionary
                event_dict = dict(zip(headers, row))
                
                # Create PbpEventRow using the from_nba_stats class method
                event_row = PbpEventRow.from_nba_stats(game_id, event_dict, source_url)
                events.append(event_row)
                
            except Exception as e:
                logger.warning("Failed to extract PBP event", 
                              game_id=game_id, event_idx=idx, error=str(e))
                continue
        
        logger.info("Extracted PBP events", game_id=game_id, count=len(events))
        
    except Exception as e:
        logger.error("Failed to extract PBP from response", 
                    game_id=game_id, error=str(e))
    
    return events


def extract_boxscore_lineups(
    boxscore_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> List[StartingLineupRow]:
    """Extract starting lineups from NBA Stats boxscore.
    
    Args:
        boxscore_data: Raw boxscore JSON response
        game_id: Game identifier  
        source_url: Source URL for provenance
        
    Returns:
        List of validated StartingLineupRow instances
    """
    lineups = []
    
    try:
        if 'resultSets' not in boxscore_data:
            logger.warning("No resultSets in boxscore data", game_id=game_id)
            return lineups
        
        # Look for PlayerStats result sets (usually separate for each team)
        for result_set in boxscore_data['resultSets']:
            result_name = result_set.get('name', '')
            
            if 'PlayerStats' not in result_name:
                continue
            
            headers = result_set.get('headers', [])
            rows = result_set.get('rowSet', [])
            
            # Determine team from first row
            if not rows:
                continue
            
            first_row_dict = dict(zip(headers, rows[0]))
            team_tricode = first_row_dict.get('TEAM_ABBREVIATION', '')
            
            for row in rows:
                try:
                    player_dict = dict(zip(headers, row))
                    
                    # Check if player was a starter
                    start_position = player_dict.get('START_POSITION')
                    if not start_position or start_position == '':
                        continue
                    
                    # Create StartingLineupRow
                    lineup_row = StartingLineupRow.from_nba_stats(
                        game_id, team_tricode, player_dict, source_url
                    )
                    lineups.append(lineup_row)
                    
                except Exception as e:
                    logger.warning("Failed to extract lineup player",
                                  game_id=game_id, error=str(e))
                    continue
        
        logger.info("Extracted starting lineups", game_id=game_id, count=len(lineups))
        
    except Exception as e:
        logger.error("Failed to extract lineups from boxscore",
                    game_id=game_id, error=str(e))
    
    return lineups


def extract_team_stats(
    boxscore_data: Dict[str, Any],
    game_id: str
) -> Dict[str, Any]:
    """Extract team statistics from boxscore for analytics.
    
    Args:
        boxscore_data: Raw boxscore JSON response
        game_id: Game identifier
        
    Returns:
        Dictionary with team stats for both teams
    """
    team_stats = {}
    
    try:
        if 'resultSets' not in boxscore_data:
            return team_stats
        
        # Look for TeamStats result set
        for result_set in boxscore_data['resultSets']:
            if result_set.get('name') == 'TeamStats':
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                for row in rows:
                    team_dict = dict(zip(headers, row))
                    team_id = team_dict.get('TEAM_ID')
                    team_abbrev = team_dict.get('TEAM_ABBREVIATION')
                    
                    if team_id:
                        team_stats[str(team_id)] = {
                            'tricode': team_abbrev,
                            'points': team_dict.get('PTS', 0),
                            'fgm': team_dict.get('FGM', 0),
                            'fga': team_dict.get('FGA', 0),
                            'fg3m': team_dict.get('FG3M', 0),
                            'fg3a': team_dict.get('FG3A', 0),
                            'ftm': team_dict.get('FTM', 0),
                            'fta': team_dict.get('FTA', 0),
                            'oreb': team_dict.get('OREB', 0),
                            'dreb': team_dict.get('DREB', 0),
                            'reb': team_dict.get('REB', 0),
                            'ast': team_dict.get('AST', 0),
                            'stl': team_dict.get('STL', 0),
                            'blk': team_dict.get('BLK', 0),
                            'tov': team_dict.get('TO', 0),
                            'pf': team_dict.get('PF', 0),
                        }
                break
        
        logger.debug("Extracted team stats", game_id=game_id, teams=len(team_stats))
        
    except Exception as e:
        logger.warning("Failed to extract team stats", game_id=game_id, error=str(e))
    
    return team_stats