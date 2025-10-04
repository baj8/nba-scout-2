"""NBA Stats API extraction functions."""

from datetime import datetime
from typing import Any, Dict, List

from ..models import GameRow, PbpEventRow, StartingLineupRow
from ..models.utils import preprocess_nba_stats_data
from ..nba_logging import get_logger

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

        # Preprocess the entire scoreboard response to handle mixed data types
        scoreboard_data = preprocess_nba_stats_data(scoreboard_data)
        
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
                
                # Additional preprocessing for the individual game data
                game_dict = preprocess_nba_stats_data(game_dict)
                
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

        # Preprocess the entire PBP response to handle mixed data types
        pbp_data = preprocess_nba_stats_data(pbp_data)
        
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
                
                # Additional preprocessing for the individual event data
                event_dict = preprocess_nba_stats_data(event_dict)
                
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

        # Preprocess the entire boxscore response to handle mixed data types
        boxscore_data = preprocess_nba_stats_data(boxscore_data)
        
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
            first_row_dict = preprocess_nba_stats_data(first_row_dict)
            team_tricode = first_row_dict.get('TEAM_ABBREVIATION', '')
            
            for row in rows:
                try:
                    player_dict = dict(zip(headers, row))
                    
                    # Additional preprocessing for the individual player data
                    player_dict = preprocess_nba_stats_data(player_dict)
                    
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


def extract_advanced_player_stats(
    advanced_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> List[Dict[str, Any]]:
    """Extract advanced player statistics from NBA Stats advanced boxscore.
    
    Args:
        advanced_data: Raw advanced boxscore JSON response
        game_id: Game identifier
        source_url: Source URL for provenance
        
    Returns:
        List of player advanced stats dictionaries
    """
    player_stats = []
    
    try:
        if 'resultSets' not in advanced_data:
            logger.warning("No resultSets in advanced boxscore data", game_id=game_id)
            return player_stats

        # Preprocess the entire response to handle mixed data types
        advanced_data = preprocess_nba_stats_data(advanced_data)
        
        # Look for PlayerStats result set with advanced metrics
        for result_set in advanced_data['resultSets']:
            result_name = result_set.get('name', '')
            
            if 'PlayerStats' in result_name:
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                for row in rows:
                    try:
                        player_dict = dict(zip(headers, row))
                        
                        # Additional preprocessing for individual player data
                        player_dict = preprocess_nba_stats_data(player_dict)
                        
                        # Extract advanced metrics
                        advanced_stats = {
                            'game_id': game_id,
                            'player_id': player_dict.get('PLAYER_ID'),
                            'player_name': player_dict.get('PLAYER_NAME'),
                            'team_id': player_dict.get('TEAM_ID'),
                            'team_abbreviation': player_dict.get('TEAM_ABBREVIATION'),
                            
                            # Advanced efficiency metrics
                            'offensive_rating': player_dict.get('OFF_RATING'),
                            'defensive_rating': player_dict.get('DEF_RATING'),
                            'net_rating': player_dict.get('NET_RATING'),
                            'assist_percentage': player_dict.get('AST_PCT'),
                            'assist_to_turnover': player_dict.get('AST_TOV'),
                            'assist_ratio': player_dict.get('AST_RATIO'),
                            'offensive_rebound_pct': player_dict.get('OREB_PCT'),
                            'defensive_rebound_pct': player_dict.get('DREB_PCT'),
                            'rebound_pct': player_dict.get('REB_PCT'),
                            'turnover_ratio': player_dict.get('TM_TOV_PCT'),
                            'effective_fg_pct': player_dict.get('EFG_PCT'),
                            'true_shooting_pct': player_dict.get('TS_PCT'),
                            'usage_pct': player_dict.get('USG_PCT'),
                            'pace': player_dict.get('PACE'),
                            'pie': player_dict.get('PIE'),  # Player Impact Estimate
                            
                            'source': 'nba_stats',
                            'source_url': source_url,
                            'ingested_at_utc': datetime.utcnow()
                        }
                        
                        player_stats.append(advanced_stats)
                        
                    except Exception as e:
                        logger.warning("Failed to extract advanced player stats",
                                      game_id=game_id, error=str(e))
                        continue
        
        logger.info("Extracted advanced player stats", game_id=game_id, count=len(player_stats))
        
    except Exception as e:
        logger.error("Failed to extract advanced player stats from response",
                    game_id=game_id, error=str(e))
    
    return player_stats


def extract_misc_player_stats(
    misc_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> List[Dict[str, Any]]:
    """Extract miscellaneous player statistics including plus/minus.
    
    Args:
        misc_data: Raw misc boxscore JSON response
        game_id: Game identifier
        source_url: Source URL for provenance
        
    Returns:
        List of player misc stats dictionaries
    """
    player_stats = []
    
    try:
        if 'resultSets' not in misc_data:
            logger.warning("No resultSets in misc boxscore data", game_id=game_id)
            return player_stats

        # Preprocess the entire response to handle mixed data types
        misc_data = preprocess_nba_stats_data(misc_data)
        
        # Look for sqlPlayersMisc result set (actual API response structure)
        for result_set in misc_data['resultSets']:
            result_name = result_set.get('name', '')
            
            if 'sqlPlayersMisc' in result_name or 'PlayerStats' in result_name:
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                for row in rows:
                    try:
                        player_dict = dict(zip(headers, row))
                        
                        # Additional preprocessing for individual player data
                        player_dict = preprocess_nba_stats_data(player_dict)
                        
                        # Extract misc metrics (plus/minus, etc.)
                        misc_stats = {
                            'game_id': game_id,
                            'player_id': player_dict.get('PLAYER_ID'),
                            'player_name': player_dict.get('PLAYER_NAME'),
                            'team_id': player_dict.get('TEAM_ID'),
                            'team_abbreviation': player_dict.get('TEAM_ABBREVIATION'),
                            
                            # Plus/minus and impact metrics
                            'plus_minus': player_dict.get('PLUS_MINUS'),
                            'nba_fantasy_pts': player_dict.get('NBA_FANTASY_PTS'),
                            'dd2': player_dict.get('DD2'),  # Double-doubles
                            'td3': player_dict.get('TD3'),  # Triple-doubles
                            'wnba_fantasy_pts': player_dict.get('WNBA_FANTASY_PTS'),
                            'fg_pct_rank': player_dict.get('FG_PCT_RANK'),
                            'ft_pct_rank': player_dict.get('FT_PCT_RANK'),
                            'fg3_pct_rank': player_dict.get('FG3_PCT_RANK'),
                            'pts_rank': player_dict.get('PTS_RANK'),
                            'reb_rank': player_dict.get('REB_RANK'),
                            'ast_rank': player_dict.get('AST_RANK'),
                            
                            'source': 'nba_stats',
                            'source_url': source_url,
                            'ingested_at_utc': datetime.utcnow()
                        }
                        
                        player_stats.append(misc_stats)
                        
                    except Exception as e:
                        logger.warning("Failed to extract misc player stats",
                                      game_id=game_id, error=str(e))
                        continue
        
        logger.info("Extracted misc player stats", game_id=game_id, count=len(player_stats))
        
    except Exception as e:
        logger.error("Failed to extract misc player stats from response",
                    game_id=game_id, error=str(e))
    
    return player_stats


def extract_usage_player_stats(
    usage_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> List[Dict[str, Any]]:
    """Extract usage statistics for players.
    
    Args:
        usage_data: Raw usage boxscore JSON response
        game_id: Game identifier
        source_url: Source URL for provenance
        
    Returns:
        List of player usage stats dictionaries
    """
    player_stats = []
    
    try:
        if 'resultSets' not in usage_data:
            logger.warning("No resultSets in usage boxscore data", game_id=game_id)
            return player_stats

        # Preprocess the entire response to handle mixed data types
        usage_data = preprocess_nba_stats_data(usage_data)
        
        # Look for sqlPlayersUsage result set (actual API response structure)
        for result_set in usage_data['resultSets']:
            result_name = result_set.get('name', '')
            
            if 'sqlPlayersUsage' in result_name or 'PlayerStats' in result_name:
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                for row in rows:
                    try:
                        player_dict = dict(zip(headers, row))
                        
                        # Additional preprocessing for individual player data
                        player_dict = preprocess_nba_stats_data(player_dict)
                        
                        # Extract usage metrics
                        usage_stats = {
                            'game_id': game_id,
                            'player_id': player_dict.get('PLAYER_ID'),
                            'player_name': player_dict.get('PLAYER_NAME'),
                            'team_id': player_dict.get('TEAM_ID'),
                            'team_abbreviation': player_dict.get('TEAM_ABBREVIATION'),
                            
                            # Usage and pace metrics
                            'usage_pct': player_dict.get('USG_PCT'),
                            'pct_fgm': player_dict.get('PCT_FGM'),
                            'pct_fga': player_dict.get('PCT_FGA'),
                            'pct_fg3m': player_dict.get('PCT_FG3M'),
                            'pct_fg3a': player_dict.get('PCT_FG3A'),
                            'pct_ftm': player_dict.get('PCT_FTM'),
                            'pct_fta': player_dict.get('PCT_FTA'),
                            'pct_oreb': player_dict.get('PCT_OREB'),
                            'pct_dreb': player_dict.get('PCT_DREB'),
                            'pct_reb': player_dict.get('PCT_REB'),
                            'pct_ast': player_dict.get('PCT_AST'),
                            'pct_tov': player_dict.get('PCT_TOV'),
                            'pct_stl': player_dict.get('PCT_STL'),
                            'pct_blk': player_dict.get('PCT_BLK'),
                            'pct_blka': player_dict.get('PCT_BLKA'),
                            'pct_pf': player_dict.get('PCT_PF'),
                            'pct_pfd': player_dict.get('PCT_PFD'),
                            'pct_pts': player_dict.get('PCT_PTS'),
                            
                            'source': 'nba_stats',
                            'source_url': source_url,
                            'ingested_at_utc': datetime.utcnow()
                        }
                        
                        player_stats.append(usage_stats)
                        
                    except Exception as e:
                        logger.warning("Failed to extract usage player stats",
                                      game_id=game_id, error=str(e))
                        continue
        
        logger.info("Extracted usage player stats", game_id=game_id, count=len(player_stats))
        
    except Exception as e:
        logger.error("Failed to extract usage player stats from response",
                    game_id=game_id, error=str(e))
    
    return player_stats


def extract_advanced_team_stats(
    advanced_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> List[Dict[str, Any]]:
    """Extract advanced team statistics from NBA Stats advanced boxscore.
    
    Args:
        advanced_data: Raw advanced boxscore JSON response
        game_id: Game identifier
        source_url: Source URL for provenance
        
    Returns:
        List of team advanced stats dictionaries
    """
    team_stats = []
    
    try:
        if 'resultSets' not in advanced_data:
            logger.warning("No resultSets in advanced boxscore data", game_id=game_id)
            return team_stats

        # Preprocess the entire response to handle mixed data types
        advanced_data = preprocess_nba_stats_data(advanced_data)
        
        # Look for TeamStats result set with advanced metrics
        for result_set in advanced_data['resultSets']:
            result_name = result_set.get('name', '')
            
            if 'TeamStats' in result_name:
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                for row in rows:
                    try:
                        team_dict = dict(zip(headers, row))
                        
                        # Additional preprocessing for individual team data
                        team_dict = preprocess_nba_stats_data(team_dict)
                        
                        # Extract advanced team metrics
                        advanced_stats = {
                            'game_id': game_id,
                            'team_id': team_dict.get('TEAM_ID'),
                            'team_abbreviation': team_dict.get('TEAM_ABBREVIATION'),
                            'team_name': team_dict.get('TEAM_NAME'),
                            
                            # Advanced team efficiency metrics
                            'offensive_rating': team_dict.get('OFF_RATING'),
                            'defensive_rating': team_dict.get('DEF_RATING'),
                            'net_rating': team_dict.get('NET_RATING'),
                            'assist_percentage': team_dict.get('AST_PCT'),
                            'assist_to_turnover': team_dict.get('AST_TOV'),
                            'assist_ratio': team_dict.get('AST_RATIO'),
                            'offensive_rebound_pct': team_dict.get('OREB_PCT'),
                            'defensive_rebound_pct': team_dict.get('DREB_PCT'),
                            'rebound_pct': team_dict.get('REB_PCT'),
                            'turnover_ratio': team_dict.get('TM_TOV_PCT'),
                            'effective_fg_pct': team_dict.get('EFG_PCT'),
                            'true_shooting_pct': team_dict.get('TS_PCT'),
                            'pace': team_dict.get('PACE'),
                            'pie': team_dict.get('PIE'),  # Player Impact Estimate (team aggregate)
                            
                            'source': 'nba_stats',
                            'source_url': source_url,
                            'ingested_at_utc': datetime.utcnow()
                        }
                        
                        team_stats.append(advanced_stats)
                        
                    except Exception as e:
                        logger.warning("Failed to extract advanced team stats",
                                      game_id=game_id, error=str(e))
                        continue
        
        logger.info("Extracted advanced team stats", game_id=game_id, count=len(team_stats))
        
    except Exception as e:
        logger.error("Failed to extract advanced team stats from response",
                    game_id=game_id, error=str(e))
    
    return team_stats


def extract_shot_chart_detail(
    shot_chart_data: Dict[str, Any],
    game_id: str,
    source_url: str
) -> Dict[str, Dict[str, Any]]:
    """Extract shot chart coordinates and details from NBA Stats response.
    
    Args:
        shot_chart_data: Raw shot chart JSON response
        game_id: Game identifier
        source_url: Source URL for provenance
        
    Returns:
        Dictionary mapping event indices to shot details
    """
    shot_details = {}
    
    try:
        if 'resultSets' not in shot_chart_data:
            logger.warning("No resultSets in shot chart data", game_id=game_id)
            return shot_details

        # Preprocess the entire response to handle mixed data types
        shot_chart_data = preprocess_nba_stats_data(shot_chart_data)
        
        # Look for Shot_Chart_Detail result set
        for result_set in shot_chart_data['resultSets']:
            result_name = result_set.get('name', '')
            
            if 'Shot_Chart_Detail' in result_name:
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                for row in rows:
                    try:
                        shot_dict = dict(zip(headers, row))
                        
                        # Additional preprocessing for individual shot data
                        shot_dict = preprocess_nba_stats_data(shot_dict)
                        
                        # Get event number to match with PBP events
                        event_num = shot_dict.get('GAME_EVENT_ID')
                        if event_num:
                            shot_details[str(event_num)] = {
                                'shot_x': shot_dict.get('LOC_X'),
                                'shot_y': shot_dict.get('LOC_Y'),
                                'shot_distance_ft': shot_dict.get('SHOT_DISTANCE'),
                                'shot_zone': shot_dict.get('SHOT_ZONE_BASIC'),
                                'shot_zone_area': shot_dict.get('SHOT_ZONE_AREA'),
                                'shot_zone_range': shot_dict.get('SHOT_ZONE_RANGE'),
                                'minutes_remaining': shot_dict.get('MINUTES_REMAINING'),
                                'seconds_remaining': shot_dict.get('SECONDS_REMAINING'),
                                'period': shot_dict.get('PERIOD'),
                            }
                        
                    except Exception as e:
                        logger.warning("Failed to extract shot detail",
                                      game_id=game_id, error=str(e))
                        continue
        
        logger.info("Extracted shot chart details", game_id=game_id, count=len(shot_details))
        
    except Exception as e:
        logger.error("Failed to extract shot chart from response",
                    game_id=game_id, error=str(e))
    
    return shot_details