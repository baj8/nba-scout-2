"""Core orchestration for NBA raw data harvesting with comprehensive error handling and manifest tracking."""

import asyncio
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional
import traceback

from .client import RawNbaClient
from .persist import write_json, update_manifest, append_quarantine, ensure_dir
from ..nba_logging import get_logger

logger = get_logger(__name__)


def _parse_season_from_scoreboard(scoreboard_data: Dict[str, Any]) -> List[str]:
    """Extract regular season game IDs from scoreboard response.
    
    Args:
        scoreboard_data: Raw scoreboard API response
        
    Returns:
        List of game IDs that are regular season games
    """
    game_ids = []
    
    try:
        if 'resultSets' not in scoreboard_data:
            logger.warning("No resultSets in scoreboard data")
            return game_ids
        
        for result_set in scoreboard_data['resultSets']:
            if result_set.get('name') == 'GameHeader':
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                # Find indices for key fields
                game_id_idx = None
                season_type_idx = None
                
                for i, header in enumerate(headers):
                    if header.upper() == 'GAME_ID':
                        game_id_idx = i
                    elif header.upper() in ['SEASON_TYPE_ID', 'SEASON_TYPE']:
                        season_type_idx = i
                
                if game_id_idx is None:
                    logger.warning("No GAME_ID column found in scoreboard")
                    continue
                
                for row in rows:
                    if len(row) > game_id_idx:
                        game_id = row[game_id_idx]
                        
                        # Filter to regular season only
                        is_regular_season = True
                        if season_type_idx is not None and len(row) > season_type_idx:
                            season_type = str(row[season_type_idx])
                            # Regular season is typically "2" or "Regular Season"
                            is_regular_season = season_type in ['2', 'Regular Season']
                        
                        if is_regular_season and game_id:
                            game_ids.append(str(game_id))
                            
        logger.info("Parsed regular season games from scoreboard", 
                   total_games=len(game_ids))
        
    except Exception as e:
        logger.error("Failed to parse scoreboard games", error=str(e))
    
    return game_ids


def _extract_team_ids_from_summary(summary_data: Dict[str, Any]) -> Optional[Dict[str, int]]:
    """Extract home and visitor team IDs from boxscore summary.
    
    Args:
        summary_data: Raw boxscore summary API response
        
    Returns:
        Dict with 'home_team_id' and 'visitor_team_id' or None if not found
    """
    try:
        if 'resultSets' not in summary_data:
            return None
        
        for result_set in summary_data['resultSets']:
            if result_set.get('name') == 'GameSummary':
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                if not rows:
                    continue
                
                # Find team ID columns
                home_team_idx = None
                visitor_team_idx = None
                
                for i, header in enumerate(headers):
                    header_upper = header.upper()
                    if 'HOME' in header_upper and 'TEAM' in header_upper and 'ID' in header_upper:
                        home_team_idx = i
                    elif ('VISITOR' in header_upper or 'AWAY' in header_upper) and 'TEAM' in header_upper and 'ID' in header_upper:
                        visitor_team_idx = i
                
                if home_team_idx is not None and visitor_team_idx is not None:
                    row = rows[0]  # Game summary should have one row
                    if len(row) > max(home_team_idx, visitor_team_idx):
                        return {
                            'home_team_id': int(row[home_team_idx]),
                            'visitor_team_id': int(row[visitor_team_idx])
                        }
        
        logger.warning("Could not extract team IDs from boxscore summary")
        return None
        
    except Exception as e:
        logger.warning("Failed to extract team IDs from summary", error=str(e))
        return None


async def harvest_date(
    date_str: str, 
    root: str = "raw", 
    rate_limit: int = 5,
    max_retries: int = 5
) -> Dict[str, Any]:
    """Harvest all NBA data for a specific date with comprehensive error handling.
    
    This is the main orchestration function that:
    1. Creates date directory structure
    2. Fetches scoreboard to discover games
    3. For each game, fetches all Tier A endpoints
    4. Handles errors gracefully with quarantine tracking
    5. Updates manifest with results
    
    Args:
        date_str: Date in YYYY-MM-DD format
        root: Root directory for raw data storage
        rate_limit: Requests per second limit
        max_retries: Maximum retries per endpoint
        
    Returns:
        Summary dictionary with harvest results
    """
    root_path = Path(root)
    date_dir = root_path / date_str
    
    # Ensure directories exist
    ensure_dir(root_path)
    ensure_dir(date_dir)
    
    summary = {
        'date': date_str,
        'games_discovered': 0,
        'games_processed': 0,
        'endpoints_succeeded': 0,
        'endpoints_failed': 0,
        'total_bytes': 0,
        'quarantined_games': [],
        'errors': []
    }
    
    async with RawNbaClient(rate_limit=rate_limit, max_retries=max_retries) as client:
        try:
            # Step 1: Fetch scoreboard for date
            logger.info("Fetching scoreboard for date", date=date_str)
            
            try:
                scoreboard_data = await client.fetch_scoreboard(date_str)
                
                # Write scoreboard data to date directory
                scoreboard_path = date_dir / "scoreboard.json"
                scoreboard_meta = write_json(scoreboard_path, scoreboard_data)
                summary['total_bytes'] += scoreboard_meta['bytes']
                
                logger.info("Wrote scoreboard data", 
                           path=str(scoreboard_path), 
                           size=scoreboard_meta['bytes'])
                
            except Exception as e:
                error_msg = f"Failed to fetch scoreboard: {str(e)}"
                logger.error(error_msg, date=date_str)
                summary['errors'].append(error_msg)
                return summary
            
            # Step 2: Parse game IDs from scoreboard
            game_ids = _parse_season_from_scoreboard(scoreboard_data)
            summary['games_discovered'] = len(game_ids)
            
            if not game_ids:
                logger.info("No regular season games found for date", date=date_str)
                return summary
            
            logger.info("Discovered regular season games", 
                       date=date_str, 
                       game_count=len(game_ids))
            
            # Step 3: Process each game
            for game_id in game_ids:
                game_success = await _harvest_single_game(
                    client, game_id, date_dir, summary
                )
                
                summary['games_processed'] += 1
                
                if not game_success:
                    summary['quarantined_games'].append(game_id)
                
                # Small delay between games to be respectful
                await asyncio.sleep(0.1)
        
        except Exception as e:
            error_msg = f"Fatal error during harvest: {str(e)}"
            logger.error(error_msg, date=date_str, traceback=traceback.format_exc())
            summary['errors'].append(error_msg)
    
    # Log final summary
    logger.info("Date harvest complete", 
               date=date_str,
               games_discovered=summary['games_discovered'],
               games_processed=summary['games_processed'],
               endpoints_succeeded=summary['endpoints_succeeded'],
               endpoints_failed=summary['endpoints_failed'],
               total_bytes=summary['total_bytes'],
               quarantined_count=len(summary['quarantined_games']))
    
    return summary


async def _harvest_single_game(
    client: RawNbaClient, 
    game_id: str, 
    date_dir: Path, 
    summary: Dict[str, Any]
) -> bool:
    """Harvest all endpoints for a single game with error tracking.
    
    Args:
        client: Raw NBA API client
        game_id: NBA game ID to harvest
        date_dir: Date directory path
        summary: Summary dict to update
        
    Returns:
        True if game was successfully harvested, False if quarantined
    """
    game_dir = date_dir / game_id
    ensure_dir(game_dir)
    
    # Track game-level results
    game_record = {
        'game_id': game_id,
        'teams': {},
        'endpoints': {},
        'errors': []
    }
    
    team_ids = None  # Will be extracted from boxscore summary
    
    # Define Tier A endpoints to fetch
    endpoints = [
        ('boxscoresummaryv2', client.fetch_boxscoresummary),
        ('boxscoretraditionalv2', client.fetch_boxscoretraditional),
        ('playbyplayv2', client.fetch_playbyplay),
    ]
    
    # Fetch core endpoints first
    for endpoint_name, fetch_func in endpoints:
        try:
            logger.debug("Fetching endpoint", game_id=game_id, endpoint=endpoint_name)
            
            endpoint_data = await fetch_func(game_id)
            
            # Write endpoint data
            endpoint_path = game_dir / f"{endpoint_name}.json"
            endpoint_meta = write_json(endpoint_path, endpoint_data)
            
            # Track success
            game_record['endpoints'][endpoint_name] = {
                'bytes': endpoint_meta['bytes'],
                'sha1': endpoint_meta['sha1'],
                'gz': endpoint_meta['gz'],
                'ok': True
            }
            
            summary['endpoints_succeeded'] += 1
            summary['total_bytes'] += endpoint_meta['bytes']
            
            # Extract team IDs from boxscore summary for shot chart
            if endpoint_name == 'boxscoresummaryv2' and not team_ids:
                team_ids = _extract_team_ids_from_summary(endpoint_data)
                if team_ids:
                    game_record['teams'] = team_ids
            
            logger.debug("Successfully fetched endpoint", 
                        game_id=game_id, 
                        endpoint=endpoint_name,
                        size=endpoint_meta['bytes'])
            
        except Exception as e:
            error_msg = f"{endpoint_name}: {str(e)}"
            logger.warning("Endpoint fetch failed", 
                          game_id=game_id, 
                          endpoint=endpoint_name,
                          error=str(e))
            
            game_record['endpoints'][endpoint_name] = {'ok': False}
            game_record['errors'].append({'endpoint': endpoint_name, 'error': str(e)})
            
            summary['endpoints_failed'] += 1
            
            # Append to quarantine file
            append_quarantine(game_id, endpoint_name, str(e))
    
    # Fetch shot chart (requires team IDs)
    try:
        logger.debug("Fetching shot chart", game_id=game_id)
        
        if team_ids:
            # Use extracted team IDs for fallback
            team_id_list = [team_ids['home_team_id'], team_ids['visitor_team_id']]
            shotchart_data = await client.fetch_shotchart(game_id, team_id_list)
        else:
            # Try without team IDs (game-scoped)
            shotchart_data = await client.fetch_shotchart(game_id)
        
        # Write shot chart data
        shotchart_path = game_dir / "shotchartdetail.json"
        shotchart_meta = write_json(shotchart_path, shotchart_data)
        
        game_record['endpoints']['shotchartdetail'] = {
            'bytes': shotchart_meta['bytes'],
            'sha1': shotchart_meta['sha1'],
            'gz': shotchart_meta['gz'],
            'ok': True
        }
        
        summary['endpoints_succeeded'] += 1
        summary['total_bytes'] += shotchart_meta['bytes']
        
        logger.debug("Successfully fetched shot chart", 
                    game_id=game_id,
                    size=shotchart_meta['bytes'])
        
    except Exception as e:
        error_msg = f"shotchartdetail: {str(e)}"
        logger.warning("Shot chart fetch failed", 
                      game_id=game_id,
                      error=str(e))
        
        game_record['endpoints']['shotchartdetail'] = {'ok': False}
        game_record['errors'].append({'endpoint': 'shotchartdetail', 'error': str(e)})
        
        summary['endpoints_failed'] += 1
        append_quarantine(game_id, 'shotchartdetail', str(e))
    
    # Update manifest with game record
    try:
        update_manifest(date_dir, game_record)
    except Exception as e:
        logger.error("Failed to update manifest for game", 
                    game_id=game_id, error=str(e))
    
    # Determine if game should be quarantined
    # Game is OK if it has at least one successful endpoint and no critical failures
    successful_endpoints = sum(1 for ep in game_record['endpoints'].values() 
                              if isinstance(ep, dict) and ep.get('ok', False))
    
    game_success = successful_endpoints >= 2  # Need at least 2 successful endpoints
    
    if not game_success:
        logger.warning("Game quarantined due to insufficient successful endpoints", 
                      game_id=game_id,
                      successful_endpoints=successful_endpoints,
                      total_endpoints=len(game_record['endpoints']))
    
    return game_success