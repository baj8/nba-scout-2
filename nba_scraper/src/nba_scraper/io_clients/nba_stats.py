"""NBA Stats API client with async wrappers and direct API access."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
import json

from ..config import get_settings
from ..http import get
from ..nba_logging import get_logger

logger = get_logger(__name__)


class NBAStatsClient:
    """Async client for NBA Stats API endpoints with direct API access."""
    
    def __init__(self) -> None:
        """Initialize NBA Stats client."""
        self.settings = get_settings()
        self.base_url = "https://stats.nba.com/stats"
        
        # Standard headers required by NBA Stats API
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'stats.nba.com',
            'Origin': 'https://www.nba.com',
            'Referer': 'https://www.nba.com/',
            'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
            'sec-ch-ua-mobile': '?0',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
        }
    
    async def fetch_scoreboard_by_date(self, date_utc: datetime) -> Dict[str, Any]:
        """Fetch scoreboard data for a specific date using direct API access.
        
        Args:
            date_utc: Date to fetch games for
            
        Returns:
            Scoreboard data dictionary
        """
        # Format date for NBA Stats API (MM/DD/YYYY)
        date_str = date_utc.strftime('%m/%d/%Y')
        
        try:
            # Direct API call with proper parameters
            params = {
                'GameDate': date_str,
                'LeagueID': '00',
                'DayOffset': '0'
            }
            
            url = f"{self.base_url}/scoreboardv2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats scoreboard", date=date_str, url=url)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            # Count games for logging
            games_count = 0
            if 'resultSets' in data:
                for result_set in data['resultSets']:
                    if result_set.get('name') == 'GameHeader':
                        games_count = len(result_set.get('rowSet', []))
                        break
            
            logger.info("Fetched NBA Stats scoreboard", 
                       date=date_str, games_count=games_count)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats scoreboard", 
                        date=date_str, error=str(e))
            raise
    
    async def fetch_boxscore(self, game_id: str) -> Dict[str, Any]:
        """Fetch boxscore data for a game using direct API access.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Boxscore data dictionary
        """
        try:
            params = {
                'GameID': game_id,
                'StartPeriod': '0',
                'EndPeriod': '10',
                'StartRange': '0',
                'EndRange': '28800',
                'RangeType': '0'
            }
            
            url = f"{self.base_url}/boxscoretraditionalv2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats boxscore", game_id=game_id)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            logger.info("Fetched NBA Stats boxscore", game_id=game_id)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats boxscore", 
                        game_id=game_id, error=str(e))
            raise
    
    async def fetch_pbp(self, game_id: str, start_period: int = 0, end_period: int = 10) -> Dict[str, Any]:
        """Fetch play-by-play data for a game using direct API access.
        
        Args:
            game_id: NBA Stats game ID
            start_period: Starting period (0 = all)
            end_period: Ending period (10 = all)
            
        Returns:
            Play-by-play data dictionary
        """
        try:
            params = {
                'GameID': game_id,
                'StartPeriod': str(start_period),
                'EndPeriod': str(end_period)
            }
            
            url = f"{self.base_url}/playbyplayv2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats PBP", game_id=game_id)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            # Count events for logging
            events_count = 0
            if 'resultSets' in data:
                for result_set in data['resultSets']:
                    if result_set.get('name') == 'PlayByPlay':
                        events_count = len(result_set.get('rowSet', []))
                        break
            
            logger.info("Fetched NBA Stats PBP", 
                       game_id=game_id, events_count=events_count)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats PBP", 
                        game_id=game_id, error=str(e))
            raise
    
    async def fetch_shotchart(self, team_id: str, game_id: str, season: str = "2023-24") -> Dict[str, Any]:
        """Fetch shot chart data for a team in a game using direct API access.
        
        Args:
            team_id: NBA Stats team ID
            game_id: NBA Stats game ID
            season: Season string (e.g., '2023-24')
            
        Returns:
            Shot chart data dictionary
        """
        try:
            params = {
                'TeamID': team_id,
                'PlayerID': '0',  # All players
                'GameID': game_id,
                'Season': season,
                'SeasonType': 'Regular Season',
                'LeagueID': '00',
                'ContextMeasure': 'FGA'
            }
            
            url = f"{self.base_url}/shotchartdetail?" + urlencode(params)
            
            logger.info("Fetching NBA Stats shot chart", 
                       team_id=team_id, game_id=game_id)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            # Count shots for logging
            shots_count = 0
            if 'resultSets' in data:
                for result_set in data['resultSets']:
                    if 'Shot' in result_set.get('name', ''):
                        shots_count = len(result_set.get('rowSet', []))
                        break
            
            logger.info("Fetched NBA Stats shot chart", 
                       team_id=team_id, game_id=game_id, shots_count=shots_count)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats shot chart", 
                        team_id=team_id, game_id=game_id, error=str(e))
            raise
    
    async def fetch_schedule(self, season: str = "2023-24") -> Dict[str, Any]:
        """Fetch full season schedule using direct API access.
        
        Args:
            season: Season string (e.g., '2023-24')
            
        Returns:
            Schedule data dictionary
        """
        try:
            params = {
                'LeagueID': '00',
                'Season': season,
                'SeasonType': 'Regular Season',
                'TeamID': '0'
            }
            
            url = f"{self.base_url}/leaguegamefinder?" + urlencode(params)
            
            logger.info("Fetching NBA Stats schedule", season=season)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            logger.info("Fetched NBA Stats schedule", season=season)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats schedule", 
                        season=season, error=str(e))
            raise
    
    async def fetch_team_roster(self, team_id: str, season: str = "2023-24") -> Dict[str, Any]:
        """Fetch team roster for a season.
        
        Args:
            team_id: NBA Stats team ID
            season: Season string (e.g., '2023-24')
            
        Returns:
            Team roster data dictionary
        """
        try:
            params = {
                'TeamID': team_id,
                'Season': season,
                'LeagueID': '00'
            }
            
            url = f"{self.base_url}/commonteamroster?" + urlencode(params)
            
            logger.info("Fetching NBA Stats team roster", team_id=team_id, season=season)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            logger.info("Fetched NBA Stats team roster", team_id=team_id, season=season)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats team roster", 
                        team_id=team_id, season=season, error=str(e))
            raise
    
    def parse_scoreboard_games(self, scoreboard_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse games from scoreboard response.
        
        Args:
            scoreboard_data: Raw scoreboard response
            
        Returns:
            List of game dictionaries
        """
        games = []
        
        try:
            # NBA Stats API returns data in resultSets format
            if 'resultSets' not in scoreboard_data:
                logger.warning("No resultSets in scoreboard data")
                return games
            
            for result_set in scoreboard_data['resultSets']:
                if result_set.get('name') == 'GameHeader':
                    headers = result_set.get('headers', [])
                    rows = result_set.get('rowSet', [])
                    
                    for row in rows:
                        if len(row) == len(headers):
                            # Convert row to dictionary using headers
                            game_dict = dict(zip(headers, row))
                            games.append(game_dict)
                        else:
                            logger.warning("Header/row mismatch in scoreboard data",
                                         headers_count=len(headers), row_count=len(row))
            
            logger.debug("Parsed scoreboard games", count=len(games))
            
        except Exception as e:
            logger.warning("Failed to parse scoreboard games", error=str(e))
        
        return games
    
    def parse_pbp_events(self, pbp_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse events from play-by-play response.
        
        Args:
            pbp_data: Raw PBP response
            
        Returns:
            List of event dictionaries
        """
        events = []
        
        try:
            if 'resultSets' not in pbp_data:
                logger.warning("No resultSets in PBP data")
                return events
            
            for result_set in pbp_data['resultSets']:
                if result_set.get('name') == 'PlayByPlay':
                    headers = result_set.get('headers', [])
                    rows = result_set.get('rowSet', [])
                    
                    for row in rows:
                        if len(row) == len(headers):
                            # Convert row to dictionary using headers
                            event_dict = dict(zip(headers, row))
                            events.append(event_dict)
                        else:
                            logger.warning("Header/row mismatch in PBP data",
                                         headers_count=len(headers), row_count=len(row))
            
            logger.debug("Parsed PBP events", count=len(events))
            
        except Exception as e:
            logger.warning("Failed to parse PBP events", error=str(e))
        
        return events
    
    def parse_boxscore_stats(self, boxscore_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse player and team stats from boxscore response.
        
        Args:
            boxscore_data: Raw boxscore response
            
        Returns:
            Dictionary with 'players' and 'teams' stats
        """
        result = {'players': [], 'teams': []}
        
        try:
            if 'resultSets' not in boxscore_data:
                return result
            
            for result_set in boxscore_data['resultSets']:
                name = result_set.get('name', '')
                headers = result_set.get('headers', [])
                rows = result_set.get('rowSet', [])
                
                if 'PlayerStats' in name:
                    for row in rows:
                        if len(row) == len(headers):
                            player_dict = dict(zip(headers, row))
                            result['players'].append(player_dict)
                
                elif 'TeamStats' in name:
                    for row in rows:
                        if len(row) == len(headers):
                            team_dict = dict(zip(headers, row))
                            result['teams'].append(team_dict)
            
            logger.debug("Parsed boxscore stats", 
                        players_count=len(result['players']),
                        teams_count=len(result['teams']))
            
        except Exception as e:
            logger.warning("Failed to parse boxscore stats", error=str(e))
        
        return result
    
    def get_available_endpoints(self) -> List[str]:
        """Get list of available API endpoints for debugging."""
        return [
            'scoreboardv2',
            'boxscoretraditionalv2', 
            'playbyplayv2',
            'shotchartdetail',
            'leaguegamefinder',
            'commonteamroster',
            'teamgamelogs',
            'playergamelogs',
            'leaguedashteamstats',
            'leaguedashplayerstats'
        ]
    
    async def close(self):
        """Close the client (no-op for this implementation)."""
        pass