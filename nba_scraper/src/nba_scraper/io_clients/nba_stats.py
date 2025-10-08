"""NBA Stats API client with async wrappers, direct API access, and content-hashing cache."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
import json

from ..config import get_settings
from ..nba_logging import get_logger
from ..models.utils import preprocess_nba_stats_data
from ..cache import get_cache_manager
from .http import HttpClient

logger = get_logger(__name__)


class NBAStatsClient:
    """Async client for NBA Stats API endpoints with direct API access and caching."""
    
    def __init__(self) -> None:
        """Initialize NBA Stats client."""
        self.settings = get_settings()
        self.base_url = "https://stats.nba.com/stats"
        
        # Initialize HTTP client
        self.http_client = HttpClient()
        
        # Cache manager for content hashing
        self.cache_manager = get_cache_manager()
        
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

    async def _fetch_with_cache(
        self, 
        endpoint: str, 
        params: Dict[str, Any],
        cache_ttl: Optional[int] = None
    ) -> Dict[str, Any]:
        """Fetch data with content-hashing cache support.
        
        Args:
            endpoint: API endpoint name
            params: Query parameters
            cache_ttl: Cache TTL override (default: 1 hour for most endpoints)
            
        Returns:
            Preprocessed API response data
        """
        url = f"{self.base_url}/{endpoint}"
        
        # Default cache TTL based on endpoint type
        if cache_ttl is None:
            if endpoint in ['scoreboardv2']:
                cache_ttl = 300  # 5 minutes for live data
            elif endpoint in ['playbyplayv2', 'boxscoretraditionalv2']:
                cache_ttl = 3600  # 1 hour for game data
            else:
                cache_ttl = 1800  # 30 minutes for other data
        
        try:
            # Make the HTTP request
            response = await self.http_client.get(
                url,
                params=params,
                headers=self.headers
            )
            
            # If response is already a dict (JSON), use it directly
            if isinstance(response, dict):
                raw_data = response
            else:
                # Otherwise parse as JSON
                raw_data = json.loads(response)
            
            return self._preprocess_api_response(raw_data)
            
        except Exception as e:
            logger.error(f"NBA Stats API request failed: {e}", endpoint=endpoint, params=params)
            raise

    def _preprocess_api_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess NBA Stats API response to handle type inconsistencies."""
        try:
            # Apply comprehensive preprocessing to the entire response
            processed_data = preprocess_nba_stats_data(data)
            
            # Additional preprocessing for nested resultSets structure
            if 'resultSets' in processed_data:
                processed_result_sets = []
                for result_set in processed_data['resultSets']:
                    processed_result_set = preprocess_nba_stats_data(result_set)
                    
                    # Process the rowSet data which contains the actual API values
                    if 'rowSet' in processed_result_set and isinstance(processed_result_set['rowSet'], list):
                        processed_rows = []
                        for row in processed_result_set['rowSet']:
                            if isinstance(row, list):
                                # Convert each value in the row to prevent type comparison issues
                                processed_row = []
                                for value in row:
                                    # Convert potentially problematic values to strings
                                    if isinstance(value, (int, float)) and value is not None:
                                        # Special handling for certain numeric fields that might be compared as enums
                                        processed_row.append(str(value))
                                    else:
                                        processed_row.append(value)
                                processed_rows.append(processed_row)
                            else:
                                processed_rows.append(row)
                        processed_result_set['rowSet'] = processed_rows
                    
                    processed_result_sets.append(processed_result_set)
                processed_data['resultSets'] = processed_result_sets
            
            return processed_data
            
        except Exception as e:
            logger.warning("Failed to preprocess API response, returning raw data", error=str(e))
            return data

    # Fetcher methods with natural keys and content hashing
    
    async def fetch_scoreboard_by_date(self, date_utc: datetime) -> Dict[str, Any]:
        """Fetch scoreboard data for a specific date with caching.
        
        Natural key: date_utc
        Cache key: SHA256(url + params)
        """
        date_str = date_utc.strftime('%m/%d/%Y')
        params = {
            'GameDate': date_str,
            'LeagueID': '00',
            'DayOffset': '0'
        }
        
        logger.info("Fetching NBA Stats scoreboard", date=date_str)
        
        data = await self._fetch_with_cache('scoreboardv2', params, cache_ttl=300)
        
        # Count games for logging
        games_count = 0
        if 'resultSets' in data:
            for result_set in data['resultSets']:
                if result_set.get('name') == 'GameHeader':
                    games_count = len(result_set.get('rowSet', []))
                    break
        
        logger.info("Fetched NBA Stats scoreboard", date=date_str, games_count=games_count)
        return data

    async def fetch_boxscore(self, game_id: str) -> Dict[str, Any]:
        """Fetch boxscore data for a game with caching.
        
        Natural key: game_id
        Cache key: SHA256(url + params)
        """
        params = {
            'GameID': game_id,
            'StartPeriod': '0',
            'EndPeriod': '10',
            'StartRange': '0',
            'EndRange': '28800',
            'RangeType': '0'
        }
        
        logger.info("Fetching NBA Stats boxscore", game_id=game_id)
        
        data = await self._fetch_with_cache('boxscoretraditionalv2', params)
        
        logger.info("Fetched NBA Stats boxscore", game_id=game_id)
        return data

    async def fetch_pbp(self, game_id: str, start_period: int = 0, end_period: int = 10) -> Dict[str, Any]:
        """Fetch play-by-play data for a game with caching.
        
        Natural key: (game_id, start_period, end_period)
        Cache key: SHA256(url + params)
        """
        params = {
            'GameID': game_id,
            'StartPeriod': str(start_period),
            'EndPeriod': str(end_period)
        }
        
        logger.info("Fetching NBA Stats PBP", game_id=game_id, start_period=start_period, end_period=end_period)
        
        data = await self._fetch_with_cache('playbyplayv2', params)
        
        # Count events for logging
        events_count = 0
        if 'resultSets' in data:
            for result_set in data['resultSets']:
                if result_set.get('name') == 'PlayByPlay':
                    events_count = len(result_set.get('rowSet', []))
                    break
        
        logger.info("Fetched NBA Stats PBP", game_id=game_id, events_count=events_count)
        return data

    async def fetch_boxscore_advanced(self, game_id: str) -> Dict[str, Any]:
        """Fetch advanced boxscore data with caching.
        
        Natural key: game_id
        Cache key: SHA256(url + params)
        """
        params = {
            'GameID': game_id,
            'StartPeriod': '0',
            'EndPeriod': '10',
            'StartRange': '0',
            'EndRange': '28800',
            'RangeType': '0'
        }
        
        logger.info("Fetching NBA Stats advanced boxscore", game_id=game_id)
        data = await self._fetch_with_cache('boxscoreadvancedv2', params)
        logger.info("Fetched NBA Stats advanced boxscore", game_id=game_id)
        return data

    async def fetch_team_game_stats(self, team_id: str, season: str = "2023-24") -> Dict[str, Any]:
        """Fetch team game logs with caching.
        
        Natural key: (team_id, season)
        Cache key: SHA256(url + params)
        """
        params = {
            'TeamID': team_id,
            'Season': season,
            'SeasonType': 'Regular Season',
            'LeagueID': '00'
        }
        
        logger.info("Fetching NBA Stats team game stats", team_id=team_id, season=season)
        data = await self._fetch_with_cache('teamgamelogs', params, cache_ttl=1800)
        logger.info("Fetched NBA Stats team game stats", team_id=team_id, season=season)
        return data

    async def close(self):
        """Close the client (no-op for this implementation)."""
        pass