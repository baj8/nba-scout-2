"""Raw NBA API client for bronze data capture with comprehensive rate limiting and retry logic."""

import asyncio
import os
import random
import time
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
import hashlib

import httpx
from tenacity import (
    retry,
    stop_after_attempt, 
    wait_exponential,
    retry_if_exception_type,
    RetryCallState
)

from ..nba_logging import get_logger

logger = get_logger(__name__)


class RawNbaClient:
    """Async NBA Stats API client optimized for raw data harvesting with bronze layer persistence."""
    
    def __init__(
        self,
        rate_limit: int = 5,
        timeout: int = 30,
        proxy: Optional[str] = None,
        max_retries: int = 5
    ):
        """Initialize raw NBA client with browser-like headers and rate limiting.
        
        Args:
            rate_limit: Requests per second (default 5)
            timeout: Request timeout in seconds
            proxy: Optional proxy URL
            max_retries: Maximum retry attempts per request
        """
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Token bucket for rate limiting
        self._tokens = float(rate_limit)
        self._last_update = time.time()
        self._lock = asyncio.Lock()
        
        # Browser-like headers to avoid detection
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Host': 'stats.nba.com',
            'Origin': 'https://www.nba.com',
            'Pragma': 'no-cache',
            'Referer': 'https://www.nba.com/',
            'sec-ch-ua': '"Google Chrome";v="120", " Not A;Brand";v="99", "Chromium";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        # HTTP client configuration
        client_kwargs = {
            'timeout': httpx.Timeout(timeout, connect=10.0),
            'headers': self.headers,
            'follow_redirects': True,
            'limits': httpx.Limits(max_keepalive_connections=10, max_connections=20)
        }
        
        if proxy:
            client_kwargs['proxies'] = proxy
            
        self.client = httpx.AsyncClient(**client_kwargs)
        self.base_url = "https://stats.nba.com/stats"
        
    @classmethod
    def from_env(cls) -> "RawNbaClient":
        """Create client from environment variables.
        
        Environment variables:
            NBA_API_RATE_LIMIT: int, default 5
            NBA_API_TIMEOUT: int seconds, default 30  
            NBA_API_PROXY: optional proxy URL
            NBA_API_MAX_RETRIES: int, default 5
        """
        return cls(
            rate_limit=int(os.getenv('NBA_API_RATE_LIMIT', '5')),
            timeout=int(os.getenv('NBA_API_TIMEOUT', '30')),
            proxy=os.getenv('NBA_API_PROXY'),
            max_retries=int(os.getenv('NBA_API_MAX_RETRIES', '5'))
        )
    
    async def _acquire_token(self) -> None:
        """Acquire rate limiting token using token bucket algorithm."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            
            # Add tokens based on elapsed time
            self._tokens = min(self.rate_limit, self._tokens + elapsed * self.rate_limit)
            self._last_update = now
            
            # Wait if no tokens available
            if self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / self.rate_limit
                logger.debug("Rate limit reached, waiting", wait_time=wait_time)
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0
    
    def _should_retry(self, retry_state: RetryCallState) -> bool:
        """Custom retry logic respecting Retry-After headers."""
        if not retry_state.outcome:
            return True
            
        if retry_state.outcome.failed:
            exception = retry_state.outcome.exception()
            if isinstance(exception, httpx.HTTPStatusError):
                status_code = exception.response.status_code
                
                # Always retry on server errors
                if 500 <= status_code < 600:
                    return True
                    
                # Retry on rate limiting with backoff
                if status_code == 429:
                    retry_after = exception.response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait_time = int(retry_after)
                            logger.info("Retry-After header found", wait_time=wait_time)
                            # Will be handled by exponential backoff
                        except ValueError:
                            pass
                    return True
                    
                # Don't retry client errors (except 429)
                if 400 <= status_code < 500:
                    return False
                    
        return True
    
    async def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make rate-limited request with comprehensive retry logic."""
        
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=10.0),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException, httpx.RequestError)),
            reraise=True
        )
        async def _request_with_retry():
            await self._acquire_token()
            
            # Add manual jitter since tenacity version doesn't support it
            jitter_delay = random.uniform(0, 0.5)
            if jitter_delay > 0:
                await asyncio.sleep(jitter_delay)
            
            logger.debug("Making NBA API request", url=url, params=params)
            
            response = await self.client.get(url, params=params)
            
            # Handle Retry-After header manually if present
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                        logger.info("Rate limited with Retry-After, waiting", wait_time=wait_time)
                        await asyncio.sleep(wait_time)
                    except ValueError:
                        pass
                # Raise error to trigger retry
                response.raise_for_status()
            
            # Raise for other HTTP errors (4xx, 5xx)
            response.raise_for_status()
            
            data = response.json()
            logger.debug("NBA API request successful", 
                        url=url, 
                        status_code=response.status_code,
                        response_size=len(response.content))
            
            return data
        
        return await _request_with_retry()
    
    async def fetch_scoreboard(self, date_str: str) -> Dict[str, Any]:
        """Fetch scoreboard data for a specific date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            Raw NBA Stats API response dict
        """
        params = {
            'GameDate': date_str,
            'LeagueID': '00',
            'DayOffset': '0'
        }
        
        url = f"{self.base_url}/scoreboardv2"
        return await self._make_request(url, params)
    
    async def fetch_boxscoresummary(self, game_id: str) -> Dict[str, Any]:
        """Fetch comprehensive boxscore summary including GameSummary, LineScore, Officials.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Raw NBA Stats API response dict
        """
        params = {
            'GameID': game_id
        }
        
        url = f"{self.base_url}/boxscoresummaryv2"
        return await self._make_request(url, params)
    
    async def fetch_boxscoretraditional(self, game_id: str) -> Dict[str, Any]:
        """Fetch traditional boxscore with TeamStats and PlayerStats totals.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Raw NBA Stats API response dict
        """
        params = {
            'GameID': game_id,
            'StartPeriod': '0',
            'EndPeriod': '10',
            'StartRange': '0',
            'EndRange': '28800',
            'RangeType': '0'
        }
        
        url = f"{self.base_url}/boxscoretraditionalv2"
        return await self._make_request(url, params)
    
    async def fetch_playbyplay(self, game_id: str) -> Dict[str, Any]:
        """Fetch complete play-by-play data for a game.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Raw NBA Stats API response dict
        """
        params = {
            'GameID': game_id,
            'StartPeriod': '0',
            'EndPeriod': '10'
        }
        
        url = f"{self.base_url}/playbyplayv2"
        return await self._make_request(url, params)
    
    async def fetch_shotchart(self, game_id: str, team_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        """Fetch shot chart detail with full-game shots including coordinates and zones.
        
        Prefers game-scoped call if possible, otherwise loops team_ids and merges with deduplication.
        
        Args:
            game_id: NBA Stats game ID
            team_ids: Optional list of team IDs for team-specific calls
            
        Returns:
            Raw NBA Stats API response dict with merged shot data
        """
        # Try game-scoped request first (most efficient)
        params = {
            'GameID': game_id,
            'Season': '2024-25',  # Will need season detection in production
            'SeasonType': 'Regular Season',
            'TeamID': '0',  # 0 = all teams
            'PlayerID': '0',  # 0 = all players
            'ContextMeasure': 'FGA',
            'DateFrom': '',
            'DateTo': '',
            'OpponentTeamID': '0',
            'Period': '0',
            'VsConference': '',
            'VsDivision': '',
            'PlayerPosition': '',
            'RookieYear': '',
            'GameSegment': '',
            'ClutchTime': '',
            'AheadBehind': '',
            'PointDiff': '',
            'RangeType': '0',
            'StartPeriod': '0',
            'EndPeriod': '10',
            'StartRange': '0',
            'EndRange': '28800',
            'SeasonSegment': '',
            'Location': '',
            'Outcome': '',
            'SeasonTypeAllStar': 'Regular Season'
        }
        
        url = f"{self.base_url}/shotchartdetail"
        
        try:
            # Try game-scoped request
            return await self._make_request(url, params)
        except Exception as e:
            logger.warning("Game-scoped shot chart failed, trying team-based approach", 
                          game_id=game_id, error=str(e))
            
            if not team_ids:
                logger.error("No team_ids provided for fallback shot chart request", game_id=game_id)
                raise
            
            # Fallback: fetch per team and merge
            all_shots = []
            headers = None
            
            for team_id in team_ids:
                try:
                    team_params = params.copy()  
                    team_params['TeamID'] = str(team_id)
                    
                    team_data = await self._make_request(url, team_params)
                    
                    if 'resultSets' in team_data:
                        for result_set in team_data['resultSets']:
                            if result_set.get('name') == 'Shot_Chart_Detail':
                                if headers is None:
                                    headers = result_set.get('headers', [])
                                
                                shots = result_set.get('rowSet', [])
                                all_shots.extend(shots)
                                
                except Exception as team_e:
                    logger.warning("Failed to fetch shots for team", 
                                  game_id=game_id, team_id=team_id, error=str(team_e))
            
            # Deduplicate using stable key
            if all_shots and headers:
                seen_shots = set()
                dedupe_shots = []
                
                # Find indices for deduplication key fields
                key_fields = ['GAME_ID', 'PLAYER_ID', 'PERIOD', 'MINUTES_REMAINING', 
                             'SECONDS_REMAINING', 'LOC_X', 'LOC_Y']
                key_indices = []
                
                for field in key_fields:
                    try:
                        key_indices.append(headers.index(field))
                    except ValueError:
                        logger.warning("Missing shot chart field for deduplication", field=field)
                
                for shot in all_shots:
                    if len(shot) >= len(headers):
                        # Create deduplication key
                        key_values = []
                        for idx in key_indices:
                            key_values.append(str(shot[idx]) if idx < len(shot) else '')
                        
                        shot_key = '|'.join(key_values)
                        
                        if shot_key not in seen_shots:
                            seen_shots.add(shot_key)
                            dedupe_shots.append(shot)
                
                logger.info("Merged and deduplicated shot chart data", 
                           game_id=game_id, 
                           total_shots=len(all_shots),
                           unique_shots=len(dedupe_shots))
                
                # Return in NBA Stats API format
                return {
                    'resource': 'shotchartdetail',
                    'parameters': params,
                    'resultSets': [{
                        'name': 'Shot_Chart_Detail',
                        'headers': headers,
                        'rowSet': dedupe_shots
                    }]
                }
            else:
                raise Exception("No shot chart data retrieved from any team")
    
    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()
        logger.info("Raw NBA client closed")
    
    async def __aenter__(self) -> "RawNbaClient":
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()