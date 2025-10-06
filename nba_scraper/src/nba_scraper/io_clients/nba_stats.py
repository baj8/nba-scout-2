"""NBA Stats API client with async wrappers and direct API access."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
import json

from ..config import get_settings
from ..http import get
from ..nba_logging import get_logger
from ..models.utils import preprocess_nba_stats_data

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

    def _preprocess_api_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess NBA Stats API response to prevent int/str comparison errors.
        
        This is the critical fix for the int/str comparison error. It ensures
        all data is properly normalized before reaching extractors or models.
        """
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
    
    async def fetch_scoreboard_by_date(self, date_utc: datetime) -> Dict[str, Any]:
        """Fetch scoreboard data for a specific date using direct API access.
        
        Args:
            date_utc: Date to fetch games for
            
        Returns:
            Preprocessed scoreboard data dictionary
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
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
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
            Preprocessed boxscore data dictionary
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
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
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
            Preprocessed play-by-play data dictionary
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
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
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
    
    async def fetch_shotchart_reliable(self, game_id: str, season: str = "2024-25") -> Dict[str, Any]:
        """Fetch shot chart data using nba_api library for reliability.
        
        Args:
            game_id: NBA Stats game ID
            season: Season string (e.g., '2024-25')
            
        Returns:
            Dictionary with shot chart data and metadata
        """
        try:
            from nba_api.stats.endpoints import shotchartdetail
            import pandas as pd
            import time
            
            logger.info("Fetching shot chart using nba_api", game_id=game_id, season=season)
            
            # Use the reliable nba_api approach with proper parameters
            resp = shotchartdetail.ShotChartDetail(
                team_id=0,                          # 0 = all teams
                player_id=0,                        # 0 = all players
                season_nullable=season,
                season_type_all_star="Regular Season",
                context_measure_simple="FGA",       # returns all attempts (makes + misses)
                date_from_nullable="",              # IMPORTANT: use "" not None
                date_to_nullable="",                # same
                game_id_nullable=game_id,           # Filter by specific game
                opponent_team_id=0,
                period=0,                           # 0 = all periods
                player_position_nullable="",
                outcome_nullable="",
                location_nullable="",
                month=0,
                season_segment_nullable="",
                last_n_games=0,
                ahead_behind_nullable="",
                clutch_time_nullable="",
                rookie_year_nullable="",
                vs_conference_nullable="",
                vs_division_nullable="",
                game_segment_nullable=""
            )
            
            # Sometimes first request 500s - add retry logic
            try:
                df = resp.get_data_frames()[0]  # Table "Shot_Chart_Detail"
            except Exception as e:
                logger.warning("First shot chart request failed, retrying", game_id=game_id, error=str(e))
                time.sleep(1)
                resp = shotchartdetail.ShotChartDetail(
                    team_id=0, player_id=0, season_nullable=season,
                    season_type_all_star="Regular Season", context_measure_simple="FGA",
                    date_from_nullable="", date_to_nullable="", game_id_nullable=game_id,
                    opponent_team_id=0, period=0, player_position_nullable="",
                    outcome_nullable="", location_nullable="", month=0,
                    season_segment_nullable="", last_n_games=0, ahead_behind_nullable="",
                    clutch_time_nullable="", rookie_year_nullable="", vs_conference_nullable="",
                    vs_division_nullable="", game_segment_nullable=""
                )
                df = resp.get_data_frames()[0]
            
            # Validate expected columns are present
            required_cols = {"LOC_X", "LOC_Y", "SHOT_DISTANCE", "GAME_EVENT_ID", "SHOT_MADE_FLAG"}
            if not required_cols.issubset(df.columns):
                missing = required_cols - set(df.columns)
                logger.warning("Missing shot chart columns", game_id=game_id, missing=list(missing))
            
            # Convert DataFrame to our expected format
            shot_data = {
                'resultSets': [{
                    'name': 'Shot_Chart_Detail',
                    'headers': df.columns.tolist(),
                    'rowSet': df.values.tolist()
                }]
            }
            
            logger.info("Successfully fetched shot chart via nba_api", 
                       game_id=game_id, shots_count=len(df))
            
            return shot_data
            
        except Exception as e:
            logger.error("Failed to fetch shot chart via nba_api", 
                        game_id=game_id, season=season, error=str(e))
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
    
    async def fetch_boxscore_advanced(self, game_id: str) -> Dict[str, Any]:
        """Fetch advanced boxscore data including efficiency metrics.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Preprocessed advanced boxscore data dictionary
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
            
            url = f"{self.base_url}/boxscoreadvancedv2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats advanced boxscore", game_id=game_id)
            
            response = await get(url, headers=self.headers)
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
            logger.info("Fetched NBA Stats advanced boxscore", game_id=game_id)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats advanced boxscore", 
                        game_id=game_id, error=str(e))
            raise

    async def fetch_boxscore_misc(self, game_id: str) -> Dict[str, Any]:
        """Fetch miscellaneous boxscore stats including plus/minus, usage rate.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Preprocessed miscellaneous stats data dictionary
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
            
            url = f"{self.base_url}/boxscoremiscv2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats misc boxscore", game_id=game_id)
            
            response = await get(url, headers=self.headers)
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
            logger.info("Fetched NBA Stats misc boxscore", game_id=game_id)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats misc boxscore", 
                        game_id=game_id, error=str(e))
            raise

    async def fetch_boxscore_usage(self, game_id: str) -> Dict[str, Any]:
        """Fetch usage stats including usage rate and pace impact.
        
        Args:
            game_id: NBA Stats game ID
            
        Returns:
            Preprocessed usage stats data dictionary
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
            
            url = f"{self.base_url}/boxscoreusagev2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats usage boxscore", game_id=game_id)
            
            response = await get(url, headers=self.headers)
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
            logger.info("Fetched NBA Stats usage boxscore", game_id=game_id)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats usage boxscore", 
                        game_id=game_id, error=str(e))
            raise

    async def fetch_team_game_stats(self, team_id: str, season: str = "2023-24") -> Dict[str, Any]:
        """Fetch team game logs with advanced metrics.
        
        Args:
            team_id: NBA Stats team ID
            season: Season string (e.g., '2023-24')
            
        Returns:
            Team game stats data dictionary
        """
        try:
            params = {
                'TeamID': team_id,
                'Season': season,
                'SeasonType': 'Regular Season',
                'LeagueID': '00'
            }
            
            url = f"{self.base_url}/teamgamelogs?" + urlencode(params)
            
            logger.info("Fetching NBA Stats team game stats", team_id=team_id, season=season)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            logger.info("Fetched NBA Stats team game stats", team_id=team_id, season=season)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats team game stats", 
                        team_id=team_id, season=season, error=str(e))
            raise

    async def fetch_player_game_stats(self, player_id: str, season: str = "2023-24") -> Dict[str, Any]:
        """Fetch player game logs with advanced metrics.
        
        Args:
            player_id: NBA Stats player ID
            season: Season string (e.g., '2023-24')
            
        Returns:
            Player game stats data dictionary
        """
        try:
            params = {
                'PlayerID': player_id,
                'Season': season,
                'SeasonType': 'Regular Season',
                'LeagueID': '00'
            }
            
            url = f"{self.base_url}/playergamelogs?" + urlencode(params)
            
            logger.info("Fetching NBA Stats player game stats", player_id=player_id, season=season)
            
            response = await get(url, headers=self.headers)
            data = response.json()
            
            logger.info("Fetched NBA Stats player game stats", player_id=player_id, season=season)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats player game stats", 
                        player_id=player_id, season=season, error=str(e))
            raise

    async def boxscore_summary(self, game_id: str) -> dict:
        """Fetch BoxScoreSummaryV2 data for a game.
        
        Args:
            game_id: NBA game ID (e.g., "0022300001")
            
        Returns:
            BoxScoreSummaryV2 API response containing GameSummary result set
        """
        try:
            params = {
                'GameID': game_id,
                'Season': '2024-25',  # Default season, may need to be dynamic
                'SeasonType': 'Regular Season'
            }
            
            url = f"{self.base_url}/boxscoresummaryv2?" + urlencode(params)
            
            logger.info("Fetching NBA Stats boxscore summary", game_id=game_id)
            
            response = await get(url, headers=self.headers)
            raw_data = response.json()
            
            # CRITICAL: Preprocess the response to prevent int/str comparison errors
            data = self._preprocess_api_response(raw_data)
            
            # Log summary of response structure for debugging
            if data and "resultSets" in data:
                result_set_names = [rs.get("name", "unknown") for rs in data["resultSets"]]
                logger.debug("BoxScoreSummary result sets", 
                            game_id=game_id, 
                            result_sets=result_set_names)
            
            logger.info("Fetched NBA Stats boxscore summary", game_id=game_id)
            
            return data
            
        except Exception as e:
            logger.error("Failed to fetch NBA Stats boxscore summary", 
                        game_id=game_id, error=str(e))
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
            'boxscoreadvancedv2',
            'boxscoremiscv2', 
            'boxscoreusagev2',
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