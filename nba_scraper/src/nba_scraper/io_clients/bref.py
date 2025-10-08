"""Basketball Reference client with HTML parsing."""

import re
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser

from ..config import get_settings
from ..nba_logging import get_logger
from .http import HttpClient

logger = get_logger(__name__)


class BRefClient:
    """Async client for Basketball Reference data."""
    
    def __init__(self) -> None:
        """Initialize Basketball Reference client."""
        self.settings = get_settings()
        self.base_url = self.settings.bref_base_url
        
        # Initialize HTTP client
        self.http_client = HttpClient()
        
        # Headers for Basketball Reference requests
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    async def _make_request(self, url: str) -> str:
        """Make HTTP request using HTTP client.
        
        Args:
            url: Request URL
            
        Returns:
            HTML response text
        """
        try:
            response = await self.http_client.get(url, headers=self.headers)
            
            # Convert response to string if it's bytes
            if isinstance(response, bytes):
                return response.decode('utf-8')
            elif isinstance(response, str):
                return response
            else:
                return str(response)
            
        except Exception as e:
            logger.error(f"Basketball Reference request failed: {e}", url=url)
            raise

    def _safe_preprocess_data(self, data: Any) -> Any:
        """Safely preprocess data to prevent int/str comparison errors.
        
        This ensures all data from Basketball Reference HTML parsing
        is properly normalized to prevent comparison errors.
        """
        try:
            if isinstance(data, dict):
                processed = {}
                for key, value in data.items():
                    # Convert numeric values that might be used in comparisons to strings
                    if isinstance(value, (int, float)) and value is not None:
                        processed[key] = str(value)
                    elif isinstance(value, dict):
                        processed[key] = self._safe_preprocess_data(value)
                    elif isinstance(value, list):
                        processed[key] = [self._safe_preprocess_data(item) for item in value]
                    else:
                        processed[key] = value
                return processed
            elif isinstance(data, list):
                return [self._safe_preprocess_data(item) for item in data]
            else:
                return data
        except Exception as e:
            logger.warning("Failed to preprocess B-Ref data, returning raw data", error=str(e))
            return data

    async def fetch_bref_box(self, bref_game_id: str) -> str:
        """Fetch Basketball Reference boxscore HTML.
        
        Args:
            bref_game_id: B-Ref game ID (e.g., '202310180LAL')
            
        Returns:
            Raw HTML content
        """
        url = f"{self.base_url}/boxscores/{bref_game_id}.html"
        
        try:
            logger.info("Fetching B-Ref boxscore", bref_game_id=bref_game_id, url=url)
            
            html_content = await self._make_request(url)
            
            logger.info("Fetched B-Ref boxscore", 
                       bref_game_id=bref_game_id, 
                       content_length=len(html_content))
            
            return html_content
            
        except Exception as e:
            logger.error("Failed to fetch B-Ref boxscore", 
                        bref_game_id=bref_game_id, error=str(e))
            raise
    
    async def resolve_bref_game_id(
        self, 
        home_tricode: str, 
        away_tricode: str, 
        local_date: date
    ) -> str:
        """Resolve Basketball Reference game ID from game details.
        
        Args:
            home_tricode: Home team tricode
            away_tricode: Away team tricode  
            local_date: Game date in local timezone
            
        Returns:
            B-Ref game ID
        """
        # B-Ref game ID format: YYYYMMDDHOMETEAM
        date_str = local_date.strftime('%Y%m%d')
        
        # Normalize team tricode for B-Ref
        home_bref = self._normalize_bref_tricode(home_tricode)
        
        bref_game_id = f"{date_str}0{home_bref}"
        
        logger.debug("Resolved B-Ref game ID", 
                    home=home_tricode, away=away_tricode, 
                    date=local_date, bref_game_id=bref_game_id)
        
        return bref_game_id
    
    async def fetch_schedule_page(self, team_tricode: str, season: str) -> str:
        """Fetch team schedule page HTML.
        
        Args:
            team_tricode: Team tricode
            season: Season (e.g., '2024')
            
        Returns:
            Raw HTML content
        """
        team_bref = self._normalize_bref_tricode(team_tricode)
        url = f"{self.base_url}/teams/{team_bref}/{season}_games.html"
        
        try:
            logger.info("Fetching B-Ref schedule", 
                       team=team_tricode, season=season, url=url)
            
            html_content = await self._make_request(url)
            
            logger.info("Fetched B-Ref schedule", 
                       team=team_tricode, season=season,
                       content_length=len(html_content))
            
            return html_content
            
        except Exception as e:
            logger.error("Failed to fetch B-Ref schedule", 
                        team=team_tricode, season=season, error=str(e))
            raise
    
    def parse_boxscore_scores(self, html_content: str) -> Dict[str, Any]:
        """Parse final scores and quarter scores from boxscore HTML.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Dictionary with score data
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Find the line score table
            line_score_table = soup.find('table', {'id': 'line_score'})
            if not line_score_table:
                logger.warning("Line score table not found")
                return {}
            
            scores = {}
            
            # Parse team rows
            tbody = line_score_table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                
                for i, row in enumerate(rows[:2]):  # First two rows are teams
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 5:  # Team, Q1, Q2, Q3, Q4, Final
                        team_key = 'away' if i == 0 else 'home'
                        
                        scores[f'{team_key}_q1'] = self._safe_int(cells[1].get_text())
                        scores[f'{team_key}_q2'] = self._safe_int(cells[2].get_text())
                        scores[f'{team_key}_q3'] = self._safe_int(cells[3].get_text())
                        scores[f'{team_key}_q4'] = self._safe_int(cells[4].get_text())
                        scores[f'{team_key}_final'] = self._safe_int(cells[-1].get_text())
            
            # Check for overtime periods
            ot_periods = 0
            header_row = line_score_table.find('thead', {'tr'})
            if header_row:
                headers = header_row.find_all('th')
                for header in headers:
                    if 'OT' in header.get_text():
                        ot_periods += 1
            
            scores['ot_periods'] = ot_periods
            
            logger.debug("Parsed boxscore scores", scores=scores)
            return self._safe_preprocess_data(scores)
            
        except Exception as e:
            logger.warning("Failed to parse boxscore scores", error=str(e))
            return {}
    
    def parse_starting_lineups(self, html_content: str) -> Dict[str, List[Dict[str, Any]]]:
        """Parse starting lineups from boxscore HTML.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Dictionary with 'home' and 'away' lineup lists
        """
        try:
            # Use selectolax for faster parsing
            tree = HTMLParser(html_content)
            
            lineups = {'home': [], 'away': []}
            
            # Find boxscore tables
            box_tables = tree.css('table[id*="box-"]')
            
            team_idx = 0
            for table in box_tables[:2]:  # First two are team boxscores
                team_key = 'away' if team_idx == 0 else 'home'
                
                # Look for starter indicators (typically marked with *)
                rows = table.css('tbody tr')
                
                for row in rows:
                    player_cell = row.css_first('th[data-stat="player"]')
                    if not player_cell:
                        continue
                    
                    player_name = player_cell.text().strip()
                    
                    # Check if player is marked as starter
                    if '*' in player_name or row.css_first('.starter'):
                        # Clean player name
                        player_name = player_name.replace('*', '').strip()
                        
                        # Get position if available
                        pos_cell = row.css_first('td[data-stat="pos"]')
                        position = pos_cell.text().strip() if pos_cell else None
                        
                        lineups[team_key].append({
                            'player': player_name,
                            'pos': position
                        })
                
                team_idx += 1
            
            logger.debug("Parsed starting lineups", 
                        home_count=len(lineups['home']), 
                        away_count=len(lineups['away']))
            
            return self._safe_preprocess_data(lineups)
            
        except Exception as e:
            logger.warning("Failed to parse starting lineups", error=str(e))
            return {'home': [], 'away': []}
    
    def parse_injury_notes(self, html_content: str) -> List[Dict[str, Any]]:
        """Parse injury/inactive notes from boxscore HTML.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            List of injury status dictionaries
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            injuries = []
            
            # Look for injury/inactive notes in various locations
            note_sections = soup.find_all(['div', 'p'], 
                                        string=re.compile(r'(inactive|out|questionable|probable)', re.I))
            
            for section in note_sections:
                text = section.get_text()
                
                # Parse injury mentions
                # Example: "LeBron James (ankle) - out"
                injury_matches = re.findall(
                    r'([A-Z][a-z]+\s+[A-Z][a-z]+).*?\(([^)]+)\).*?-\s*(\w+)',
                    text, re.IGNORECASE
                )
                
                for match in injury_matches:
                    player_name, injury_reason, status = match
                    injuries.append({
                        'player': player_name.strip(),
                        'reason': injury_reason.strip(),
                        'status': status.upper().strip()
                    })
            
            logger.debug("Parsed injury notes", count=len(injuries))
            return self._safe_preprocess_data(injuries)
            
        except Exception as e:
            logger.warning("Failed to parse injury notes", error=str(e))
            return []
    
    def _normalize_bref_tricode(self, tricode: str) -> str:
        """Normalize tricode for Basketball Reference URLs."""
        # B-Ref uses some different abbreviations
        bref_map = {
            'BRK': 'BRK',  # Brooklyn Nets
            'CHA': 'CHO',  # Charlotte (sometimes CHO on B-Ref)
            'PHX': 'PHO',  # Phoenix (sometimes PHO on B-Ref)
        }
        
        return bref_map.get(tricode.upper(), tricode.upper())
    
    def _safe_int(self, text: str) -> Optional[int]:
        """Safely convert text to integer."""
        try:
            return int(text.strip()) if text.strip() else None
        except (ValueError, AttributeError):
            return None