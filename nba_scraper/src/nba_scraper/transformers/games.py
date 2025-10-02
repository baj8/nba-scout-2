"""Game data transformers."""

from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional
import re
from difflib import get_close_matches

from . import BaseTransformer
from ..models.game_rows import GameRow
from ..models.crosswalk_rows import GameIdCrosswalkRow
from ..models.derived_rows import OutcomesRow
from ..models.enums import GameStatus
from ..logging import get_logger

logger = get_logger(__name__)


class BRefCrosswalkResolver:
    """Handles Basketball Reference game ID resolution with edge cases."""
    
    def __init__(self):
        # Common B-Ref tricode mappings
        self.bref_tricode_map = {
            'BRK': 'BRK',  # Brooklyn Nets
            'CHA': 'CHO',  # Charlotte (sometimes CHO on B-Ref)
            'PHX': 'PHO',  # Phoenix (sometimes PHO on B-Ref)
            'NOP': 'NOH',  # New Orleans (historically)
        }
        
        # Reverse mapping for fallback
        self.reverse_bref_map = {v: k for k, v in self.bref_tricode_map.items()}
    
    def resolve_bref_game_id(
        self, 
        game_id: str,
        home_team: str, 
        away_team: str, 
        game_date: date,
        game_status: GameStatus = GameStatus.SCHEDULED,
        **kwargs
    ) -> Optional[str]:
        """Resolve Basketball Reference game ID with fuzzy matching for edge cases.
        
        Args:
            game_id: NBA Stats game ID
            home_team: Home team tricode
            away_team: Away team tricode
            game_date: Scheduled game date
            game_status: Game status (for postponed/suspended detection)
            **kwargs: Additional context (actual_date, makeup_info, etc.)
            
        Returns:
            Basketball Reference game ID or None if unresolvable
        """
        logger.debug("Resolving B-Ref game ID", 
                    game_id=game_id, home=home_team, away=away_team, 
                    date=game_date, status=game_status)
        
        # Handle edge cases for postponed/suspended games FIRST
        if game_status in [GameStatus.POSTPONED, GameStatus.SUSPENDED, GameStatus.CANCELLED]:
            postponed_result = self._resolve_postponed_game(
                game_id, home_team, away_team, game_date, **kwargs
            )
            if postponed_result:
                return postponed_result
        
        # Try fuzzy date matching (±3 days) for makeup games
        if kwargs.get('is_makeup') or game_status == GameStatus.RESCHEDULED:
            makeup_result = self._resolve_makeup_game(
                game_id, home_team, away_team, game_date, **kwargs
            )
            if makeup_result:
                return makeup_result
        
        # Try primary resolution
        bref_id = self._try_primary_resolution(home_team, game_date)
        if bref_id:
            return bref_id
        
        # Try alternative tricode mappings
        bref_id = self._try_alternative_tricodes(home_team, game_date)
        if bref_id:
            return bref_id
        
        # Final fallback: fuzzy date range (±7 days)
        return self._fuzzy_date_fallback(home_team, away_team, game_date)
    
    def _try_primary_resolution(self, home_team: str, game_date: date) -> Optional[str]:
        """Try primary B-Ref ID format: YYYYMMDDHOMETEAM."""
        date_str = game_date.strftime('%Y%m%d')
        home_bref = self._normalize_bref_tricode(home_team)
        
        # Standard format: YYYYMMDD0TEAM
        bref_id = f"{date_str}0{home_bref}"
        
        logger.debug("Primary B-Ref ID", bref_id=bref_id)
        return bref_id
    
    def _resolve_postponed_game(
        self, 
        game_id: str,
        home_team: str, 
        away_team: str, 
        original_date: date,
        **kwargs
    ) -> Optional[str]:
        """Handle postponed/suspended games that may be played on different dates."""
        
        # Check if we have the actual played date
        actual_date = kwargs.get('actual_date') or kwargs.get('makeup_date')
        if actual_date and isinstance(actual_date, date):
            logger.info("Using actual date for postponed game",
                       game_id=game_id, original=original_date, actual=actual_date)
            return self._try_primary_resolution(home_team, actual_date)
        
        # Try to extract date from NBA game ID if it contains date info
        extracted_date = self._extract_date_from_game_id(game_id)
        if extracted_date and extracted_date != original_date:
            logger.info("Extracted different date from game ID",
                       game_id=game_id, original=original_date, extracted=extracted_date)
            return self._try_primary_resolution(home_team, extracted_date)
        
        # For suspended games, they might resume on the next available date
        if kwargs.get('status') == GameStatus.SUSPENDED:
            # Try next few days
            for days_ahead in range(1, 8):
                resume_date = original_date + timedelta(days=days_ahead)
                bref_id = self._try_primary_resolution(home_team, resume_date)
                if bref_id:
                    logger.info("Found suspended game resume date",
                               game_id=game_id, resume_date=resume_date)
                    return bref_id
        
        return None
    
    def _resolve_makeup_game(
        self, 
        game_id: str,
        home_team: str, 
        away_team: str, 
        scheduled_date: date,
        **kwargs
    ) -> Optional[str]:
        """Handle makeup games with fuzzy date matching."""
        
        # Makeup games are often scheduled within ±30 days of original
        search_window = kwargs.get('makeup_window_days', 30)
        
        logger.info("Resolving makeup game with fuzzy matching",
                   game_id=game_id, scheduled=scheduled_date, window=search_window)
        
        # Try dates before and after scheduled date
        for days_offset in range(-search_window, search_window + 1):
            if days_offset == 0:
                continue  # Already tried primary date
                
            candidate_date = scheduled_date + timedelta(days=days_offset)
            
            # Skip dates too far in the future (games can't be made up before they're scheduled)
            if candidate_date < scheduled_date - timedelta(days=7):
                continue
                
            bref_id = self._try_primary_resolution(home_team, candidate_date)
            if bref_id:
                logger.info("Found makeup game date via fuzzy matching",
                           game_id=game_id, original=scheduled_date, 
                           makeup=candidate_date, offset_days=days_offset)
                return bref_id
        
        return None
    
    def _try_alternative_tricodes(self, home_team: str, game_date: date) -> Optional[str]:
        """Try alternative tricode mappings for teams with multiple B-Ref abbreviations."""
        
        # Try direct mapping
        alt_tricode = self.bref_tricode_map.get(home_team)
        if alt_tricode and alt_tricode != home_team:
            bref_id = self._try_primary_resolution(alt_tricode, game_date)
            if bref_id:
                logger.info("Found B-Ref ID with alternative tricode",
                           original=home_team, alternative=alt_tricode)
                return bref_id
        
        # Try reverse mapping
        alt_tricode = self.reverse_bref_map.get(home_team)
        if alt_tricode:
            bref_id = self._try_primary_resolution(alt_tricode, game_date)
            if bref_id:
                logger.info("Found B-Ref ID with reverse tricode mapping",
                           original=home_team, alternative=alt_tricode)
                return bref_id
        
        # Try common historical variations
        historical_variations = self._get_historical_variations(home_team)
        for variation in historical_variations:
            bref_id = self._try_primary_resolution(variation, game_date)
            if bref_id:
                logger.info("Found B-Ref ID with historical variation",
                           original=home_team, variation=variation)
                return bref_id
        
        return None
    
    def _fuzzy_date_fallback(
        self, 
        home_team: str, 
        away_team: str, 
        target_date: date
    ) -> Optional[str]:
        """Final fallback with broader date range for special cases."""
        
        logger.info("Attempting fuzzy date fallback",
                   home=home_team, away=away_team, target=target_date)
        
        # Try ±7 days with all tricode variations
        tricodes_to_try = [
            home_team,
            self.bref_tricode_map.get(home_team, home_team),
            self.reverse_bref_map.get(home_team, home_team)
        ]
        tricodes_to_try = list(set(tricodes_to_try))  # Remove duplicates
        
        for days_offset in range(-7, 8):
            candidate_date = target_date + timedelta(days=days_offset)
            
            for tricode in tricodes_to_try:
                bref_id = self._try_primary_resolution(tricode, candidate_date)
                if bref_id:
                    logger.warning("Found B-Ref ID via broad fuzzy matching",
                                 home=home_team, tricode=tricode, 
                                 target=target_date, actual=candidate_date,
                                 offset_days=days_offset)
                    return bref_id
        
        logger.warning("Could not resolve B-Ref game ID after all attempts",
                      home=home_team, away=away_team, date=target_date)
        return None
    
    def _normalize_bref_tricode(self, tricode: str) -> str:
        """Normalize tricode for Basketball Reference URLs."""
        if not tricode:
            return tricode
            
        tricode = tricode.upper()
        return self.bref_tricode_map.get(tricode, tricode)
    
    def _extract_date_from_game_id(self, game_id: str) -> Optional[date]:
        """Extract date from NBA game ID if it contains date information."""
        if not game_id:
            return None
            
        # NBA game IDs often contain date: 0022300567 -> season 2023-24, game 567
        # Try to extract YYYYMMDD pattern if present
        date_pattern = r'(\d{8})'
        match = re.search(date_pattern, game_id)
        
        if match:
            try:
                date_str = match.group(1)
                return datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError:
                pass
        
        # Try alternative patterns specific to different data sources
        # Format: YYYY-MM-DD
        iso_pattern = r'(\d{4}-\d{2}-\d{2})'
        match = re.search(iso_pattern, game_id)
        if match:
            try:
                return datetime.strptime(match.group(1), '%Y-%m-%d').date()
            except ValueError:
                pass
        
        return None
    
    def _get_historical_variations(self, tricode: str) -> List[str]:
        """Get historical tricode variations for teams that have changed names/locations."""
        
        variations_map = {
            'BRK': ['NJN', 'BKN'],  # Brooklyn Nets (formerly New Jersey)
            'CHA': ['CHO', 'CHH'],  # Charlotte (various abbreviations)
            'NOP': ['NOH', 'NOK'],  # New Orleans (formerly Hornets)
            'OKC': ['SEA'],         # Oklahoma City (formerly Seattle)
            'MEM': ['VAN'],         # Memphis (formerly Vancouver)
            'TOR': ['TOR'],         # Toronto (consistent)
        }
        
        return variations_map.get(tricode, [])
    
    def create_crosswalk_with_confidence(
        self,
        game_id: str,
        bref_game_id: Optional[str],
        resolution_method: str,
        confidence_score: float,
        source_url: str
    ) -> GameIdCrosswalkRow:
        """Create crosswalk entry with confidence metadata."""
        
        if not bref_game_id:
            # Create placeholder entry for failed resolution
            logger.warning("Creating crosswalk with no B-Ref ID", 
                          game_id=game_id, method=resolution_method)
            
        return GameIdCrosswalkRow(
            game_id=game_id,
            bref_game_id=bref_game_id or f"UNRESOLVED_{game_id}",
            nba_stats_game_id=game_id,
            source=f"bref_resolver_{resolution_method}",
            source_url=source_url,
            # Note: We could add confidence_score and resolution_method 
            # as additional fields if the model is extended
        )


class GameTransformer(BaseTransformer[GameRow]):
    """Transformer for game data."""
    
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List[GameRow]:
        """Transform raw game data to GameRow instances.
        
        Args:
            raw_data: Raw game data from source
            **kwargs: Additional context
            
        Returns:
            List of GameRow instances
        """
        if self.source == 'nba_stats':
            return self._transform_nba_stats(raw_data, **kwargs)
        elif self.source == 'bref':
            return self._transform_bref(raw_data, **kwargs)
        else:
            raise ValueError(f"Unsupported source: {self.source}")
    
    def _transform_nba_stats(self, raw_data: Dict[str, Any], **kwargs) -> List[GameRow]:
        """Transform NBA.com stats data."""
        games = []
        
        # Handle different NBA Stats API response structures
        if 'resultSets' in raw_data:
            # Standard NBA Stats API format
            for result_set in raw_data['resultSets']:
                if result_set.get('name') == 'GameHeader':
                    headers = result_set.get('headers', [])
                    for row in result_set.get('rowSet', []):
                        game_data = dict(zip(headers, row))
                        games.append(self._create_game_from_nba_stats(game_data, **kwargs))
        elif 'games' in raw_data:
            # Scoreboard format
            for game_data in raw_data['games']:
                games.append(self._create_game_from_nba_stats(game_data, **kwargs))
        else:
            # Single game format
            games.append(self._create_game_from_nba_stats(raw_data, **kwargs))
        
        return games
    
    def _create_game_from_nba_stats(self, game_data: Dict[str, Any], **kwargs) -> GameRow:
        """Create GameRow from NBA Stats game data."""
        # Extract game ID
        game_id = self._safe_get(game_data, 'GAME_ID') or self._safe_get(game_data, 'gameId')
        
        # Parse date
        game_date_str = self._safe_get(game_data, 'GAME_DATE_EST') or self._safe_get(game_data, 'gameTimeUTC')
        game_date_utc = None
        if game_date_str:
            try:
                if 'T' in game_date_str:
                    game_date_utc = datetime.fromisoformat(game_date_str.replace('Z', '+00:00'))
                else:
                    game_date_utc = datetime.strptime(game_date_str, '%Y-%m-%d')
            except ValueError:
                pass
        
        # Extract team info
        home_team = self._safe_get(game_data, 'HOME_TEAM_ID') or self._safe_get(game_data, 'homeTeam', {}).get('teamId')
        away_team = self._safe_get(game_data, 'VISITOR_TEAM_ID') or self._safe_get(game_data, 'awayTeam', {}).get('teamId')
        
        # Parse status
        status_text = self._safe_get(game_data, 'GAME_STATUS_TEXT') or self._safe_get(game_data, 'gameStatusText')
        status = GameStatus.SCHEDULED
        if status_text:
            if 'Final' in status_text:
                status = GameStatus.FINAL
            elif any(x in status_text for x in ['Q', 'Period', 'Half']):
                status = GameStatus.IN_PROGRESS
        
        return GameRow(
            game_id=str(game_id),
            bref_game_id=None,
            season=self._parse_int(kwargs.get('season')),
            game_date_utc=game_date_utc,
            game_date_local=game_date_utc,  # Will be adjusted based on arena timezone
            arena_tz=None,
            home_team_tricode=self._safe_get(game_data, 'HOME_TEAM_ABBREVIATION') or 
                             self._safe_get(game_data, 'homeTeam', {}).get('teamTricode'),
            away_team_tricode=self._safe_get(game_data, 'VISITOR_TEAM_ABBREVIATION') or
                             self._safe_get(game_data, 'awayTeam', {}).get('teamTricode'),
            home_team_id=self._parse_int(home_team),
            away_team_id=self._parse_int(away_team),
            odds_join_key=None,
            status=status,
            period=self._parse_int(self._safe_get(game_data, 'PERIOD')),
            time_remaining=self._safe_get(game_data, 'TIME_REMAINING'),
            arena_name=None,
            attendance=self._parse_int(self._safe_get(game_data, 'ATTENDANCE')),
            source=self.source,
            source_url=kwargs.get('source_url'),
        )
    
    def _transform_bref(self, raw_data: Dict[str, Any], **kwargs) -> List[GameRow]:
        """Transform Basketball Reference data."""
        games = []
        
        # Handle Basketball Reference schedule format
        if 'games' in raw_data:
            for game_data in raw_data['games']:
                games.append(self._create_game_from_bref(game_data, **kwargs))
        else:
            games.append(self._create_game_from_bref(raw_data, **kwargs))
        
        return games
    
    def _create_game_from_bref(self, game_data: Dict[str, Any], **kwargs) -> GameRow:
        """Create GameRow from Basketball Reference data."""
        # Extract game identifiers
        bref_game_id = self._safe_get(game_data, 'bref_game_id') or kwargs.get('bref_game_id')
        
        # Parse date
        date_str = self._safe_get(game_data, 'date')
        game_date_utc = None
        if date_str:
            try:
                game_date_utc = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                pass
        
        # Determine status
        pts_home = self._safe_get(game_data, 'pts_home')
        pts_away = self._safe_get(game_data, 'pts_away')
        status = GameStatus.FINAL if pts_home and pts_away else GameStatus.SCHEDULED
        
        return GameRow(
            game_id=bref_game_id or f"bref_{self._safe_get(game_data, 'home_team')}_{date_str}",
            bref_game_id=bref_game_id,
            season=self._parse_int(kwargs.get('season')),
            game_date_utc=game_date_utc,
            game_date_local=game_date_utc,
            arena_tz=None,
            home_team_tricode=self._safe_get(game_data, 'home_team'),
            away_team_tricode=self._safe_get(game_data, 'away_team'),
            home_team_id=None,
            away_team_id=None,
            odds_join_key=None,
            status=status,
            period=None,
            time_remaining=None,
            arena_name=None,
            attendance=self._parse_int(self._safe_get(game_data, 'attendance')),
            source=self.source,
            source_url=kwargs.get('source_url'),
        )


class GameCrosswalkTransformer(BaseTransformer[GameIdCrosswalkRow]):
    """Transformer for game ID crosswalk data with enhanced B-Ref resolution."""
    
    def __init__(self, source: str):
        super().__init__(source)
        self.bref_resolver = BRefCrosswalkResolver()
    
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List[GameIdCrosswalkRow]:
        """Transform raw crosswalk data to GameIdCrosswalkRow instances."""
        crosswalks = []
        
        # Handle different formats
        if 'crosswalks' in raw_data:
            for crosswalk_data in raw_data['crosswalks']:
                crosswalks.append(self._create_crosswalk(crosswalk_data, **kwargs))
        else:
            crosswalks.append(self._create_crosswalk(raw_data, **kwargs))
        
        return crosswalks
    
    def resolve_bref_crosswalk(
        self,
        game_id: str,
        home_team: str,
        away_team: str,
        game_date: date,
        game_status: GameStatus = GameStatus.SCHEDULED,
        source_url: str = "",
        **kwargs
    ) -> GameIdCrosswalkRow:
        """Resolve Basketball Reference crosswalk with edge case handling."""
        
        bref_game_id = self.bref_resolver.resolve_bref_game_id(
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
            game_date=game_date,
            game_status=game_status,
            **kwargs
        )
        
        # Determine resolution method for metadata
        resolution_method = "primary"
        confidence_score = 1.0
        
        if not bref_game_id:
            resolution_method = "failed"
            confidence_score = 0.0
        elif bref_game_id != self.bref_resolver._try_primary_resolution(home_team, game_date):
            resolution_method = "fuzzy_matched"
            confidence_score = 0.8
        
        return self.bref_resolver.create_crosswalk_with_confidence(
            game_id=game_id,
            bref_game_id=bref_game_id,
            resolution_method=resolution_method,
            confidence_score=confidence_score,
            source_url=source_url or f"https://www.basketball-reference.com/boxscores/{bref_game_id}.html"
        )
    
    def _create_crosswalk(self, data: Dict[str, Any], **kwargs) -> GameIdCrosswalkRow:
        """Create GameIdCrosswalkRow from data."""
        return GameIdCrosswalkRow(
            game_id=self._safe_get(data, 'game_id') or kwargs.get('game_id'),
            bref_game_id=self._safe_get(data, 'bref_game_id'),
            nba_stats_game_id=self._safe_get(data, 'nba_stats_game_id'),
            espn_game_id=self._safe_get(data, 'espn_game_id'),
            yahoo_game_id=self._safe_get(data, 'yahoo_game_id'),
            source=self.source,
            source_url=kwargs.get('source_url'),
        )


class OutcomesTransformer(BaseTransformer[OutcomesRow]):
    """Transformer for game outcomes data."""
    
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List[OutcomesRow]:
        """Transform raw outcomes data to OutcomesRow instances."""
        outcomes = []
        
        if self.source == 'nba_stats':
            outcomes.extend(self._transform_nba_stats_outcomes(raw_data, **kwargs))
        elif self.source == 'bref':
            outcomes.extend(self._transform_bref_outcomes(raw_data, **kwargs))
        
        return outcomes
    
    def _transform_nba_stats_outcomes(self, raw_data: Dict[str, Any], **kwargs) -> List[OutcomesRow]:
        """Transform NBA Stats outcomes data."""
        outcomes = []
        
        if 'resultSets' in raw_data:
            for result_set in raw_data['resultSets']:
                if result_set.get('name') == 'LineScore':
                    headers = result_set.get('headers', [])
                    for row in result_set.get('rowSet', []):
                        row_data = dict(zip(headers, row))
                        if self._safe_get(row_data, 'TEAM_ID'):
                            # Process team scores to create outcome
                            outcome = self._create_outcome_from_nba_stats(row_data, **kwargs)
                            if outcome:
                                outcomes.append(outcome)
        
        return outcomes
    
    def _create_outcome_from_nba_stats(self, data: Dict[str, Any], **kwargs) -> Optional[OutcomesRow]:
        """Create OutcomesRow from NBA Stats line score data."""
        game_id = kwargs.get('game_id') or self._safe_get(data, 'GAME_ID')
        if not game_id:
            return None
        
        # This would need to be called twice (once for each team) and combined
        # For now, create a basic structure
        return OutcomesRow(
            game_id=str(game_id),
            home_team_tricode=kwargs.get('home_team_tricode'),
            away_team_tricode=kwargs.get('away_team_tricode'),
            q1_home_points=self._parse_int(self._safe_get(data, 'PTS_QTR1')),
            q1_away_points=None,  # Would need away team data
            final_home_points=self._parse_int(self._safe_get(data, 'PTS')),
            final_away_points=None,  # Would need away team data
            total_points=None,  # Calculated after both teams
            home_win=None,  # Calculated after both teams
            margin=None,  # Calculated after both teams
            overtime_periods=None,
            source=self.source,
            source_url=kwargs.get('source_url'),
        )
    
    def _transform_bref_outcomes(self, raw_data: Dict[str, Any], **kwargs) -> List[OutcomesRow]:
        """Transform Basketball Reference outcomes data."""
        outcomes = []
        
        if 'games' in raw_data:
            for game_data in raw_data['games']:
                outcome = self._create_outcome_from_bref(game_data, **kwargs)
                if outcome:
                    outcomes.append(outcome)
        else:
            outcome = self._create_outcome_from_bref(raw_data, **kwargs)
            if outcome:
                outcomes.append(outcome)
        
        return outcomes
    
    def _create_outcome_from_bref(self, data: Dict[str, Any], **kwargs) -> Optional[OutcomesRow]:
        """Create OutcomesRow from Basketball Reference data."""
        game_id = kwargs.get('game_id') or self._safe_get(data, 'game_id')
        if not game_id:
            return None
        
        home_points = self._parse_int(self._safe_get(data, 'pts_home'))
        away_points = self._parse_int(self._safe_get(data, 'pts_away'))
        
        if home_points is None or away_points is None:
            return None
        
        total_points = home_points + away_points
        home_win = home_points > away_points
        margin = abs(home_points - away_points)
        
        return OutcomesRow(
            game_id=str(game_id),
            home_team_tricode=self._safe_get(data, 'home_team'),
            away_team_tricode=self._safe_get(data, 'away_team'),
            q1_home_points=self._parse_int(self._safe_get(data, 'q1_home')),
            q1_away_points=self._parse_int(self._safe_get(data, 'q1_away')),
            final_home_points=home_points,
            final_away_points=away_points,
            total_points=total_points,
            home_win=home_win,
            margin=margin,
            overtime_periods=self._parse_int(self._safe_get(data, 'overtime_periods')),
            source=self.source,
            source_url=kwargs.get('source_url'),
        )