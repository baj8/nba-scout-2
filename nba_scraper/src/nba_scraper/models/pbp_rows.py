"""Play-by-play event row Pydantic model."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .enums import EventType, EventSubtype, ShotResult, ShotType, ShotZone
from .ref_rows import normalize_name_slug


class PbpEventRow(BaseModel):
    """Play-by-play event row model."""
    
    game_id: str = Field(..., description="Game identifier")
    period: int = Field(..., description="Period number")
    event_idx: int = Field(..., description="Event index within period")
    event_id: Optional[str] = Field(None, description="Source event ID")
    time_remaining: Optional[str] = Field(None, description="Time remaining in period")
    seconds_elapsed: Optional[float] = Field(None, description="Seconds elapsed in period")
    score_home: Optional[int] = Field(None, description="Home team score after event")
    score_away: Optional[int] = Field(None, description="Away team score after event")
    event_type: EventType = Field(..., description="Event type")
    event_subtype: Optional[EventSubtype] = Field(None, description="Event subtype")
    description: Optional[str] = Field(None, description="Event description")
    team_tricode: Optional[str] = Field(None, description="Team associated with event")
    
    # Player fields
    player1_name_slug: Optional[str] = Field(None, description="Primary player name slug")
    player1_display_name: Optional[str] = Field(None, description="Primary player display name")
    player1_id: Optional[str] = Field(None, description="Primary player ID")
    player2_name_slug: Optional[str] = Field(None, description="Secondary player name slug")
    player2_display_name: Optional[str] = Field(None, description="Secondary player display name")
    player2_id: Optional[str] = Field(None, description="Secondary player ID")
    player3_name_slug: Optional[str] = Field(None, description="Tertiary player name slug")
    player3_display_name: Optional[str] = Field(None, description="Tertiary player display name")
    player3_id: Optional[str] = Field(None, description="Tertiary player ID")
    
    # Shot-specific fields
    shot_made: Optional[bool] = Field(None, description="Whether shot was made")
    shot_value: Optional[int] = Field(None, description="Point value of shot (2 or 3)")
    shot_type: Optional[ShotType] = Field(None, description="Shot type classification")
    shot_zone: Optional[ShotZone] = Field(None, description="Shot zone classification")
    shot_distance_ft: Optional[float] = Field(None, description="Shot distance in feet")
    shot_x: Optional[float] = Field(None, description="Shot X coordinate")
    shot_y: Optional[float] = Field(None, description="Shot Y coordinate")
    
    # Game situation
    shot_clock_seconds: Optional[float] = Field(None, description="Shot clock seconds remaining")
    possession_team: Optional[str] = Field(None, description="Team with possession")
    
    # Enrichment flags
    is_transition: bool = Field(default=False, description="Event in transition")
    is_early_clock: bool = Field(default=False, description="Event in early shot clock")
    
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")

    @model_validator(mode='before')
    @classmethod
    def preprocess_data(cls, data: Any) -> Any:
        """Apply comprehensive preprocessing before field validation to prevent int/str comparison errors."""
        if isinstance(data, dict):
            # Import here to avoid circular imports
            from .utils import preprocess_nba_stats_data
            from .nba_stats_enums import preprocess_pbp_event_data
            
            # Apply NBA Stats preprocessing if this looks like NBA Stats data
            processed_data = preprocess_nba_stats_data(data)
            
            # Apply comprehensive PBP preprocessing to handle all enum conversions
            processed_data = preprocess_pbp_event_data(processed_data)
            
            return processed_data
        return data

    @field_validator('player1_name_slug', 'player2_name_slug', 'player3_name_slug', mode='before')
    @classmethod
    def normalize_player_name_slugs(cls, v: Optional[str]) -> Optional[str]:
        """Normalize player name slugs."""
        return normalize_name_slug(v) if v else None
    
    @field_validator('team_tricode', 'possession_team')
    @classmethod
    def normalize_team_tricode(cls, v: Optional[str]) -> Optional[str]:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else None
    
    @field_validator('event_type', mode='before')
    @classmethod
    def normalize_event_type(cls, v) -> EventType:
        """Normalize event type from various sources."""
        # FIXED: If already a valid EventType enum, return it directly
        if isinstance(v, EventType):
            return v
        
        # CRITICAL: Convert any input to string first to prevent int/str comparison errors
        if v is None:
            event_str = '1'  # Default to SHOT_MADE
        else:
            # Handle EventType enum instances that might be converted to string representation
            if hasattr(v, 'value'):  # This is likely an enum
                event_str = v.value
            elif str(v).startswith('EventType.'):
                # Handle "EventType.FOUL" format by extracting the enum value
                event_str = str(v).replace('EventType.', '')
            else:
                # Ensure we always work with a string, regardless of input type
                event_str = str(v).strip()
        
        # Map NBA Stats event types to our enum - handle both string and integer inputs
        event_map = {
            # Integer string mappings (from NBA Stats API)
            '1': EventType.SHOT_MADE,
            '2': EventType.SHOT_MISSED,
            '3': EventType.FREE_THROW_MADE,
            '4': EventType.FREE_THROW_MISSED,
            '5': EventType.REBOUND,
            '6': EventType.TURNOVER,
            '7': EventType.FOUL,
            '8': EventType.SUBSTITUTION,
            '9': EventType.TIMEOUT,
            '10': EventType.JUMP_BALL,
            '11': EventType.EJECTION,
            '12': EventType.PERIOD_BEGIN,
            '13': EventType.PERIOD_END,
            '18': EventType.INSTANT_REPLAY,
            # Canonical string mappings (from preprocessing or B-Ref)
            'shot': EventType.SHOT_MADE,
            'free_throw': EventType.FREE_THROW_MADE,
            'rebound': EventType.REBOUND,
            'turnover': EventType.TURNOVER,
            'foul': EventType.FOUL,
            'violation': EventType.VIOLATION,
            'substitution': EventType.SUBSTITUTION,
            'timeout': EventType.TIMEOUT,
            'jump_ball': EventType.JUMP_BALL,
            'ejection': EventType.EJECTION,
            'period_start': EventType.PERIOD_BEGIN,
            'period_end': EventType.PERIOD_END,
            'technical': EventType.TECHNICAL_FOUL,
            'flagrant': EventType.FLAGRANT_FOUL,
            'instant_replay': EventType.INSTANT_REPLAY,
            # Legacy string mappings
            'SHOT_MADE': EventType.SHOT_MADE,
            'SHOT_MISSED': EventType.SHOT_MISSED,
            'FREE_THROW_MADE': EventType.FREE_THROW_MADE,
            'FREE_THROW_MISSED': EventType.FREE_THROW_MISSED,
            'REBOUND': EventType.REBOUND,
            'ASSIST': EventType.ASSIST,
            'TURNOVER': EventType.TURNOVER,
            'STEAL': EventType.STEAL,
            'BLOCK': EventType.BLOCK,
            'FOUL': EventType.FOUL,
            'PERSONAL_FOUL': EventType.PERSONAL_FOUL,
            'TIMEOUT': EventType.TIMEOUT,
            'SUBSTITUTION': EventType.SUBSTITUTION,
            'JUMP_BALL': EventType.JUMP_BALL,
            'EJECTION': EventType.EJECTION,
            'TECHNICAL_FOUL': EventType.TECHNICAL_FOUL,
            'FLAGRANT_FOUL': EventType.FLAGRANT_FOUL,
            'PERIOD_BEGIN': EventType.PERIOD_BEGIN,
            'PERIOD_END': EventType.PERIOD_END,
            'GAME_END': EventType.GAME_END,
            'INSTANT_REPLAY': EventType.INSTANT_REPLAY,
            'VIOLATION': EventType.VIOLATION,
        }
        
        # Return mapped value or default to SHOT_MADE if not found
        return event_map.get(event_str, EventType.SHOT_MADE)
    
    @model_validator(mode='after')
    def derive_clock_fields(self) -> 'PbpEventRow':
        """Derive clock_seconds and seconds_elapsed with NBA time remaining logic.
        
        NBA assumptions:
        - Regulation periods = 12 minutes (720 seconds)
        - Overtime periods = 5 minutes (300 seconds)
        - time_remaining format: M:SS, MM:SS, optional .fff, or PTxMxS[.fff]
        - If negative elapsed time, auto-flip to elapsed once (safety)
        """
        if not self.time_remaining:
            return self
        
        try:
            # Parse clock_seconds from time_remaining
            clock_seconds = self._parse_clock_to_seconds(self.time_remaining)
            
            if clock_seconds is not None:
                # Store raw clock seconds
                object.__setattr__(self, 'clock_seconds', clock_seconds)
                
                # Calculate seconds_elapsed using NBA period logic
                period_length = self._get_period_length_seconds(self.period)
                seconds_elapsed = period_length - clock_seconds
                
                # Safety: if negative, auto-flip to elapsed once
                if seconds_elapsed < 0:
                    seconds_elapsed = abs(seconds_elapsed)
                
                # Only update if we don't already have a value or if our derived value is more accurate
                if self.seconds_elapsed is None:
                    object.__setattr__(self, 'seconds_elapsed', float(seconds_elapsed))
                    
        except Exception as e:
            # Don't fail the whole model for clock parsing issues
            from ..nba_logging import get_logger
            logger = get_logger(__name__)
            logger.debug("Clock parsing failed", 
                        game_id=self.game_id, 
                        period=self.period,
                        time_remaining=self.time_remaining, 
                        error=str(e))
        
        return self
    
    @staticmethod
    def _parse_clock_to_seconds(time_str: str) -> Optional[float]:
        """Parse various clock formats to seconds remaining.
        
        Supports:
        - M:SS (e.g., "5:30")
        - MM:SS (e.g., "12:00")
        - MM:SS.fff (e.g., "11:45.500")
        - PTxMxS[.fff] (e.g., "PT11M45S" or "PT11M45.500S")
        
        Args:
            time_str: Time string in various formats
            
        Returns:
            Seconds remaining as float, or None if parsing fails
        """
        import re
        
        if not time_str or not isinstance(time_str, str):
            return None
            
        time_clean = time_str.strip()
        
        # Handle ISO 8601 duration format: PT11M45S or PT11M45.500S
        iso_match = re.match(r'^PT(\d+)M(\d+(?:\.\d+)?)S$', time_clean)
        if iso_match:
            minutes = int(iso_match.group(1))
            seconds = float(iso_match.group(2))
            return minutes * 60 + seconds
        
        # Handle MM:SS or MM:SS.fff format
        clock_match = re.match(r'^(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?$', time_clean)
        if clock_match:
            minutes = int(clock_match.group(1))
            seconds = int(clock_match.group(2))
            fractional = int(clock_match.group(3) or 0)
            
            # Convert fractional part to decimal (e.g., 500 milliseconds = 0.5 seconds)
            if fractional > 0:
                fractional_seconds = fractional / (10 ** len(str(fractional)))
            else:
                fractional_seconds = 0.0
                
            return minutes * 60 + seconds + fractional_seconds
        
        return None
    
    @staticmethod
    def _get_period_length_seconds(period: int) -> int:
        """Get period length in seconds using NBA rules.
        
        Args:
            period: Period number (1-4 for regulation, 5+ for OT)
            
        Returns:
            Period length in seconds
        """
        if period <= 4:
            return 12 * 60  # Regulation: 12 minutes = 720 seconds
        else:
            return 5 * 60   # Overtime: 5 minutes = 300 seconds

    @classmethod
    def from_nba_stats(
        cls, 
        game_id: str,
        pbp_data: Dict[str, Any], 
        source_url: str
    ) -> 'PbpEventRow':
        """Create from NBA Stats play-by-play data."""
        # Preprocess data to handle mixed data types before validation
        from .utils import preprocess_nba_stats_data
        pbp_data = preprocess_nba_stats_data(pbp_data)
        
        # Parse NBA Stats PBP format - CRITICAL FIX: Always convert to string first
        raw_event_type = pbp_data.get('EVENTMSGTYPE', '')
        event_type_id = str(raw_event_type).strip() if raw_event_type is not None else '1'
        
        # Map NBA Stats event types to our enum (using the mapping from normalize_event_type)
        event_map = {
            '1': EventType.SHOT_MADE,
            '2': EventType.SHOT_MISSED,
            '3': EventType.FREE_THROW_MADE,
            '4': EventType.FREE_THROW_MISSED,
            '5': EventType.REBOUND,
            '6': EventType.TURNOVER,
            '7': EventType.FOUL,
            '8': EventType.SUBSTITUTION,
            '9': EventType.TIMEOUT,
            '10': EventType.JUMP_BALL,
            '11': EventType.EJECTION,
            '12': EventType.PERIOD_BEGIN,
            '13': EventType.PERIOD_END,
            '18': EventType.INSTANT_REPLAY,
        }
        
        # CRITICAL FIX: Ensure we always get an EventType enum, never an integer
        event_type = event_map.get(event_type_id, EventType.SHOT_MADE)
        
        # Verify event_type is actually an EventType enum (additional safety check)
        if not isinstance(event_type, EventType):
            event_type = EventType.SHOT_MADE
        
        # Determine shot information - now safe to use enum comparisons
        shot_made = None
        shot_value = None
        shot_type = None
        
        if event_type in [EventType.SHOT_MADE, EventType.SHOT_MISSED]:
            shot_made = event_type == EventType.SHOT_MADE
            # Parse shot value from description or action type
            description = pbp_data.get('HOMEDESCRIPTION', '') or pbp_data.get('VISITORDESCRIPTION', '')
            if '3PT' in description:
                shot_value = 3
                shot_type = ShotType.THREE_POINT
            else:
                shot_value = 2
                shot_type = ShotType.TWO_POINT
        elif event_type in [EventType.FREE_THROW_MADE, EventType.FREE_THROW_MISSED]:
            shot_made = event_type == EventType.FREE_THROW_MADE
            shot_value = 1
            shot_type = ShotType.FREE_THROW
        
        # Parse time
        time_str = pbp_data.get('PCTIMESTRING', '')
        seconds_elapsed = None
        if time_str:
            try:
                # Convert MM:SS to seconds elapsed in period
                minutes, seconds = map(int, time_str.split(':'))
                # NBA periods are 12 minutes, so elapsed = 12*60 - (minutes*60 + seconds)
                seconds_elapsed = (12 * 60) - (minutes * 60 + seconds)
            except (ValueError, AttributeError):
                pass
        
        # Parse scores from combined score string like "27 - 46"
        score_home = None
        score_away = None
        score_str = pbp_data.get('SCORE', '')
        if score_str and isinstance(score_str, str) and ' - ' in score_str:
            try:
                home_str, away_str = score_str.strip().split(' - ', 1)
                score_home = int(home_str.strip())
                score_away = int(away_str.strip())
            except (ValueError, AttributeError):
                # If parsing fails, leave scores as None
                pass
        
        return cls(
            game_id=game_id,
            period=int(pbp_data.get('PERIOD', 1)),
            event_idx=int(pbp_data.get('EVENTNUM', 0)),
            event_id=str(pbp_data.get('EVENTMSGACTIONTYPE', '')),
            time_remaining=time_str,
            seconds_elapsed=seconds_elapsed,
            score_home=score_home,
            score_away=score_away,
            event_type=event_type,
            description=pbp_data.get('HOMEDESCRIPTION') or pbp_data.get('VISITORDESCRIPTION'),
            player1_name_slug=normalize_name_slug(pbp_data.get('PLAYER1_NAME', '')),
            player1_display_name=pbp_data.get('PLAYER1_NAME'),
            player1_id=str(pbp_data.get('PLAYER1_ID', '')) if pbp_data.get('PLAYER1_ID') else None,
            player2_name_slug=normalize_name_slug(pbp_data.get('PLAYER2_NAME', '')),
            player2_display_name=pbp_data.get('PLAYER2_NAME'),
            player2_id=str(pbp_data.get('PLAYER2_ID', '')) if pbp_data.get('PLAYER2_ID') else None,
            shot_made=shot_made,
            shot_value=shot_value,
            shot_type=shot_type,
            source='nba_stats',
            source_url=source_url,
        )
    
    @classmethod
    def enrich_with_shot_chart(cls, pbp_row: 'PbpEventRow', shot_data: Dict[str, Any]) -> 'PbpEventRow':
        """Enrich PBP row with shot chart data."""
        if pbp_row.event_type not in [EventType.SHOT_MADE, EventType.SHOT_MISSED]:
            return pbp_row
        
        # Update shot location and zone
        pbp_row.shot_distance_ft = shot_data.get('SHOT_DISTANCE')
        pbp_row.shot_x = shot_data.get('LOC_X')
        pbp_row.shot_y = shot_data.get('LOC_Y')
        
        # Classify shot zone based on coordinates
        shot_zone = cls._classify_shot_zone(
            pbp_row.shot_x, 
            pbp_row.shot_y, 
            pbp_row.shot_distance_ft
        )
        pbp_row.shot_zone = shot_zone
        
        return pbp_row
    
    @staticmethod
    def _classify_shot_zone(x: Optional[float], y: Optional[float], distance: Optional[float]) -> Optional[ShotZone]:
        """Classify shot zone based on coordinates."""
        if distance is None:
            return None
        
        # Simple zone classification based on distance
        # This would be more sophisticated in production
        if distance <= 4:
            return ShotZone.RESTRICTED_AREA
        elif distance <= 10:
            return ShotZone.PAINT
        elif distance <= 23:
            return ShotZone.MID_RANGE
        else:
            # Could further classify corner vs above break 3s using x,y coordinates
            return ShotZone.ABOVE_BREAK_3