"""Play-by-play event row Pydantic model."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

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
    def normalize_event_type(cls, v: str) -> EventType:
        """Normalize event type from various sources."""
        if isinstance(v, EventType):
            return v
        
        # Map NBA Stats event types to our enum
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
            # Text mappings
            'SHOT_MADE': EventType.SHOT_MADE,
            'SHOT_MISSED': EventType.SHOT_MISSED,
            'MADE': EventType.SHOT_MADE,
            'MISSED': EventType.SHOT_MISSED,
            'FT_MADE': EventType.FREE_THROW_MADE,
            'FT_MISSED': EventType.FREE_THROW_MISSED,
        }
        
        return event_map.get(str(v).upper(), EventType.SHOT_MADE)
    
    @classmethod
    def from_nba_stats(
        cls, 
        game_id: str,
        pbp_data: Dict[str, Any], 
        source_url: str
    ) -> 'PbpEventRow':
        """Create from NBA Stats play-by-play data."""
        # Parse NBA Stats PBP format
        event_type_id = str(pbp_data.get('EVENTMSGTYPE', ''))
        event_type = cls.normalize_event_type(event_type_id)  # Fixed: removed extra None parameter
        
        # Determine shot information
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
        
        return cls(
            game_id=game_id,
            period=int(pbp_data.get('PERIOD', 1)),
            event_idx=int(pbp_data.get('EVENTNUM', 0)),
            event_id=str(pbp_data.get('EVENTMSGACTIONTYPE', '')),
            time_remaining=time_str,
            seconds_elapsed=seconds_elapsed,
            score_home=pbp_data.get('SCORE'),
            score_away=pbp_data.get('SCORE'),  # Would need parsing from score string
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