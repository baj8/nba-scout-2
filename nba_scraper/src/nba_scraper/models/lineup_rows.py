"""Starting lineup row Pydantic model."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .enums import Position
from .ref_rows import normalize_name_slug


class StartingLineupRow(BaseModel):
    """Starting lineup row model."""
    
    game_id: str = Field(..., description="Game identifier")
    team_tricode: str = Field(..., description="Team tricode")
    player_name_slug: str = Field(..., description="Player name in PascalCase")
    player_display_name: str = Field(..., description="Player display name")
    player_id: Optional[str] = Field(None, description="Player ID from data source")
    position: Optional[Position] = Field(None, description="Player position")
    jersey_number: Optional[int] = Field(None, description="Jersey number")
    final_pre_tip: bool = Field(default=True, description="Final lineup before tip-off")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @model_validator(mode='before')
    @classmethod
    def preprocess_data(cls, data: Any) -> Any:
        """Apply comprehensive preprocessing before field validation to prevent int/str comparison errors."""
        if isinstance(data, dict):
            # Import here to avoid circular imports
            from .utils import preprocess_nba_stats_data
            
            # Apply NBA Stats preprocessing to handle mixed data types
            processed_data = preprocess_nba_stats_data(data)
            
            return processed_data
        return data

    @field_validator('player_name_slug', mode='before')
    @classmethod
    def normalize_player_name_slug(cls, v: str) -> str:
        """Normalize player name to slug format."""
        return normalize_name_slug(v)
    
    @field_validator('team_tricode')
    @classmethod
    def normalize_team_tricode(cls, v: str) -> str:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else v

    @field_validator('position', mode='before')
    @classmethod
    def normalize_position(cls, v) -> Position:
        """Normalize player position from various sources."""
        if isinstance(v, Position):
            return v
        
        # CRITICAL: Convert any input to string first to prevent int/str comparison errors
        if v is None:
            pos_str = 'G'
        else:
            # Ensure we always work with a string, regardless of input type
            pos_str = str(v).strip().upper()
        
        # Map various position formats to our enum
        position_map = {
            'PG': Position.PG,
            'SG': Position.SG, 
            'SF': Position.SF,
            'PF': Position.PF,
            'C': Position.C,
            'G': Position.G,
            'F': Position.F,
            'POINT_GUARD': Position.PG,
            'SHOOTING_GUARD': Position.SG,
            'SMALL_FORWARD': Position.SF,
            'POWER_FORWARD': Position.PF,
            'CENTER': Position.C,
            'GUARD': Position.G,
            'FORWARD': Position.F,
            '1': Position.PG,  # Handle numeric position codes
            '2': Position.SG,
            '3': Position.SF,
            '4': Position.PF,
            '5': Position.C,
        }
        
        return position_map.get(pos_str, Position.G)

    @classmethod
    def from_nba_stats(
        cls, 
        game_id: str, 
        team_tricode: str,
        player_data: Dict[str, Any], 
        source_url: str
    ) -> 'StartingLineupRow':
        """Create from NBA Stats boxscore data."""
        player_name = player_data.get('PLAYER_NAME', '')
        position_text = player_data.get('START_POSITION', '')
        
        # Map position text to enum
        position = None
        if position_text:
            try:
                position = Position(position_text.upper())
            except ValueError:
                # Handle non-standard position formats
                pos_map = {
                    'GUARD': Position.G,
                    'FORWARD': Position.F,
                    'CENTER': Position.C,
                }
                position = pos_map.get(position_text.upper())
        
        return cls(
            game_id=game_id,
            team_tricode=team_tricode,
            player_name_slug=normalize_name_slug(player_name),
            player_display_name=player_name,
            player_id=str(player_data.get('PLAYER_ID', '')),
            position=position,
            jersey_number=player_data.get('JERSEY_NUM'),
            final_pre_tip=True,
            source='nba_stats',
            source_url=source_url,
        )

    @classmethod
    def from_bref(
        cls, 
        game_id: str, 
        team_tricode: str,
        player_data: Dict[str, Any], 
        source_url: str
    ) -> 'StartingLineupRow':
        """Create from Basketball Reference boxscore data."""
        player_name = player_data.get('player', '')
        
        return cls(
            game_id=game_id,
            team_tricode=team_tricode,
            player_name_slug=normalize_name_slug(player_name),
            player_display_name=player_name,
            position=player_data.get('pos'),
            final_pre_tip=True,
            source='bref',
            source_url=source_url,
        )