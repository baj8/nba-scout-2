"""Injury status row Pydantic model."""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

from .enums import InjuryStatus
from .ref_rows import normalize_name_slug


class InjuryStatusRow(BaseModel):
    """Injury status row model."""
    
    game_id: str = Field(..., description="Game identifier")
    team_tricode: str = Field(..., description="Team tricode")
    player_name_slug: str = Field(..., description="Player name in PascalCase")
    player_display_name: str = Field(..., description="Player display name")
    player_id: Optional[str] = Field(None, description="Player ID from data source")
    status: InjuryStatus = Field(..., description="Injury/availability status")
    reason: Optional[str] = Field(None, description="Injury reason or description")
    snapshot_utc: Optional[datetime] = Field(None, description="When status was captured")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
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
    
    @field_validator('status', mode='before')
    @classmethod
    def normalize_status(cls, v: str) -> InjuryStatus:
        """Normalize injury status text to enum."""
        if isinstance(v, InjuryStatus):
            return v
            
        # Clean up status text
        status_text = v.upper().strip() if v else ''
        
        # Handle common variations
        status_map = {
            'OUT': InjuryStatus.OUT,
            'QUESTIONABLE': InjuryStatus.QUESTIONABLE,
            'PROBABLE': InjuryStatus.PROBABLE,
            'ACTIVE': InjuryStatus.ACTIVE,
            'DNP': InjuryStatus.DNP,
            'INACTIVE': InjuryStatus.INACTIVE,
            'DOUBTFUL': InjuryStatus.QUESTIONABLE,  # Map doubtful to questionable
            'GTD': InjuryStatus.QUESTIONABLE,  # Game-time decision
        }
        
        return status_map.get(status_text, InjuryStatus.ACTIVE)
    
    @classmethod
    def from_bref_notes(
        cls, 
        game_id: str, 
        team_tricode: str,
        player_data: Dict[str, Any], 
        source_url: str,
        snapshot_utc: Optional[datetime] = None
    ) -> 'InjuryStatusRow':
        """Create from Basketball Reference injury notes."""
        player_name = player_data.get('player', '')
        status_text = player_data.get('status', 'ACTIVE')
        reason = player_data.get('reason', '')
        
        return cls(
            game_id=game_id,
            team_tricode=team_tricode,
            player_name_slug=normalize_name_slug(player_name),
            player_display_name=player_name,
            status=cls.normalize_status(None, status_text),
            reason=reason if reason else None,
            snapshot_utc=snapshot_utc,
            source='bref_notes',
            source_url=source_url,
        )
    
    @classmethod
    def from_team_report(
        cls, 
        game_id: str, 
        team_tricode: str,
        player_data: Dict[str, Any], 
        source_url: str,
        snapshot_utc: Optional[datetime] = None
    ) -> 'InjuryStatusRow':
        """Create from team injury report."""
        player_name = player_data.get('name', '')
        status_text = player_data.get('status', 'ACTIVE')
        reason = player_data.get('injury', '')
        
        return cls(
            game_id=game_id,
            team_tricode=team_tricode,
            player_name_slug=normalize_name_slug(player_name),
            player_display_name=player_name,
            player_id=player_data.get('player_id'),
            status=cls.normalize_status(None, status_text),
            reason=reason if reason else None,
            snapshot_utc=snapshot_utc,
            source='team_report',
            source_url=source_url,
        )