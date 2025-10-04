"""Shot event model with coordinate data for Tranche 2."""

from pydantic import BaseModel, Field
from typing import Optional


class ShotEvent(BaseModel):
    """Shot event model with coordinate data."""
    
    game_id: str = Field(..., min_length=1)
    player_id: int = Field(..., description="Player who took the shot")
    team_id: Optional[int] = Field(None, description="Team ID (nullable)")
    period: int = Field(..., ge=1, le=10)
    shot_made_flag: int = Field(..., ge=0, le=1, description="1 if made, 0 if missed")
    loc_x: int = Field(..., description="X coordinate on court")
    loc_y: int = Field(..., description="Y coordinate on court")
    event_num: Optional[int] = Field(None, description="PBP event number for linking")