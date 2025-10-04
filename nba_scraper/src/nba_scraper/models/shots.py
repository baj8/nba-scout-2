"""Shot models for NBA shot coordinate data."""

from pydantic import BaseModel, Field
from typing import Optional

class ShotEvent(BaseModel):
    """NBA shot event with court coordinates."""
    
    game_id: str = Field(..., description="NBA game ID")
    player_id: int = Field(..., description="Player who took the shot")
    team_id: Optional[int] = Field(None, description="Team ID")
    period: int = Field(..., description="Game period")
    shot_made_flag: int = Field(..., description="1 if made, 0 if missed")
    loc_x: int = Field(..., description="X coordinate on court")
    loc_y: int = Field(..., description="Y coordinate on court")
    event_num: Optional[int] = Field(None, description="PBP event number if available")
    
    class Config:
        """Pydantic configuration."""
        str_strip_whitespace = True
        validate_assignment = True