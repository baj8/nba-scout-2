"""Game models for NBA game metadata."""

from pydantic import BaseModel, Field
from typing import Optional

class Game(BaseModel):
    """NBA game metadata model."""
    
    game_id: str = Field(..., min_length=10, description="NBA game ID")
    season: str = Field(..., description="Season string like '2023-24'")
    game_date: str = Field(..., description="Game date in YYYY-MM-DD format")
    home_team_id: int = Field(..., description="Home team ID")
    away_team_id: int = Field(..., description="Away team ID") 
    status: str = Field(default="Final", description="Game status")
    
    class Config:
        """Pydantic configuration."""
        str_strip_whitespace = True