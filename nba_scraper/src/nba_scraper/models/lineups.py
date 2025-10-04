"""Lineup models for NBA lineup stint data."""

from pydantic import BaseModel, Field
from typing import List

class LineupStint(BaseModel):
    """NBA lineup stint with exactly 5 players on court."""
    
    game_id: str = Field(..., description="NBA game ID")
    team_id: int = Field(..., description="Team ID")
    period: int = Field(..., description="Game period")
    lineup: List[int] = Field(..., min_items=5, max_items=5, description="Exactly 5 player IDs")
    seconds_played: int = Field(..., description="Seconds this lineup was on court")
    
    class Config:
        """Pydantic configuration."""
        str_strip_whitespace = True
        validate_assignment = True