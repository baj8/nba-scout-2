"""Lineup stint model for foundation data."""

from pydantic import BaseModel, Field
from typing import List


class LineupStint(BaseModel):
    """Lineup stint model with array-based player tracking."""
    
    game_id: str = Field(..., min_length=1)
    team_id: int = Field(..., description="Team ID")
    period: int = Field(..., ge=1, le=10)
    lineup: List[int] = Field(..., min_length=5, max_length=5, description="Player IDs in lineup")
    seconds_played: int = Field(..., ge=0, description="Seconds this lineup was on court")