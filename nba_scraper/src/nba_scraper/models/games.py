"""Game model with season derivation and validation."""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import date
from ..utils.season import derive_season_smart, validate_season_format


class Game(BaseModel):
    """Game model with smart season derivation."""
    
    game_id: str = Field(..., min_length=10, description="NBA game ID")
    season: str = Field(..., description="Season (e.g., '2023-24')")
    game_date: str = Field(..., description="Game date (YYYY-MM-DD)")
    home_team_id: int = Field(..., description="Home team ID")
    away_team_id: int = Field(..., description="Away team ID")
    status: str = Field(default="Final", description="Game status")
    
    @field_validator("season")
    @classmethod
    def validate_season_format_field(cls, v: str) -> str:
        """Validate season format."""
        if not validate_season_format(v):
            raise ValueError(f"Invalid season format: {v}. Expected format: '2023-24'")
        return v
    
    @field_validator("game_date")
    @classmethod
    def validate_game_date_format(cls, v: str) -> str:
        """Validate game date format."""
        try:
            # This will raise ValueError if invalid format
            date.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError(f"Invalid date format: {v}. Expected format: 'YYYY-MM-DD'")
    
    @model_validator(mode="after")
    def derive_season_if_unknown(self) -> "Game":
        """Derive season if it's 'UNKNOWN' using game_id and date."""
        if self.season == "UNKNOWN":
            derived_season = derive_season_smart(
                game_id=self.game_id,
                game_date=self.game_date
            )
            if derived_season and derived_season != "UNKNOWN":
                self.season = derived_season
        
        return self