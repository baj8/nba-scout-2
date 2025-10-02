"""Derived analytics row Pydantic models."""

from datetime import date, datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

from .enums import EarlyShockType, Severity


class Q1WindowRow(BaseModel):
    """Q1 window analysis (12:00 to 8:00) row model."""
    
    game_id: str = Field(..., description="Game identifier")
    home_team_tricode: str = Field(..., description="Home team tricode")
    away_team_tricode: str = Field(..., description="Away team tricode")
    possessions_elapsed: int = Field(..., description="Number of possessions in window")
    pace48_actual: Optional[float] = Field(None, description="Actual pace per 48 minutes")
    pace48_expected: Optional[float] = Field(None, description="Expected pace per 48 minutes")
    home_efg_actual: Optional[float] = Field(None, description="Home team actual eFG%")
    home_efg_expected: Optional[float] = Field(None, description="Home team expected eFG%")
    away_efg_actual: Optional[float] = Field(None, description="Away team actual eFG%")
    away_efg_expected: Optional[float] = Field(None, description="Away team expected eFG%")
    home_to_rate: Optional[float] = Field(None, description="Home team turnover rate")
    away_to_rate: Optional[float] = Field(None, description="Away team turnover rate")
    home_ft_rate: Optional[float] = Field(None, description="Home team free throw rate")
    away_ft_rate: Optional[float] = Field(None, description="Away team free throw rate")
    home_orb_pct: Optional[float] = Field(None, description="Home team offensive rebound %")
    home_drb_pct: Optional[float] = Field(None, description="Home team defensive rebound %")
    away_orb_pct: Optional[float] = Field(None, description="Away team offensive rebound %")
    away_drb_pct: Optional[float] = Field(None, description="Away team defensive rebound %")
    bonus_time_home_sec: float = Field(default=0, description="Home team bonus time in seconds")
    bonus_time_away_sec: float = Field(default=0, description="Away team bonus time in seconds")
    transition_rate: Optional[float] = Field(None, description="Transition possession rate")
    early_clock_rate: Optional[float] = Field(None, description="Early shot clock rate")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @field_validator('home_team_tricode', 'away_team_tricode')
    @classmethod
    def normalize_team_tricode(cls, v: str) -> str:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else v


class EarlyShockRow(BaseModel):
    """Early shock event row model."""
    
    game_id: str = Field(..., description="Game identifier")
    team_tricode: str = Field(..., description="Team tricode")
    player_slug: str = Field(..., description="Player name slug")
    shock_type: EarlyShockType = Field(..., description="Type of early shock event")
    shock_seq: int = Field(..., description="Sequence number for this shock type/player")
    period: int = Field(default=1, description="Period when event occurred (always 1 for early shocks)")
    clock_hhmmss: str = Field(..., description="Game clock when event occurred (HH:MM:SS)")
    event_idx_start: int = Field(..., description="Starting event index")
    event_idx_end: Optional[int] = Field(None, description="Ending event index (for multi-event shocks)")
    immediate_sub: bool = Field(default=False, description="Whether player was immediately substituted")
    poss_since_event: int = Field(default=0, description="Possessions since the triggering event")
    notes: Optional[str] = Field(None, description="Short diagnostic notes")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @field_validator('team_tricode')
    @classmethod
    def normalize_team_tricode(cls, v: str) -> str:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else v


class ScheduleTravelRow(BaseModel):
    """Schedule and travel analysis row model."""
    
    game_id: str = Field(..., description="Game identifier")
    team_tricode: str = Field(..., description="Team tricode")
    is_back_to_back: bool = Field(default=False, description="Game is back-to-back")
    is_3_in_4: bool = Field(default=False, description="3 games in 4 days")
    is_5_in_7: bool = Field(default=False, description="5 games in 7 days")
    days_rest: int = Field(..., description="Days of rest before game")
    timezone_shift_hours: float = Field(default=0, description="Timezone shift in hours")
    circadian_index: Optional[float] = Field(None, description="Circadian disruption index")
    altitude_change_m: float = Field(default=0, description="Altitude change in meters")
    travel_distance_km: float = Field(default=0, description="Travel distance in kilometers")
    prev_game_date: Optional[date] = Field(None, description="Previous game date")
    prev_arena_tz: Optional[str] = Field(None, description="Previous arena timezone")
    prev_lat: Optional[float] = Field(None, description="Previous arena latitude")
    prev_lon: Optional[float] = Field(None, description="Previous arena longitude")
    prev_altitude_m: Optional[float] = Field(None, description="Previous arena altitude")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @field_validator('team_tricode')
    @classmethod
    def normalize_team_tricode(cls, v: str) -> str:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else v


class OutcomesRow(BaseModel):
    """Game outcomes row model."""
    
    game_id: str = Field(..., description="Game identifier")
    home_team_tricode: str = Field(..., description="Home team tricode")
    away_team_tricode: str = Field(..., description="Away team tricode")
    q1_home_points: Optional[int] = Field(None, description="Home team Q1 points")
    q1_away_points: Optional[int] = Field(None, description="Away team Q1 points")
    final_home_points: int = Field(..., description="Home team final points")
    final_away_points: int = Field(..., description="Away team final points")
    total_points: int = Field(..., description="Total points scored")
    home_win: bool = Field(..., description="Home team won")
    margin: int = Field(..., description="Final margin (home - away)")
    overtime_periods: int = Field(default=0, description="Number of OT periods")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @field_validator('home_team_tricode', 'away_team_tricode')
    @classmethod
    def normalize_team_tricode(cls, v: str) -> str:
        """Normalize team tricode to uppercase."""
        return v.upper() if v else v
    
    @classmethod
    def from_box_score(
        cls,
        game_id: str,
        home_team: str,
        away_team: str,
        box_data: Dict[str, Any],
        source: str,
        source_url: str
    ) -> 'OutcomesRow':
        """Create from box score data."""
        home_final = int(box_data.get('home_final', 0))
        away_final = int(box_data.get('away_final', 0))
        
        return cls(
            game_id=game_id,
            home_team_tricode=home_team,
            away_team_tricode=away_team,
            q1_home_points=box_data.get('home_q1'),
            q1_away_points=box_data.get('away_q1'),
            final_home_points=home_final,
            final_away_points=away_final,
            total_points=home_final + away_final,
            home_win=home_final > away_final,
            margin=home_final - away_final,
            overtime_periods=box_data.get('ot_periods', 0),
            source=source,
            source_url=source_url,
        )