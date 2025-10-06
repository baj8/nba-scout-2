"""PBP event model with clock handling and model-level derivation."""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from ..utils.clock import parse_clock_to_seconds, compute_seconds_elapsed


class PbpEvent(BaseModel):
    """PBP event model with order-safe clock derivation."""
    
    game_id: str = Field(..., min_length=1)
    event_num: int = Field(..., ge=1)
    period: int = Field(..., ge=1, le=10)  # Allow up to 10 OT periods
    clock: str = Field(..., description="Time remaining in period")
    
    # Optional fields with proper nullable handling
    team_id: Optional[int] = Field(None, description="Team ID (nullable)")
    player1_id: Optional[int] = Field(None, description="Primary player ID (nullable)")
    action_type: Optional[int] = Field(None, description="NBA Stats event type")
    action_subtype: Optional[int] = Field(None, description="NBA Stats event subtype")
    description: Optional[str] = Field(None, description="Event description")
    
    # Derived fields - computed by model validators
    clock_seconds: Optional[float] = Field(None, description="Clock time in seconds")
    seconds_elapsed: Optional[float] = Field(None, description="Seconds elapsed in period")

    @model_validator(mode="after")
    def derive_clock_fields(self) -> "PbpEvent":
        """Derive clock_seconds and seconds_elapsed with NBA time remaining logic."""
        if not self.clock:
            return self
        
        try:
            # Parse clock_seconds from time string
            if self.clock_seconds is None:
                clock_seconds = parse_clock_to_seconds(self.clock)
                if clock_seconds is not None:
                    object.__setattr__(self, 'clock_seconds', clock_seconds)
            
            # Derive seconds_elapsed if we have clock_seconds
            if self.seconds_elapsed is None and self.clock_seconds is not None:
                seconds_elapsed = compute_seconds_elapsed(self.clock, self.period) 
                if seconds_elapsed is not None:
                    object.__setattr__(self, 'seconds_elapsed', seconds_elapsed)
                    
        except Exception:
            # Don't fail the whole model for clock parsing issues
            pass
        
        return self