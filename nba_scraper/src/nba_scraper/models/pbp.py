"""PBP event model with order-safe derivation and clock handling."""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
import re
from ..utils.clock import parse_clock_to_seconds, compute_seconds_elapsed, validate_clock_format

# Clock field validation pattern
_CLOCK_FIELD_RE = re.compile(r"^\d{1,2}:[0-5]\d(?:\.\d{1,3})?$|^PT\d+M\d+(?:\.\d{1,3})?S$")


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
    seconds_elapsed: Optional[float] = Field(None, description="Total game seconds elapsed")

    @field_validator("clock")
    @classmethod
    def validate_clock_format(cls, v: str) -> str:
        """Validate clock format and ensure it's a valid time string."""
        if not v or not validate_clock_format(v):
            raise ValueError(f"Invalid clock format: {v!r}")
        return v.strip()

    @field_validator("clock_seconds", mode="before")
    @classmethod
    def derive_clock_seconds(cls, v, info):
        """Derive clock_seconds from clock if not provided."""
        if v is not None:
            return v
            
        # Try to get clock from other validated fields
        if hasattr(info, 'data') and info.data and 'clock' in info.data:
            clock = info.data['clock']
            return parse_clock_to_seconds(clock)
        return None

    @model_validator(mode="after")
    def derive_seconds_elapsed_after(self) -> "PbpEvent":
        """Derive seconds_elapsed using order-safe model-level validation."""
        if self.seconds_elapsed is None and self.clock_seconds is not None:
            self.seconds_elapsed = compute_seconds_elapsed(
                self.period, 
                self.clock_seconds,
                mode="remaining"  # NBA default
            )
        return self

    def model_dump(self, **kwargs) -> dict:
        """Custom model dump that ensures derived fields are included."""
        data = super().model_dump(**kwargs)
        
        # Ensure derived fields are computed if missing
        if data.get('clock_seconds') is None and data.get('clock'):
            data['clock_seconds'] = parse_clock_to_seconds(data['clock'])
        
        if data.get('seconds_elapsed') is None and data.get('clock_seconds') is not None:
            data['seconds_elapsed'] = compute_seconds_elapsed(
                data.get('period'), 
                data.get('clock_seconds')
            )
        
        return data