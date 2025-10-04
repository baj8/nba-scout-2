"""Play-by-play event models with robust clock parsing and validation."""

from pydantic import BaseModel, field_validator, model_validator
from typing import Optional
import re
from ..utils.clock import parse_clock_to_seconds, compute_seconds_elapsed

_CLOCK_FIELD_RE = re.compile(r"^\d{1,2}:[0-5]\d(?:\.\d{1,3})?$|^PT\d+M\d+(?:\.\d{1,3})?S$")

class PbpEvent(BaseModel):
    """Play-by-play event with automatic clock parsing and elapsed time calculation."""
    
    game_id: str
    event_num: int
    period: int
    clock: str
    team_id: Optional[int] = None
    player1_id: Optional[int] = None
    action_type: Optional[int] = None
    action_subtype: Optional[int] = None
    description: Optional[str] = None
    clock_seconds: Optional[float] = None
    seconds_elapsed: Optional[float] = None

    @field_validator("clock")
    @classmethod
    def validate_clock_format(cls, v: str) -> str:
        """Validate clock format matches expected patterns."""
        if v and not _CLOCK_FIELD_RE.match(v.strip()):
            raise ValueError(f"Invalid clock format: {v!r}")
        return v

    @field_validator("clock_seconds", mode="before")
    @classmethod
    def derive_clock_seconds(cls, v, info):
        """Derive clock_seconds from clock string if not provided."""
        if v is None and info.data and (clock := info.data.get("clock")):
            return parse_clock_to_seconds(clock)
        return v

    @model_validator(mode="after")
    def derive_seconds_elapsed_after(self) -> "PbpEvent":
        """Derive seconds_elapsed after all other fields are set."""
        if self.seconds_elapsed is None:
            self.seconds_elapsed = compute_seconds_elapsed(self.period, self.clock_seconds)
        return self

    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        str_strip_whitespace = True