"""PBP event model with clock handling and model-level derivation."""

from typing import Optional

from pydantic import BaseModel, Field, model_validator

from ..utils.clock import parse_clock_to_ms, period_length_ms


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
    clock_ms_remaining: Optional[int] = Field(
        None, description="Clock time in milliseconds remaining"
    )
    seconds_elapsed: Optional[float] = Field(None, description="Seconds elapsed in period")

    @model_validator(mode="after")
    def derive_clock_fields(self) -> "PbpEvent":
        """Derive clock_ms_remaining and seconds_elapsed with NBA time remaining logic."""
        if not self.clock:
            return self

        try:
            # Parse clock_ms_remaining from time string
            if self.clock_ms_remaining is None:
                clock_ms = parse_clock_to_ms(self.clock, self.period)
                object.__setattr__(self, "clock_ms_remaining", clock_ms)

            # Derive seconds_elapsed from clock_ms_remaining
            if self.seconds_elapsed is None and self.clock_ms_remaining is not None:
                period_ms = period_length_ms(self.period)
                elapsed_ms = period_ms - self.clock_ms_remaining
                seconds_elapsed = elapsed_ms / 1000.0
                object.__setattr__(self, "seconds_elapsed", seconds_elapsed)

        except Exception:
            # Don't fail the whole model for clock parsing issues
            pass

        return self
