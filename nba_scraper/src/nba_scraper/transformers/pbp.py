"""PBP transformation functions - pure, synchronous."""

from typing import Any, Dict, Iterable, List

from ..models.pbp import PbpEvent
from ..utils.coerce import to_int_or_none
from ..utils.preprocess import (
    normalize_clock_time,
    normalize_player_id,
    normalize_team_id,
    preprocess_nba_stats_data,
)


def transform_pbp(events: Iterable[Dict[str, Any]], game_id: str) -> List[PbpEvent]:
    """Transform extracted PBP events to validated PbpEvent models.

    Args:
        events: Iterable of dictionaries from extract_pbp_from_response
        game_id: Game identifier for linking

    Returns:
        List of validated PbpEvent instances with clock_seconds and seconds_elapsed derived
    """
    rows: List[PbpEvent] = []

    for e in events:
        # Apply preprocessing to handle mixed data types safely
        e = preprocess_nba_stats_data(e)

        # Extract required fields with safe defaults using robust coercion
        event_num = to_int_or_none(e.get("EVENTNUM")) or 0
        period = to_int_or_none(e.get("PERIOD")) or 1

        # Handle clock time - preserve as string for model validation
        clock_raw = e.get("PCTIMESTRING") or e.get("CLOCK") or e.get("TIME_REMAINING") or "12:00"
        clock = normalize_clock_time(clock_raw)

        # Safely extract nullable fields
        team_id = normalize_team_id(e.get("TEAM_ID"))
        player1_id = normalize_player_id(e.get("PLAYER1_ID"))

        # Extract action types with robust coercion
        action_type = to_int_or_none(e.get("EVENTMSGTYPE"))
        action_subtype = to_int_or_none(e.get("EVENTMSGACTIONTYPE"))

        # Extract description from various possible fields
        description = (
            e.get("HOMEDESCRIPTION")
            or e.get("NEUTRALDESCRIPTION")
            or e.get("VISITORDESCRIPTION")
            or e.get("DESCRIPTION")
        )

        # Create PbpEvent - model validators will derive clock_seconds and seconds_elapsed
        try:
            pbp_event = PbpEvent(
                game_id=game_id,
                event_num=event_num,
                period=period,
                clock=clock,
                team_id=team_id,
                player1_id=player1_id,
                action_type=action_type,
                action_subtype=action_subtype,
                description=str(description) if description is not None else None,
                # clock_seconds and seconds_elapsed will be derived by model validators
            )
            rows.append(pbp_event)

        except Exception as e:
            # Skip invalid events but don't fail the entire batch
            continue

    return rows
