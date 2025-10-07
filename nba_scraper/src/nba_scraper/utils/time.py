from __future__ import annotations

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Optional: a small map we can grow; fallback is ET.
TEAM_TZ = {
    # examples; expand as needed via home team id or tricode
    # "DEN": "America/Denver",
    "1610612747": "America/Los_Angeles",  # LAL Lakers
    "LAL": "America/Los_Angeles",  # Also support tricode
    # "NYK": "America/New_York",
}


def get_venue_tz(team_key: str | None) -> ZoneInfo | None:
    """Get venue timezone for a team key (id or tricode)."""
    if not team_key:
        return None
    tz = TEAM_TZ.get(team_key)
    return ZoneInfo(tz) if tz else None


def official_game_date(start_time_utc: datetime, venue_tz: ZoneInfo | None) -> date:
    """
    Return NBA 'official' game date: calendar date in venue tz (fallback ET).
    start_time_utc MUST be tz-aware in UTC.
    """
    if start_time_utc.tzinfo is None or start_time_utc.tzinfo.utcoffset(start_time_utc) is None:
        raise ValueError("start_time_utc must be timezone-aware UTC")
    tz = venue_tz or ZoneInfo("America/New_York")
    if venue_tz is None:
        logger.warning(
            "games_bridge.date",
            extra={"reason": "venue_tz_missing", "fallback": "America/New_York"},
        )
    return start_time_utc.astimezone(tz).date()
