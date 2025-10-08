"""Scheduler jobs for daily and backfill operations."""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Iterable, Tuple
import logging
from sqlalchemy import create_engine

from nba_scraper.config import get_settings
from nba_scraper.state.watermarks import get_watermark, set_watermark, ensure_tables
from nba_scraper.schedule.discovery import discover_game_ids_for_date, discover_game_ids_for_date_range

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def _get_sync_engine():
    """Get synchronous SQLAlchemy engine for scheduler."""
    settings = get_settings()
    # Convert async URL to sync (remove +asyncpg)
    db_url = settings.get_database_url().replace("+asyncpg", "").replace("postgresql://", "postgresql+psycopg2://")
    return create_engine(db_url, pool_pre_ping=True)


def _yesterday_et(today_utc: datetime | None = None) -> date:
    """Get yesterday's date in America/New_York timezone."""
    now_utc = today_utc or datetime.now(timezone.utc)
    et_now = now_utc.astimezone(ET)
    yesterday = et_now - timedelta(days=1)
    return yesterday.date()


def _dates_in_chunks(start: date, end: date, chunk_days: int) -> Iterable[Tuple[date, date]]:
    """Split date range into chunks of specified size."""
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=chunk_days - 1), end)
        yield (cur, stop)
        cur = stop + timedelta(days=1)


def run_daily() -> int:
    """
    Discover yesterday's ET games, run full pipeline, update watermark 'daily' with ISO date.
    
    Returns:
        Non-zero count of failures encountered
    """
    engine = _get_sync_engine()
    with engine.begin() as conn:
        ensure_tables(conn)
    
    target = _yesterday_et()
    logger.info("job.start", extra={"job": "daily", "target_date": target.isoformat()})
    
    # Discover games for target date
    game_ids = discover_game_ids_for_date(target)
    failures = 0
    
    if not game_ids:
        logger.info("job.no_games", extra={"date": target.isoformat()})
    else:
        try:
            from nba_scraper.cli_pipeline import run_pipeline_for_games
            run_pipeline_for_games(game_ids)
        except Exception as e:
            logger.exception("job.batch_error", extra={"job": "daily", "date": target.isoformat()})
            failures += 1
    
    # Update watermark
    with engine.begin() as conn:
        set_watermark(conn, stage="schedule", key="daily", value=target.isoformat())
    
    logger.info("job.end", extra={"job": "daily", "failures": failures, "games": len(game_ids)})
    return failures


def run_backfill(season: str, since_game_id: str | None = None, chunk_days: int = 7) -> int:
    """
    Walk the season from Oct 1 → Sep 30 ET in chunks. Resume from watermark if present.
    
    Watermark key = season (e.g., '2024-25'), value = last processed game_id (lexicographic).
    
    Args:
        season: Season string (e.g., '2024-25')
        since_game_id: Optional explicit game ID to resume from (overrides watermark)
        chunk_days: Number of days per chunk
        
    Returns:
        Non-zero count of failures encountered
    """
    from nba_scraper.utils.season_utils import season_bounds
    
    start, end = season_bounds(season)
    engine = _get_sync_engine()
    
    with engine.begin() as conn:
        ensure_tables(conn)
        last = get_watermark(conn, stage="backfill", key=season)
    
    resume_from = since_game_id or last
    total_failures = 0
    
    logger.info("job.start", extra={
        "job": "backfill",
        "season": season,
        "resume_from": resume_from
    })
    
    for chunk_start, chunk_end in _dates_in_chunks(start, end, chunk_days):
        # Discover games in [chunk_start, chunk_end]
        games = discover_game_ids_for_date_range(chunk_start, chunk_end)
        
        if resume_from:
            # Lexicographic filter; game_ids are fixed-length so safe
            games = [g for g in games if g > resume_from]
        
        if not games:
            continue
        
        try:
            from nba_scraper.cli_pipeline import run_pipeline_for_games
            run_pipeline_for_games(games)
            
            # Update watermark to the max game_id processed in this chunk
            with engine.begin() as conn:
                set_watermark(conn, stage="backfill", key=season, value=max(games))
        except Exception:
            logger.exception("job.chunk_error", extra={
                "job": "backfill",
                "season": season,
                "chunk_start": chunk_start.isoformat(),
                "chunk_end": chunk_end.isoformat()
            })
            total_failures += 1
            # Continue; watermark remains at last successful batch
    
    logger.info("job.end", extra={
        "job": "backfill",
        "season": season,
        "failures": total_failures
    })
    return total_failures
