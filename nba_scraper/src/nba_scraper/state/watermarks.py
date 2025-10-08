"""Watermark tracking for resumable scheduler operations."""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import Table, Column, String, DateTime, MetaData, insert, select, update, UniqueConstraint
from sqlalchemy.engine import Connection
import logging

logger = logging.getLogger(__name__)

metadata = MetaData()
ingest_watermarks = Table(
    "ingest_watermarks", metadata,
    Column("stage", String, nullable=False),
    Column("key", String, nullable=False),  # e.g., season or 'daily'
    Column("value", String, nullable=False),  # e.g., last processed game_id or ISO date
    Column("updated_at", DateTime, nullable=False),
    UniqueConstraint("stage", "key", name="uq_ingest_watermarks_stage_key"),
)


def ensure_tables(conn: Connection) -> None:
    """Create watermarks table if it doesn't exist (idempotent)."""
    metadata.create_all(conn.engine, tables=[ingest_watermarks])


def get_watermark(conn: Connection, *, stage: str, key: str) -> Optional[str]:
    """Get watermark value for a given stage and key."""
    row = conn.execute(
        select(ingest_watermarks.c.value).where(
            ingest_watermarks.c.stage == stage,
            ingest_watermarks.c.key == key
        )
    ).fetchone()
    return row[0] if row else None


def set_watermark(conn: Connection, *, stage: str, key: str, value: str) -> None:
    """Set watermark value for a given stage and key (upsert)."""
    now = datetime.now(timezone.utc)
    existing = get_watermark(conn, stage=stage, key=key)
    if existing is None:
        conn.execute(
            insert(ingest_watermarks).values(
                stage=stage, key=key, value=value, updated_at=now
            )
        )
    else:
        conn.execute(
            update(ingest_watermarks)
            .where(
                ingest_watermarks.c.stage == stage,
                ingest_watermarks.c.key == key
            )
            .values(value=value, updated_at=now)
        )
    logger.debug("watermark.updated", extra={"stage": stage, "key": key, "value": value})
