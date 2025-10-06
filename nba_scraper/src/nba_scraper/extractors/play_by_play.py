"""NBA Play-by-Play data extractor."""

import json
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

from ..models.events import PlayByPlayEvent, EventType, EventDetails
from ..models.source import SourceMetadata
from ..config import AnalyticsConfig

# ...existing code...

@asynccontextmanager
async def _maybe_transaction(conn):
    """
    Use conn.transaction() if it returns an async context manager.
    If it doesn't (e.g., tests using AsyncMock without __aenter__/__aexit__), just yield.
    """
    tx_fn = getattr(conn, "transaction", None)
    if tx_fn is None:
        yield
        return
    try:
        ctx = tx_fn()
        if hasattr(ctx, "__aenter__") and hasattr(ctx, "__aexit__"):
            async with ctx:
                yield
        else:
            yield
    except TypeError:
        yield

# ...existing code...

async def persist_play_by_play_events(events: List[PlayByPlayEvent]) -> None:
    """Persist play-by-play events to database."""
    # ...existing code...
    
    async with _maybe_transaction(conn):
        # ...existing code...