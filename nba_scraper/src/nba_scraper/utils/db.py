"""Database utilities for handling transactions across different connection types."""

from contextlib import asynccontextmanager
from typing import Any


@asynccontextmanager
async def maybe_transaction(conn: Any):
    """
    Async context manager that safely handles transactions across different connection types.
    
    - Uses conn.transaction() if available
    - If the returned object has async __aenter__/__aexit__, explicitly await them
    - If it's an awaitable (e.g., AsyncMock), await it and then yield (no-op)
    - On TypeError or missing pieces, just yield (no-op)
    
    Args:
        conn: Database connection object (real or mock)
        
    Yields:
        None: Context for transaction block
    """
    tx_fn = getattr(conn, "transaction", None)
    if tx_fn is None:
        yield
        return
    
    try:
        ctx = tx_fn()
        
        # Check if it's an async context manager
        if hasattr(ctx, "__aenter__") and hasattr(ctx, "__aexit__"):
            async with ctx:
                yield
        # Check if it's an awaitable (like AsyncMock)
        elif hasattr(ctx, "__await__"):
            await ctx
            yield
        else:
            # Fallback: just yield without transaction
            yield
            
    except (TypeError, AttributeError):
        # Fallback for any other cases
        yield