"""Loader facade providing a unified interface to all function-based loaders."""

from typing import Iterable, Sequence, Any, List, Dict
import asyncpg

# Re-export existing upsert functions; import from their real modules
try:
    from .games import upsert_game  # (conn, game)
except (ImportError, ModuleNotFoundError):
    async def upsert_game(conn: asyncpg.Connection, game: Any) -> None:  # type: ignore
        """Stub implementation - upsert_game not available."""
        raise NotImplementedError("upsert_game not implemented")

try:
    from .pbp import upsert_pbp  # (conn, events: list)
except (ImportError, ModuleNotFoundError):
    async def upsert_pbp(conn: asyncpg.Connection, events: List[Any]) -> int:  # type: ignore
        """Stub implementation - upsert_pbp not available."""
        raise NotImplementedError("upsert_pbp not implemented")

try:
    from .lineups import upsert_lineups
    
    async def upsert_starting_lineups(conn: asyncpg.Connection, starters: List[Any]) -> int:
        """Alias for upsert_lineups to match expected interface."""
        return await upsert_lineups(conn, starters)
        
except (ImportError, ModuleNotFoundError):
    async def upsert_starting_lineups(conn: asyncpg.Connection, starters: List[Any]) -> int:  # type: ignore
        """Stub implementation - upsert_starting_lineups not available."""
        return 0

try:
    from .shots import upsert_shots  # (conn, shots: list)
except (ImportError, ModuleNotFoundError):
    async def upsert_shots(conn: asyncpg.Connection, shots: List[Any]) -> int:  # type: ignore
        """Stub implementation - upsert_shots not available."""
        raise NotImplementedError("upsert_shots not implemented")

# Optional loaders (create stubs if missing so imports don't crash)
try:
    from .refs import upsert_officials  # (conn, officials: list)
except (ImportError, ModuleNotFoundError):
    async def upsert_officials(conn: asyncpg.Connection, officials: List[Any]) -> int:  # type: ignore
        """Stub implementation - upsert_officials not available."""
        return 0

# Additional commonly expected loaders
try:
    from .advanced_metrics import upsert_adv_metrics
except (ImportError, ModuleNotFoundError):
    async def upsert_adv_metrics(conn: asyncpg.Connection, metrics: List[Any]) -> int:  # type: ignore
        """Stub implementation - upsert_adv_metrics not available."""
        return 0

# Class-based loader compatibility wrappers
class GameLoader:
    """Compatibility wrapper for class-based GameLoader interface."""
    
    @staticmethod
    async def upsert(conn: asyncpg.Connection, game: Any) -> None:
        """Delegate to function-based upsert_game."""
        return await upsert_game(conn, game)


class PbpLoader:
    """Compatibility wrapper for class-based PbpLoader interface."""
    
    @staticmethod
    async def upsert_batch(conn: asyncpg.Connection, events: List[Any]) -> int:
        """Delegate to function-based upsert_pbp."""
        return await upsert_pbp(conn, events)


class ShotLoader:
    """Compatibility wrapper for class-based ShotLoader interface."""
    
    @staticmethod
    async def upsert_batch(conn: asyncpg.Connection, shots: List[Any]) -> int:
        """Delegate to function-based upsert_shots."""
        return await upsert_shots(conn, shots)


class RefLoader:
    """Compatibility wrapper for class-based RefLoader interface."""
    
    @staticmethod
    async def upsert_batch(conn: asyncpg.Connection, officials: List[Any]) -> int:
        """Delegate to function-based upsert_officials."""
        return await upsert_officials(conn, officials)


class LineupLoader:
    """Compatibility wrapper for class-based LineupLoader interface."""
    
    @staticmethod
    async def upsert_batch(conn: asyncpg.Connection, starters: List[Any]) -> int:
        """Delegate to function-based upsert_starting_lineups."""
        return await upsert_starting_lineups(conn, starters)


__all__ = [
    # Function-based interface (preferred)
    "upsert_game",
    "upsert_pbp", 
    "upsert_shots",
    "upsert_officials",
    "upsert_starting_lineups",
    "upsert_adv_metrics",
    
    # Class-based compatibility interface (for legacy code)
    "GameLoader",
    "PbpLoader", 
    "ShotLoader",
    "RefLoader",
    "LineupLoader",
]