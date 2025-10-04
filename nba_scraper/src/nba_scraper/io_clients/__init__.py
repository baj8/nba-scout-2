"""IO facade that exposes consistent method names for NBA data API clients."""

from typing import Dict, Any
from .nba_stats import NBAStatsClient
from .bref import BRefClient
from .gamebooks import GamebooksClient


class IoFacade:
    """Facade that wraps any IO implementation and exposes consistent method names."""
    
    def __init__(self, impl):
        """Initialize with an IO implementation (e.g., NBAStatsClient)."""
        self.impl = impl

    async def fetch_boxscore(self, game_id: str) -> Dict[str, Any]:
        """Fetch boxscore data for a game."""
        for name in ("boxscore", "fetch_boxscore", "get_boxscore"):
            if hasattr(self.impl, name):
                return await getattr(self.impl, name)(game_id)
        raise AttributeError(f"No boxscore method found on {type(self.impl).__name__}")

    async def fetch_pbp(self, game_id: str) -> Dict[str, Any]:
        """Fetch play-by-play data for a game."""
        for name in ("pbp", "fetch_pbp", "get_pbp", "play_by_play"):
            if hasattr(self.impl, name):
                return await getattr(self.impl, name)(game_id)
        raise AttributeError(f"No PBP method found on {type(self.impl).__name__}")

    async def fetch_lineups(self, game_id: str) -> Dict[str, Any]:
        """Fetch lineup data for a game."""
        for name in ("lineups", "fetch_lineups", "get_lineups", "lineup_stints"):
            if hasattr(self.impl, name):
                return await getattr(self.impl, name)(game_id)
        raise AttributeError(f"No lineups method found on {type(self.impl).__name__}")

    async def fetch_shots(self, game_id: str) -> Dict[str, Any]:
        """Fetch shot chart data for a game."""
        for name in ("shots", "fetch_shots", "get_shots", "shot_chart", "shot_chart_detail"):
            if hasattr(self.impl, name):
                return await getattr(self.impl, name)(game_id)
        raise AttributeError(f"No shots method found on {type(self.impl).__name__}")

    async def fetch_scoreboard(self, date_str: str) -> Dict[str, Any]:
        """Fetch scoreboard/games data for a date."""
        for name in ("scoreboard", "fetch_scoreboard", "games_for_date", "games_by_date", "fetch_scoreboard_by_date"):
            if hasattr(self.impl, name):
                # Handle different parameter expectations
                method = getattr(self.impl, name)
                try:
                    # Try string parameter first
                    return await method(date_str)
                except TypeError:
                    # Try datetime parameter
                    from datetime import datetime
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    return await method(date_obj)
        raise AttributeError(f"No scoreboard method found on {type(self.impl).__name__}")

    async def close(self):
        """Close the underlying implementation if it supports it."""
        if hasattr(self.impl, 'close'):
            await self.impl.close()
        elif hasattr(self.impl, 'aclose'):
            await self.impl.aclose()


__all__ = [
    "NBAStatsClient",
    "BRefClient", 
    "GamebooksClient",
    "IoFacade",
]