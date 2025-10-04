"""Loaders facade - finds and re-exports existing loader functions without rewriting them."""

from importlib import import_module
from types import ModuleType
from typing import Callable, Optional

# Candidate modules where loaders might exist
_CANDIDATE_MODULES = [
    "nba_scraper.loaders.games",
    "nba_scraper.loaders.game_loader", 
    "nba_scraper.loaders.core",
    "nba_scraper.loaders.base",
    "nba_scraper.loaders.metrics",
    "nba_scraper.loaders.events",
    "nba_scraper.loaders.pbp",
    "nba_scraper.loaders.lineups",
    "nba_scraper.loaders.shots",
]

# Expected function name mappings
_NAME_MAP = {
    "upsert_game": ["upsert_game", "insert_or_update_game", "load_game"],
    "upsert_pbp": ["upsert_pbp", "load_pbp", "insert_pbp_events", "upsert_pbp_events"],
    "upsert_lineups": ["upsert_lineups", "load_lineups", "insert_lineup_stints"],
    "upsert_shots": ["upsert_shots", "load_shots", "insert_shots", "upsert_shot_coords"],
    "upsert_adv_metrics": ["upsert_adv_metrics", "load_adv_metrics", "insert_adv_metrics"],
}


def _find_func(name_candidates: list[str]) -> Optional[Callable]:
    """Find a function by trying multiple names across multiple modules."""
    for mod_name in _CANDIDATE_MODULES:
        try:
            mod: ModuleType = import_module(mod_name)
        except Exception:
            continue
            
        for func_name in name_candidates:
            fn = getattr(mod, func_name, None)
            if callable(fn):
                return fn
    return None


# Find and expose loader functions
upsert_game = _find_func(_NAME_MAP["upsert_game"])
upsert_pbp = _find_func(_NAME_MAP["upsert_pbp"])
upsert_lineups = _find_func(_NAME_MAP["upsert_lineups"])
upsert_shots = _find_func(_NAME_MAP["upsert_shots"])
upsert_adv_metrics = _find_func(_NAME_MAP["upsert_adv_metrics"])

# Only export functions that were successfully found
__all__ = [
    name for name, fn in {
        "upsert_game": upsert_game,
        "upsert_pbp": upsert_pbp,
        "upsert_lineups": upsert_lineups,
        "upsert_shots": upsert_shots,
        "upsert_adv_metrics": upsert_adv_metrics,
    }.items() if fn is not None
]