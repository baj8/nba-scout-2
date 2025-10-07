"""Loaders facade with dynamic resolution of canonical callables."""

import importlib
from typing import Any, Callable, Optional

# Import derived module to make it available for test mocking
from . import derived

# Module scan order that tests expect
_MODULE_SCAN_ORDER = (
    "nba_scraper.loaders.derived",
    "nba_scraper.loaders.base",
    "nba_scraper.loaders",
)

# Required canonical callables
_REQUIRED_CALLABLES = [
    "upsert_game",
    "upsert_pbp",
    "upsert_lineups",
    "upsert_shots",
    "upsert_adv_metrics",
]

# Likely modules to scan for each callable
_MODULE_SCAN_MAP = {
    "upsert_game": ["games", "game", "game_loader"],
    "upsert_pbp": ["pbp", "play_by_play", "events"],
    "upsert_lineups": ["lineups", "lineup", "starters"],
    "upsert_shots": ["shots", "shot", "shooting"],
    "upsert_adv_metrics": ["advanced_metrics", "metrics", "adv_metrics", "advanced"],
}


def _resolve_callable(callable_name: str) -> Optional[Callable]:
    """Resolve a callable by scanning likely modules in order."""
    errors = []

    # First try the module scan order for package-level resolution
    for modname in _MODULE_SCAN_ORDER:
        try:
            mod = importlib.import_module(modname)
            if hasattr(mod, callable_name):
                func = getattr(mod, callable_name)
                if callable(func):
                    return func
        except Exception as e:
            errors.append((modname, e))

    # Then try the local module scan map
    possible_modules = _MODULE_SCAN_MAP.get(callable_name, [callable_name])

    for module_name in possible_modules:
        try:
            module = importlib.import_module(f".{module_name}", package=__package__)
            if hasattr(module, callable_name):
                func = getattr(module, callable_name)
                if callable(func):
                    return func
        except ImportError:
            continue

    # Return None if not found (don't raise errors during module loading)
    return None


# Dynamically resolve all required callables (gracefully handle missing ones)
upsert_game = _resolve_callable("upsert_game")
upsert_pbp = _resolve_callable("upsert_pbp")
upsert_lineups = _resolve_callable("upsert_lineups")
upsert_shots = _resolve_callable("upsert_shots")
upsert_adv_metrics = _resolve_callable("upsert_adv_metrics")

# Create aliases for backward compatibility
upsert_pbp_events = upsert_pbp
upsert_lineup_stints = upsert_lineups
upsert_shot_events = upsert_shots

# Export clean __all__ of resolved callables
__all__ = []
for callable_name in _REQUIRED_CALLABLES:
    resolved = globals().get(callable_name)
    if resolved is not None:
        __all__.append(callable_name)

# Add aliases to exports if base callable exists
if upsert_pbp is not None:
    __all__.append("upsert_pbp_events")
if upsert_lineups is not None:
    __all__.append("upsert_lineup_stints")
if upsert_shots is not None:
    __all__.append("upsert_shot_events")


# Expose _MODULE_SCAN_ORDER for tests that check scan order
def get_module_scan_order():
    """Return the module scan order for testing."""
    return _MODULE_SCAN_ORDER
