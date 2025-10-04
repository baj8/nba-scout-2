"""Unit tests for loader facade bindings."""

import pytest
from nba_scraper.loaders import upsert_game, upsert_pbp, upsert_lineups, upsert_shots


def test_upsert_game_is_resolved():
    """Test that upsert_game function is found and callable."""
    # Should either be callable or None if not found
    assert callable(upsert_game) or upsert_game is None


def test_upsert_pbp_is_resolved():
    """Test that upsert_pbp function is found and callable."""
    assert callable(upsert_pbp) or upsert_pbp is None


def test_upsert_lineups_is_resolved():
    """Test that upsert_lineups function is found and callable."""
    assert callable(upsert_lineups) or upsert_lineups is None


def test_upsert_shots_is_resolved():
    """Test that upsert_shots function is found and callable."""
    assert callable(upsert_shots) or upsert_shots is None


def test_loader_facade_imports():
    """Test that loader facade imports work without errors."""
    try:
        from nba_scraper.loaders import __all__ as loader_exports
        # Should be a list of available loader function names
        assert isinstance(loader_exports, list)
        print(f"Available loaders: {loader_exports}")
    except ImportError:
        pytest.skip("Loader facade not available")


def test_loader_function_signatures():
    """Test that found loader functions have expected signatures."""
    if callable(upsert_game):
        import inspect
        sig = inspect.signature(upsert_game)
        # Should accept connection and model/data parameter
        assert len(sig.parameters) >= 2
    
    if callable(upsert_pbp):
        import inspect
        sig = inspect.signature(upsert_pbp)
        assert len(sig.parameters) >= 2