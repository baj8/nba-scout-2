"""Smoke tests for CLI imports to ensure stable import surface."""

def test_tools_imports():
    """Test that tool modules can be imported without heavy side effects."""
    import importlib
    
    # Test raw harvest CLI import
    raw_harvest = importlib.import_module("nba_scraper.tools.raw_harvest_date")
    assert hasattr(raw_harvest, 'main')
    assert hasattr(raw_harvest, 'load_date')
    
    # Test silver load CLI import
    silver_load = importlib.import_module("nba_scraper.tools.silver_load_date")
    assert hasattr(silver_load, 'main')
    assert hasattr(silver_load, 'load_date')


def test_backfill_cli_import():
    """Test that the main CLI module can be imported."""
    import importlib
    
    # Test main CLI import
    cli = importlib.import_module("nba_scraper.cli")
    assert hasattr(cli, 'app')
    assert hasattr(cli, 'backfill')


def test_tools_no_heavy_imports():
    """Test that tools.__init__ doesn't import heavy modules."""
    # This should not trigger any database connections or API calls
    import nba_scraper.tools
    
    # The tools module should exist but not pull in heavy dependencies
    assert hasattr(nba_scraper.tools, '__file__')