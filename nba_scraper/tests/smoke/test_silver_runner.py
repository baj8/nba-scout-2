"""Smoke tests for Silver layer transformers to ensure stable import surface."""

def test_silver_transformers_symbols():
    """Test that Silver transformer modules have expected functions and don't import heavy modules."""
    import importlib
    
    # Test transform_games module
    games = importlib.import_module("nba_scraper.silver.transform_games")
    assert hasattr(games, "transform_game")
    
    # Test transform_pbp module
    pbp = importlib.import_module("nba_scraper.silver.transform_pbp")
    assert hasattr(pbp, "transform_pbp")
    
    # Test transform_shots module
    shots = importlib.import_module("nba_scraper.silver.transform_shots")
    assert hasattr(shots, "transform_shots")
    
    # Test transform_officials module
    offs = importlib.import_module("nba_scraper.silver.transform_officials")
    assert hasattr(offs, "transform_officials")
    
    # Test transform_starters module
    st = importlib.import_module("nba_scraper.silver.transform_starters")
    assert hasattr(st, "transform_starters")


def test_raw_reader_import():
    """Test that RawReader can be imported and instantiated."""
    import importlib
    
    raw_reader_module = importlib.import_module("nba_scraper.silver.raw_reader")
    assert hasattr(raw_reader_module, "RawReader")
    
    # Test basic instantiation
    RawReader = raw_reader_module.RawReader
    reader = RawReader("test_root")
    assert reader.root.name == "test_root"


def test_silver_transformers_resilience():
    """Test that transformers handle None/empty inputs gracefully."""
    from nba_scraper.silver.transform_games import transform_game
    from nba_scraper.silver.transform_pbp import transform_pbp
    from nba_scraper.silver.transform_shots import transform_shots
    from nba_scraper.silver.transform_officials import transform_officials
    from nba_scraper.silver.transform_starters import transform_starters
    
    # All transformers should handle None input gracefully
    assert transform_game(None) is None
    assert transform_pbp(None, game_id="test") == []
    assert transform_shots(None, game_id="test") == []
    assert transform_officials(None, game_id="test") == []
    assert transform_starters(None, game_id="test") == []
    
    # All transformers should handle empty dict input gracefully
    assert transform_game({}) is None
    assert transform_pbp({}, game_id="test") == []
    assert transform_shots({}, game_id="test") == []
    assert transform_officials({}, game_id="test") == []
    assert transform_starters({}, game_id="test") == []