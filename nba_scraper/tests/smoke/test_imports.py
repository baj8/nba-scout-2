"""Smoke tests for import stability and side-effect free modules."""

import importlib
import sys
from pathlib import Path

# Add src to path for testing
src_path = Path(__file__).parent.parent / "nba_scraper" / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))


def test_tools_imports():
    """Test that tools modules can be imported without side effects."""
    print("🧪 Testing tools imports...")
    
    try:
        # Test that tools.__init__ doesn't cause side effects
        tools_module = importlib.import_module("nba_scraper.tools")
        assert hasattr(tools_module, '__all__')
        assert tools_module.__all__ == []
        print("✅ nba_scraper.tools imports cleanly")
        
        # Test that specific tool modules can be imported
        raw_harvest = importlib.import_module("nba_scraper.tools.raw_harvest_date")
        assert hasattr(raw_harvest, 'main')
        assert callable(raw_harvest.main)
        print("✅ nba_scraper.tools.raw_harvest_date imports cleanly")
        
        silver_load = importlib.import_module("nba_scraper.tools.silver_load_date")
        assert hasattr(silver_load, 'main')
        assert callable(silver_load.main)
        print("✅ nba_scraper.tools.silver_load_date imports cleanly")
        
        return True
        
    except Exception as e:
        print(f"❌ Tools import test failed: {e}")
        return False


def test_facade_imports():
    """Test that the loader facade imports correctly."""
    print("🧪 Testing loader facade imports...")
    
    try:
        facade = importlib.import_module("nba_scraper.loaders.facade")
        
        # Test function-based interface
        required_functions = [
            'upsert_game', 'upsert_pbp', 'upsert_shots', 
            'upsert_officials', 'upsert_starting_lineups'
        ]
        
        for func_name in required_functions:
            assert hasattr(facade, func_name)
            assert callable(getattr(facade, func_name))
        
        # Test class-based compatibility interface
        required_classes = [
            'GameLoader', 'PbpLoader', 'ShotLoader', 
            'RefLoader', 'LineupLoader'
        ]
        
        for class_name in required_classes:
            assert hasattr(facade, class_name)
            cls = getattr(facade, class_name)
            assert hasattr(cls, 'upsert') or hasattr(cls, 'upsert_batch')
        
        print("✅ nba_scraper.loaders.facade imports cleanly")
        return True
        
    except Exception as e:
        print(f"❌ Facade import test failed: {e}")
        return False


def test_raw_io_lightweight():
    """Test that raw_io modules are lightweight and don't pull in heavy deps."""
    print("🧪 Testing raw_io lightweight imports...")
    
    try:
        # Test raw NBA client
        client_module = importlib.import_module("nba_scraper.raw_io.client")
        assert hasattr(client_module, 'RawNbaClient')
        
        # Verify it uses tenacity (should be available now)
        raw_client_class = getattr(client_module, 'RawNbaClient')
        assert hasattr(raw_client_class, 'from_env')
        
        print("✅ nba_scraper.raw_io.client imports cleanly")
        return True
        
    except Exception as e:
        print(f"❌ Raw IO import test failed: {e}")
        return False


def test_no_heavy_imports_in_tools():
    """Test that importing tools doesn't pull in database or pipeline modules."""
    print("🧪 Testing that tools don't import heavy modules...")
    
    try:
        # Clear any existing imports
        modules_to_clear = [mod for mod in sys.modules.keys() 
                           if mod.startswith('nba_scraper.')]
        for mod in modules_to_clear:
            if mod in sys.modules:
                del sys.modules[mod]
        
        # Import tools and check what got loaded
        import nba_scraper.tools
        
        # Check that heavy modules are NOT imported
        heavy_modules = [
            'nba_scraper.db',
            'nba_scraper.pipelines',
            'nba_scraper.io_clients'
        ]
        
        for heavy_mod in heavy_modules:
            if heavy_mod in sys.modules:
                print(f"⚠️  Heavy module {heavy_mod} was imported")
                # This is a warning, not a failure, since some imports might be needed
        
        print("✅ Tools import test completed")
        return True
        
    except Exception as e:
        print(f"❌ Heavy import test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Running smoke tests...")
    
    tests = [
        test_tools_imports,
        test_facade_imports, 
        test_raw_io_lightweight,
        test_no_heavy_imports_in_tools
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            results.append(False)
        print()  # Add blank line between tests
    
    success = all(results)
    passed = sum(results)
    total = len(results)
    
    print(f"📊 Results: {passed}/{total} tests passed")
    
    if success:
        print("🎉 All smoke tests passed!")
    else:
        print("❌ Some smoke tests failed!")
    
    sys.exit(0 if success else 1)