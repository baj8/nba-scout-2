#!/usr/bin/env python3
"""Test backfill pipeline path resolution and imports."""

import sys
from pathlib import Path

# Add src to Python path for testing
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))


def test_backfill_imports():
    """Test that backfill pipeline imports work correctly."""
    print("🧪 Testing NBA Scraper backfill pipeline imports...")
    
    try:
        # Test core imports
        print("\n📦 Testing core imports...")
        import nba_scraper
        print("✅ nba_scraper imported successfully")
        
        from nba_scraper.config import get_settings
        print("✅ Config imported successfully")
        
        from nba_scraper.db import get_connection
        print("✅ Database imported successfully")
        
        from nba_scraper.pipelines.backfill import BackfillOrchestrator
        print("✅ BackfillOrchestrator imported successfully")
        
        # Test BackfillOrchestrator initialization 
        
        print("\n1️⃣ Testing BackfillOrchestrator initialization...")
        backfill_orchestrator = BackfillOrchestrator()
        print("✅ BackfillOrchestrator initialized successfully")
        
    except Exception as e:
        print(f"❌ BackfillOrchestrator initialization failed: {e}")
        return False
    
    # Test method availability
    print("\n2️⃣ Testing BackfillOrchestrator methods...")
    try:
        # Check that expected methods exist
        required_methods = [
            'backfill_date_range',
            'backfill_season', 
            'retry_quarantined_games'
        ]
        
        for method_name in required_methods:
            if hasattr(backfill_orchestrator, method_name):
                print(f"✅ Method '{method_name}' found")
            else:
                print(f"❌ Method '{method_name}' missing")
                return False
                
    except Exception as e:
        print(f"❌ Method check failed: {e}")
        return False
    
    print("\n🎉 All backfill pipeline tests passed!")
    return True


if __name__ == "__main__":
    success = test_backfill_imports()
    sys.exit(0 if success else 1)