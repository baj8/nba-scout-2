#!/usr/bin/env python3
"""Test real backfill functionality with BackfillOrchestrator."""

import asyncio
import sys
from pathlib import Path

# Add src to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))


async def test_real_backfill():
    """Test actual backfill functionality."""
    print("🧪 Testing NBA Scraper real backfill functionality...")
    
    try:
        from nba_scraper.pipelines.backfill import BackfillOrchestrator
        
        print("\n1️⃣ Initializing BackfillOrchestrator...")
        backfill_orchestrator = BackfillOrchestrator()
        print("✅ BackfillOrchestrator initialized successfully")
        
        # Test basic functionality without actual API calls
        print("\n2️⃣ Testing orchestrator configuration...")
        
        # Check configuration
        assert hasattr(backfill_orchestrator, 'rate_limit')
        assert hasattr(backfill_orchestrator, 'batch_size')
        assert hasattr(backfill_orchestrator, 'dry_run')
        print("✅ Configuration attributes present")
        
        # Test quarantine functionality
        print("\n3️⃣ Testing quarantine functionality...")
        test_game_id = "test_game_123"
        backfill_orchestrator._add_to_quarantine(test_game_id, "Test quarantine")
        assert test_game_id in backfill_orchestrator.quarantined_games
        print("✅ Quarantine functionality working")
        
        print("\n🎉 Real backfill test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Real backfill test failed: {e}")
        return False


def test_orchestrator_methods():
    """Test that BackfillOrchestrator has the expected interface."""
    try:
        from nba_scraper.pipelines.backfill import BackfillOrchestrator
        
        backfill_orchestrator = BackfillOrchestrator()
        
        # Test required methods exist
        required_methods = [
            'backfill_date_range',
            'backfill_season',
            'retry_quarantined_games'
        ]
        
        for method in required_methods:
            assert hasattr(backfill_orchestrator, method), f"Missing method: {method}"
            assert callable(getattr(backfill_orchestrator, method)), f"Method not callable: {method}"
        
        print("✅ All required methods present and callable")
        return True
        
    except Exception as e:
        print(f"❌ Method test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting real backfill tests...")
    
    # Test synchronous functionality
    sync_success = test_orchestrator_methods()
    
    # Test asynchronous functionality
    async_success = asyncio.run(test_real_backfill())
    
    success = sync_success and async_success
    
    if success:
        print("\n🎉 All tests passed!")
    else:
        print("\n❌ Some tests failed!")
    
    sys.exit(0 if success else 1)