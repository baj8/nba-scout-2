#!/usr/bin/env python3
"""Test the actual backfill pipeline execution path to find the int/str comparison error."""

import sys
import traceback
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_actual_backfill_path():
    """Test the actual backfill execution path that's failing."""
    print("🔍 Testing actual backfill execution path...\n")
    
    try:
        # Import the actual backfill components
        from nba_scraper.pipelines.backfill import BackfillPipeline
        from nba_scraper.pipelines.season_pipeline import SeasonPipeline
        from nba_scraper.pipelines.game_pipeline import GamePipeline
        from nba_scraper.io_clients import BRefClient, NBAStatsClient, GamebooksClient
        from nba_scraper.rate_limit import RateLimiter
        print("✅ Backfill imports successful")
    except Exception as e:
        print(f"❌ Backfill import failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN BACKFILL IMPORTS!")
            traceback.print_exc()
            return False
        return False

    try:
        # Test BackfillPipeline initialization 
        print("\n1️⃣ Testing BackfillPipeline initialization...")
        backfill_pipeline = BackfillPipeline()
        print("✅ BackfillPipeline initialized successfully")
    except Exception as e:
        print(f"❌ BackfillPipeline initialization failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN BACKFILL PIPELINE INITIALIZATION!")
            traceback.print_exc()
            return False
        return False

    try:
        # Test SeasonPipeline initialization (this is what processes individual games)
        print("\n2️⃣ Testing SeasonPipeline initialization...")
        
        # Create the required dependencies
        bref_client = BRefClient()
        nba_stats_client = NBAStatsClient()
        gamebooks_client = GamebooksClient()
        rate_limiter = RateLimiter()
        
        game_pipeline = GamePipeline(
            bref_client=bref_client,
            nba_stats_client=nba_stats_client,
            gamebooks_client=gamebooks_client,
            rate_limiter=rate_limiter
        )
        
        # Use correct constructor signature
        season_pipeline = SeasonPipeline(
            game_pipeline=game_pipeline,
            rate_limiter=rate_limiter
        )
        print("✅ SeasonPipeline initialized successfully")
    except Exception as e:
        print(f"❌ SeasonPipeline initialization failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN SEASON PIPELINE INITIALIZATION!")
            traceback.print_exc()
            return False
        return False

    try:
        # This is the exact method that's called in the backfill and failing
        print("\n3️⃣ Testing season pipeline processing...")
        result = await season_pipeline.process_season('2024-25', sources=['nba_stats'])
        print(f"✅ Season pipeline completed: processed {result.games_processed} games")
    except Exception as e:
        print(f"❌ Season pipeline processing failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN SEASON PIPELINE PROCESSING!")
            print("This is likely where the backfill is failing!")
            traceback.print_exc()
            return False
        return False

    print("\n🎉 Backfill path test completed successfully!")
    return True

async def test_individual_game_processing():
    """Test processing a single game through the full pipeline stack."""
    print("\n🔍 Testing individual game processing through full stack...\n")
    
    try:
        from nba_scraper.pipelines.game_pipeline import GamePipeline
        from nba_scraper.io_clients import BRefClient, NBAStatsClient, GamebooksClient
        from nba_scraper.rate_limit import RateLimiter
        
        # Create the full pipeline as used in production
        bref_client = BRefClient()
        nba_stats_client = NBAStatsClient()
        gamebooks_client = GamebooksClient()
        rate_limiter = RateLimiter()
        
        pipeline = GamePipeline(
            bref_client=bref_client,
            nba_stats_client=nba_stats_client,
            gamebooks_client=gamebooks_client,
            rate_limiter=rate_limiter
        )
        
        print("✅ Full GamePipeline stack created successfully")
        
        # Test with the same game ID from the failing backfill
        print("📋 Testing with game ID 0012400050 (from failing backfill)...")
        
        # Test each source that was failing in the backfill
        for source in ['nba_stats', 'bref', 'gamebooks']:
            try:
                print(f"   Testing {source}...")
                result = await pipeline.process_game('0012400050', sources=[source], dry_run=False)
                print(f"   ✅ {source}: {result.success} (processed: {result.sources_processed})")
            except Exception as e:
                print(f"   ❌ {source} failed: {e}")
                if "'<' not supported between instances" in str(e):
                    print(f"   🎯 FOUND THE ERROR IN {source.upper()} PROCESSING!")
                    traceback.print_exc()
                    return False
        
        print("🎉 Individual game processing completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Individual game processing setup failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN GAME PROCESSING SETUP!")
            traceback.print_exc()
            return False
        return False

async def main():
    """Run both backfill path tests."""
    print("🎯 Testing the exact backfill execution paths that are failing...\n")
    
    # Test 1: Backfill pipeline path
    success1 = await test_actual_backfill_path()
    
    # Test 2: Individual game processing  
    success2 = await test_individual_game_processing()
    
    return success1 and success2

if __name__ == "__main__":
    success = asyncio.run(main())
    if success:
        print("\n🤔 All backfill path tests passed - the error might be in real API data or database operations")
    else:
        print("\n🎯 FOUND THE SOURCE OF THE INT/STR COMPARISON ERROR!")
    sys.exit(0 if success else 1)