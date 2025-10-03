#!/usr/bin/env python3
"""Targeted test to find exactly where the int/str comparison error occurs in the pipeline."""

import sys
import traceback
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_game_pipeline_components():
    """Test individual components of the game pipeline to isolate the error."""
    print("🔍 Testing game pipeline components to find int/str comparison error...\n")
    
    # Test 1: Import all pipeline components
    print("1️⃣ Testing imports...")
    try:
        from nba_scraper.pipelines.game_pipeline import GamePipeline
        from nba_scraper.extractors import nba_stats, bref, gamebooks
        from nba_scraper.transformers.games import GameTransformer
        from nba_scraper.transformers.refs import RefTransformer
        from nba_scraper.transformers.lineups import LineupTransformer
        from nba_scraper.io_clients import BRefClient, NBAStatsClient, GamebooksClient
        from nba_scraper.rate_limit import RateLimiter
        print("✅ All imports successful")
    except Exception as e:
        print(f"❌ Import failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN IMPORTS!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    # Test 2: Create transformer instances (most likely source of enum issues)
    print("\n2️⃣ Testing transformer initialization...")
    try:
        game_transformer = GameTransformer(source='nba_stats')
        ref_transformer = RefTransformer(source='nba_stats')
        lineup_transformer = LineupTransformer(source='nba_stats')
        print("✅ All transformers initialized successfully")
    except Exception as e:
        print(f"❌ Transformer initialization failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN TRANSFORMER INITIALIZATION!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    # Test 3: Test extractor functions with sample data
    print("\n3️⃣ Testing NBA Stats extractor functions...")
    try:
        # Sample NBA Stats game data that might cause the error
        sample_scoreboard_data = {
            'resultSets': [{
                'name': 'GameHeader',
                'headers': ['GAME_ID', 'GAME_DATE_EST', 'GAME_STATUS_ID', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID', 'SEASON'],
                'rowSet': [['0012400050', '2024-10-15', 2, 1610612747, 1610612738, '2024-25']]
            }]
        }
        
        games = nba_stats.extract_games_from_scoreboard(sample_scoreboard_data, 'test://url')
        print(f"   ✅ NBA Stats scoreboard extraction: {len(games)} game(s)")
        
    except Exception as e:
        print(f"   ❌ NBA Stats extraction failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("   🎯 FOUND THE ERROR IN NBA STATS EXTRACTION!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    # Test 4: Test PBP extraction (most complex with lots of enums)
    print("\n4️⃣ Testing PBP extraction...")
    try:
        sample_pbp_data = {
            'resultSets': [{
                'name': 'PlayByPlay',
                'headers': ['EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 'PCTIMESTRING', 'SCORE'],
                'rowSet': [[1, 12, 0, 1, '12:00', None], [2, 1, 1, 1, '11:45', '2 - 0']]
            }]
        }
        
        events = nba_stats.extract_pbp_from_response(sample_pbp_data, '0012400050', 'test://url')
        print(f"   ✅ NBA Stats PBP extraction: {len(events)} event(s)")
        
    except Exception as e:
        print(f"   ❌ PBP extraction failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("   🎯 FOUND THE ERROR IN PBP EXTRACTION!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    # Test 5: Test client initialization
    print("\n5️⃣ Testing client initialization...")
    try:
        bref_client = BRefClient()
        nba_stats_client = NBAStatsClient()  
        gamebooks_client = GamebooksClient()
        rate_limiter = RateLimiter()  # No arguments needed
        print("✅ All clients initialized successfully")
    except Exception as e:
        print(f"❌ Client initialization failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN CLIENT INITIALIZATION!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    # Test 6: Test GamePipeline creation
    print("\n6️⃣ Testing GamePipeline initialization...")
    try:
        pipeline = GamePipeline(
            bref_client=bref_client,
            nba_stats_client=nba_stats_client,
            gamebooks_client=gamebooks_client,
            rate_limiter=rate_limiter
        )
        print("✅ GamePipeline initialized successfully")
    except Exception as e:
        print(f"❌ Pipeline initialization failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN PIPELINE INITIALIZATION!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    # Test 7: Test a simple pipeline operation
    print("\n7️⃣ Testing pipeline process method...")
    try:
        # This is the method that gets called in the backfill and fails
        result = await pipeline.process_game('0012400050', sources=['nba_stats'], dry_run=True)
        print(f"✅ Pipeline process completed: {result.success}")
    except Exception as e:
        print(f"❌ Pipeline process failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN PIPELINE PROCESS METHOD!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

    print("\n🎉 All pipeline component tests passed!")
    print("🤔 The error might be in specific data processing or database operations...")
    return True

async def main():
    """Run the targeted pipeline tests."""
    try:
        success = await test_game_pipeline_components()
        return success
    except Exception as e:
        print(f"💥 Unexpected error in main: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND THE ERROR IN MAIN EXECUTION!")
            traceback.print_exc()
            return False
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    print(f"\n{'🎯 SUCCESS' if success else '💥 FAILED'}: Pipeline component test {'passed' if success else 'found the error'}")
    sys.exit(0 if success else 1)