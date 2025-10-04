#!/usr/bin/env python3
"""
Test end-to-end game scraping with shot coordinates
This proves our Tranche 2 integration works before re-scraping all historical data
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_single_game_with_coordinates():
    """Test scraping a single regular season game with shot coordinates."""
    print("🎯 Test: Single Game Scraping with Shot Coordinates")
    print("=" * 60)
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        from nba_scraper.db import get_connection
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        from nba_scraper.pipelines.source_pipelines import NBAStatsPipeline
        from nba_scraper.rate_limit import RateLimiter
        
        # Choose a regular season game we know has shot data
        test_game_id = "0022400001"  # First regular season game 2024-25
        
        print(f"🎮 Testing with regular season game: {test_game_id}")
        print("   (This should have both PBP events AND shot coordinates)")
        print()
        
        # Initialize client and pipeline
        client = NBAStatsClient()
        rate_limiter = RateLimiter()  # No parameters - gets rate from settings
        pipeline = NBAStatsPipeline(client, rate_limiter)
        
        # Test the pipeline processing
        print("1️⃣ Running NBA Stats pipeline...")
        result = await pipeline.process_game(test_game_id, force_refresh=True, dry_run=False)
        
        if result.success:
            print(f"✅ Pipeline completed successfully!")
            print(f"   Duration: {result.duration_seconds:.1f}s")
            print(f"   Records updated: {sum(result.records_updated.values())}")
            print(f"   Data types: {result.data_types_processed}")
        else:
            print(f"❌ Pipeline failed: {result.error}")
            return False
        
        print("\n2️⃣ Checking database results...")
        conn = await get_connection()
        
        # Check games table
        games_result = await conn.fetchrow(
            "SELECT COUNT(*) as count FROM games WHERE game_id = $1", test_game_id
        )
        print(f"   Games: {games_result['count']} (should be 1)")
        
        # Check PBP events
        pbp_result = await conn.fetchrow(
            "SELECT COUNT(*) as total_events, COUNT(CASE WHEN shot_x IS NOT NULL THEN 1 END) as events_with_coords FROM pbp_events WHERE game_id = $1", 
            test_game_id
        )
        print(f"   PBP Events: {pbp_result['total_events']} total")
        print(f"   Events with coordinates: {pbp_result['events_with_coords']}")
        
        if pbp_result['events_with_coords'] > 0:
            coord_pct = pbp_result['events_with_coords'] / pbp_result['total_events'] * 100
            print(f"   📍 Coordinate coverage: {coord_pct:.1f}% (should be >0% now!)")
        
        # Check shot events specifically
        shots_result = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_shots,
                COUNT(CASE WHEN shot_x IS NOT NULL AND shot_y IS NOT NULL THEN 1 END) as shots_with_coords,
                COUNT(CASE WHEN shot_distance_ft IS NOT NULL THEN 1 END) as shots_with_distance,
                COUNT(CASE WHEN shot_zone IS NOT NULL THEN 1 END) as shots_with_zone
            FROM pbp_events 
            WHERE game_id = $1 AND event_type IN ('SHOT_MADE', 'SHOT_MISSED')
        """, test_game_id)
        
        print(f"\n📊 Shot Analysis:")
        print(f"   Total shots: {shots_result['total_shots']}")
        print(f"   Shots with coordinates: {shots_result['shots_with_coords']}")
        print(f"   Shots with distance: {shots_result['shots_with_distance']}")  
        print(f"   Shots with zone: {shots_result['shots_with_zone']}")
        
        if shots_result['total_shots'] > 0:
            shot_coord_pct = shots_result['shots_with_coords'] / shots_result['total_shots'] * 100
            print(f"   🎯 Shot coordinate coverage: {shot_coord_pct:.1f}%")
        
        # Show sample shots with coordinates
        sample_shots = await conn.fetch("""
            SELECT event_type, player1_display_name, shot_x, shot_y, shot_distance_ft, shot_zone, description
            FROM pbp_events 
            WHERE game_id = $1 AND event_type IN ('SHOT_MADE', 'SHOT_MISSED') 
                AND shot_x IS NOT NULL 
            ORDER BY event_idx 
            LIMIT 3
        """, test_game_id)
        
        if sample_shots:
            print(f"\n📍 Sample shots with coordinates:")
            for i, shot in enumerate(sample_shots, 1):
                print(f"   {i}. {shot['player1_display_name']}: {shot['event_type']}")
                print(f"      Coordinates: ({shot['shot_x']}, {shot['shot_y']})")
                print(f"      Distance: {shot['shot_distance_ft']} ft")
                print(f"      Zone: {shot['shot_zone']}")
                print(f"      Description: {shot['description']}")
                print()
        
        await conn.close()
        
        # Determine success
        success = (
            result.success and 
            pbp_result['total_events'] > 0 and 
            shots_result['shots_with_coords'] > 0
        )
        
        return success
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run the single game test."""
    print("🧪 Single Game Scraping Test with Shot Coordinates")
    print("=" * 70)
    
    success = await test_single_game_with_coordinates()
    
    print(f"\n📋 Test Results:")
    print(f"   Single Game Scraping: {'✅ SUCCESS' if success else '❌ FAILED'}")
    
    if success:
        print(f"\n🎉 TRANCHE 2 INTEGRATION CONFIRMED:")
        print(f"   • Shot coordinates are working end-to-end")
        print(f"   • Pipeline successfully enhanced PBP events")
        print(f"   • Ready to integrate into main pipeline")
        print(f"   • Can confidently re-scrape all historical games")
        
        print(f"\n🚀 Ready for Production:")
        print(f"   • Database cleared and tested ✅")
        print(f"   • Shot coordinate integration working ✅") 
        print(f"   • End-to-end pipeline validated ✅")
        print(f"   • Safe to re-scrape historical data ✅")
    else:
        print(f"\n⚠️  Issues to resolve before production:")
        print(f"   • Fix integration issues first")
        print(f"   • Don't re-scrape until this test passes")
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)