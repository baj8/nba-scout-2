#!/usr/bin/env python3
"""
Quick test to verify NBA Stats shot chart API provides coordinates
This will help us confirm the API works and identify any issues
"""

import sys
import asyncio
from pathlib import Path
from pprint import pprint

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_shot_chart_api():
    """Test shot chart API to see if we can get coordinates."""
    print("🎯 Testing NBA Stats Shot Chart API")
    print("=" * 50)
    
    try:
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        
        client = NBAStatsClient()
        
        # Test with game IDs that we know exist in our database
        test_game_ids = ["0012400050", "0012400051", "0012400052"]  # Games from our analysis
        
        for game_id in test_game_ids:
            print(f"📡 Fetching shot chart for game: {game_id}")
            print("   (Testing with different team ID parameters)")
            
            # Try different approaches to fetch shot chart data
            success = False
            
            # Approach 1: Try with specific team ID (Lakers)
            try:
                print("   Trying with TeamID=1610612747 (Lakers)...")
                shot_data = await client.fetch_shotchart(
                    team_id="1610612747",  # Lakers team ID
                    game_id=game_id,
                    season="2024-25"
                )
                success = True
                print("   ✅ Lakers team ID worked!")
            except Exception as e:
                print(f"   ❌ Lakers team ID failed: {str(e)[:100]}...")
            
            # Approach 2: Try with different team ID (Celtics)  
            if not success:
                try:
                    print("   Trying with TeamID=1610612738 (Celtics)...")
                    shot_data = await client.fetch_shotchart(
                        team_id="1610612738",  # Celtics team ID
                        game_id=game_id,
                        season="2024-25"
                    )
                    success = True
                    print("   ✅ Celtics team ID worked!")
                except Exception as e:
                    print(f"   ❌ Celtics team ID failed: {str(e)[:100]}...")
            
            if success:
                print(f"✅ Shot chart API call successful for game {game_id}!")
                print(f"📊 Response structure:")
                
                if 'resultSets' in shot_data:
                    for i, result_set in enumerate(shot_data['resultSets']):
                        name = result_set.get('name', 'Unknown')
                        headers = result_set.get('headers', [])
                        rows = result_set.get('rowSet', [])
                        
                        print(f"  {i+1}. ResultSet: '{name}'")
                        print(f"     Headers: {len(headers)} columns")
                        print(f"     Rows: {len(rows)} shots")
                        
                        if name == 'Shot_Chart_Detail' and len(rows) > 0:
                            print(f"  🎯 Found Shot_Chart_Detail with {len(rows)} shots!")
                            print(f"     Key headers: {[h for h in headers if 'LOC' in h or 'SHOT' in h or 'EVENT' in h]}")
                            
                            # Show first shot as example
                            if len(rows) > 0:
                                first_shot = dict(zip(headers, rows[0]))
                                print(f"  📍 Sample shot data:")
                                for key, value in first_shot.items():
                                    if key in ['LOC_X', 'LOC_Y', 'SHOT_DISTANCE', 'SHOT_ZONE_BASIC', 'GAME_EVENT_ID', 'SHOT_MADE_FLAG']:
                                        print(f"     {key}: {value}")
                return True
            
        print("❌ All shot chart attempts failed")
        return False
        
    except Exception as e:
        print(f"❌ Shot chart API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_pbp_api():
    """Test PBP API to see event structure."""
    print(f"\n🎮 Testing NBA Stats Play-by-Play API")
    print("=" * 50)
    
    try:
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        
        client = NBAStatsClient()
        
        # Test with games that we know exist
        test_game_ids = ["0012400050", "0012400051"]
        
        for game_id in test_game_ids:
            print(f"📡 Fetching PBP for game: {game_id}")
            
            pbp_data = await client.fetch_pbp(game_id)
            
            print(f"✅ PBP API call successful for {game_id}!")
            
            if 'resultSets' in pbp_data:
                for result_set in pbp_data['resultSets']:
                    if result_set.get('name') == 'PlayByPlay':
                        headers = result_set.get('headers', [])
                        rows = result_set.get('rowSet', [])
                        
                        print(f"🎯 Found PlayByPlay with {len(rows)} events")
                        print(f"   Key headers: {[h for h in headers if 'EVENT' in h or 'PLAYER' in h]}")
                        
                        # Find shot events - fix the filtering
                        shot_events = []
                        for row in rows:
                            event_dict = dict(zip(headers, row))
                            event_type = event_dict.get('EVENTMSGTYPE')
                            # Convert to int for comparison
                            if isinstance(event_type, (int, str)):
                                try:
                                    event_type_int = int(event_type)
                                    if event_type_int in [1, 2]:  # Made/Missed shots
                                        shot_events.append(event_dict)
                                except (ValueError, TypeError):
                                    continue
                        
                        print(f"🏀 Found {len(shot_events)} shot events in PBP")
                        
                        if shot_events:
                            sample_shot = shot_events[0]
                            print(f"📍 Sample shot event:")
                            for key, value in sample_shot.items():
                                if key in ['EVENTNUM', 'EVENTMSGTYPE', 'PLAYER1_NAME', 'HOMEDESCRIPTION', 'VISITORDESCRIPTION']:
                                    print(f"     {key}: {value}")
                        
                        return True
        
        return False
        
    except Exception as e:
        print(f"❌ PBP API test failed: {e}")
        return False

async def main():
    """Test both APIs to verify data availability."""
    print("🔍 NBA Stats API Coordinate Data Test")
    print("=" * 60)
    
    shot_success = await test_shot_chart_api()
    pbp_success = await test_pbp_api()
    
    print(f"\n📋 Test Results:")
    print(f"   Shot Chart API: {'✅ SUCCESS' if shot_success else '❌ FAILED'}")
    print(f"   Play-by-Play API: {'✅ SUCCESS' if pbp_success else '❌ FAILED'}")
    
    if shot_success and pbp_success:
        print(f"\n🎉 SOLUTION IDENTIFIED:")
        print(f"   • NBA Stats APIs work correctly")
        print(f"   • Shot coordinates ARE available via shotchartdetail endpoint")
        print(f"   • Problem: Pipeline isn't calling shot chart API")
        print(f"   • Fix: Add shot chart integration to pipeline")
    else:
        print(f"\n⚠️  API Issues Detected - need to investigate further")

if __name__ == "__main__":
    asyncio.run(main())