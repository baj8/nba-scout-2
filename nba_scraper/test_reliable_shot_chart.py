#!/usr/bin/env python3
"""
Test the reliable nba_api shot chart implementation
This should solve our coordinate data issues
"""

import sys
import asyncio
from pathlib import Path
from pprint import pprint

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_reliable_shot_chart():
    """Test the new reliable shot chart implementation."""
    print("🎯 Testing Reliable NBA API Shot Chart Implementation")
    print("=" * 60)
    
    try:
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        
        client = NBAStatsClient()
        
        # Test with regular season games only (002 prefix for regular season)
        test_game_ids = [
            "0022400001",  # First regular season game 2024-25
            "0022400002",  # Second regular season game 2024-25
            "0022400010",  # Later regular season game 2024-25
            "0022400020",  # Even later regular season game 2024-25
        ]
        
        for game_id in test_game_ids:
            print(f"📡 Testing reliable shot chart for REGULAR SEASON game: {game_id}")
            print("   Using nba_api library instead of raw REST calls")
            
            try:
                # Use the new reliable method
                shot_data = await client.fetch_shotchart_reliable(game_id, "2024-25")
                
                if 'resultSets' in shot_data:
                    for result_set in shot_data['resultSets']:
                        if result_set.get('name') == 'Shot_Chart_Detail':
                            rows = result_set.get('rowSet', [])
                            if len(rows) > 0:
                                headers = result_set.get('headers', [])
                                print(f"  🎯 Found {len(rows)} shots for regular season game {game_id}!")
                                
                                # Show coordinate columns
                                coord_headers = [h for h in headers if any(x in h for x in ['LOC', 'SHOT', 'EVENT', 'DISTANCE', 'ZONE'])]
                                print(f"     Coordinate headers: {coord_headers}")
                                
                                # Show first few shots
                                print(f"  📍 Sample shot data:")
                                for j, row in enumerate(rows[:2]):
                                    shot_dict = dict(zip(headers, row))
                                    print(f"     Shot {j+1}:")
                                    for key in ['GAME_EVENT_ID', 'PLAYER_NAME', 'LOC_X', 'LOC_Y', 'SHOT_DISTANCE', 'SHOT_ZONE_BASIC', 'SHOT_MADE_FLAG']:
                                        if key in shot_dict:
                                            print(f"       {key}: {shot_dict[key]}")
                                    print()
                                
                                # Test extraction
                                from nba_scraper.extractors.nba_stats import extract_shot_chart_detail
                                source_url = f"https://stats.nba.com/stats/shotchartdetail?GameID={game_id}"
                                extracted_data = extract_shot_chart_detail(shot_data, game_id, source_url)
                                
                                print(f"✅ Extraction successful for regular season game {game_id}!")
                                print(f"📊 Extracted {len(extracted_data)} shot details")
                                
                                if extracted_data:
                                    sample_event_id = list(extracted_data.keys())[0]
                                    sample_shot = extracted_data[sample_event_id]
                                    print(f"📍 Sample extracted shot (Event {sample_event_id}):")
                                    for key, value in sample_shot.items():
                                        print(f"   {key}: {value}")
                                
                                return True  # Found working regular season game with shot data
                            else:
                                print(f"  ⚠️  Regular season game {game_id}: 0 shots (game might not exist yet)")
                
            except Exception as e:
                print(f"  ❌ Regular season game {game_id} failed: {str(e)[:100]}...")
                continue
        
        print("❌ No regular season games with shot data found")
        return False
        
    except Exception as e:
        print(f"❌ Reliable shot chart test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Test the reliable shot chart implementation."""
    print("🔍 Reliable NBA API Shot Chart Test")
    print("=" * 50)
    
    success = await test_reliable_shot_chart()
    
    print(f"\n📋 Test Results:")
    print(f"   Reliable Shot Chart: {'✅ SUCCESS' if success else '❌ FAILED'}")
    
    if success:
        print(f"\n🎉 SOLUTION CONFIRMED:")
        print(f"   • nba_api library solves parameter issues")
        print(f"   • Shot coordinates ARE available and working")
        print(f"   • Ready to integrate into pipeline for Tranche 2")
        print(f"   • This will fix the 0% coordinate coverage issue")
    else:
        print(f"\n⚠️  Still have issues to resolve")

if __name__ == "__main__":
    asyncio.run(main())