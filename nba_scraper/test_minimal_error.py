#!/usr/bin/env python3
"""Minimal test to isolate the int/str comparison error."""

import sys
import traceback
sys.path.insert(0, 'src')

from nba_scraper.models.game_rows import GameRow

def test_game_creation():
    """Test creating a GameRow with real NBA API data that might cause the error."""
    
    # Simulate NBA Stats API data with integer status codes (like the real API sends)
    nba_stats_data = {
        'GAME_ID': '0012400050',
        'GAME_DATE_EST': '2024-10-15',
        'HOME_TEAM_ABBREVIATION': 'LAL',
        'VISITOR_TEAM_ABBREVIATION': 'GSW',
        'HOME_TEAM_ID': 1610612747,
        'VISITOR_TEAM_ID': 1610612744,
        'GAME_STATUS_TEXT': 3,  # INTEGER STATUS - This might be the problem!
        'PERIOD': 4,
        'ATTENDANCE': 18997
    }
    
    try:
        print("Testing GameRow creation with integer status...")
        
        # Try to create a GameRow using the from_nba_stats method
        game_row = GameRow.from_nba_stats(nba_stats_data, "http://test.com")
        
        print(f"✅ SUCCESS: Created GameRow with status: {game_row.status}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"ERROR TYPE: {type(e)}")
        print("\n📍 FULL TRACEBACK:")
        traceback.print_exc()
        
        # Check if this is the int/str comparison error we're looking for
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE ERROR! This is the int/str comparison issue!")
        
        return False
    
    return True

if __name__ == "__main__":
    success = test_game_creation()
    if success:
        print("\n🎉 Test passed - no int/str comparison error found here")
    else:
        print("\n💥 Test failed - this might be where the error occurs")
