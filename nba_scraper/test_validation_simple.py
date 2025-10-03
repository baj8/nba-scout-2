#!/usr/bin/env python3
"""Simple test to trigger the int/str comparison error directly."""

import sys
import traceback
sys.path.insert(0, 'src')

def test_enum_validation_direct():
    """Test direct enum validation that might cause int/str comparison."""
    
    try:
        from nba_scraper.models.enums import GameStatus, EventType
        
        print("Testing direct enum validation...")
        
        # Test GameStatus with integer (like NBA API sends)
        print("Testing GameStatus with integer 3...")
        status = GameStatus(3)  # This might cause the error!
        print(f"✅ GameStatus(3) = {status}")
        
        # Test EventType with integer (like NBA API sends)  
        print("Testing EventType with integer 1...")
        event = EventType(1)  # This might cause the error!
        print(f"✅ EventType(1) = {event}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        
        if "'<' not supported between instances" in str(e):
            print(f"🎯 FOUND IT! This is the int/str comparison error!")
            traceback.print_exc()
            return False
        else:
            print(f"Different error: {type(e).__name__}")
    
    return True

def test_pydantic_validation():
    """Test Pydantic model validation that might cause the error."""
    
    try:
        from nba_scraper.models.game_rows import GameRow
        from datetime import datetime
        
        print("\nTesting Pydantic GameRow validation...")
        
        # Create GameRow with integer status (like NBA API)
        game_data = {
            'game_id': '0012400050',
            'season': '2024-25',
            'game_date_utc': datetime.now(),
            'game_date_local': datetime.now().date(),
            'arena_tz': 'US/Eastern',
            'home_team_tricode': 'LAL',
            'away_team_tricode': 'GSW',
            'status': 3,  # INTEGER STATUS - might cause error!
            'period': 4,
            'source': 'nba_stats',
            'source_url': 'http://test.com'
        }
        
        game = GameRow(**game_data)
        print(f"✅ GameRow created with status: {game.status}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        
        if "'<' not supported between instances" in str(e):
            print(f"🎯 FOUND IT! Pydantic validation causes int/str comparison error!")
            traceback.print_exc()
            return False
        else:
            print(f"Different error: {type(e).__name__}")
    
    return True

if __name__ == "__main__":
    print("🎯 Testing direct validation to trigger int/str comparison error...")
    print("=" * 50)
    
    enum_success = test_enum_validation_direct()
    pydantic_success = test_pydantic_validation()
    
    print("=" * 50)
    if not enum_success or not pydantic_success:
        print("💥 FOUND THE SOURCE OF THE ERROR!")
    else:
        print("🤔 Still haven't triggered the error...")
