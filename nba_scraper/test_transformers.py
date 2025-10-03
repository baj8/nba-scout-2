#!/usr/bin/env python3
"""Test transformers directly to find the int/str comparison error."""

import sys
import traceback
sys.path.insert(0, 'src')

def test_game_transformer():
    """Test GameTransformer with NBA Stats data."""
    
    try:
        from nba_scraper.transformers.games import GameTransformer
        
        print("Testing GameTransformer with NBA Stats data...")
        
        transformer = GameTransformer(source='nba_stats')
        
        # Create NBA Stats data with integer status (like the real API sends)
        nba_stats_data = {
            'resultSets': [{
                'name': 'GameHeader',
                'headers': ['GAME_ID', 'GAME_STATUS_TEXT', 'HOME_TEAM_ABBREVIATION', 'VISITOR_TEAM_ABBREVIATION'],
                'rowSet': [['0012400050', 3, 'LAL', 'GSW']]  # INTEGER status = 3
            }]
        }
        
        games = transformer.transform(nba_stats_data, season=2024)
        print(f"✅ SUCCESS: GameTransformer returned {len(games)} games")
        
        if games:
            print(f"   Game status: {games[0].status} (type: {type(games[0].status)})")
        
    except Exception as e:
        print(f"❌ ERROR in GameTransformer: {e}")
        
        if "'<' not supported between instances" in str(e):
            print(f"🎯 FOUND THE ERROR in GameTransformer!")
            traceback.print_exc()
            return False
        else:
            print(f"Different error: {type(e).__name__}")
    
    return True

def test_ref_transformer():
    """Test RefTransformer which might also cause the error."""
    
    try:
        from nba_scraper.transformers.refs import RefTransformer
        
        print("Testing RefTransformer...")
        
        transformer = RefTransformer(source='nba_stats')
        
        # Test data that might cause enum comparison issues
        ref_data = {
            'officials': [{
                'FIRST_NAME': 'John',
                'LAST_NAME': 'Doe', 
                'JERSEY_NUM': 1,  # This might be an integer that causes comparison issues
            }]
        }
        
        refs = transformer.transform(ref_data, game_id='0012400050')
        print(f"✅ SUCCESS: RefTransformer returned {len(refs)} refs")
        
    except Exception as e:
        print(f"❌ ERROR in RefTransformer: {e}")
        
        if "'<' not supported between instances" in str(e):
            print(f"�� FOUND THE ERROR in RefTransformer!")
            traceback.print_exc()
            return False
        else:
            print(f"Different error: {type(e).__name__}")
    
    return True

if __name__ == "__main__":
    print("Testing transformer functions to find int/str comparison error...\n")
    
    game_success = test_game_transformer()
    print()
    ref_success = test_ref_transformer() 
    
    print(f"\n🎯 Results:")
    print(f"   GameTransformer: {'✅ PASS' if game_success else '❌ FAIL'}")
    print(f"   RefTransformer: {'✅ PASS' if ref_success else '❌ FAIL'}")
    
    if not game_success or not ref_success:
        print(f"\n💥 Found the source of the int/str comparison error!")
    else:
        print(f"\n🤔 Still searching for the exact source...")
