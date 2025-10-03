#!/usr/bin/env python3
"""
Test if the GameTransformer fix resolves the int/str comparison error.
This test doesn't require dotenv or external dependencies.
"""

import sys
import traceback
from datetime import datetime

def test_game_transformer_fix():
    """Test GameTransformer with NBA Stats data that previously caused int/str comparison errors."""
    
    try:
        # Import required modules
        sys.path.insert(0, 'src')
        from nba_scraper.transformers.games import GameTransformer
        
        print("✅ Successfully imported GameTransformer")
        
        # Create a transformer instance
        transformer = GameTransformer(source='nba_stats')
        print("✅ Successfully created GameTransformer instance")
        
        # Test data that mimics NBA Stats API response with integer status codes
        # This is the exact data structure that was causing the int/str comparison error
        test_data = {
            'resultSets': [{
                'name': 'GameHeader',
                'headers': [
                    'GAME_ID', 'GAME_STATUS_TEXT', 'HOME_TEAM_ABBREVIATION', 
                    'VISITOR_TEAM_ABBREVIATION', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID',
                    'GAME_DATE_EST', 'PERIOD', 'ATTENDANCE'
                ],
                'rowSet': [
                    [
                        '0012400050',  # GAME_ID
                        3,             # INTEGER STATUS - this was causing the error!
                        'LAL',         # HOME_TEAM_ABBREVIATION  
                        'GSW',         # VISITOR_TEAM_ABBREVIATION
                        1610612747,    # HOME_TEAM_ID (integer)
                        1610612744,    # VISITOR_TEAM_ID (integer)  
                        '2024-10-15',  # GAME_DATE_EST
                        4,             # PERIOD (integer)
                        18997          # ATTENDANCE (integer)
                    ]
                ]
            }]
        }
        
        print("🔍 Testing transformation with integer status codes...")
        
        # This is where the int/str comparison error was occurring
        games = transformer.transform(test_data, season='2024-25')
        
        print(f"✅ SUCCESS! Transformation completed without int/str comparison error")
        print(f"   Created {len(games)} game(s)")
        
        if games:
            game = games[0] 
            print(f"   Game ID: {game.game_id}")
            print(f"   Status: {game.status} (type: {type(game.status)})")
            print(f"   Home team: {game.home_team_tricode}")
            print(f"   Away team: {game.away_team_tricode}")
            
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"ERROR TYPE: {type(e)}")
        
        # Check if this is the specific int/str comparison error we were hunting
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE INT/STR COMPARISON ERROR!")
            print("The error is still occurring in GameTransformer!")
        
        print("\n📍 FULL TRACEBACK:")
        traceback.print_exc()
        return False

def test_preprocessing_function():
    """Test the preprocessing function directly."""
    
    try:
        sys.path.insert(0, 'src')
        from nba_scraper.models.utils import preprocess_nba_stats_data
        
        print("\n🔍 Testing preprocessing function directly...")
        
        # Test data with integer status (the problematic case)
        test_data = {
            'GAME_STATUS_TEXT': 3,  # Integer status
            'PERIOD': 4,
            'HOME_TEAM_ID': 1610612747,
            'EVENTMSGTYPE': 1
        }
        
        processed = preprocess_nba_stats_data(test_data)
        
        print("✅ Preprocessing completed successfully")
        print(f"   Original GAME_STATUS_TEXT: {test_data['GAME_STATUS_TEXT']} (type: {type(test_data['GAME_STATUS_TEXT'])})")
        print(f"   Processed GAME_STATUS_TEXT: {processed['GAME_STATUS_TEXT']} (type: {type(processed['GAME_STATUS_TEXT'])})")
        
        # Verify all potentially problematic fields are now strings
        for field in ['GAME_STATUS_TEXT', 'EVENTMSGTYPE']:
            if field in processed:
                if not isinstance(processed[field], str):
                    print(f"❌ WARNING: {field} is still {type(processed[field])}, not string!")
                    return False
        
        return True
        
    except Exception as e:
        print(f"❌ Preprocessing function error: {e}")
        traceback.print_exc() 
        return False

if __name__ == "__main__":
    print("🎯 Testing GameTransformer fix for int/str comparison error...")
    
    # Test 1: Direct preprocessing function
    preprocessing_success = test_preprocessing_function()
    
    # Test 2: Full GameTransformer pipeline 
    transformer_success = test_game_transformer_fix()
    
    print(f"\n🏁 RESULTS:")
    print(f"   Preprocessing function: {'✅ PASS' if preprocessing_success else '❌ FAIL'}")
    print(f"   GameTransformer: {'✅ PASS' if transformer_success else '❌ FAIL'}")
    
    if preprocessing_success and transformer_success:
        print(f"\n🎉 SUCCESS! The int/str comparison error has been FIXED!")
        print(f"The GameTransformer can now handle integer status codes from NBA Stats API")
    else:
        print(f"\n⚠️  The fix needs more work - some tests are still failing")
        
    sys.exit(0 if (preprocessing_success and transformer_success) else 1)