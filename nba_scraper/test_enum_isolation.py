#!/usr/bin/env python3
"""
Isolate the exact source of the int/str comparison error.
"""

import sys
import traceback
sys.path.insert(0, 'src')

def test_direct_enum_instantiation():
    """Test direct enum instantiation with problematic values."""
    print("🔍 Testing direct enum instantiation...")
    
    try:
        from nba_scraper.models.enums import EventType, GameStatus
        
        # Test cases that might trigger the error
        test_cases = [
            ("EventType", EventType, 1),
            ("EventType", EventType, "1"),
            ("GameStatus", GameStatus, 3),
            ("GameStatus", GameStatus, "3"),
        ]
        
        for enum_name, enum_class, test_value in test_cases:
            try:
                print(f"   Testing {enum_name}({test_value}) - type: {type(test_value)}")
                result = enum_class(test_value)
                print(f"   ✅ SUCCESS: {enum_name}({test_value}) = {result}")
            except Exception as e:
                if "'<' not supported between instances" in str(e):
                    print(f"   🎯 FOUND IT! {enum_name}({test_value}) caused the error!")
                    print(f"   Error: {e}")
                    return False
                else:
                    print(f"   ❌ Different error: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Import or setup error: {e}")
        return False

def test_model_instantiation():
    """Test model instantiation with problematic NBA data."""
    print("\n🔍 Testing model instantiation...")
    
    try:
        from nba_scraper.models.pbp_rows import PbpEventRow
        from nba_scraper.models.game_rows import GameRow
        
        # Test PbpEventRow with integer event_type (NBA API style)
        print("   Testing PbpEventRow with integer event_type...")
        try:
            pbp_data = {
                'game_id': '0012400050',
                'period': 1,
                'event_idx': 1,
                'event_type': 1,  # INTEGER - this might cause the error!
                'seconds_elapsed': 0.0,
                'team_tricode': 'LAL',
                'source': 'test',
                'source_url': 'http://test.com'
            }
            
            pbp_row = PbpEventRow(**pbp_data)
            print(f"   ✅ SUCCESS: PbpEventRow created with event_type={pbp_row.event_type}")
            
        except Exception as e:
            if "'<' not supported between instances" in str(e):
                print(f"   🎯 FOUND IT! PbpEventRow caused the error!")
                print(f"   Error: {e}")
                return False
            else:
                print(f"   ❌ Different error: {e}")
        
        # Test GameRow with integer status (NBA API style)
        print("   Testing GameRow with integer status...")
        try:
            game_data = {
                'game_id': '0012400050',
                'season': '2024-25',
                'game_date_local': '2024-10-15',
                'home_team_tricode': 'LAL',
                'away_team_tricode': 'GSW',
                'status': 3,  # INTEGER - this might cause the error!
                'source': 'test',
                'source_url': 'http://test.com'
            }
            
            game_row = GameRow(**game_data)
            print(f"   ✅ SUCCESS: GameRow created with status={game_row.status}")
            
        except Exception as e:
            if "'<' not supported between instances" in str(e):
                print(f"   🎯 FOUND IT! GameRow caused the error!")
                print(f"   Error: {e}")
                return False
            else:
                print(f"   ❌ Different error: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Model import or setup error: {e}")
        return False

def test_transformer_processing():
    """Test transformer processing with NBA data."""
    print("\n🔍 Testing transformer processing...")
    
    try:
        from nba_scraper.transformers.games import GameTransformer
        
        # Test GameTransformer with realistic NBA Stats data
        print("   Testing GameTransformer with NBA Stats data...")
        try:
            transformer = GameTransformer(source='nba_stats')
            
            # Realistic NBA Stats API response structure
            nba_data = {
                'resultSets': [{
                    'name': 'GameHeader',
                    'headers': ['GAME_ID', 'GAME_STATUS_TEXT', 'HOME_TEAM_ABBREVIATION', 'VISITOR_TEAM_ABBREVIATION'],
                    'rowSet': [['0012400050', 3, 'LAL', 'GSW']]  # INTEGER STATUS
                }]
            }
            
            games = transformer.transform(nba_data, season='2024-25')
            print(f"   ✅ SUCCESS: GameTransformer processed {len(games)} games")
            
        except Exception as e:
            if "'<' not supported between instances" in str(e):
                print(f"   🎯 FOUND IT! GameTransformer caused the error!")
                print(f"   Error: {e}")
                traceback.print_exc()
                return False
            else:
                print(f"   ❌ Different error: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Transformer import or setup error: {e}")
        return False

if __name__ == "__main__":
    print("🎯 ISOLATING THE INT/STR COMPARISON ERROR")
    print("=" * 50)
    
    # Test in order of increasing complexity
    enum_success = test_direct_enum_instantiation()
    model_success = test_model_instantiation() if enum_success else False
    transformer_success = test_transformer_processing() if model_success else False
    
    print("\n" + "=" * 50)
    print("📋 ISOLATION TEST RESULTS:")
    print(f"   Direct enum instantiation: {'✅ PASS' if enum_success else '❌ FAIL'}")
    print(f"   Model instantiation: {'✅ PASS' if model_success else '❌ FAIL'}")
    print(f"   Transformer processing: {'✅ PASS' if transformer_success else '❌ FAIL'}")
    
    if not enum_success:
        print("\n💥 ERROR FOUND AT: Direct enum instantiation level")
    elif not model_success:
        print("\n💥 ERROR FOUND AT: Model instantiation level")
    elif not transformer_success:
        print("\n💥 ERROR FOUND AT: Transformer processing level")
    else:
        print("\n🤔 ERROR NOT REPRODUCED - may be in pipeline integration")