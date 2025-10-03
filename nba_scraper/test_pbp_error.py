#!/usr/bin/env python3
"""Test PBP model creation to find the int/str comparison error."""

import sys
import traceback
sys.path.insert(0, 'src')

from nba_scraper.models.pbp_rows import PbpEventRow

def test_pbp_creation():
    """Test creating a PbpEventRow with real NBA API data."""
    
    # Simulate NBA Stats PBP data with integer enum codes (like the real API sends)
    pbp_data = {
        'GAME_ID': '0012400050',
        'PERIOD': 1,
        'EVENTNUM': 1,
        'EVENTMSGTYPE': 1,  # INTEGER EVENT TYPE - This is likely the problem!
        'EVENTMSGACTIONTYPE': 0,
        'PCTIMESTRING': '12:00',
        'HOMEDESCRIPTION': 'Made shot',
        'PLAYER1_NAME': 'LeBron James',
        'PLAYER1_ID': 2544,
    }
    
    try:
        print("Testing PbpEventRow creation with integer event type...")
        
        # Try to create a PbpEventRow using the from_nba_stats method
        pbp_row = PbpEventRow.from_nba_stats('0012400050', pbp_data, "http://test.com")
        
        print(f"✅ SUCCESS: Created PbpEventRow with event_type: {pbp_row.event_type}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"ERROR TYPE: {type(e)}")
        print("\n📍 FULL TRACEBACK:")
        traceback.print_exc()
        
        # Check if this is the int/str comparison error we're looking for
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE ERROR! This is the int/str comparison issue!")
            
            # Find the exact line where the comparison failed
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb = exc_traceback
            while tb is not None:
                frame = tb.tb_frame
                filename = frame.f_code.co_filename
                line_number = tb.tb_lineno
                function_name = frame.f_code.co_name
                
                print(f"   📂 File: {filename}")
                print(f"   📍 Line: {line_number}")
                print(f"   🔧 Function: {function_name}")
                    
                tb = tb.tb_next
        
        return False
    
    return True

if __name__ == "__main__":
    success = test_pbp_creation()
    if success:
        print("\n🎉 Test passed - no int/str comparison error found here")
    else:
        print("\n💥 Test failed - found the source of the error!")
