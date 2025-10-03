#!/usr/bin/env python3
"""Test the actual extractor functions to find the int/str comparison error."""

import sys
import traceback
sys.path.insert(0, 'src')

def test_game_outcomes_extractor():
    """Test the extract_game_outcomes function that might cause the error."""
    
    try:
        from nba_scraper.extractors.bref import extract_game_outcomes
        
        print("Testing Basketball Reference extract_game_outcomes...")
        
        # Create minimal HTML that might trigger the error
        mock_html = "<html><body>Test</body></html>"
        
        outcomes = extract_game_outcomes(mock_html)
        print(f"✅ SUCCESS: extract_game_outcomes returned {len(outcomes)} outcomes")
        
    except Exception as e:
        print(f"❌ ERROR in extract_game_outcomes: {e}")
        
        if "'<' not supported between instances" in str(e):
            print(f"🎯 FOUND THE ERROR in Basketball Reference extractor!")
            traceback.print_exc()
            return False
        else:
            print(f"Different error (not the int/str comparison): {type(e).__name__}")
    
    return True

def test_nba_stats_extractor():
    """Test NBA Stats extractors."""
    
    try:
        from nba_scraper.extractors.nba_stats import extract_games_from_scoreboard
        
        print("Testing NBA Stats extract_games_from_scoreboard...")
        
        # Create minimal NBA Stats response that might trigger the error
        mock_response = {
            "resultSets": [
                {
                    "name": "GameHeader",
                    "headers": ["GAME_ID", "GAME_STATUS_TEXT"],
                    "rowSet": [["0012400050", 3]]  # INTEGER STATUS - might cause the error!
                }
            ]
        }
        
        games = extract_games_from_scoreboard(mock_response, "http://test.com")
        print(f"✅ SUCCESS: extract_games_from_scoreboard returned {len(games)} games")
        
    except Exception as e:
        print(f"❌ ERROR in extract_games_from_scoreboard: {e}")
        
        if "'<' not supported between instances" in str(e):
            print(f"🎯 FOUND THE ERROR in NBA Stats extractor!")
            traceback.print_exc()
            return False
        else:
            print(f"Different error (not the int/str comparison): {type(e).__name__}")
    
    return True

if __name__ == "__main__":
    print("Testing extractor functions to find int/str comparison error...\n")
    
    bref_success = test_game_outcomes_extractor()
    print()
    nba_success = test_nba_stats_extractor()
    
    print(f"\n🎯 Results:")
    print(f"   Basketball Reference: {'✅ PASS' if bref_success else '❌ FAIL'}")
    print(f"   NBA Stats: {'✅ PASS' if nba_success else '❌ FAIL'}")
    
    if not bref_success or not nba_success:
        print(f"\n💥 Found the source of the int/str comparison error!")
    else:
        print(f"\n🤔 Still haven't found the exact source...")
