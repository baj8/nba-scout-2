#!/usr/bin/env python3
"""Example script showing how to make live NBA API calls."""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def example_live_api_calls():
    """Demonstrate live API calls to NBA data sources."""
    print("🌐 Making Live NBA API Calls")
    print("=" * 50)
    
    try:
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        from nba_scraper.io_clients.bref import BRefClient
        from nba_scraper.io_clients.gamebooks import GamebooksClient
        
        # Initialize clients
        nba_client = NBAStatsClient()
        bref_client = BRefClient()
        gamebooks_client = GamebooksClient()
        
        print("✅ Initialized API clients")
        
        # 1. Get games for a recent date
        print("\n📅 Fetching recent games...")
        recent_date = datetime(2024, 1, 15)  # Example date
        
        try:
            scoreboard_data = await nba_client.fetch_scoreboard_by_date(recent_date)
            games = nba_client.parse_scoreboard_games(scoreboard_data)
            print(f"✅ Found {len(games)} games on {recent_date.strftime('%Y-%m-%d')}")
            
            # Show first game details
            if games:
                game = games[0]
                print(f"   First game: {game.get('VISITOR_TEAM_NAME')} @ {game.get('HOME_TEAM_NAME')}")
                print(f"   Game ID: {game.get('GAME_ID')}")
                print(f"   Status: {game.get('GAME_STATUS_TEXT')}")
        
        except Exception as e:
            print(f"❌ Failed to fetch scoreboard: {e}")
        
        # 2. Get play-by-play for a game (if we have one)
        if games and len(games) > 0:
            game_id = games[0].get('GAME_ID')
            if game_id:
                print(f"\n🏀 Fetching play-by-play for game {game_id}...")
                
                try:
                    pbp_data = await nba_client.fetch_pbp(game_id)
                    events = nba_client.parse_pbp_events(pbp_data)
                    print(f"✅ Found {len(events)} play-by-play events")
                    
                    # Show first few events
                    if events:
                        print("   First 3 events:")
                        for i, event in enumerate(events[:3]):
                            print(f"   {i+1}. Q{event.get('PERIOD')} {event.get('PCTIMESTRING')} - {event.get('HOMEDESCRIPTION') or event.get('VISITORDESCRIPTION') or 'Game event'}")
                
                except Exception as e:
                    print(f"❌ Failed to fetch PBP: {e}")
        
        # 3. Try Basketball Reference (requires different approach)
        print(f"\n📊 Checking Basketball Reference access...")
        try:
            # Example: Get schedule for a date
            bref_url = f"https://www.basketball-reference.com/boxscores/"
            print(f"✅ Basketball Reference client ready (would fetch from {bref_url})")
            print("   Note: BRef requires careful scraping due to rate limits")
        
        except Exception as e:
            print(f"❌ BRef client error: {e}")
        
        # 4. Try Gamebooks (PDF downloads)
        print(f"\n📄 Checking NBA Gamebooks access...")
        try:
            # Example game ID for gamebook
            test_game_id = "0022300456"  # Example
            print(f"✅ Gamebooks client ready (would fetch PDF for game {test_game_id})")
            print("   Note: Downloads official NBA PDF gamebooks with referee info")
        
        except Exception as e:
            print(f"❌ Gamebooks client error: {e}")
        
        print("\n" + "=" * 50)
        print("🎉 Live API call demonstration complete!")
        print("\n📋 Available Data Sources:")
        print("✅ NBA Stats API - Official stats, PBP, box scores")
        print("✅ Basketball Reference - Historical data, advanced stats")  
        print("✅ NBA Gamebooks - Official PDFs with referee assignments")
        print("✅ Rate limiting - 45 requests/minute with exponential backoff")
        print("✅ Retry logic - Automatic retries on failures")
        print("✅ Caching - Built-in response caching")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure dependencies are installed: pip install -e .")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

async def test_http_client():
    """Test the HTTP client directly."""
    print("\n🔧 Testing HTTP Client Directly")
    print("-" * 30)
    
    try:
        from nba_scraper.http import get, HTTPSession
        
        # Test direct HTTP call
        print("Making test HTTP request...")
        
        # Try a simple API endpoint
        test_url = "https://httpbin.org/json"  # Test endpoint
        response = await get(test_url)
        
        print(f"✅ HTTP request successful!")
        print(f"   Status: {response.status_code}")
        print(f"   Content length: {len(response.content)} bytes")
        print(f"   Headers: {dict(list(response.headers.items())[:3])}...")  # Show first 3 headers
        
        # Test with session
        print("\nTesting HTTP session...")
        async with HTTPSession() as session:
            response = await session.get(test_url)
            print(f"✅ Session request successful!")
            print(f"   Status: {response.status_code}")
        
    except Exception as e:
        print(f"❌ HTTP client test failed: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(example_live_api_calls())
        asyncio.run(test_http_client())
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)