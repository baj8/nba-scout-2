#!/usr/bin/env python3
"""Isolate the exact location of the int/str comparison error."""

import sys
import traceback
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_api_calls():
    """Test just the API calls without processing."""
    print("🔍 Testing API calls in isolation...\n")
    
    try:
        from nba_scraper.io_clients import BRefClient, NBAStatsClient, GamebooksClient
        
        # Test NBA Stats client
        print("1️⃣ Testing NBA Stats API calls...")
        nba_client = NBAStatsClient()
        
        # This might be where the error occurs - during actual API calls
        try:
            scoreboard_data = await nba_client.fetch_scoreboard('2024-10-15')
            print(f"   ✅ Scoreboard fetch: Got {type(scoreboard_data)} data")
        except Exception as e:
            print(f"   ❌ Scoreboard fetch failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN NBA STATS API CALLS!")
                traceback.print_exc()
                return False
        
        try:
            pbp_data = await nba_client.fetch_pbp('0012400050')  
            print(f"   ✅ PBP fetch: Got {type(pbp_data)} data")
        except Exception as e:
            print(f"   ❌ PBP fetch failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN NBA STATS PBP API!")
                traceback.print_exc() 
                return False
        
        # Test Basketball Reference client
        print("\n2️⃣ Testing Basketball Reference API calls...")
        bref_client = BRefClient()
        
        try:
            bref_data = await bref_client.fetch_bref_box('0012400050')
            print(f"   ✅ B-Ref fetch: Got {type(bref_data)} data")
        except Exception as e:
            print(f"   ❌ B-Ref fetch failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN BREF API CALLS!")
                traceback.print_exc()
                return False
        
        print("✅ All API calls completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ API call test setup failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND ERROR IN API CLIENT SETUP!")
            traceback.print_exc()
            return False
        return False

async def test_database_operations():
    """Test database operations in isolation."""
    print("\n🔍 Testing database operations...\n")
    
    try:
        from nba_scraper.loaders import GameLoader, RefLoader, LineupLoader, PbpLoader
        from nba_scraper.models import GameRow, PbpEventRow
        from datetime import datetime, date, UTC
        from dateutil import tz
        
        print("1️⃣ Testing loader initialization...")
        game_loader = GameLoader()
        ref_loader = RefLoader()
        lineup_loader = LineupLoader()
        pbp_loader = PbpLoader()
        print("   ✅ All loaders initialized successfully")
        
        print("\n2️⃣ Testing model creation with enum data...")
        
        # Create a sample GameRow with enum data that might cause issues
        try:
            game_row = GameRow(
                game_id='test123',
                season='2024-25',
                game_date_utc=datetime.now(UTC),
                game_date_local=date.today(),
                arena_tz='America/New_York',
                home_team_tricode='LAL',
                away_team_tricode='BOS',
                status='FINAL',  # This might cause int/str comparison if not preprocessed correctly
                source='test',
                source_url='test://url'
            )
            print("   ✅ GameRow creation successful")
        except Exception as e:
            print(f"   ❌ GameRow creation failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN GAMEROW ENUM HANDLING!")
                traceback.print_exc()
                return False
        
        print("\n3️⃣ Testing database upsert operations...")
        
        # Test actual database operations
        try:
            # This might be where the enum comparison error happens
            result = await game_loader.upsert_games([game_row])
            print(f"   ✅ Database upsert successful: {result} record(s)")
        except Exception as e:
            print(f"   ❌ Database upsert failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN DATABASE UPSERT!")
                traceback.print_exc()
                return False
        
        print("✅ All database operations completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Database operations test failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND ERROR IN DATABASE OPERATIONS SETUP!")
            traceback.print_exc()
            return False
        return False

async def test_transformation_pipeline():
    """Test the transformation pipeline with real data structures."""
    print("\n🔍 Testing transformation pipeline...\n")
    
    try:
        from nba_scraper.transformers.games import GameTransformer
        from nba_scraper.transformers.pbp import PbpTransformer
        
        print("1️⃣ Testing transformer with NBA Stats-like data...")
        game_transformer = GameTransformer(source='nba_stats')
        
        # Simulate real NBA Stats response structure that might cause enum issues
        nba_stats_game_data = {
            'GAME_ID': '0012400050',
            'GAME_DATE_EST': '2024-10-15',
            'GAME_STATUS_ID': 3,  # Integer status that might cause comparison error
            'GAME_STATUS_TEXT': 'Final',  # String status
            'HOME_TEAM_ID': 1610612747,
            'VISITOR_TEAM_ID': 1610612738,
            'SEASON': '2024-25',
            'LIVE_PERIOD': 4,
            'LIVE_PC_TIME': '00:00'
        }
        
        try:
            transformed_games = game_transformer.transform(nba_stats_game_data, game_id='0012400050')
            print(f"   ✅ Game transformation successful: {len(transformed_games)} games")
        except Exception as e:
            print(f"   ❌ Game transformation failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN GAME TRANSFORMATION!")
                traceback.print_exc()
                return False
        
        print("\n2️⃣ Testing PBP transformer with enum-heavy data...")
        pbp_transformer = PbpTransformer(source='nba_stats')
        
        # PBP data with lots of enums that might cause issues
        nba_stats_pbp_data = {
            'EVENTNUM': 1,
            'EVENTMSGTYPE': 12,  # Integer enum value
            'EVENTMSGACTIONTYPE': 0,  # Integer enum value
            'PERIOD': 1,
            'WCTIMESTRING': '7:00 PM',
            'PCTIMESTRING': '12:00',
            'HOMEDESCRIPTION': None,
            'VISITORDESCRIPTION': 'Jump Ball',
            'SCORE': None
        }
        
        try:
            transformed_events = pbp_transformer.transform(nba_stats_pbp_data, game_id='0012400050')
            print(f"   ✅ PBP transformation successful: {len(transformed_events)} events")
        except Exception as e:
            print(f"   ❌ PBP transformation failed: {e}")
            if "'<' not supported between instances" in str(e):
                print("   🎯 FOUND ERROR IN PBP TRANSFORMATION!")
                traceback.print_exc()
                return False
        
        print("✅ All transformations completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Transformation pipeline test failed: {e}")
        if "'<' not supported between instances" in str(e):
            print("🎯 FOUND ERROR IN TRANSFORMATION SETUP!")
            traceback.print_exc()
            return False
        return False

async def main():
    """Run all targeted tests to isolate the int/str comparison error."""
    print("🎯 Isolating the exact source of the int/str comparison error...\n")
    
    success_api = await test_api_calls()
    success_db = await test_database_operations() 
    success_transform = await test_transformation_pipeline()
    
    if success_api and success_db and success_transform:
        print("\n🤔 All individual components work - the error might be in the integration or specific data values...")
    else:
        print("\n🎯 FOUND THE EXACT SOURCE OF THE INT/STR COMPARISON ERROR!")
    
    return success_api and success_db and success_transform

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)