#!/usr/bin/env python3
"""Debug script to trace the exact source of the int/str comparison error by calling operations directly."""

import asyncio
import sys
import traceback
sys.path.insert(0, 'src')

from nba_scraper.io_clients import BRefClient, NBAStatsClient, GamebooksClient
from nba_scraper.rate_limit import RateLimiter
from nba_scraper.loaders import GameLoader, RefLoader, LineupLoader, PbpLoader
from nba_scraper.extractors import (
    extract_pbp_from_response,
    extract_boxscore_lineups,
    extract_game_outcomes,
    extract_starting_lineups,
    extract_injury_notes,
    extract_referee_assignments,
    extract_referee_alternates,
)

async def debug_direct_operations():
    """Call the underlying operations that are failing directly."""
    
    print("🔬 Testing direct operations that might cause int/str comparison errors...")
    
    rate_limiter = RateLimiter()
    bref_client = BRefClient()
    nba_stats_client = NBAStatsClient()
    gamebooks_client = GamebooksClient()
    
    # Initialize loaders
    game_loader = GameLoader()
    ref_loader = RefLoader()
    lineup_loader = LineupLoader()
    pbp_loader = PbpLoader()
    
    game_id = '0012400050'
    
    print(f"\n🎯 Testing NBA Stats operations for game {game_id}...")
    
    # Test NBA Stats PBP loading (this is where the error likely occurs)
    try:
        print("   📡 Fetching NBA Stats PBP...")
        await rate_limiter.acquire('nba_stats')
        pbp_response = await nba_stats_client.fetch_pbp(game_id)
        
        if pbp_response:
            print("   ✅ PBP fetch successful, extracting events...")
            source_url = f"https://stats.nba.com/game/{game_id}"
            pbp_events = extract_pbp_from_response(pbp_response, game_id, source_url)
            
            print(f"   ✅ Extracted {len(pbp_events)} PBP events, now loading to database...")
            # This is likely where the int/str error occurs
            result = await pbp_loader.upsert_events(pbp_events)
            print(f"   ✅ NBA Stats PBP loading successful: {result}")
        else:
            print("   ❌ No PBP response data")
            
    except Exception as e:
        print(f"   ❌ NBA Stats PBP operation failed: {e}")
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE ERROR IN NBA STATS PBP LOADING!")
            print(f"\n📍 FULL STACK TRACE:")
            traceback.print_exc()
            return False
    
    print(f"\n🎯 Testing Basketball Reference operations for game {game_id}...")
    
    # Test B-Ref operations 
    try:
        print("   📡 Fetching B-Ref box score...")
        await rate_limiter.acquire('bref')
        outcomes_html = await bref_client.fetch_bref_box(game_id)
        
        if outcomes_html:
            print("   ✅ B-Ref fetch successful, extracting outcomes...")
            source_url = f"https://www.basketball-reference.com/boxscores/{game_id}.html"
            outcomes = extract_game_outcomes(outcomes_html, game_id, source_url)
            
            if outcomes:
                print(f"   ✅ Extracted {len(outcomes)} outcomes, now loading to database...")
                # This might be where the int/str error occurs
                result = await game_loader.upsert_games(outcomes)
                print(f"   ✅ B-Ref outcomes loading successful: {result}")
            else:
                print("   ❌ No outcomes extracted")
        else:
            print("   ❌ No B-Ref response data")
            
    except Exception as e:
        print(f"   ❌ B-Ref operation failed: {e}")
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE ERROR IN B-REF LOADING!")
            print(f"\n📍 FULL STACK TRACE:")
            traceback.print_exc()
            return False
    
    print(f"\n🎯 Testing database operations directly...")
    
    # Test direct database operations that might cause comparison errors
    try:
        print("   🗄️ Testing direct database query...")
        from nba_scraper.db import get_connection
        
        conn = await get_connection()
        
        # Test a query that might trigger the problematic index
        query = """
        SELECT game_id, home_team_tricode, away_team_tricode 
        FROM games 
        WHERE game_id = $1
        ORDER BY home_team_tricode, away_team_tricode
        LIMIT 1
        """
        
        result = await conn.fetchrow(query, game_id)
        print(f"   ✅ Database query successful: {result}")
        
    except Exception as e:
        print(f"   ❌ Database operation failed: {e}")
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE ERROR IN DATABASE OPERATIONS!")
            print(f"\n📍 FULL STACK TRACE:")
            traceback.print_exc()
            return False
    
    print("\n✅ All direct operations completed successfully!")
    return True

if __name__ == "__main__":
    success = asyncio.run(debug_direct_operations())
    if success:
        print("\n🎉 Direct operations debug test passed")
    else:
        print("\n💥 Direct operations debug test failed - found the exact error source!")