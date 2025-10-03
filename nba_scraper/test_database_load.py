#!/usr/bin/env python3
"""Test loading games from database to find the int/str comparison error."""

import asyncio
import sys
import traceback
sys.path.insert(0, 'src')

from nba_scraper.db import get_connection

async def test_database_load():
    """Test loading games from the database where the error might occur."""
    
    try:
        print("Testing database game loading...")
        
        conn = await get_connection()
        
        # Try to load the games that were causing errors
        rows = await conn.fetch("SELECT * FROM games WHERE game_id IN ('0012400050', '0012400051') LIMIT 2")
        
        print(f"✅ SUCCESS: Loaded {len(rows)} games from database")
        
        for row in rows:
            print(f"   Game {row['game_id']}: status={row['status']} (type: {type(row['status'])})")
            
            # Check if status field contains integers that might cause comparison issues
            if isinstance(row['status'], int):
                print(f"   🚨 FOUND INTEGER STATUS: {row['status']}")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"ERROR TYPE: {type(e)}")
        print("\n📍 FULL TRACEBACK:")
        traceback.print_exc()
        
        # Check if this is the int/str comparison error
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 FOUND THE ERROR! This is the int/str comparison issue!")
        
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(test_database_load())
    if success:
        print("\n🎉 Database load test passed")
    else:
        print("\n💥 Database load test failed - found the source!")
