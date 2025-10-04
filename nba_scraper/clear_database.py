#!/usr/bin/env python3
"""
Clear database for clean Tranche 2 testing
"""

import sys
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def clear_database():
    """Clear all data tables to start fresh."""
    print("🗑️ Clearing Database for Clean Tranche 2 Testing")
    print("=" * 50)
    
    try:
        from nba_scraper.db import get_connection
        
        conn = await get_connection()
        
        print('🗑️ Clearing all data tables...')
        
        # Clear all data tables but keep schema
        tables_to_clear = [
            'pbp_events',
            'starting_lineups', 
            'games',
            'game_id_crosswalk',
            'ref_assignments',
            'ref_alternates',
            'outcomes',
            'q1_window_12_8',
            'early_shocks',
            'schedule_travel',
            'advanced_player_stats',
            'advanced_team_stats', 
            'misc_player_stats',
            'usage_player_stats',
            'pipeline_state'
        ]
        
        total_cleared = 0
        for table in tables_to_clear:
            try:
                result = await conn.execute(f'DELETE FROM {table}')
                # Parse the result to get row count
                if result and 'DELETE' in result:
                    count = int(result.split()[-1])
                    total_cleared += count
                    print(f'  ✅ Cleared {table}: {count} rows deleted')
                else:
                    print(f'  ✅ Cleared {table}: 0 rows')
            except Exception as e:
                print(f'  ⚠️ {table}: {str(e)[:100]}...')
        
        await conn.close()
        print(f'\n🎯 Database cleared successfully!')
        print(f'📊 Total rows cleared: {total_cleared}')
        print(f'✅ Ready for clean Tranche 2 testing with shot coordinates!')
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to clear database: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Clear the database and prepare for testing."""
    success = await clear_database()
    
    if success:
        print(f"\n🚀 Next Steps:")
        print(f"  1. Test shot coordinate integration on a single game")
        print(f"  2. Integrate reliable shot chart into main pipeline") 
        print(f"  3. Test end-to-end with clean data")
        print(f"  4. Re-scrape historical games once everything works 100%")
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)