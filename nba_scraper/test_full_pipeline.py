#!/usr/bin/env python3
"""Test the full pipeline flow to catch the int/str comparison error."""

import asyncio
import sys
import traceback
import warnings
sys.path.insert(0, 'src')

# Suppress the cryptography warning to see our error clearly
warnings.filterwarnings("ignore", message="ARC4 has been moved")

async def test_single_game_pipeline():
    """Test processing a single game through the full pipeline."""
    
    try:
        from nba_scraper.pipelines.game_pipeline import GamePipeline
        from nba_scraper.io_clients.bref import BRefClient
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        from nba_scraper.io_clients.gamebooks import GamebooksClient
        from nba_scraper.rate_limit import RateLimiter
        
        print("🔧 Setting up pipeline components...")
        
        # Create the exact same setup as the backfill
        rate_limiter = RateLimiter()
        bref_client = BRefClient()
        nba_client = NBAStatsClient()
        gamebooks_client = GamebooksClient()
        
        pipeline = GamePipeline(bref_client, nba_client, gamebooks_client, rate_limiter)
        
        print("🎯 Testing single game processing...")
        
        # Process one of the games that was failing in the backfill
        result = await pipeline.process_game('0012400050', sources=['nba_stats'], dry_run=False)
        
        print(f"✅ SUCCESS: {result}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"ERROR TYPE: {type(e)}")
        
        # Check if this is THE error we're looking for
        if "'<' not supported between instances" in str(e):
            print(f"\n🎯 JACKPOT! Found the int/str comparison error!")
            print(f"\n📍 FULL STACK TRACE:")
            traceback.print_exc()
            
            # Walk through the stack trace to find the exact line
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print(f"\n🔍 DETAILED TRACE:")
            tb = exc_traceback
            while tb is not None:
                frame = tb.tb_frame
                filename = frame.f_code.co_filename
                line_number = tb.tb_lineno
                function_name = frame.f_code.co_name
                
                print(f"   📂 {filename.split('/')[-1]}:{line_number} in {function_name}()")
                
                # Try to show the actual problematic line
                if "nba_scraper" in filename:
                    try:
                        with open(filename, 'r') as f:
                            lines = f.readlines()
                            if line_number <= len(lines):
                                print(f"      💻 {lines[line_number-1].strip()}")
                    except:
                        pass
                        
                tb = tb.tb_next
            
            return False
        else:
            print(f"Different error: {type(e).__name__}")
            traceback.print_exc()
        
        return False
    
    return True

if __name__ == "__main__":
    print("🕵️ Testing full pipeline to catch the int/str comparison error...")
    print("=" * 60)
    
    success = asyncio.run(test_single_game_pipeline())
    
    print("=" * 60)
    if success:
        print("�� Pipeline test passed - no int/str error found")
    else:
        print("💥 Pipeline test failed - FOUND THE ERROR SOURCE!")
