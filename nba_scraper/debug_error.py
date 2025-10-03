import asyncio
import traceback
import sys
import os
import warnings

# Add the src directory to Python path
sys.path.insert(0, '/Users/benjaminjarrett/NBA Scout 2/nba_scraper/src')

# Suppress warnings to see our error clearly
warnings.filterwarnings("ignore", message="ARC4 has been moved")

async def debug_error():
    try:
        print("🔧 Setting up pipeline components...")
        
        # Import and create all required components
        from nba_scraper.pipelines.game_pipeline import GamePipeline
        from nba_scraper.io_clients.bref import BRefClient
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        from nba_scraper.io_clients.gamebooks import GamebooksClient
        from nba_scraper.rate_limit import RateLimiter
        
        # Create components properly
        rate_limiter = RateLimiter()
        bref_client = BRefClient()
        nba_client = NBAStatsClient()
        gamebooks_client = GamebooksClient()
        
        # Create game pipeline
        game_pipeline = GamePipeline(bref_client, nba_client, gamebooks_client, rate_limiter)
        
        print("🎯 Directly calling the failing NBA Stats processing method...")
        
        # Call the EXACT method that's failing to get the unhandled error
        result = await game_pipeline._process_nba_stats_source('0012400050', False)
        print(f"✅ SUCCESS: {result}")
        
    except Exception as e:
        print(f"\n🚨 ERROR CAUGHT: {e}")
        print(f"ERROR TYPE: {type(e)}")
        
        # Check if this is THE error we're hunting for
        error_message = str(e)
        if "'<' not supported between instances" in error_message:
            print(f"\n🎯 JACKPOT! Found the int/str comparison error!")
            
        print("\n📍 FULL STACK TRACE:")
        traceback.print_exc()
        
        # Walk through the traceback to find the exact problematic line
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        print(f"\n🔍 DETAILED TRACE ANALYSIS:")
        tb = exc_traceback
        trace_count = 0
        while tb is not None:
            frame = tb.tb_frame
            filename = frame.f_code.co_filename
            line_number = tb.tb_lineno
            function_name = frame.f_code.co_name
            
            # Only show NBA scraper related files
            if "nba_scraper" in filename:
                trace_count += 1
                print(f"\n   {trace_count}. 📂 {filename.split('/')[-1]}:{line_number} in {function_name}()")
                
                # Try to show the actual line of code that failed
                try:
                    with open(filename, 'r') as f:
                        lines = f.readlines()
                        if line_number <= len(lines):
                            problematic_line = lines[line_number-1].strip()
                            print(f"      💻 Code: {problematic_line}")
                            
                            # Check if this line contains comparison operations
                            if any(op in problematic_line for op in ['<', '>', '==', '!=', 'in ', '<=', '>=']):
                                print(f"      🚨 POTENTIAL COMPARISON LINE!")
                except Exception:
                    pass
                    
            tb = tb.tb_next
        
        return False
    
    return True

async def debug_extractor_direct():
    """Call the extractor functions directly to bypass pipeline error handling."""
    
    try:
        print("🔧 Testing NBA Stats PBP extractor directly...")
        
        from nba_scraper.extractors.nba_stats import extract_pbp_from_response
        from nba_scraper.io_clients.nba_stats import NBAStatsClient
        from nba_scraper.rate_limit import RateLimiter
        
        # Create NBA Stats client
        rate_limiter = RateLimiter()
        nba_client = NBAStatsClient()
        
        print("📡 Fetching real NBA API data...")
        # Get real PBP data that's causing the error
        pbp_response = await nba_client.fetch_pbp('0012400050')
        
        if pbp_response:
            print("🎯 Calling extract_pbp_from_response with real data...")
            # This should trigger the int/str comparison error!
            pbp_events = extract_pbp_from_response(pbp_response, '0012400050', 'http://test.com')
            print(f"✅ SUCCESS: Extracted {len(pbp_events)} PBP events")
        else:
            print("❌ No PBP response from NBA API")
        
    except Exception as e:
        print(f"\n🚨 ERROR CAUGHT: {e}")
        print(f"ERROR TYPE: {type(e)}")
        
        # Check if this is THE error we're hunting for
        error_message = str(e)
        if "'<' not supported between instances" in error_message:
            print(f"\n🎯 JACKPOT! Found the int/str comparison error!")
            
        print("\n📍 FULL STACK TRACE:")
        traceback.print_exc()
        
        # Walk through the traceback to find the exact problematic line
        import sys
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        print(f"\n🔍 DETAILED TRACE ANALYSIS:")
        tb = exc_traceback
        trace_count = 0
        while tb is not None:
            frame = tb.tb_frame
            filename = frame.f_code.co_filename
            line_number = tb.tb_lineno
            function_name = frame.f_code.co_name
            
            # Only show NBA scraper related files
            if "nba_scraper" in filename:
                trace_count += 1
                print(f"\n   {trace_count}. 📂 {filename.split('/')[-1]}:{line_number} in {function_name}()")
                
                # Try to show the actual line of code that failed
                try:
                    with open(filename, 'r') as f:
                        lines = f.readlines()
                        if line_number <= len(lines):
                            problematic_line = lines[line_number-1].strip()
                            print(f"      💻 Code: {problematic_line}")
                            
                            # Highlight comparison operations
                            if any(op in problematic_line for op in ['<', '>', '==', '!=', 'in ', '<=', '>=']):
                                print(f"      🚨 CONTAINS COMPARISON OPERATION!")
                except Exception:
                    pass
                    
            tb = tb.tb_next
        
        return False
    
    return True

if __name__ == "__main__":
    print("🕵️ Running debug script to catch the NBA Stats processing error directly...")
    print("=" * 70)
    
    success = asyncio.run(debug_error())
    
    print("=" * 70)
    if success:
        print("🎉 Debug test passed - no int/str error found")
    else:
        print("💥 Debug test failed - FOUND THE ERROR SOURCE!")
    
    print("🕵️ Calling extractors directly to bypass pipeline error handling...")
    print("=" * 70)
    
    success = asyncio.run(debug_extractor_direct())
    
    print("=" * 70)
    if success:
        print("🤔 No error found - int/str issue might be elsewhere")
    else:
        print("💥 FOUND THE EXACT SOURCE OF THE INT/STR COMPARISON ERROR!")
