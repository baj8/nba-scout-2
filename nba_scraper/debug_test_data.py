#!/usr/bin/env python3
"""Debug script to examine the actual integration test data."""

import sys
sys.path.insert(0, 'src')

# Import the test fixture
import pytest
from tests.integration.test_end_to_end_sample_date import sample_game_pbp_events
from nba_scraper.transformers.early_shocks import EarlyShocksTransformer

def debug_integration_test_data():
    """Debug the actual integration test data."""
    
    # Get the test data (simulate the fixture)
    base_url = "https://stats.nba.com/test/game/0022300001"
    
    from nba_scraper.models.pbp_rows import PbpEventRow
    from nba_scraper.models.enums import EventType
    
    # Recreate the LeBron foul events from the integration test
    lebron_events = [
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=10,
            event_type=EventType.FOUL,
            seconds_elapsed=180,  # 9:00 remaining
            time_remaining="09:00",
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            player1_display_name="LeBron James",
            description="Personal foul by LeBron James",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=25,
            event_type=EventType.FOUL,
            seconds_elapsed=300,  # 7:00 remaining
            time_remaining="07:00", 
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            player1_display_name="LeBron James",
            description="Personal foul by LeBron James",
            source="nba_stats_test",
            source_url=base_url
        ),
    ]
    
    print("🔍 Debugging LeBron's foul events:")
    for i, event in enumerate(lebron_events):
        print(f"Event {i+1}:")
        print(f"  event_type: {event.event_type} ({type(event.event_type)})")
        print(f"  seconds_elapsed: {event.seconds_elapsed} ({type(event.seconds_elapsed)})")
        print(f"  player1_name_slug: {event.player1_name_slug}")
        print(f"  team_tricode: {event.team_tricode}")
        print(f"  description: {event.description}")
    
    # Test the transformer
    transformer = EarlyShocksTransformer(source="debug", early_foul_threshold_sec=360.0)
    
    # Test the _is_technical_or_flagrant_foul method specifically
    print(f"\n🔬 Testing _is_technical_or_flagrant_foul:")
    for i, event in enumerate(lebron_events):
        is_tech_flagrant = transformer._is_technical_or_flagrant_foul(event)
        print(f"Event {i+1}: {is_tech_flagrant}")
    
    # Test early foul detection directly
    print(f"\n🎯 Testing _detect_early_foul_trouble directly:")
    early_foul_shocks = transformer._detect_early_foul_trouble(lebron_events, base_url)
    print(f"Detected {len(early_foul_shocks)} early foul shocks")
    
    for shock in early_foul_shocks:
        print(f"  {shock.shock_type.value}: {shock.player_slug}")
    
    # Test full transform
    print(f"\n🚀 Testing full transform:")
    all_shocks = transformer.transform(lebron_events, base_url)
    print(f"Total shocks: {len(all_shocks)}")
    
    return len(all_shocks)

if __name__ == "__main__":
    result = debug_integration_test_data()
    print(f"\n✅ Debug completed with {result} shocks detected")
