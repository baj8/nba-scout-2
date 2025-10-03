#!/usr/bin/env python3
"""Debug script for early foul trouble detection."""

import sys
sys.path.insert(0, 'src')

from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.models.enums import EventType
from nba_scraper.transformers.early_shocks import EarlyShocksTransformer

def test_early_foul_detection():
    """Test early foul trouble detection with LeBron's two fouls."""
    
    # Create test events matching the integration test data
    events = [
        # LeBron's first foul at 9:00 (180 seconds elapsed)
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=10,
            event_type=EventType.FOUL,
            seconds_elapsed=180.0,  # Explicitly float
            time_remaining="09:00",
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            player1_display_name="LeBron James",
            description="Personal foul by LeBron James",
            source="test",
            source_url="https://test.com"
        ),
        
        # LeBron's second foul at 7:00 (300 seconds elapsed)
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=25,
            event_type=EventType.FOUL,
            seconds_elapsed=300.0,  # Explicitly float
            time_remaining="07:00", 
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            player1_display_name="LeBron James",
            description="Personal foul by LeBron James",
            source="test",
            source_url="https://test.com"
        ),
        
        # Substitution right after second foul
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=26,
            event_type=EventType.SUBSTITUTION,
            seconds_elapsed=305.0,
            time_remaining="06:55",
            team_tricode="LAL", 
            player1_name_slug="LebronJames",
            player1_display_name="LeBron James",
            description="LeBron James substituted out",
            source="test",
            source_url="https://test.com"
        ),
    ]
    
    print("🔍 Testing early foul trouble detection...")
    print(f"Threshold: 360 seconds (6:00)")
    print(f"LeBron foul 1: {events[0].seconds_elapsed}s elapsed")
    print(f"LeBron foul 2: {events[1].seconds_elapsed}s elapsed")
    print(f"Both within threshold: {events[0].seconds_elapsed <= 360 and events[1].seconds_elapsed <= 360}")
    
    transformer = EarlyShocksTransformer(source="debug")
    early_shocks = transformer.transform(events, "https://test.com")
    
    print(f"\n📊 Results:")
    print(f"Total shocks detected: {len(early_shocks)}")
    for shock in early_shocks:
        print(f"  - {shock.shock_type.value}: {shock.player_slug} ({shock.team_tricode})")
        print(f"    Events: {shock.event_idx_start} -> {shock.event_idx_end}")
        print(f"    Immediate sub: {shock.immediate_sub}")
    
    # Test the individual methods
    print(f"\n🔬 Debug individual detection:")
    
    # Test _is_technical_or_flagrant_foul for each event
    for i, event in enumerate(events):
        is_tech_flagrant = transformer._is_technical_or_flagrant_foul(event)
        print(f"Event {i+1} ({event.event_type.value}): is_technical_or_flagrant = {is_tech_flagrant}")
    
    # Test early foul detection specifically
    early_foul_shocks = transformer._detect_early_foul_trouble(events, "https://test.com")
    print(f"Early foul shocks: {len(early_foul_shocks)}")
    
    return len(early_shocks)

if __name__ == "__main__":
    result = test_early_foul_detection()
    print(f"\n✅ Test completed with {result} shocks detected")
