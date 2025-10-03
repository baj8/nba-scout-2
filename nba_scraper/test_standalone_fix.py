#!/usr/bin/env python3
"""Test early shocks detection without model preprocessing interference."""

import sys
sys.path.insert(0, 'src')

# Direct imports to avoid preprocessing
from nba_scraper.models.enums import EventType, EarlyShockType
from nba_scraper.transformers.early_shocks import EarlyShocksTransformer
from pydantic import BaseModel, Field

# Create a minimal event model without preprocessing
class SimpleEvent(BaseModel):
    game_id: str
    period: int
    event_idx: int
    event_type: EventType
    seconds_elapsed: float
    team_tricode: str
    player1_name_slug: str
    description: str
    source: str = "test"
    source_url: str = "https://test.com"

def test_simple_early_foul_detection():
    """Test with simple events that don't go through model preprocessing."""
    
    # Create simple events that match the transformer's expected interface
    events = [
        SimpleEvent(
            game_id="0022300001",
            period=1,
            event_idx=10,
            event_type=EventType.FOUL,  # Direct enum assignment
            seconds_elapsed=180.0,
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            description="Personal foul by LeBron James"
        ),
        SimpleEvent(
            game_id="0022300001",
            period=1,
            event_idx=25,
            event_type=EventType.FOUL,  # Direct enum assignment
            seconds_elapsed=300.0,
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            description="Personal foul by LeBron James"
        ),
        SimpleEvent(
            game_id="0022300001",
            period=1,
            event_idx=26,
            event_type=EventType.SUBSTITUTION,
            seconds_elapsed=305.0,
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            description="LeBron James substituted out"
        ),
    ]
    
    print("🧪 Testing with simple events (no preprocessing):")
    for i, event in enumerate(events):
        print(f"Event {i+1}: {event.event_type.value} - {event.player1_name_slug}")
    
    # Test the transformer
    transformer = EarlyShocksTransformer(source="simple_test", early_foul_threshold_sec=360.0)
    
    # Test early foul detection directly
    early_foul_shocks = transformer._detect_early_foul_trouble(events, "https://test.com")
    print(f"\n📊 Early foul shocks detected: {len(early_foul_shocks)}")
    
    for shock in early_foul_shocks:
        print(f"  - {shock.shock_type.value}: {shock.player_slug} ({shock.team_tricode})")
        print(f"    Events: {shock.event_idx_start} -> {shock.event_idx_end}")
        print(f"    Immediate sub: {shock.immediate_sub}")
        print(f"    Notes: {shock.notes}")
    
    # Test full transform
    all_shocks = transformer.transform(events, "https://test.com")
    print(f"\n🚀 Total shocks from full transform: {len(all_shocks)}")
    
    return len(all_shocks)

if __name__ == "__main__":
    result = test_simple_early_foul_detection()
    print(f"\n✅ Simple test completed with {result} shocks detected")
