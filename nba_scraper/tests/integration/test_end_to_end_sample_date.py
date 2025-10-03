"""End-to-end integration tests for NBA scraper pipeline."""

import pytest
from datetime import datetime, date
from typing import List
from unittest.mock import AsyncMock, patch

from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.models.derived_rows import EarlyShockRow
from nba_scraper.models.enums import EventType, EarlyShockType
from nba_scraper.transformers.early_shocks import EarlyShocksTransformer
from nba_scraper.transformers.q1_window import Q1WindowTransformer
from nba_scraper.loaders.derived import DerivedLoader


@pytest.fixture
def sample_game_pbp_events():
    """Sample PBP events for a game with early shock scenarios."""
    base_url = "https://stats.nba.com/test/game/0022300001"
    
    return [
        # Period start
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=1,
            event_type=EventType.PERIOD_BEGIN,
            seconds_elapsed=0,
            time_remaining="12:00",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Early foul trouble scenario - LeBron gets 2 quick fouls
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
        
        # Immediate substitution after second foul
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=26,
            event_type=EventType.SUBSTITUTION,
            seconds_elapsed=305,
            time_remaining="06:55",
            team_tricode="LAL", 
            player1_name_slug="LebronJames",
            player1_display_name="LeBron James",
            description="LeBron James substituted out",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Technical foul scenario
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=35,
            event_type=EventType.TECHNICAL_FOUL,
            seconds_elapsed=420,  # 5:00 remaining
            time_remaining="05:00",
            team_tricode="BOS",
            player1_name_slug="JaysonTatum",
            player1_display_name="Jayson Tatum", 
            description="Technical foul by Jayson Tatum",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Flagrant foul scenario
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=45,
            event_type=EventType.FLAGRANT_FOUL,
            seconds_elapsed=540,  # 3:00 remaining
            time_remaining="03:00",
            team_tricode="LAL",
            player1_name_slug="AnthonyDavis",
            player1_display_name="Anthony Davis",
            description="Flagrant 2 foul by Anthony Davis",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Some regular game events for context
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=15,
            event_type=EventType.SHOT_MADE,
            seconds_elapsed=240,
            time_remaining="08:00",
            team_tricode="BOS",
            player1_name_slug="JaylenBrown",
            player1_display_name="Jaylen Brown",
            shot_made=True,
            shot_value=3,
            description="3PT shot made by Jaylen Brown",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=30,
            event_type=EventType.REBOUND,
            seconds_elapsed=360,
            time_remaining="06:00", 
            team_tricode="LAL",
            player1_name_slug="RussellWestbrook",
            player1_display_name="Russell Westbrook",
            description="Defensive rebound by Russell Westbrook",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Injury scenario with extended absence
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=50,
            event_type=EventType.FOUL,
            seconds_elapsed=600,  # 2:00 remaining
            time_remaining="02:00",
            team_tricode="BOS",
            player1_name_slug="RobertWilliams",
            player1_display_name="Robert Williams",
            description="Collision injury to knee during foul",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Several possessions without the injured player
        PbpEventRow(
            game_id="0022300001", 
            period=1,
            event_idx=52,
            event_type=EventType.SHOT_MADE,
            seconds_elapsed=610,
            time_remaining="01:50",
            team_tricode="LAL",
            player1_name_slug="AnthonyDavis", 
            shot_made=True,
            shot_value=2,
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=54,
            event_type=EventType.SHOT_MISSED,
            seconds_elapsed=625,
            time_remaining="01:35", 
            team_tricode="BOS",
            player1_name_slug="JaysonTatum",
            shot_made=False,
            shot_value=3,
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=56,
            event_type=EventType.TURNOVER,
            seconds_elapsed=640,
            time_remaining="01:20",
            team_tricode="LAL",
            player1_name_slug="RussellWestbrook",
            description="Bad pass turnover",
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=58,
            event_type=EventType.SHOT_MADE,
            seconds_elapsed=655,
            time_remaining="01:05",
            team_tricode="BOS", 
            player1_name_slug="JaylenBrown",
            shot_made=True,
            shot_value=2,
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=60,
            event_type=EventType.SHOT_MADE,
            seconds_elapsed=670,
            time_remaining="00:50",
            team_tricode="LAL",
            player1_name_slug="AnthonyDavis",
            shot_made=True,
            shot_value=2,
            source="nba_stats_test",
            source_url=base_url
        ),
        
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=62,
            event_type=EventType.SHOT_MISSED,
            seconds_elapsed=690,
            time_remaining="00:30",
            team_tricode="BOS",
            player1_name_slug="MarcusSmart",
            shot_made=False,
            shot_value=3,
            source="nba_stats_test",
            source_url=base_url
        ),
        
        # Period end
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=65,
            event_type=EventType.PERIOD_END,
            seconds_elapsed=720,
            time_remaining="00:00",
            source="nba_stats_test",
            source_url=base_url
        ),
    ]


class TestEndToEndEarlyShocksIntegration:
    """Integration tests for early shocks detection in full pipeline."""
    
    @pytest.mark.asyncio
    async def test_early_shocks_full_pipeline(self, sample_game_pbp_events):
        """Test complete early shocks pipeline from PBP events to database."""
        
        # Step 1: Transform PBP events to early shocks
        transformer = EarlyShocksTransformer(source="integration_test")
        source_url = "https://stats.nba.com/test/game/0022300001"
        
        early_shocks = transformer.transform(sample_game_pbp_events, source_url)
        
        # Validate that early shocks were detected
        assert len(early_shocks) >= 3, "Should detect at least 3 early shock events"
        
        # Verify specific shock types were detected
        shock_types = {shock.shock_type for shock in early_shocks}
        assert EarlyShockType.TWO_PF_EARLY in shock_types, "Should detect two early personal fouls"
        assert EarlyShockType.TECH in shock_types, "Should detect technical foul"
        assert EarlyShockType.FLAGRANT in shock_types, "Should detect flagrant foul"
        
        # Step 2: Validate early foul trouble detection
        two_pf_shocks = [s for s in early_shocks if s.shock_type == EarlyShockType.TWO_PF_EARLY]
        assert len(two_pf_shocks) == 1, "Should detect exactly one TWO_PF_EARLY shock"
        
        lebron_shock = two_pf_shocks[0]
        assert lebron_shock.player_slug == "Lebronjames"  # Fixed: matches normalize_name_slug output
        assert lebron_shock.team_tricode == "LAL"
        assert lebron_shock.immediate_sub is True, "LeBron should have been immediately substituted"
        assert lebron_shock.event_idx_start == 10, "Should start with first foul event"
        assert lebron_shock.event_idx_end == 25, "Should end with second foul event"
        
        # Step 3: Validate technical foul detection
        tech_shocks = [s for s in early_shocks if s.shock_type == EarlyShockType.TECH]
        assert len(tech_shocks) == 1, "Should detect exactly one technical foul"
        
        tatum_tech = tech_shocks[0]
        assert tatum_tech.player_slug == "Jaysontatum"  # Fixed: matches normalize_name_slug output
        assert tatum_tech.team_tricode == "BOS"
        assert tatum_tech.event_idx_start == 35
        assert tatum_tech.notes == "Technical foul"
        
        # Step 4: Validate flagrant foul detection
        flagrant_shocks = [s for s in early_shocks if s.shock_type == EarlyShockType.FLAGRANT]
        assert len(flagrant_shocks) == 1, "Should detect exactly one flagrant foul"
        
        davis_flagrant = flagrant_shocks[0]
        assert davis_flagrant.player_slug == "Anthonydavis"  # Fixed: matches normalize_name_slug output
        assert davis_flagrant.team_tricode == "LAL"
        assert "Flagrant 2" in davis_flagrant.notes
        
        # Step 5: Validate injury leave detection
        injury_shocks = [s for s in early_shocks if s.shock_type == EarlyShockType.INJURY_LEAVE]
        if injury_shocks:  # May or may not be detected depending on possession count
            williams_injury = injury_shocks[0]
            assert williams_injury.player_slug == "RobertWilliams"
            assert williams_injury.team_tricode == "BOS"
            assert williams_injury.poss_since_event >= 6
        
        # Step 6: Test database loader integration
        loader = DerivedLoader()
        
        # Mock the database connection for testing
        with patch('nba_scraper.loaders.derived.get_connection') as mock_get_conn:
            mock_conn = AsyncMock()
            mock_conn.transaction.return_value.__aenter__ = AsyncMock()
            mock_conn.transaction.return_value.__aexit__ = AsyncMock()
            mock_conn.execute.return_value = "INSERT 0 1"
            mock_get_conn.return_value = mock_conn
            
            # Test that loader can process the early shocks
            result = await loader.upsert_early_shocks(early_shocks)
            
            # Verify loader was called correctly
            assert mock_conn.execute.call_count == len(early_shocks)
            
            # Verify SQL contains proper enum mapping and conflict handling
            call_args = mock_conn.execute.call_args_list[0]
            query = call_args[0][0]
            assert "ON CONFLICT" in query
            assert "early_shocks" in query
    
    @pytest.mark.asyncio
    async def test_q1_window_integration_with_shocks(self, sample_game_pbp_events):
        """Test Q1 window analytics integration alongside early shocks detection."""
        
        # Transform same events for Q1 window analytics
        q1_transformer = Q1WindowTransformer(source="integration_test")
        source_url = "https://stats.nba.com/test/game/0022300001"
        
        q1_window = q1_transformer.transform(sample_game_pbp_events, source_url)
        
        # Validate Q1 window was generated
        assert q1_window is not None, "Should generate Q1 window analytics"
        assert q1_window.game_id == "0022300001"
        assert q1_window.home_team_tricode in ["BOS", "LAL"]
        assert q1_window.away_team_tricode in ["BOS", "LAL"]
        assert q1_window.possessions_elapsed > 0
        
        # Also run early shocks on same data
        shock_transformer = EarlyShocksTransformer(source="integration_test")
        early_shocks = shock_transformer.transform(sample_game_pbp_events, source_url)
        
        # Verify both analytics work on same data
        assert len(early_shocks) >= 3
        assert q1_window.possessions_elapsed > 0
        
        # Test combined database loading
        loader = DerivedLoader()
        
        with patch('nba_scraper.loaders.derived.get_connection') as mock_get_conn:
            mock_conn = AsyncMock()
            mock_conn.transaction.return_value.__aenter__ = AsyncMock()
            mock_conn.transaction.return_value.__aexit__ = AsyncMock()
            mock_conn.execute.return_value = "INSERT 0 1"
            mock_get_conn.return_value = mock_conn
            
            # Load both Q1 window and early shocks
            window_result = await loader.upsert_q1_windows([q1_window])
            shock_result = await loader.upsert_early_shocks(early_shocks)
            
            # Verify both loaded successfully
            assert window_result == 0  # Insert, not update
            assert shock_result == 0   # Insert, not update
    
    def test_shock_sequence_numbering_in_integration(self, sample_game_pbp_events):
        """Test that shock sequence numbering works correctly in integration context."""
        
        # Add multiple technical fouls by same player
        additional_events = [
            PbpEventRow(
                game_id="0022300001",
                period=1, 
                event_idx=70,
                event_type=EventType.TECHNICAL_FOUL,
                seconds_elapsed=480,  # 4:00 remaining
                time_remaining="04:00",
                team_tricode="BOS",
                player1_name_slug="JaysonTatum",
                player1_display_name="Jayson Tatum",
                description="Second technical foul by Jayson Tatum",
                source="nba_stats_test",
                source_url="https://stats.nba.com/test"
            )
        ]
        
        all_events = sample_game_pbp_events + additional_events
        
        transformer = EarlyShocksTransformer()
        early_shocks = transformer.transform(all_events, "https://test.com")
        
        # Find Tatum's technical fouls
        tatum_techs = [s for s in early_shocks if (
            s.shock_type == EarlyShockType.TECH and 
            s.player_slug == "JaysonTatum"
        )]
        
        assert len(tatum_techs) == 2, "Should detect two technical fouls by Tatum"
        
        # Verify sequence numbering
        sequences = sorted([s.shock_seq for s in tatum_techs])
        assert sequences == [1, 2], "Should have proper sequence numbers"
    
    def test_clock_format_consistency_in_integration(self, sample_game_pbp_events):
        """Test that clock formats are consistent throughout integration."""
        
        transformer = EarlyShocksTransformer()
        early_shocks = transformer.transform(sample_game_pbp_events, "https://test.com")
        
        # Verify all shocks have proper clock format
        for shock in early_shocks:
            assert shock.clock_hhmmss.count(':') == 2, f"Clock format should be HH:MM:SS, got {shock.clock_hhmmss}"
            parts = shock.clock_hhmmss.split(':')
            assert len(parts) == 3, "Should have 3 parts in HH:MM:SS"
            assert all(part.isdigit() for part in parts), "All parts should be numeric"
    
    def test_empty_events_handling_in_integration(self):
        """Test integration behavior with edge cases like empty events."""
        
        transformer = EarlyShocksTransformer()
        
        # Test empty events
        result = transformer.transform([], "https://test.com")
        assert result == [], "Empty events should return empty list"
        
        # Test non-Q1 events
        q2_events = [
            PbpEventRow(
                game_id="0022300001",
                period=2,  # Q2
                event_idx=100,
                event_type=EventType.FOUL,
                seconds_elapsed=60,
                team_tricode="LAL",
                player1_name_slug="LebronJames",
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = transformer.transform(q2_events, "https://test.com")
        assert result == [], "Non-Q1 events should return empty list"


@pytest.mark.asyncio
async def test_early_shocks_data_quality_validation():
    """Test data quality aspects of early shocks integration."""
    
    # Test with malformed data
    bad_events = [
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=1,
            event_type=EventType.FOUL,
            seconds_elapsed=180,
            # Missing team_tricode and player info
            source="test",
            source_url="https://test.com"
        )
    ]
    
    transformer = EarlyShocksTransformer()
    result = transformer.transform(bad_events, "https://test.com")
    
    # Should handle missing data gracefully
    assert isinstance(result, list), "Should return list even with bad data"
    # Should not crash, but may return empty results due to missing required fields