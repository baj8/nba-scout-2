"""Tests for early shocks transformer."""

import pytest
from typing import List
from unittest.mock import patch

from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.models.derived_rows import EarlyShockRow
from nba_scraper.models.enums import EarlyShockType, EventType, FoulType
from nba_scraper.transformers.early_shocks import EarlyShocksTransformer


class TestEarlyShocksTransformer:
    """Test cases for EarlyShocksTransformer."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.transformer = EarlyShocksTransformer(source="test_pbp")
        self.base_source_url = "https://stats.nba.com/test"
    
    def _create_pbp_event(
        self,
        event_idx: int,
        event_type: EventType,
        seconds_elapsed: float,
        player1_name_slug: str = None,
        team_tricode: str = "LAL",
        description: str = None
    ) -> PbpEventRow:
        """Helper to create PBP event for testing."""
        return PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=event_idx,
            time_remaining="11:00",
            seconds_elapsed=seconds_elapsed,
            event_type=event_type,
            description=description,
            team_tricode=team_tricode,
            player1_name_slug=player1_name_slug,
            player1_display_name=f"Player {player1_name_slug}" if player1_name_slug else None,
            source="test",
            source_url=self.base_source_url
        )
    
    def test_two_early_personal_fouls_detected(self):
        """Test detection of two personal fouls by same player before 6:00."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # First foul at 5:00 elapsed (7:00 remaining)
            self._create_pbp_event(10, EventType.FOUL, 300, "LebronJames", "LAL", "Personal foul"),
            # Second foul at 5:30 elapsed (6:30 remaining) 
            self._create_pbp_event(20, EventType.FOUL, 330, "LebronJames", "LAL", "Personal foul"),
            # Some other events
            self._create_pbp_event(30, EventType.SHOT_MADE, 400, "AnthonyDavis", "LAL"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        # Should detect one TWO_PF_EARLY shock
        two_pf_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TWO_PF_EARLY]
        assert len(two_pf_shocks) == 1
        
        shock = two_pf_shocks[0]
        assert shock.game_id == "0022300001"
        assert shock.team_tricode == "LAL"
        assert shock.player_slug == "LebronJames"
        assert shock.shock_seq == 1
        assert shock.period == 1
        assert shock.event_idx_start == 10
        assert shock.event_idx_end == 20
        assert "2 PF in 330.0s" in shock.notes
    
    def test_two_fouls_after_threshold_not_detected(self):
        """Test that two fouls after 6:00 elapsed don't trigger early shock."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # First foul at 4:00 elapsed (8:00 remaining) - within threshold
            self._create_pbp_event(10, EventType.FOUL, 240, "LebronJames", "LAL", "Personal foul"),
            # Second foul at 7:00 elapsed (5:00 remaining) - after threshold
            self._create_pbp_event(20, EventType.FOUL, 420, "LebronJames", "LAL", "Personal foul"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        # Should NOT detect TWO_PF_EARLY shock because second foul is after 6:00 elapsed
        two_pf_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TWO_PF_EARLY]
        assert len(two_pf_shocks) == 0
    
    def test_technical_foul_detected(self):
        """Test detection of technical foul in Q1."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            self._create_pbp_event(15, EventType.TECHNICAL_FOUL, 200, "LebronJames", "LAL", "Technical foul"),
            self._create_pbp_event(25, EventType.SHOT_MADE, 250, "AnthonyDavis", "LAL"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        tech_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TECH]
        assert len(tech_shocks) == 1
        
        shock = tech_shocks[0]
        assert shock.player_slug == "LebronJames"
        assert shock.team_tricode == "LAL"
        assert shock.shock_seq == 1
        assert shock.event_idx_start == 15
        assert shock.notes == "Technical foul"
    
    def test_flagrant_foul_detected(self):
        """Test detection of flagrant foul in Q1."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            self._create_pbp_event(18, EventType.FLAGRANT_FOUL, 180, "RussellWestbrook", "LAL", "Flagrant 1 foul"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        flagrant_shocks = [s for s in shocks if s.shock_type == EarlyShockType.FLAGRANT]
        assert len(flagrant_shocks) == 1
        
        shock = flagrant_shocks[0]
        assert shock.player_slug == "RussellWestbrook"
        assert shock.team_tricode == "LAL"
        assert shock.shock_seq == 1
        assert shock.event_idx_start == 18
        assert "Flagrant 1" in shock.notes
    
    def test_injury_leave_detected(self):
        """Test detection of injury leave when player doesn't return for 6+ possessions."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Injury event
            self._create_pbp_event(10, EventType.FOUL, 150, "AnthonyDavis", "LAL", "Collision injury to ankle"),
            # Several possession changes without the injured player
            self._create_pbp_event(11, EventType.SHOT_MADE, 160, "LebronJames", "LAL"),  # Possession 1
            self._create_pbp_event(12, EventType.SHOT_MISSED, 170, "JaysonTatum", "BOS"),  # Possession 2
            self._create_pbp_event(13, EventType.REBOUND, 175, "JaylenBrown", "BOS"),
            self._create_pbp_event(14, EventType.SHOT_MADE, 180, "JaylenBrown", "BOS"),  # Possession 3
            self._create_pbp_event(15, EventType.SHOT_MISSED, 190, "LebronJames", "LAL"),  # Possession 4
            self._create_pbp_event(16, EventType.REBOUND, 195, "RussellWestbrook", "LAL"),
            self._create_pbp_event(17, EventType.TURNOVER, 200, "RussellWestbrook", "LAL"),  # Possession 5
            self._create_pbp_event(18, EventType.SHOT_MADE, 210, "JaysonTatum", "BOS"),  # Possession 6
            self._create_pbp_event(19, EventType.SHOT_MISSED, 220, "LebronJames", "LAL"),  # Possession 7
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        injury_shocks = [s for s in shocks if s.shock_type == EarlyShockType.INJURY_LEAVE]
        assert len(injury_shocks) == 1
        
        shock = injury_shocks[0]
        assert shock.player_slug == "AnthonyDavis"
        assert shock.team_tricode == "LAL"
        assert shock.event_idx_start == 10
        assert shock.poss_since_event >= 6
        assert "Absent" in shock.notes
    
    def test_immediate_substitution_detected(self):
        """Test detection of immediate substitution after multiple early fouls."""
        detector = EarlyShocksTransformer()
        
        # Add multiple fouls first to trigger TWO_PF_EARLY
        detector.process_event(PBPEvent(
            event_id="foul1",
            game_id="test_game",
            period=1,
            game_clock="11:30",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-early-fouls",
            description="Personal foul by Player Early Fouls"
        ))
        
        detector.process_event(PBPEvent(
            event_id="foul2", 
            game_id="test_game",
            period=1,
            game_clock="10:45",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-early-fouls",
            description="Personal foul by Player Early Fouls"
        ))
        
        # Now add immediate substitution
        detector.process_event(PBPEvent(
            event_id="sub1",
            game_id="test_game", 
            period=1,
            game_clock="10:44",
            event_type=EventType.SUBSTITUTION,
            player1_team_id="team_a",
            player1_name_slug="player-early-fouls",
            player2_name_slug="replacement-player",
            description="Substitution: Player Early Fouls replaced by Replacement Player"
        ))
        
        shocks = detector.get_detected_shocks()
        shock_types = [s.shock_type for s in shocks]
        
        # Should detect both early fouls and immediate substitution
        assert ShockType.TWO_PF_EARLY in shock_types
        assert ShockType.SUB_IMMEDIATE in shock_types
    
    def test_multiple_shock_types_same_game(self):
        """Test that multiple shock types are detected in same game."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Two early fouls
            self._create_pbp_event(10, EventType.FOUL, 200, "LebronJames", "LAL", "Personal foul"),
            self._create_pbp_event(12, EventType.FOUL, 250, "LebronJames", "LAL", "Personal foul"),
            # Technical foul by different player
            self._create_pbp_event(15, EventType.TECHNICAL_FOUL, 280, "RussellWestbrook", "LAL", "Technical foul"),
            # Flagrant by opponent
            self._create_pbp_event(20, EventType.FLAGRANT_FOUL, 320, "JaysonTatum", "BOS", "Flagrant 2"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        # Should detect 3 different shock types
        shock_types = {s.shock_type for s in shocks}
        assert EarlyShockType.TWO_PF_EARLY in shock_types
        assert EarlyShockType.TECH in shock_types
        assert EarlyShockType.FLAGRANT in shock_types
        assert len(shocks) == 3
    
    def test_technical_fouls_by_description(self):
        """Test technical foul detection via description parsing."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Regular foul with technical in description
            self._create_pbp_event(10, EventType.FOUL, 200, "LebronJames", "LAL", "Technical foul on player"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        tech_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TECH]
        assert len(tech_shocks) == 1
    
    def test_flagrant_foul_type_extraction(self):
        """Test extraction of flagrant foul type from description."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            self._create_pbp_event(10, EventType.FOUL, 200, "Player1", "LAL", "Flagrant 2 foul excessive contact"),
            self._create_pbp_event(20, EventType.FOUL, 300, "Player2", "BOS", "Flagrant 1 foul unnecessary contact"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        flagrant_shocks = [s for s in shocks if s.shock_type == EarlyShockType.FLAGRANT]
        assert len(flagrant_shocks) == 2
        
        # Check notes contain correct flagrant types
        notes = [s.notes for s in flagrant_shocks]
        assert any("Flagrant 2" in note for note in notes)
        assert any("Flagrant 1" in note for note in notes)
    
    def test_no_q1_events_returns_empty(self):
        """Test that no Q1 events returns empty list."""
        events = [
            # Only Q2 events
            PbpEventRow(
                game_id="0022300001",
                period=2,
                event_idx=100,
                event_type=EventType.FOUL,
                team_tricode="LAL",
                player1_name_slug="LebronJames",
                source="test",
                source_url=self.base_source_url
            )
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        assert len(shocks) == 0
    
    def test_empty_events_returns_empty(self):
        """Test that empty events list returns empty."""
        shocks = self.transformer.transform([], self.base_source_url)
        assert len(shocks) == 0
    
    def test_clock_formatting(self):
        """Test proper clock formatting."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            self._create_pbp_event(10, EventType.TECHNICAL_FOUL, 300, "LebronJames", "LAL", "Technical foul"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        # 300 seconds elapsed = 5:00 elapsed, so 7:00 remaining
        assert shocks[0].clock_hhmmss == "00:07:00"
    
    def test_shock_sequence_numbering(self):
        """Test that multiple shocks of same type get proper sequence numbers."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Two technical fouls by same player
            self._create_pbp_event(10, EventType.TECHNICAL_FOUL, 200, "LebronJames", "LAL", "Technical foul"),
            self._create_pbp_event(20, EventType.TECHNICAL_FOUL, 400, "LebronJames", "LAL", "Technical foul"),
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        tech_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TECH]
        assert len(tech_shocks) == 2
        
        # Should have sequence numbers 1 and 2
        sequences = sorted([s.shock_seq for s in tech_shocks])
        assert sequences == [1, 2]
    
    def test_edge_case_missing_player_info(self):
        """Test handling of events with missing player information."""
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Foul without player name
            PbpEventRow(
                game_id="0022300001",
                period=1,
                event_idx=10,
                event_type=EventType.FOUL,
                seconds_elapsed=200,
                team_tricode="LAL",
                player1_name_slug=None,  # Missing player
                description="Personal foul",
                source="test",
                source_url=self.base_source_url
            )
        ]
        
        shocks = self.transformer.transform(events, self.base_source_url)
        
        # Should not crash and should not detect any shocks without player info
        two_pf_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TWO_PF_EARLY]
        assert len(two_pf_shocks) == 0
    
    def test_configurable_early_foul_threshold(self):
        """Test that early foul threshold is configurable."""
        # Create transformer with 4:00 threshold (240 seconds)
        transformer = EarlyShocksTransformer(early_foul_threshold_sec=240.0)
        
        events = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Two fouls at 3:00 and 3:30 elapsed (within new threshold)
            self._create_pbp_event(10, EventType.FOUL, 180, "LebronJames", "LAL", "Personal foul"),
            self._create_pbp_event(20, EventType.FOUL, 210, "LebronJames", "LAL", "Personal foul"),
        ]
        
        shocks = transformer.transform(events, self.base_source_url)
        
        two_pf_shocks = [s for s in shocks if s.shock_type == EarlyShockType.TWO_PF_EARLY]
        assert len(two_pf_shocks) == 1
        
        # Now test with fouls outside new threshold
        events2 = [
            self._create_pbp_event(1, EventType.PERIOD_BEGIN, 0),
            # Two fouls at 3:00 and 5:00 elapsed (second outside threshold)
            self._create_pbp_event(10, EventType.FOUL, 180, "LebronJames", "LAL", "Personal foul"),
            self._create_pbp_event(20, EventType.FOUL, 300, "LebronJames", "LAL", "Personal foul"),
        ]
        
        shocks2 = transformer.transform(events2, self.base_source_url)
        two_pf_shocks2 = [s for s in shocks2 if s.shock_type == EarlyShockType.TWO_PF_EARLY]
        assert len(two_pf_shocks2) == 0

    def test_two_fouls_early_detected(self):
        """Test detection of two fouls in first 3 minutes."""
        detector = EarlyShocksTransformer()
        
        # First foul at 2:30 remaining (9:30 elapsed)
        detector.process_event(PBPEvent(
            event_id="foul1",
            game_id="test_game",
            period=1,
            game_clock="09:30",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-two-fouls",
            description="Personal foul by Player Two Fouls"
        ))
        
        # Second foul at 1:45 remaining (10:15 elapsed) 
        detector.process_event(PBPEvent(
            event_id="foul2",
            game_id="test_game", 
            period=1,
            game_clock="01:45",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-two-fouls",
            description="Personal foul by Player Two Fouls"
        ))
        
        shocks = detector.get_detected_shocks()
        assert len(shocks) == 1
        assert shocks[0].shock_type == ShockType.TWO_PF_EARLY
        assert shocks[0].player_name_slug == "player-two-fouls"

    def test_three_fouls_early_detected(self):
        """Test detection of three fouls in first 3 minutes."""
        detector = EarlyShocksTransformer()
        
        # First foul at 2:30 remaining (9:30 elapsed)
        detector.process_event(PBPEvent(
            event_id="foul1",
            game_id="test_game",
            period=1,
            game_clock="09:30",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-three-fouls",
            description="Personal foul by Player Three Fouls"
        ))
        
        # Second foul at 1:45 remaining (10:15 elapsed)
        detector.process_event(PBPEvent(
            event_id="foul2", 
            game_id="test_game",
            period=1,
            game_clock="01:45",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-three-fouls",
            description="Personal foul by Player Three Fouls"
        ))
        
        # Third foul at 1:00 remaining (11:00 elapsed)
        detector.process_event(PBPEvent(
            event_id="foul3",
            game_id="test_game",
            period=1, 
            game_clock="01:00",
            event_type=EventType.FOUL,
            player1_team_id="team_a",
            player1_name_slug="player-three-fouls",
            description="Personal foul by Player Three Fouls"
        ))
        
        shocks = detector.get_detected_shocks()
        assert len(shocks) == 2  # Both TWO_PF_EARLY and THREE_PF_EARLY
        shock_types = [shock.shock_type for shock in shocks]
        assert ShockType.TWO_PF_EARLY in shock_types
        assert ShockType.THREE_PF_EARLY in shock_types


@pytest.fixture
def sample_pbp_events():
    """Sample PBP events for integration testing."""
    return [
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=1,
            event_type=EventType.PERIOD_BEGIN,
            seconds_elapsed=0,
            source="test",
            source_url="https://stats.nba.com/test"
        ),
        PbpEventRow(
            game_id="0022300001", 
            period=1,
            event_idx=10,
            event_type=EventType.FOUL,
            seconds_elapsed=250,
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            description="Personal foul",
            source="test",
            source_url="https://stats.nba.com/test"
        ),
        PbpEventRow(
            game_id="0022300001",
            period=1, 
            event_idx=20,
            event_type=EventType.FOUL,
            seconds_elapsed=300,
            team_tricode="LAL",
            player1_name_slug="LebronJames",
            description="Personal foul",
            source="test",
            source_url="https://stats.nba.com/test"
        )
    ]


def test_early_shocks_integration(sample_pbp_events):
    """Integration test for early shocks detection."""
    transformer = EarlyShocksTransformer()
    shocks = transformer.transform(sample_pbp_events, "https://stats.nba.com/test")
    
    # Should detect at least one early shock from the sample data
    assert len(shocks) >= 1
    assert all(isinstance(shock, EarlyShockRow) for shock in shocks)
    assert all(shock.period == 1 for shock in shocks)
    assert all(shock.source == "pbp_derived" for shock in shocks)


class TestEarlyShocksTransformer:
    """Test the EarlyShocksTransformer class."""

    def test_init(self):
        """Test transformer initialization."""
        transformer = EarlyShocksTransformer()
        assert transformer.source == "pbp_derived"
        assert transformer.early_foul_threshold_sec == 360.0

    def test_transform_empty_events(self):
        """Test transform with empty events list."""
        transformer = EarlyShocksTransformer()
        result = transformer.transform([], "http://test.com")
        assert result == []

    def test_transform_no_q1_events(self):
        """Test transform with no Q1 events."""
        transformer = EarlyShocksTransformer()
        
        # Create a Q2 event
        event = PbpEventRow(
            game_id="test_game",
            event_idx=1,
            period=2,  # Q2
            seconds_elapsed=1800.0,
            event_type=EventType.PERSONAL_FOUL,
            description="Personal foul",
            team_tricode="LAL",
            player1_name_slug="lebron-james",  # Fixed: use player1_name_slug instead of player_slug
            source="test",  # Added required field
            source_url="https://test.com"  # Added required field
        )
        
        result = transformer.transform([event], "http://test.com")
        assert result == []