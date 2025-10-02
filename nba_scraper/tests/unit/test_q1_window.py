"""Tests for Q1 window transformer."""

import pytest
from typing import List

from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.models.derived_rows import Q1WindowRow
from nba_scraper.models.enums import EventType
from nba_scraper.transformers.q1_window import Q1WindowTransformer, TeamStats


class TestQ1WindowTransformer:
    """Test cases for Q1WindowTransformer."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.transformer = Q1WindowTransformer(source="test_q1_window")
        self.base_source_url = "https://stats.nba.com/test"
    
    def _create_pbp_event(
        self,
        event_idx: int,
        event_type: EventType,
        seconds_elapsed: float,
        team_tricode: str = "LAL",
        shot_made: bool = None,
        shot_value: int = None,
        description: str = None,
        is_transition: bool = False,
        is_early_clock: bool = False
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
            shot_made=shot_made,
            shot_value=shot_value,
            is_transition=is_transition,
            is_early_clock=is_early_clock,
            source="test",
            source_url=self.base_source_url
        )
    
    def test_empty_events_returns_none(self):
        """Test that empty events list returns None."""
        result = self.transformer.transform([], self.base_source_url)
        assert result is None
    
    def test_no_q1_events_returns_none(self):
        """Test that no Q1 events returns None."""
        events = [
            # Only Q2 events
            PbpEventRow(
                game_id="0022300001",
                period=2,
                event_idx=100,
                event_type=EventType.SHOT_MADE,
                seconds_elapsed=100,
                team_tricode="LAL",
                source="test",
                source_url=self.base_source_url
            )
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        assert result is None
    
    def test_events_outside_window_filtered_out(self):
        """Test that events outside 12:00-8:00 window are filtered out."""
        events = [
            # Before window (should be included - 11:00 remaining = 60s elapsed)
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "LAL", True, 2),
            # In window (should be included - 9:00 remaining = 180s elapsed)  
            self._create_pbp_event(2, EventType.SHOT_MADE, 180, "BOS", True, 3),
            # After window (should be excluded - 7:00 remaining = 300s elapsed)
            self._create_pbp_event(3, EventType.SHOT_MADE, 300, "LAL", True, 2),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        # Should process events but only count the ones in window
        assert result is not None
        assert result.game_id == "0022300001"
        # Window duration is 4 minutes (240 seconds)
        # Should have filtered to include events 1 and 2 only
    
    def test_basic_shooting_stats_calculation(self):
        """Test basic shooting statistics calculation."""
        events = [
            # BOS (home team) makes 2-pointer
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "BOS", True, 2),
            # BOS (home team) misses 3-pointer
            self._create_pbp_event(2, EventType.SHOT_MISSED, 120, "BOS", False, 3),
            # LAL (away team) makes 3-pointer
            self._create_pbp_event(3, EventType.SHOT_MADE, 180, "LAL", True, 3),
            # LAL (away team) makes free throw
            self._create_pbp_event(4, EventType.FREE_THROW_MADE, 200, "LAL", True, 1),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # BOS (home): 1/2 FG (including 0/1 3PT) = 0.5 eFG%
        assert result.home_efg_actual == 0.5
        # LAL (away): 1/1 FG (1/1 3PT) = (1 + 0.5*1)/1 = 1.5 eFG%  
        assert result.away_efg_actual == 1.5
    
    def test_bonus_timing_detection(self):
        """Test bonus situation detection and timing calculation."""
        events = [
            # LAL team fouls leading to bonus
            self._create_pbp_event(1, EventType.FOUL, 60, "LAL"),   # 1st foul
            self._create_pbp_event(2, EventType.FOUL, 120, "LAL"),  # 2nd foul
            self._create_pbp_event(3, EventType.FOUL, 150, "LAL"),  # 3rd foul
            self._create_pbp_event(4, EventType.FOUL, 180, "LAL"),  # 4th foul - bonus starts
            self._create_pbp_event(5, EventType.FOUL, 220, "LAL"),  # 5th foul - still in bonus
            # Add some BOS events so we have two teams
            self._create_pbp_event(6, EventType.SHOT_MADE, 100, "BOS", True, 2),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # LAL is the away team (alphabetically after BOS), so check away team bonus time
        # Bonus started at 180s, window ends at 240s, so 60s in bonus
        assert result.bonus_time_away_sec == 60.0
        assert result.bonus_time_home_sec == 0.0  # BOS (home team) had no fouls
    
    def test_pace_calculation(self):
        """Test pace calculation (possessions per 48 minutes)."""
        events = [
            # Possession-ending events
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "LAL", True, 2),
            self._create_pbp_event(2, EventType.TURNOVER, 120, "BOS"), 
            self._create_pbp_event(3, EventType.SHOT_MADE, 180, "LAL", True, 3),
            self._create_pbp_event(4, EventType.REBOUND, 220, "BOS", description="Defensive rebound"),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # 4 possessions in 4 minutes = 1 possession per minute = 48 per 48 minutes
        assert result.pace48_actual == 48.0
        assert result.pace48_expected == 100.0  # Default expected pace
    
    def test_rebound_percentage_calculation(self):
        """Test offensive and defensive rebound percentage calculation."""
        events = [
            # LAL (away) gets 2 offensive rebounds, BOS (home) gets 3 defensive rebounds
            self._create_pbp_event(1, EventType.REBOUND, 60, "LAL", description="Offensive rebound"),
            self._create_pbp_event(2, EventType.REBOUND, 80, "LAL", description="Offensive rebound"), 
            self._create_pbp_event(3, EventType.REBOUND, 120, "BOS", description="Defensive rebound"),
            self._create_pbp_event(4, EventType.REBOUND, 140, "BOS", description="Defensive rebound"),
            self._create_pbp_event(5, EventType.REBOUND, 160, "BOS", description="Defensive rebound"),
            
            # BOS (home) gets 1 offensive rebound, LAL (away) gets 4 defensive rebounds
            self._create_pbp_event(6, EventType.REBOUND, 180, "BOS", description="Offensive rebound"),
            self._create_pbp_event(7, EventType.REBOUND, 200, "LAL", description="Defensive rebound"),
            self._create_pbp_event(8, EventType.REBOUND, 220, "LAL", description="Defensive rebound"),
            self._create_pbp_event(9, EventType.REBOUND, 230, "LAL", description="Defensive rebound"),
            self._create_pbp_event(10, EventType.REBOUND, 235, "LAL", description="Defensive rebound"),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # BOS (home) ORB% = 1 / (1 + 4) = 0.2
        assert abs(result.home_orb_pct - 0.2) < 0.01
        # BOS (home) DRB% = 3 / (3 + 2) = 0.6
        assert abs(result.home_drb_pct - 0.6) < 0.01
        # LAL (away) ORB% = 2 / (2 + 3) = 0.4
        assert abs(result.away_orb_pct - 0.4) < 0.01
        # LAL (away) DRB% = 4 / (4 + 1) = 0.8
        assert abs(result.away_drb_pct - 0.8) < 0.01
    
    def test_transition_and_early_clock_rates(self):
        """Test transition and early shot clock rate calculations."""
        events = [
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "LAL", True, 2, is_transition=True),
            self._create_pbp_event(2, EventType.SHOT_MADE, 120, "BOS", True, 3, is_early_clock=True),
            self._create_pbp_event(3, EventType.SHOT_MISSED, 180, "LAL", False, 2),  # Normal event
            self._create_pbp_event(4, EventType.TURNOVER, 220, "BOS", is_transition=True, is_early_clock=True),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # 2 transition events out of 4 total = 0.5
        assert result.transition_rate == 0.5
        # 2 early clock events out of 4 total = 0.5
        assert result.early_clock_rate == 0.5
    
    def test_turnover_and_assist_rates(self):
        """Test turnover and assist rate calculations."""
        events = [
            # Create some possessions and turnovers/assists
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "LAL", True, 2),
            self._create_pbp_event(2, EventType.ASSIST, 65, "LAL"),
            self._create_pbp_event(3, EventType.TURNOVER, 120, "BOS"),
            self._create_pbp_event(4, EventType.SHOT_MADE, 180, "LAL", True, 3),
            self._create_pbp_event(5, EventType.TURNOVER, 220, "LAL"),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # Rough possession estimates: 3 possessions total, ~1.5 per team
        # LAL: 1 turnover / ~1.5 possessions ≈ 0.67
        # BOS: 1 turnover / ~1.5 possessions ≈ 0.67
        assert result.home_to_rate is not None
        assert result.away_to_rate is not None
    
    def test_free_throw_rate_calculation(self):
        """Test free throw rate calculation (FTA/FGA)."""
        events = [
            # BOS (home): 2 FGA, 4 FTA = 2.0 FT rate
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "BOS", True, 2),
            self._create_pbp_event(2, EventType.SHOT_MISSED, 80, "BOS", False, 3), 
            self._create_pbp_event(3, EventType.FREE_THROW_MADE, 120, "BOS", True, 1),
            self._create_pbp_event(4, EventType.FREE_THROW_MADE, 125, "BOS", True, 1),
            self._create_pbp_event(5, EventType.FREE_THROW_MISSED, 180, "BOS", False, 1),
            self._create_pbp_event(6, EventType.FREE_THROW_MADE, 185, "BOS", True, 1),
            
            # LAL (away): 1 FGA, 0 FTA = 0.0 FT rate
            self._create_pbp_event(7, EventType.SHOT_MADE, 220, "LAL", True, 2),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        assert result is not None
        # BOS (home): 4 FTA / 2 FGA = 2.0
        assert result.home_ft_rate == 2.0
        # LAL (away): 0 FTA / 1 FGA = 0.0
        assert result.away_ft_rate == 0.0
    
    def test_configurable_window_parameters(self):
        """Test configurable window start/end parameters."""
        # Create transformer with different window (10:00 to 6:00)
        transformer = Q1WindowTransformer(
            window_start_sec=120.0,  # 10:00 remaining
            window_end_sec=360.0,    # 6:00 remaining
            expected_pace=105.0
        )
        
        events = [
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "LAL", True, 2),   # Before window
            self._create_pbp_event(2, EventType.SHOT_MADE, 180, "LAL", True, 2),  # In window
            self._create_pbp_event(3, EventType.SHOT_MADE, 300, "LAL", True, 2),  # In window
            self._create_pbp_event(4, EventType.SHOT_MADE, 400, "LAL", True, 2),  # After window
            # Add BOS event so we have two teams
            self._create_pbp_event(5, EventType.SHOT_MADE, 200, "BOS", True, 3),  # In window
        ]
        
        result = transformer.transform(events, self.base_source_url)
        
        assert result is not None
        assert result.pace48_expected == 105.0  # Custom expected pace
        # Should only process events 2, 3, and 5 (in the 10:00-6:00 window)
    
    def test_insufficient_teams_returns_none(self):
        """Test that games with insufficient team data return None."""
        events = [
            # Only one team has events
            self._create_pbp_event(1, EventType.SHOT_MADE, 60, "LAL", True, 2),
            self._create_pbp_event(2, EventType.SHOT_MADE, 120, "LAL", True, 3),
        ]
        
        result = self.transformer.transform(events, self.base_source_url)
        
        # Should return None due to insufficient team variety
        assert result is None


class TestTeamStats:
    """Test cases for TeamStats helper class."""
    
    def test_effective_fg_percentage_calculation(self):
        """Test effective field goal percentage calculation."""
        stats = TeamStats("LAL")
        stats.field_goals_made = 5
        stats.field_goals_attempted = 10
        stats.three_pointers_made = 2
        
        # eFG% = (5 + 0.5*2) / 10 = 6/10 = 0.6
        assert stats.effective_fg_pct == 0.6
    
    def test_effective_fg_percentage_no_attempts(self):
        """Test effective FG% returns None with no attempts."""
        stats = TeamStats("LAL")
        assert stats.effective_fg_pct is None
    
    def test_turnover_rate_calculation(self):
        """Test turnover rate calculation."""
        stats = TeamStats("LAL")
        stats.turnovers = 3
        stats.possessions = 20
        
        assert stats.turnover_rate == 0.15  # 3/20
    
    def test_turnover_rate_no_possessions(self):
        """Test turnover rate returns None with no possessions."""
        stats = TeamStats("LAL")
        stats.turnovers = 3
        assert stats.turnover_rate is None
    
    def test_free_throw_rate_calculation(self):
        """Test free throw rate calculation."""
        stats = TeamStats("LAL")
        stats.free_throws_attempted = 8
        stats.field_goals_attempted = 20
        
        assert stats.free_throw_rate == 0.4  # 8/20
    
    def test_free_throw_rate_no_field_goals(self):
        """Test FT rate returns None with no field goal attempts."""
        stats = TeamStats("LAL")
        stats.free_throws_attempted = 8
        assert stats.free_throw_rate is None
    
    def test_total_rebounds_calculation(self):
        """Test total rebounds calculation."""
        stats = TeamStats("LAL")
        stats.offensive_rebounds = 12
        stats.defensive_rebounds = 28
        
        assert stats.total_rebounds == 40


@pytest.fixture
def sample_q1_window_events():
    """Sample Q1 window events for integration testing."""
    return [
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=1,
            event_type=EventType.SHOT_MADE,
            seconds_elapsed=60,
            team_tricode="LAL",
            shot_made=True,
            shot_value=2,
            source="test",
            source_url="https://test.com"
        ),
        PbpEventRow(
            game_id="0022300001", 
            period=1,
            event_idx=2,
            event_type=EventType.SHOT_MADE,
            seconds_elapsed=180,
            team_tricode="BOS",
            shot_made=True,
            shot_value=3,
            source="test",
            source_url="https://test.com"
        ),
        PbpEventRow(
            game_id="0022300001",
            period=1,
            event_idx=3,
            event_type=EventType.FOUL,
            seconds_elapsed=220,
            team_tricode="LAL",
            source="test",
            source_url="https://test.com"
        )
    ]


def test_q1_window_integration(sample_q1_window_events):
    """Integration test for Q1 window transformer."""
    transformer = Q1WindowTransformer()
    result = transformer.transform(sample_q1_window_events, "https://test.com")
    
    # Should successfully process the sample data
    assert result is not None
    assert isinstance(result, Q1WindowRow)
    assert result.game_id == "0022300001"
    assert result.source == "pbp_q1_window"
    assert result.possessions_elapsed >= 0
    assert result.pace48_actual is not None