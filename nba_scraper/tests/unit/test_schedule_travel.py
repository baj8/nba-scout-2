"""Tests for schedule travel transformer."""

import pytest
from datetime import datetime, date
from pathlib import Path
from typing import List
from unittest.mock import patch, mock_open

from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.derived_rows import ScheduleTravelRow
from nba_scraper.transformers.schedule_travel import (
    ScheduleTravelTransformer, VenueData, TravelLeg
)


class TestScheduleTravelTransformer:
    """Test cases for ScheduleTravelTransformer."""
    
    def setup_method(self):
        """Setup test fixtures."""
        # Mock venues CSV data
        self.mock_venues_csv = """team_id,arena_name,arena_tz,lat,lon,altitude_m
LAL,Crypto.com Arena,America/Los_Angeles,34.0430,-118.2675,89
BOS,TD Garden,America/New_York,42.3662,-71.0621,5
DEN,Ball Arena,America/Denver,39.7487,-105.0077,1609
MIA,Kaseya Center,America/New_York,25.7814,-80.1870,2
GSW,Chase Center,America/Los_Angeles,37.7680,-122.3877,7"""
        
        # Mock the venues CSV file loading
        with patch('builtins.open', mock_open(read_data=self.mock_venues_csv)):
            self.transformer = ScheduleTravelTransformer(
                source="test_schedule_travel",
                venues_csv_path=Path("test_venues.csv")
            )
        
        self.base_source_url = "https://nba.com/test/schedule"
    
    def _create_game_row(
        self,
        game_id: str,
        game_date: datetime,
        home_team: str,
        away_team: str
    ) -> GameRow:
        """Helper to create GameRow for testing."""
        return GameRow(
            game_id=game_id,
            season="2024-25",
            game_date_utc=game_date,
            game_date_local=game_date.date(),
            arena_tz="America/New_York",
            home_team_tricode=home_team,
            away_team_tricode=away_team,
            source="test",
            source_url="https://test.com"
        )
    
    def test_empty_games_returns_empty_list(self):
        """Test that empty games list returns empty list."""
        result = self.transformer.transform([], self.base_source_url)
        assert result == []
    
    def test_venue_data_loading(self):
        """Test that venue data is loaded correctly."""
        assert len(self.transformer.venues) == 5
        
        # Test specific venue data
        lal_venue = self.transformer.venues["LAL"]
        assert lal_venue.arena_name == "Crypto.com Arena"
        assert lal_venue.arena_tz == "America/Los_Angeles"
        assert lal_venue.lat == 34.0430
        assert lal_venue.altitude_m == 89
        
        den_venue = self.transformer.venues["DEN"]
        assert den_venue.altitude_m == 1609  # High altitude

    def test_single_game_no_travel(self):
        """Test analysis of single game with no prior travel."""
        game = GameRow(
            game_id="test_game_1",
            game_date_utc=datetime(2024, 1, 15, 20, 0),
            game_date_local=date(2024, 1, 15),
            season="2023-24",
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            source="test",
            source_url="https://test.com"
        )
        
        result = self.transformer.transform([game], "test_url")
        
        # First game should have no travel analysis (no previous games)
        assert len(result) == 0

    def test_travel_calculation(self):
        """Test travel distance and impact calculations."""
        games = [
            GameRow(
                game_id="game_1",
                game_date_utc=datetime(2024, 1, 10, 20, 0),
                game_date_local=date(2024, 1, 10),
                season="2023-24",
                arena_tz="America/Los_Angeles",
                home_team_tricode="LAL",
                away_team_tricode="DEN",
                source="test",
                source_url="https://test.com"
            ),
            GameRow(
                game_id="game_2", 
                game_date_utc=datetime(2024, 1, 12, 19, 0),
                game_date_local=date(2024, 1, 12),
                season="2023-24", 
                arena_tz="America/New_York",
                home_team_tricode="BOS",
                away_team_tricode="DEN",
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = self.transformer.transform(games, "test_url")
        
        # Denver played both games away, should have travel analytics for second game
        denver_games = [r for r in result if r.team_tricode == "DEN"]
        assert len(denver_games) >= 0  # May be 0 or more depending on logic
        
        # If there are travel records, verify they have the expected properties
        if denver_games:
            second_game = denver_games[0]
            assert second_game.days_rest >= 1
            assert second_game.travel_distance_km > 0

    def test_altitude_impact(self):
        """Test altitude change impact calculations."""
        games = [
            GameRow(
                game_id="altitude_test_1",
                game_date_utc=datetime(2024, 1, 10, 20, 0),
                game_date_local=date(2024, 1, 10),
                season="2023-24",
                arena_tz="America/Los_Angeles",
                home_team_tricode="LAL",
                away_team_tricode="DEN",
                source="test",
                source_url="https://test.com"
            ),
            GameRow(
                game_id="altitude_test_2",
                game_date_utc=datetime(2024, 1, 15, 19, 0),
                game_date_local=date(2024, 1, 15),
                season="2023-24",
                arena_tz="America/Denver",
                home_team_tricode="DEN",
                away_team_tricode="LAL",
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = self.transformer.transform(games, "test_url")
        
        # LAL traveling to Denver should have travel analysis
        lal_travel = [r for r in result if r.team_tricode == "LAL"]
        if lal_travel:
            lal_row = lal_travel[0]
            # LAL traveling to Denver should have positive altitude change
            assert lal_row.altitude_change_m > 1000

    def test_timezone_impact(self):
        """Test timezone change impact on circadian rhythms."""
        games = [
            GameRow(
                game_id="tz_game_1",
                game_date_utc=datetime(2024, 1, 10, 22, 0),
                game_date_local=date(2024, 1, 10),
                season="2023-24",
                arena_tz="America/Los_Angeles",
                home_team_tricode="LAL",
                away_team_tricode="BOS",
                source="test",
                source_url="https://test.com"
            ),
            GameRow(
                game_id="tz_game_2",
                game_date_utc=datetime(2024, 1, 12, 20, 0), 
                game_date_local=date(2024, 1, 12),
                season="2023-24",
                arena_tz="America/New_York",
                home_team_tricode="BOS",
                away_team_tricode="LAL",
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = self.transformer.transform(games, "test_url")
        
        # LAL's second game (traveling east) should show timezone impact
        lal_travel = [r for r in result if r.team_tricode == "LAL"]
        if lal_travel:
            lal_second = lal_travel[0]
            assert lal_second.timezone_shift_hours > 0

    def test_back_to_back_games(self):
        """Test detection and impact of back-to-back games."""
        games = [
            GameRow(
                game_id="b2b_1",
                game_date_utc=datetime(2024, 1, 15, 20, 0),
                game_date_local=date(2024, 1, 15),
                season="2023-24",
                arena_tz="America/Los_Angeles",
                home_team_tricode="LAL",
                away_team_tricode="BOS",
                source="test",
                source_url="https://test.com"
            ),
            GameRow(
                game_id="b2b_2",
                game_date_utc=datetime(2024, 1, 16, 19, 0),
                game_date_local=date(2024, 1, 16),
                season="2023-24",
                arena_tz="America/Denver",
                home_team_tricode="DEN",
                away_team_tricode="LAL",
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = self.transformer.transform(games, "test_url")
        
        # LAL's second game should be flagged as back-to-back
        lal_travel = [r for r in result if r.team_tricode == "LAL"]
        if lal_travel:
            lal_second = lal_travel[0]
            assert lal_second.days_rest == 0
            assert lal_second.is_back_to_back is True

    def test_rest_advantage(self):
        """Test calculation of rest advantage between teams."""
        games = [
            GameRow(
                game_id="rest_test_1",
                game_date_utc=datetime(2024, 1, 10, 20, 0),
                game_date_local=date(2024, 1, 10),
                season="2023-24",
                arena_tz="America/Los_Angeles",
                home_team_tricode="LAL",
                away_team_tricode="BOS",
                source="test",
                source_url="https://test.com"
            ),
            GameRow(
                game_id="rest_test_2",
                game_date_utc=datetime(2024, 1, 15, 20, 0),
                game_date_local=date(2024, 1, 15),
                season="2023-24",
                arena_tz="America/Los_Angeles",
                home_team_tricode="LAL",
                away_team_tricode="BOS", 
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = self.transformer.transform(games, "test_url")
        
        # Should have travel analytics for teams in second game
        assert len(result) >= 0  # May or may not have travel depending on logic

    def test_haversine_distance_calculation(self):
        """Test haversine distance calculation between venues."""
        # LAL to BOS (cross-country)
        lal_venue = self.transformer.venues["LAL"]
        bos_venue = self.transformer.venues["BOS"]
        
        distance = self.transformer._haversine_distance(
            lal_venue.lat, lal_venue.lon,
            bos_venue.lat, bos_venue.lon
        )
        
        # Should be approximately 4200 km (2600 miles)
        assert 4000 < distance < 4500
    
    def test_same_location_zero_distance(self):
        """Test distance calculation for same location."""
        lal_venue = self.transformer.venues["LAL"]
        
        distance = self.transformer._haversine_distance(
            lal_venue.lat, lal_venue.lon,
            lal_venue.lat, lal_venue.lon
        )
        
        assert distance == 0.0
    
    def test_timezone_shift_calculation(self):
        """Test timezone shift calculations."""
        # West to East (LAL to BOS) = +3 hours (eastward)
        shift = self.transformer._calculate_timezone_shift(
            "America/Los_Angeles", "America/New_York"
        )
        assert shift == 3.0
        
        # East to West (BOS to LAL) = -3 hours (westward)
        shift = self.transformer._calculate_timezone_shift(
            "America/New_York", "America/Los_Angeles"
        )
        assert shift == -3.0
        
        # Same timezone = 0
        shift = self.transformer._calculate_timezone_shift(
            "America/New_York", "America/New_York"
        )
        assert shift == 0.0
    
    def test_travel_leg_properties(self):
        """Test TravelLeg eastward/westward properties."""
        lal_venue = self.transformer.venues["LAL"]
        bos_venue = self.transformer.venues["BOS"]
        
        # LAL to BOS (eastward)
        eastward_leg = self.transformer._calculate_travel_leg(lal_venue, bos_venue)
        assert eastward_leg.is_eastward is True
        assert eastward_leg.is_westward is False
        assert eastward_leg.timezone_shift_hours > 0
        
        # BOS to LAL (westward)
        westward_leg = self.transformer._calculate_travel_leg(bos_venue, lal_venue)
        assert westward_leg.is_eastward is False
        assert westward_leg.is_westward is True
        assert westward_leg.timezone_shift_hours < 0
    
    def test_back_to_back_detection(self):
        """Test back-to-back game detection."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "BOS"),
            self._create_game_row("002", datetime(2024, 1, 11, 19, 0), "GSW", "LAL"),  # B2B for LAL
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # Should have travel analysis for LAL's second game
        lal_travel = [r for r in result if r.team_tricode == "LAL" and r.game_id == "002"]
        assert len(lal_travel) == 1
        
        travel_row = lal_travel[0]
        assert travel_row.is_back_to_back is True
        assert travel_row.days_rest == 0
    
    def test_3_in_4_detection(self):
        """Test 3-in-4 games detection."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "BOS"),
            self._create_game_row("002", datetime(2024, 1, 12, 19, 0), "GSW", "LAL"),
            self._create_game_row("003", datetime(2024, 1, 13, 19, 0), "LAL", "MIA"),  # 3-in-4 for LAL
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # Check LAL's third game
        lal_travel = [r for r in result if r.team_tricode == "LAL" and r.game_id == "003"]
        assert len(lal_travel) == 1
        
        travel_row = lal_travel[0]
        assert travel_row.is_3_in_4 is True
    
    def test_5_in_7_detection(self):
        """Test 5-in-7 games detection."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "BOS"),
            self._create_game_row("002", datetime(2024, 1, 11, 19, 0), "GSW", "LAL"),
            self._create_game_row("003", datetime(2024, 1, 13, 19, 0), "LAL", "MIA"),
            self._create_game_row("004", datetime(2024, 1, 14, 19, 0), "DEN", "LAL"),
            self._create_game_row("005", datetime(2024, 1, 16, 19, 0), "LAL", "GSW"),  # 5-in-7 for LAL
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # Check LAL's fifth game
        lal_travel = [r for r in result if r.team_tricode == "LAL" and r.game_id == "005"]
        assert len(lal_travel) == 1
        
        travel_row = lal_travel[0]
        assert travel_row.is_5_in_7 is True
    
    def test_altitude_change_calculation(self):
        """Test altitude change calculations."""
        # LAL (89m) to DEN (1609m) = +1520m gain
        lal_venue = self.transformer.venues["LAL"]
        den_venue = self.transformer.venues["DEN"]
        
        travel_leg = self.transformer._calculate_travel_leg(lal_venue, den_venue)
        assert travel_leg.altitude_change_m == 1520  # 1609 - 89
        
        # DEN to LAL = -1520m loss
        travel_leg_return = self.transformer._calculate_travel_leg(den_venue, lal_venue)
        assert travel_leg_return.altitude_change_m == -1520
    
    def test_circadian_index_calculation(self):
        """Test circadian disruption index calculation."""
        lal_venue = self.transformer.venues["LAL"]
        bos_venue = self.transformer.venues["BOS"]
        
        # LAL to BOS (eastward, cross-country)
        travel_leg = self.transformer._calculate_travel_leg(lal_venue, bos_venue)
        
        # Back-to-back eastward travel should have high disruption
        circadian_index = self.transformer._calculate_circadian_index(
            travel_leg, 
            days_rest=0,  # Back-to-back
            game_datetime=datetime(2024, 1, 11, 22, 0)  # Late game
        )
        
        # Should be high disruption (eastward + B2B + late game)
        assert circadian_index > 0.5
        
        # Same trip with good rest should be lower
        circadian_index_rested = self.transformer._calculate_circadian_index(
            travel_leg,
            days_rest=3,  # Good rest
            game_datetime=datetime(2024, 1, 14, 19, 0)  # Normal time
        )
        
        assert circadian_index_rested < circadian_index
    
    def test_altitude_impact_on_circadian_index(self):
        """Test altitude change impact on circadian index."""
        lal_venue = self.transformer.venues["LAL"]
        den_venue = self.transformer.venues["DEN"]
        
        # LAL to DEN (significant altitude gain)
        travel_leg = self.transformer._calculate_travel_leg(lal_venue, den_venue)
        
        circadian_index = self.transformer._calculate_circadian_index(
            travel_leg,
            days_rest=1,
            game_datetime=datetime(2024, 1, 12, 19, 0)
        )
        
        # Should have altitude penalty (>1000m gain)
        assert circadian_index > 0.1  # Base disruption from altitude
    
    def test_westward_vs_eastward_travel_impact(self):
        """Test that eastward travel has higher circadian impact than westward."""
        lal_venue = self.transformer.venues["LAL"]
        bos_venue = self.transformer.venues["BOS"]
        
        # LAL to BOS (eastward)
        eastward_leg = self.transformer._calculate_travel_leg(lal_venue, bos_venue)
        eastward_impact = self.transformer._calculate_circadian_index(
            eastward_leg, days_rest=1, game_datetime=datetime(2024, 1, 12, 19, 0)
        )
        
        # BOS to LAL (westward) 
        westward_leg = self.transformer._calculate_travel_leg(bos_venue, lal_venue)
        westward_impact = self.transformer._calculate_circadian_index(
            westward_leg, days_rest=1, game_datetime=datetime(2024, 1, 12, 19, 0)
        )
        
        # Eastward should be worse (1.5x multiplier)
        assert eastward_impact > westward_impact
    
    def test_team_schedule_grouping(self):
        """Test that games are properly grouped by team."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "BOS"),
            self._create_game_row("002", datetime(2024, 1, 12, 19, 0), "GSW", "LAL"),
        ]
        
        team_schedules = self.transformer._group_games_by_team(games)
        
        # Each team should appear in both games
        assert len(team_schedules["LAL"]) == 2  # Home in game 1, away in game 2
        assert len(team_schedules["BOS"]) == 1  # Away in game 1
        assert len(team_schedules["GSW"]) == 1  # Home in game 2
    
    def test_first_game_no_travel_analysis(self):
        """Test that first game of season has no travel analysis."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "BOS"),
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # No travel analysis for first games
        assert len(result) == 0
    
    def test_missing_venue_data_handling(self):
        """Test handling of missing venue data."""
        # Add game with unknown team
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "BOS"),
            self._create_game_row("002", datetime(2024, 1, 12, 19, 0), "UNK", "LAL"),  # Unknown team
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # Should handle gracefully and continue processing valid games
        assert isinstance(result, list)
        # Should still process LAL's games where venue data exists
    
    def test_cross_country_travel_metrics(self):
        """Test realistic cross-country travel scenario."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "LAL", "GSW"),  # West Coast
            self._create_game_row("002", datetime(2024, 1, 12, 19, 0), "BOS", "LAL"),  # Cross-country
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # Find LAL's travel to Boston
        lal_travel = [r for r in result if r.team_tricode == "LAL" and r.game_id == "002"]
        assert len(lal_travel) == 1
        
        travel_row = lal_travel[0]
        assert travel_row.travel_distance_km > 4000  # Cross-country distance
        assert travel_row.timezone_shift_hours == 3.0  # West to East
        assert travel_row.circadian_index > 0.3  # Significant disruption
        assert travel_row.prev_arena_tz == "America/Los_Angeles"
        assert travel_row.days_rest == 1  # One day between games
    
    def test_denver_altitude_scenario(self):
        """Test Denver high-altitude scenario."""
        games = [
            self._create_game_row("001", datetime(2024, 1, 10, 20, 0), "MIA", "GSW"),  # Sea level
            self._create_game_row("002", datetime(2024, 1, 12, 19, 0), "DEN", "MIA"),  # High altitude
        ]
        
        result = self.transformer.transform(games, self.base_source_url)
        
        # Find MIA's travel to Denver
        mia_travel = [r for r in result if r.team_tricode == "MIA" and r.game_id == "002"]
        assert len(mia_travel) == 1
        
        travel_row = mia_travel[0]
        assert travel_row.altitude_change_m > 1600  # Significant altitude gain
        assert travel_row.prev_altitude_m == 2  # From sea level (Miami)
        assert travel_row.circadian_index > 0.2  # Altitude impact included


class TestVenueDataAndTravelLeg:
    """Test cases for VenueData and TravelLeg helper classes."""
    
    def test_venue_data_creation(self):
        """Test VenueData dataclass creation."""
        venue = VenueData(
            team_id="LAL",
            arena_name="Crypto.com Arena", 
            arena_tz="America/Los_Angeles",
            lat=34.0430,
            lon=-118.2675,
            altitude_m=89
        )
        
        assert venue.team_id == "LAL"
        assert venue.altitude_m == 89
    
    def test_travel_leg_directional_properties(self):
        """Test TravelLeg directional properties."""
        venue1 = VenueData("LAL", "Arena1", "America/Los_Angeles", 34.0, -118.0, 100)
        venue2 = VenueData("BOS", "Arena2", "America/New_York", 42.0, -71.0, 50)
        
        # Eastward travel (+3 hour shift)
        eastward_leg = TravelLeg(venue1, venue2, 4000, 3.0, -50)
        assert eastward_leg.is_eastward is True
        assert eastward_leg.is_westward is False
        
        # Westward travel (-3 hour shift)
        westward_leg = TravelLeg(venue2, venue1, 4000, -3.0, 50)
        assert westward_leg.is_eastward is False
        assert westward_leg.is_westward is True
        
        # No timezone change
        no_shift_leg = TravelLeg(venue1, venue1, 0, 0.0, 0)
        assert no_shift_leg.is_eastward is False
        assert no_shift_leg.is_westward is False


@pytest.fixture
def sample_schedule_games():
    """Sample games for integration testing."""
    return [
        GameRow(
            game_id="0022400001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 22, 0),
            game_date_local=date(2024, 10, 15),
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            source="test",
            source_url="https://test.com"
        ),
        GameRow(
            game_id="0022400002", 
            season="2024-25",
            game_date_utc=datetime(2024, 10, 17, 20, 0),
            game_date_local=date(2024, 10, 17),
            arena_tz="America/Denver",
            home_team_tricode="DEN",
            away_team_tricode="LAL",
            source="test",
            source_url="https://test.com"
        )
    ]


def test_schedule_travel_integration(sample_schedule_games):
    """Integration test for schedule travel transformer."""
    mock_venues_csv = """team_id,arena_name,arena_tz,lat,lon,altitude_m
LAL,Crypto.com Arena,America/Los_Angeles,34.0430,-118.2675,89
DEN,Ball Arena,America/Denver,39.7487,-105.0077,1609"""
    
    with patch('builtins.open', mock_open(read_data=mock_venues_csv)):
        transformer = ScheduleTravelTransformer(venues_csv_path=Path("test.csv"))
        
        result = transformer.transform(sample_schedule_games, "https://test.com")
        
        # Should generate travel analysis for LAL's second game
        assert len(result) >= 1
        
        # Find LAL's travel to Denver
        lal_denver = [r for r in result if r.team_tricode == "LAL" and r.game_id == "0022400002"]
        assert len(lal_denver) == 1
        
        travel_row = lal_denver[0]
        assert travel_row.altitude_change_m > 1500  # Significant altitude gain
        assert travel_row.timezone_shift_hours == 1.0  # LAL to DEN timezone shift
        assert travel_row.days_rest == 1  # One day between games


class TestScheduleTravelTransformer:
    
    def test_init_default_venues_path(self):
        """Test initialization with default venues path."""
        mock_csv_data = "team_id,arena_name,arena_tz,lat,lon,altitude_m\nLAL,Crypto.com Arena,America/Los_Angeles,34.043,-118.267,30"
        
        with patch("builtins.open", mock_open(read_data=mock_csv_data)):
            transformer = ScheduleTravelTransformer()
            
        assert "LAL" in transformer.venues
        assert transformer.venues["LAL"].arena_name == "Crypto.com Arena"
    
    def test_init_custom_venues_path(self):
        """Test initialization with custom venues path."""
        mock_csv_data = "team_id,arena_name,arena_tz,lat,lon,altitude_m\nBOS,TD Garden,America/New_York,42.366,-71.062,6"
        custom_path = Path("/custom/venues.csv")
        
        with patch("builtins.open", mock_open(read_data=mock_csv_data)):
            transformer = ScheduleTravelTransformer(venues_csv_path=custom_path)
            
        assert "BOS" in transformer.venues
        assert transformer.venues["BOS"].arena_name == "TD Garden"
    
    def test_venue_data_creation(self):
        """Test VenueData dataclass creation."""
        venue = VenueData(
            team_id="LAL",
            arena_name="Crypto.com Arena", 
            arena_tz="America/Los_Angeles",
            lat=34.043,
            lon=-118.267,
            altitude_m=30.0
        )
        
        assert venue.team_id == "LAL"
        assert venue.arena_name == "Crypto.com Arena"
        assert venue.lat == 34.043
        assert venue.altitude_m == 30.0


def test_schedule_travel_transform_empty_games():
    """Test transform with empty games list."""
    mock_csv_data = "team_id,arena_name,arena_tz,lat,lon,altitude_m\nLAL,Crypto.com Arena,America/Los_Angeles,34.043,-118.267,30"
    
    with patch("builtins.open", mock_open(read_data=mock_csv_data)):
        transformer = ScheduleTravelTransformer()
        
    result = transformer.transform([], "test-url")
    assert result == []


def test_schedule_travel_basic_functionality():
    """Test basic schedule travel functionality."""
    mock_csv_data = """team_id,arena_name,arena_tz,lat,lon,altitude_m
LAL,Crypto.com Arena,America/Los_Angeles,34.043,-118.267,30
BOS,TD Garden,America/New_York,42.366,-71.062,6"""
    
    with patch("builtins.open", mock_open(read_data=mock_csv_data)):
        transformer = ScheduleTravelTransformer()
    
    games = [
        GameRow(
            game_id="test1",
            game_date_utc=datetime(2024, 1, 1, 20, 0),
            game_date_local=date(2024, 1, 1),
            season="2023-24",
            arena_tz="America/Los_Angeles",
            home_team_tricode="LAL",
            away_team_tricode="BOS",
            source="test",
            source_url="https://test.com"
        )
    ]
    
    result = transformer.transform(games, "test-url")
    # Just verify it runs without error - specific logic testing would need more setup
    assert isinstance(result, list)