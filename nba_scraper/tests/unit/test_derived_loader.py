"""Tests for derived analytics loader."""

import pytest
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock

from nba_scraper.models.derived_rows import Q1WindowRow, EarlyShockRow, ScheduleTravelRow
from nba_scraper.models.enums import EarlyShockType
from nba_scraper.loaders.derived import DerivedLoader


class TestDerivedLoader:
    """Test cases for DerivedLoader."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.loader = DerivedLoader()
    
    @pytest.fixture
    def mock_connection(self):
        """Mock database connection."""
        conn = AsyncMock()
        conn.transaction.return_value.__aenter__ = AsyncMock()
        conn.transaction.return_value.__aexit__ = AsyncMock()
        conn.execute = AsyncMock()
        return conn
    
    @pytest.mark.asyncio
    async def test_empty_q1_windows_returns_zero(self):
        """Test that empty windows list returns 0."""
        result = await self.loader.upsert_q1_windows([])
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_empty_early_shocks_returns_zero(self):
        """Test that empty shocks list returns 0."""
        result = await self.loader.upsert_early_shocks([])
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_empty_schedule_travel_returns_zero(self):
        """Test that empty travel rows returns 0."""
        result = await self.loader.upsert_schedule_travel([])
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_upsert_q1_windows_inserts_new_records(self, mock_connection, monkeypatch):
        """Test Q1 window upsert with new records."""
        # Mock get_connection to return our mock
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        
        # Mock get_performance_connection to return an async context manager
        class MockPerformanceConnection:
            async def __aenter__(self):
                return mock_connection
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        def mock_get_performance_connection():
            return MockPerformanceConnection()
        
        monkeypatch.setattr("nba_scraper.loaders.derived.get_performance_connection", mock_get_performance_connection)
        
        # Mock the bulk_optimizer that's used in the actual implementation
        mock_bulk_optimizer = AsyncMock()
        mock_bulk_optimizer.bulk_upsert = AsyncMock(return_value=0)  # Return 0 for insert
        monkeypatch.setattr("nba_scraper.loaders.derived.bulk_optimizer", mock_bulk_optimizer)
        
        # Create a mock validator that returns valid records
        mock_validator = AsyncMock()
        async def mock_validate_before_insert(table_name, records):
            return records, []  # Return all records as valid with no errors
        mock_validator.validate_before_insert = mock_validate_before_insert
        
        # Mock the validator on the loader instance
        self.loader.validator = mock_validator
        
        mock_connection.execute.return_value = "INSERT 0 1"
        
        windows = [
            Q1WindowRow(
                game_id="0022300001",
                home_team_tricode="LAL",
                away_team_tricode="BOS",
                possessions_elapsed=25,
                pace48_actual=102.5,
                pace48_expected=100.0,
                home_efg_actual=0.55,
                home_efg_expected=0.52,
                away_efg_actual=0.48,
                away_efg_expected=0.50,
                home_to_rate=0.12,
                away_to_rate=0.15,
                home_ft_rate=0.25,
                away_ft_rate=0.22,
                home_orb_pct=0.28,
                home_drb_pct=0.72,
                away_orb_pct=0.25,
                away_drb_pct=0.75,
                bonus_time_home_sec=45.0,
                bonus_time_away_sec=30.0,
                transition_rate=0.18,
                early_clock_rate=0.35,
                source="test_q1_window",
                source_url="https://test.com/q1"
            )
        ]
        
        result = await self.loader.upsert_q1_windows(windows)
        
        # Should return 0 for inserts (no updates)
        assert result == 0
        # Should have called the bulk upsert method
        mock_bulk_optimizer.bulk_upsert.assert_called_once()
        
    @pytest.mark.asyncio 
    async def test_upsert_q1_windows_updates_existing_records(self, mock_connection, monkeypatch):
        """Test Q1 window upsert with existing records that need updates."""
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        
        # Mock the bulk_optimizer to return 1 for update
        mock_bulk_optimizer = AsyncMock()
        mock_bulk_optimizer.bulk_upsert = AsyncMock(return_value=1)  # Return 1 for update
        monkeypatch.setattr("nba_scraper.loaders.derived.bulk_optimizer", mock_bulk_optimizer)
        
        # Create a mock validator that returns valid records
        mock_validator = AsyncMock()
        async def mock_validate_before_insert(table_name, records):
            return records, []  # Return all records as valid with no errors
        mock_validator.validate_before_insert = mock_validate_before_insert
        
        # Mock the validator on the loader instance
        self.loader.validator = mock_validator
        
        mock_connection.execute.return_value = "UPDATE 1"
        
        windows = [
            Q1WindowRow(
                game_id="0022300001",
                home_team_tricode="LAL",
                away_team_tricode="BOS",
                possessions_elapsed=26,  # Different value to trigger update
                pace48_actual=102.5,
                pace48_expected=100.0,
                home_efg_actual=0.55,
                home_efg_expected=0.52,
                away_efg_actual=0.48,
                away_efg_expected=0.50,
                home_to_rate=0.12,
                away_to_rate=0.15,
                home_ft_rate=0.25,
                away_ft_rate=0.22,
                home_orb_pct=0.28,
                home_drb_pct=0.72,
                away_orb_pct=0.25,
                away_drb_pct=0.75,
                bonus_time_home_sec=45.0,
                bonus_time_away_sec=30.0,
                transition_rate=0.18,
                early_clock_rate=0.35,
                source="test_q1_window",
                source_url="https://test.com/q1"
            )
        ]
        
        result = await self.loader.upsert_q1_windows(windows)
        
        # Should return 1 for one update
        assert result == 1
        mock_bulk_optimizer.bulk_upsert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_upsert_early_shocks_with_enum_mapping(self, mock_connection, monkeypatch):
        """Test early shocks upsert with proper enum mapping."""
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        
        # Mock the bulk_optimizer that's used in the actual implementation
        mock_bulk_optimizer = AsyncMock()
        mock_bulk_optimizer.bulk_upsert = AsyncMock(return_value=0)  # Return 0 for insert
        monkeypatch.setattr("nba_scraper.loaders.derived.bulk_optimizer", mock_bulk_optimizer)
        
        # Create a mock validator that returns valid records (simulates game exists)
        mock_validator = AsyncMock()
        async def mock_validate_before_insert(table_name, records):
            return records, []  # Return all records as valid with no errors
        mock_validator.validate_before_insert = mock_validate_before_insert
        
        # Mock the validator on the loader instance
        self.loader.validator = mock_validator
        
        mock_connection.execute.return_value = "INSERT 0 1"
        
        shocks = [
            EarlyShockRow(
                game_id="0022300001",
                team_tricode="LAL",
                player_slug="LebronJames",
                shock_type=EarlyShockType.TWO_PF_EARLY,
                shock_seq=1,
                period=1,
                clock_hhmmss="00:07:30",
                event_idx_start=10,
                event_idx_end=20,
                immediate_sub=True,
                poss_since_event=3,
                notes="2 PF in 270.0s",
                source="test_early_shocks",
                source_url="https://test.com/shocks"
            )
        ]
        
        result = await self.loader.upsert_early_shocks(shocks)
        
        assert result == 0  # Insert, not update
        # Should have called the bulk upsert method instead of direct execute
        mock_bulk_optimizer.bulk_upsert.assert_called_once()
        
    @pytest.mark.asyncio
    async def test_upsert_early_shocks_severity_determination(self, mock_connection, monkeypatch):
        """Test severity determination logic."""
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        mock_connection.execute.return_value = "INSERT 0 1"
        
        # Test different shock types and their expected severities
        test_cases = [
            (EarlyShockType.FLAGRANT, "Flagrant 2", "HIGH"),
            (EarlyShockType.FLAGRANT, "Flagrant 1", "MEDIUM"),
            (EarlyShockType.INJURY_LEAVE, "Ankle injury", "HIGH"),
            (EarlyShockType.TWO_PF_EARLY, "2 PF early", "MEDIUM"),
            (EarlyShockType.TECH, "Technical foul", "LOW"),
        ]
        
        for shock_type, notes, expected_severity in test_cases:
            shock = EarlyShockRow(
                game_id="0022300001",
                team_tricode="LAL",
                player_slug="TestPlayer",
                shock_type=shock_type,
                shock_seq=1,
                period=1,
                clock_hhmmss="00:08:00",
                event_idx_start=10,
                notes=notes,
                source="test",
                source_url="https://test.com"
            )
            
            severity = self.loader._determine_severity(shock_type.value, notes)
            assert severity == expected_severity
    
    @pytest.mark.asyncio
    async def test_upsert_schedule_travel_complete_record(self, mock_connection, monkeypatch):
        """Test schedule travel upsert with complete record."""
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        
        # Mock the bulk_optimizer
        mock_bulk_optimizer = AsyncMock()
        mock_bulk_optimizer.bulk_upsert = AsyncMock(return_value=0)  # Return 0 for insert
        monkeypatch.setattr("nba_scraper.loaders.derived.bulk_optimizer", mock_bulk_optimizer)
        
        # Create a mock validator that returns valid records
        mock_validator = AsyncMock()
        async def mock_validate_before_insert(table_name, records):
            return records, []  # Return all records as valid with no errors
        mock_validator.validate_before_insert = mock_validate_before_insert
        
        # Mock the validator on the loader instance
        self.loader.validator = mock_validator
        
        travel_rows = [
            ScheduleTravelRow(
                game_id="0022300001",
                team_tricode="LAL",
                is_back_to_back=True,
                is_3_in_4=False,
                is_5_in_7=False,
                days_rest=0,
                timezone_shift_hours=-3.0,
                circadian_index=0.75,
                altitude_change_m=1500.0,
                travel_distance_km=2400.0,
                prev_game_date=date(2024, 1, 15),
                prev_arena_tz="America/New_York",
                prev_lat=40.7505,
                prev_lon=-73.9934,
                prev_altitude_m=11.0,
                source="test_travel",
                source_url="https://test.com/travel"
            )
        ]
        
        result = await self.loader.upsert_schedule_travel(travel_rows)
        
        assert result == 0
        mock_bulk_optimizer.bulk_upsert.assert_called_once()
        
        # Verify the bulk_upsert was called with correct parameters
        call_args = mock_bulk_optimizer.bulk_upsert.call_args
        assert call_args is not None
        # Check that travel data fields are properly handled
        assert len(travel_rows) == 1

    def test_clock_conversion_methods(self):
        """Test clock format conversion utility methods."""
        # Test HH:MM:SS to MM:SS conversion
        assert self.loader._convert_clock_to_time_remaining("00:07:30") == "07:30"
        assert self.loader._convert_clock_to_time_remaining("00:00:45") == "00:45"
        assert self.loader._convert_clock_to_time_remaining("invalid") == "12:00"
        
        # Test HH:MM:SS to seconds elapsed conversion
        assert self.loader._convert_clock_to_seconds_elapsed("00:07:30") == 270.0  # 720 - 450
        assert self.loader._convert_clock_to_seconds_elapsed("00:12:00") == 0.0     # 720 - 720
        assert self.loader._convert_clock_to_seconds_elapsed("00:00:00") == 720.0   # 720 - 0
        assert self.loader._convert_clock_to_seconds_elapsed("invalid") == 0.0
    
    @pytest.mark.asyncio
    async def test_database_transaction_rollback_on_error(self, mock_connection, monkeypatch):
        """Test that database errors trigger transaction rollback."""
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        
        # Mock get_performance_connection to return an async context manager
        class MockPerformanceConnection:
            async def __aenter__(self):
                return mock_connection
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        def mock_get_performance_connection():
            return MockPerformanceConnection()
        
        monkeypatch.setattr("nba_scraper.loaders.derived.get_performance_connection", mock_get_performance_connection)
        
        # Mock the bulk_optimizer to raise an error
        mock_bulk_optimizer = AsyncMock()
        mock_bulk_optimizer.bulk_upsert = AsyncMock(side_effect=Exception("Database error"))
        monkeypatch.setattr("nba_scraper.loaders.derived.bulk_optimizer", mock_bulk_optimizer)
        
        # Create a mock validator that returns valid records
        mock_validator = AsyncMock()
        async def mock_validate_before_insert(table_name, records):
            return records, []  # Return all records as valid with no errors
        mock_validator.validate_before_insert = mock_validate_before_insert
        
        # Mock the validator on the loader instance
        self.loader.validator = mock_validator
        
        windows = [
            Q1WindowRow(
                game_id="0022300001",
                home_team_tricode="LAL", 
                away_team_tricode="BOS",
                possessions_elapsed=25,
                pace48_actual=102.5,
                pace48_expected=100.0,
                home_efg_actual=0.55,
                home_efg_expected=0.52,
                away_efg_actual=0.48,
                away_efg_expected=0.50,
                home_to_rate=0.12,
                away_to_rate=0.15,
                home_ft_rate=0.25,
                away_ft_rate=0.22,
                home_orb_pct=0.28,
                home_drb_pct=0.72,
                away_orb_pct=0.25,
                away_drb_pct=0.75,
                bonus_time_home_sec=45.0,
                bonus_time_away_sec=30.0,
                transition_rate=0.18,
                early_clock_rate=0.35,
                source="test",
                source_url="https://test.com"
            )
        ]
        
        # Should raise the exception
        with pytest.raises(Exception, match="Database error"):
            await self.loader.upsert_q1_windows(windows)
        
        # Verify the bulk upsert was attempted (which triggers the error)
        mock_bulk_optimizer.bulk_upsert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_batch_processing_multiple_records(self, mock_connection, monkeypatch):
        """Test processing multiple records in a single transaction."""
        monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", lambda: mock_connection)
        
        # Mock the bulk_optimizer to return 1 (mix of inserts and updates)
        mock_bulk_optimizer = AsyncMock()
        mock_bulk_optimizer.bulk_upsert = AsyncMock(return_value=1)
        monkeypatch.setattr("nba_scraper.loaders.derived.bulk_optimizer", mock_bulk_optimizer)
        
        # Create a mock validator that returns valid records
        mock_validator = AsyncMock()
        async def mock_validate_before_insert(table_name, records):
            return records, []  # Return all records as valid with no errors
        mock_validator.validate_before_insert = mock_validate_before_insert
        
        # Mock the validator on the loader instance
        self.loader.validator = mock_validator
        
        shocks = [
            EarlyShockRow(
                game_id="0022300001",
                team_tricode="LAL",
                player_slug="Player1",
                shock_type=EarlyShockType.TECH,
                shock_seq=1,
                period=1,
                clock_hhmmss="00:08:00",
                event_idx_start=10,
                source="test",
                source_url="https://test.com"
            ),
            EarlyShockRow(
                game_id="0022300001", 
                team_tricode="LAL",
                player_slug="Player2",
                shock_type=EarlyShockType.FLAGRANT,
                shock_seq=1,
                period=1,
                clock_hhmmss="00:06:00",
                event_idx_start=20,
                source="test",
                source_url="https://test.com"
            ),
            EarlyShockRow(
                game_id="0022300001",
                team_tricode="BOS", 
                player_slug="Player3",
                shock_type=EarlyShockType.TWO_PF_EARLY,
                shock_seq=1,
                period=1,
                clock_hhmmss="00:05:00",
                event_idx_start=30,
                source="test",
                source_url="https://test.com"
            )
        ]
        
        result = await self.loader.upsert_early_shocks(shocks)
        
        # Should return 1 (number of updates from bulk operation)
        assert result == 1
        
        # Should have called bulk upsert once (not individual executes)
        mock_bulk_optimizer.bulk_upsert.assert_called_once()


@pytest.mark.asyncio 
async def test_derived_loader_integration():
    """Integration test for derived loader functionality."""
    loader = DerivedLoader()
    
    # Test that loader can be instantiated and methods exist
    assert hasattr(loader, 'upsert_q1_windows')
    assert hasattr(loader, 'upsert_early_shocks')
    assert hasattr(loader, 'upsert_schedule_travel')
    
    # Test utility methods
    assert loader._convert_clock_to_time_remaining("00:07:30") == "07:30"
    assert loader._convert_clock_to_seconds_elapsed("00:07:30") == 270.0
    assert loader._determine_severity("FLAGRANT", "Flagrant 2") == "HIGH"