"""Integration tests for data quality validation and FK checks."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

from nba_scraper.validation import DataQualityValidator, ValidationResult
from nba_scraper.loaders.derived import DerivedLoader
from nba_scraper.models.derived_rows import Q1WindowRow, EarlyShockRow
from nba_scraper.models.enums import EarlyShockType


class TestDataQualityValidation:
    """Test data quality validation functionality."""
    
    @pytest.fixture
    def validator(self):
        """Create a DataQualityValidator instance."""
        return DataQualityValidator()
    
    @pytest.fixture
    def sample_validation_results(self):
        """Sample validation results for testing."""
        return [
            ValidationResult(
                table_name="q1_window_12_8",
                check_type="foreign_key_validation",
                is_valid=True,
                record_count=100,
                invalid_count=0,
                issues=[]
            ),
            ValidationResult(
                table_name="early_shocks",
                check_type="foreign_key_validation",
                is_valid=False,
                record_count=50,
                invalid_count=5,
                issues=["Invalid game_id references: ['0022400999', '0022401000']"],
                details={"invalid_game_ids": ["0022400999", "0022401000"]}
            ),
            ValidationResult(
                table_name="games",
                check_type="orphaned_records",
                is_valid=False,
                record_count=3,
                invalid_count=3,
                issues=[
                    "Game 0022400123 (2024-01-15) missing: outcomes, pbp_events",
                    "Game 0022400124 (2024-01-16) missing: ref_assignments"
                ]
            )
        ]
    
    @pytest.mark.asyncio
    async def test_validate_before_insert_valid_records(self, validator):
        """Test validation with all valid records."""
        # Mock database connection and results
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {'game_id': '0022400123'},
            {'game_id': '0022400124'}
        ]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            records = [
                {'game_id': '0022400123', 'some_field': 'value1'},
                {'game_id': '0022400124', 'some_field': 'value2'}
            ]
            
            valid_records, errors = await validator.validate_before_insert('test_table', records)
            
            assert len(valid_records) == 2
            assert len(errors) == 0
            assert valid_records == records
    
    @pytest.mark.asyncio
    async def test_validate_before_insert_invalid_game_ids(self, validator):
        """Test validation with some invalid game_ids."""
        # Mock database connection - only one game exists
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{'game_id': '0022400123'}]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            records = [
                {'game_id': '0022400123', 'some_field': 'value1'},  # Valid
                {'game_id': '0022400999', 'some_field': 'value2'},  # Invalid
                {'game_id': '0022401000', 'some_field': 'value3'}   # Invalid
            ]
            
            valid_records, errors = await validator.validate_before_insert('test_table', records)
            
            assert len(valid_records) == 1
            assert len(errors) == 1
            assert valid_records[0]['game_id'] == '0022400123'
            assert 'Missing game_id references' in errors[0]
            assert '0022400999' in errors[0]
    
    @pytest.mark.asyncio
    async def test_validate_before_insert_no_game_ids(self, validator):
        """Test validation with records that don't have game_ids."""
        records = [
            {'some_field': 'value1'},
            {'other_field': 'value2'}
        ]
        
        valid_records, errors = await validator.validate_before_insert('test_table', records)
        
        assert len(valid_records) == 2
        assert len(errors) == 0
        assert valid_records == records
    
    @pytest.mark.asyncio
    async def test_validate_core_foreign_keys(self, validator):
        """Test core foreign key validation."""
        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {'total_count': 100, 'invalid_count': 0}
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_core_foreign_keys(cutoff_time)
            
            assert len(results) == 1
            assert results[0].table_name == "game_id_crosswalk"
            assert results[0].check_type == "foreign_key_validation"
            assert results[0].is_valid is True
            assert results[0].record_count == 100
            assert results[0].invalid_count == 0
    
    @pytest.mark.asyncio
    async def test_validate_core_foreign_keys_with_violations(self, validator):
        """Test core foreign key validation with violations."""
        # Mock database connection with violations
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {'total_count': 100, 'invalid_count': 5}
        mock_conn.fetch.return_value = [
            {'game_id': '0022400999'},
            {'game_id': '0022401000'}
        ]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_core_foreign_keys(cutoff_time)
            
            assert len(results) == 1
            assert results[0].table_name == "game_id_crosswalk"
            assert results[0].is_valid is False
            assert results[0].invalid_count == 5
            assert len(results[0].issues) == 1
            assert "Invalid game_id references" in results[0].issues[0]
    
    @pytest.mark.asyncio
    async def test_detect_orphaned_records(self, validator):
        """Test orphaned record detection."""
        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {
                'game_id': '0022400123',
                'season': '2023-24',
                'game_date_local': '2024-01-15',
                'has_outcome': 0,
                'has_pbp': 0,
                'has_refs': 1,
                'has_lineups': 1
            },
            {
                'game_id': '0022400124',
                'season': '2023-24', 
                'game_date_local': '2024-01-16',
                'has_outcome': 1,
                'has_pbp': 1,
                'has_refs': 0,
                'has_lineups': 1
            }
        ]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._detect_orphaned_records(cutoff_time)
            
            assert len(results) == 1
            assert results[0].table_name == "games"
            assert results[0].check_type == "orphaned_records"
            assert results[0].is_valid is False
            assert results[0].record_count == 2
            assert len(results[0].issues) == 2
            assert "missing: outcomes, pbp_events" in results[0].issues[0]
            assert "missing: ref_assignments" in results[0].issues[1]
    
    @pytest.mark.asyncio
    async def test_validate_uniqueness_constraints(self, validator):
        """Test uniqueness constraint validation."""
        # Mock database connection
        mock_conn = AsyncMock()
        # First query: duplicate bref_game_ids
        mock_conn.fetch.side_effect = [
            [{'bref_game_id': 'BOS202401150LAL', 'count': 2}],
            []  # No duplicate referee assignments
        ]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_uniqueness_constraints(cutoff_time)
            
            assert len(results) == 2
            
            # Check games uniqueness result
            games_result = next(r for r in results if r.table_name == "games")
            assert games_result.check_type == "uniqueness_validation"
            assert games_result.is_valid is False
            assert len(games_result.issues) == 1
            assert "Duplicate bref_game_id" in games_result.issues[0]
            
            # Check ref_assignments uniqueness result
            refs_result = next(r for r in results if r.table_name == "ref_assignments")
            assert refs_result.is_valid is True
    
    @pytest.mark.asyncio
    async def test_validate_cross_table_consistency(self, validator):
        """Test cross-table consistency validation."""
        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.fetch.side_effect = [
            # Team tricode inconsistencies
            [{
                'game_id': '0022400123',
                'game_home': 'LAL',
                'game_away': 'BOS',
                'q1_home': 'LAK',  # Different!
                'q1_away': 'BOS'
            }],
            # Missing outcomes for final games
            [{
                'game_id': '0022400124',
                'game_date_local': '2024-01-16',
                'status': 'FINAL',
                'outcome_status': 'missing'
            }]
        ]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_cross_table_consistency(cutoff_time)
            
            assert len(results) == 2
            
            # Check team consistency
            team_result = next(r for r in results if r.table_name == "q1_window_12_8")
            assert team_result.is_valid is False
            assert "teams mismatch" in team_result.issues[0]
            
            # Check outcome consistency
            outcome_result = next(r for r in results if r.table_name == "outcomes")
            assert outcome_result.is_valid is False
            assert "missing outcome data" in outcome_result.issues[0]
    
    @pytest.mark.asyncio
    async def test_validate_pbp_monotonicity_gaps(self, validator):
        """Test PBP event index gap detection."""
        # Mock database connection with gap data
        mock_conn = AsyncMock()
        
        # Mock gap query results - events with missing indices
        mock_conn.fetch.side_effect = [
            # Gap query results
            [
                {
                    'game_id': '0022400123',
                    'period': 1,
                    'prev_event_idx': 10,
                    'event_idx': 15,
                    'gap_size': 5
                },
                {
                    'game_id': '0022400124',
                    'period': 2,
                    'prev_event_idx': 50,
                    'event_idx': 54,
                    'gap_size': 4
                }
            ],
            # Overlap query results (empty for this test)
            [],
            # Clock progression query results (empty)
            [],
            # Period boundary query results (empty)
            [],
            # Stats query result (empty)
        ]
        
        # Mock stats query result
        mock_conn.fetchrow.return_value = {
            'games_with_pbp': 10,
            'total_events': 1000,
            'q1_coverage': 0.9,
            'q4_coverage': 0.85,
            'events_missing_time': 5
        }
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_pbp_monotonicity(cutoff_time)
            
            # Should have 5 validation results (gaps, overlaps, clock, periods, completeness)
            assert len(results) == 5
            
            # Check gap detection result
            gap_result = next(r for r in results if r.check_type == "pbp_monotonicity_gaps")
            assert gap_result.is_valid is False  # Has gaps
            assert gap_result.invalid_count == 2  # Two gaps found
            assert "Missing events between idx 10 and 15" in gap_result.issues[0]
            assert "gap of 4" in gap_result.issues[0]
            
            # Check that other validations passed (no issues)
            overlap_result = next(r for r in results if r.check_type == "pbp_monotonicity_overlaps")
            assert overlap_result.is_valid is True  # No overlaps
            
            clock_result = next(r for r in results if r.check_type == "pbp_clock_progression")
            assert clock_result.is_valid is True  # No clock issues
    
    @pytest.mark.asyncio
    async def test_validate_pbp_monotonicity_overlaps(self, validator):
        """Test PBP event index overlap detection."""
        mock_conn = AsyncMock()
        
        # Mock query results with overlaps
        mock_conn.fetch.side_effect = [
            # Gap query results (empty)
            [],
            # Overlap query results
            [
                {
                    'game_id': '0022400123',
                    'period': 1, 
                    'event_idx': 25,
                    'duplicate_count': 3,
                    'event_types': 'SHOT_MADE, REBOUND, ASSIST'
                },
                {
                    'game_id': '0022400124',
                    'period': 3,
                    'event_idx': 88,
                    'duplicate_count': 2,
                    'event_types': 'FOUL, FREE_THROW_MADE'
                }
            ],
            # Other queries empty
            [],
            [],
        ]
        
        mock_conn.fetchrow.return_value = {
            'games_with_pbp': 5,
            'total_events': 500,
            'q1_coverage': 1.0,
            'q4_coverage': 1.0,
            'events_missing_time': 0
        }
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_pbp_monotonicity(cutoff_time)
            
            # Check overlap detection result
            overlap_result = next(r for r in results if r.check_type == "pbp_monotonicity_overlaps")
            assert overlap_result.is_valid is False  # Has overlaps
            assert overlap_result.invalid_count == 2  # Two overlaps found
            assert "Duplicate event_idx 25 (3 occurrences)" in overlap_result.issues[0]
            assert "types: SHOT_MADE, REBOUND, ASSIST" in overlap_result.issues[0]
    
    @pytest.mark.asyncio
    async def test_validate_pbp_clock_progression(self, validator):
        """Test PBP clock progression validation."""
        mock_conn = AsyncMock()
        
        # Mock query results with clock issues
        mock_conn.fetch.side_effect = [
            # Gap and overlap queries (empty)
            [],
            [],
            # Clock progression query results
            [
                {
                    'game_id': '0022400123',
                    'period': 2,
                    'event_idx': 45,
                    'seconds_elapsed': 600,
                    'prev_seconds': 620,
                    'time_diff': -20,
                    'time_remaining': '8:00',
                    'prev_time_remaining': '7:40'
                }
            ],
            # Period boundary query (empty)
            [],
        ]
        
        mock_conn.fetchrow.return_value = {
            'games_with_pbp': 3,
            'total_events': 300,
            'q1_coverage': 0.9,
            'q4_coverage': 0.9,
            'events_missing_time': 0
        }
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_pbp_monotonicity(cutoff_time)
            
            # Check clock progression result
            clock_result = next(r for r in results if r.check_type == "pbp_clock_progression")
            assert clock_result.is_valid is False  # Has clock issues
            assert clock_result.invalid_count == 1  # One clock issue found
            assert "Clock moved backwards at event 45" in clock_result.issues[0]
            assert "from 620s to 600s (diff: -20s)" in clock_result.issues[0]
    
    @pytest.mark.asyncio
    async def test_validate_pbp_period_boundaries(self, validator):
        """Test PBP period boundary validation."""
        mock_conn = AsyncMock()
        
        # Mock query results with period boundary issues
        mock_conn.fetch.side_effect = [
            # Gap, overlap, clock queries (empty)
            [],
            [],
            [],
            # Period boundary query results
            [
                {
                    'game_id': '0022400123',
                    'period': 1,
                    'last_event_idx': 100,
                    'period_end_seconds': 720,
                    'next_period': 2,
                    'next_first_idx': 95,  # Overlap!
                    'next_start_seconds': 721
                }
            ],
        ]
        
        mock_conn.fetchrow.return_value = {
            'games_with_pbp': 2,
            'total_events': 200,
            'q1_coverage': 1.0,
            'q4_coverage': 0.5,  # Low Q4 coverage will trigger issue
            'events_missing_time': 0
        }
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_pbp_monotonicity(cutoff_time)
            
            # Check period boundary result
            period_result = next(r for r in results if r.check_type == "pbp_period_boundaries")
            assert period_result.is_valid is False  # Has period issues
            assert period_result.invalid_count == 1
            assert "Period boundary issue between Q1 and Q2" in period_result.issues[0]
            
            # Check completeness result (should flag low Q4 coverage)
            completeness_result = next(r for r in results if r.check_type == "pbp_completeness")
            assert completeness_result.is_valid is False  # Low Q4 coverage
            assert "Low Q4 coverage: 50.00% of games" in completeness_result.issues
    
    @pytest.mark.asyncio
    async def test_validate_pbp_completeness_issues(self, validator):
        """Test PBP completeness validation with various issues."""
        mock_conn = AsyncMock()
        
        # Mock queries (all empty except stats)
        mock_conn.fetch.side_effect = [[], [], [], []]
        
        # Mock stats with multiple completeness issues
        mock_conn.fetchrow.return_value = {
            'games_with_pbp': 100,
            'total_events': 5000,
            'q1_coverage': 0.7,  # Low Q1 coverage
            'q4_coverage': 0.75,  # Low Q4 coverage
            'events_missing_time': 50  # Missing timestamps
        }
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_pbp_monotonicity(cutoff_time)
            
            # Check completeness result
            completeness_result = next(r for r in results if r.check_type == "pbp_completeness")
            assert completeness_result.is_valid is False
            assert completeness_result.invalid_count == 50  # Missing timestamps
            
            # Should have 3 issues: missing timestamps, low Q1, low Q4
            assert len(completeness_result.issues) == 3
            assert "50 events missing seconds_elapsed timestamps" in completeness_result.issues
            assert "Low Q1 coverage: 70.00% of games" in completeness_result.issues
            assert "Low Q4 coverage: 75.00% of games" in completeness_result.issues
            
            # Check details
            assert completeness_result.details['games_analyzed'] == 100
            assert completeness_result.details['stats']['total_events'] == 5000
    
    @pytest.mark.asyncio
    async def test_validate_pbp_all_valid(self, validator):
        """Test PBP validation with no issues found."""
        mock_conn = AsyncMock()
        
        # Mock all queries returning no issues
        mock_conn.fetch.side_effect = [[], [], [], []]
        
        # Mock perfect stats
        mock_conn.fetchrow.return_value = {
            'games_with_pbp': 50,
            'total_events': 2500,
            'q1_coverage': 1.0,  # Perfect coverage
            'q4_coverage': 1.0,  # Perfect coverage
            'events_missing_time': 0  # No missing timestamps
        }
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            results = await validator._validate_pbp_monotonicity(cutoff_time)
            
            # All 5 validation checks should pass
            assert len(results) == 5
            for result in results:
                assert result.is_valid is True
                assert result.invalid_count == 0
                assert len(result.issues) == 0
            
            # Check that all expected validation types are present
            check_types = {r.check_type for r in results}
            expected_types = {
                'pbp_monotonicity_gaps',
                'pbp_monotonicity_overlaps', 
                'pbp_clock_progression',
                'pbp_period_boundaries',
                'pbp_completeness'
            }
            assert check_types == expected_types
    
    def test_get_validation_summary(self, validator, sample_validation_results):
        """Test validation summary generation."""
        summary = validator.get_validation_summary(sample_validation_results)
        
        assert summary['total_checks'] == 3
        assert summary['failed_checks'] == 2
        assert summary['success_rate'] == 1/3
        assert summary['total_issues'] == 3  # 2 + 1 from sample data
        assert summary['total_records_checked'] == 153  # 100 + 50 + 3
        assert summary['total_invalid_records'] == 8  # 0 + 5 + 3
        assert 'validation_timestamp' in summary
        assert 'data_quality_score' in summary
        assert summary['failed_tables'] == ['early_shocks', 'games']


class TestDerivedLoaderValidation:
    """Test FK validation integration in DerivedLoader."""
    
    @pytest.fixture
    def loader(self):
        """Create a DerivedLoader instance."""
        return DerivedLoader()
    
    @pytest.fixture
    def sample_q1_windows(self):
        """Sample Q1 window data."""
        return [
            Q1WindowRow(
                game_id="0022400123",
                home_team_tricode="LAL",
                away_team_tricode="BOS",
                possessions_elapsed=15,
                pace48_actual=98.5,
                pace48_expected=96.2,
                home_efg_actual=0.55,
                home_efg_expected=0.52,
                away_efg_actual=0.48,
                away_efg_expected=0.50,
                home_to_rate=0.12,
                away_to_rate=0.15,
                home_ft_rate=0.25,
                away_ft_rate=0.22,
                home_orb_pct=0.28,
                home_drb_pct=0.75,
                away_orb_pct=0.25,
                away_drb_pct=0.72,
                bonus_time_home_sec=45.0,
                bonus_time_away_sec=0.0,
                transition_rate=0.18,
                early_clock_rate=0.35,
                source="test",
                source_url="test://url"
            ),
            Q1WindowRow(
                game_id="0022400999",  # Invalid game_id
                home_team_tricode="GSW",
                away_team_tricode="MIA",
                possessions_elapsed=12,
                pace48_actual=102.1,
                pace48_expected=99.8,
                home_efg_actual=0.58,
                home_efg_expected=0.54,
                away_efg_actual=0.51,
                away_efg_expected=0.49,
                home_to_rate=0.08,
                away_to_rate=0.11,
                home_ft_rate=0.28,
                away_ft_rate=0.24,
                home_orb_pct=0.32,
                home_drb_pct=0.78,
                away_orb_pct=0.22,
                away_drb_pct=0.68,
                bonus_time_home_sec=0.0,
                bonus_time_away_sec=30.0,
                transition_rate=0.22,
                early_clock_rate=0.40,
                source="test",
                source_url="test://url"
            )
        ]
    
    @pytest.mark.asyncio
    async def test_upsert_q1_windows_with_valid_records(self, loader, sample_q1_windows):
        """Test Q1 window upsert with all valid records."""
        # Mock validator to return all records as valid
        mock_validator = AsyncMock()
        mock_validator.validate_before_insert.return_value = (
            [{'game_id': '0022400123'}, {'game_id': '0022400999'}],
            []
        )
        loader.validator = mock_validator

        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "INSERT 0 2"  # Simulate inserts
        mock_conn.transaction.return_value.__aenter__.return_value = None
        mock_conn.transaction.return_value.__aexit__.return_value = None

        # Mock bulk_optimizer to return coroutine
        with patch('nba_scraper.loaders.derived.bulk_optimizer') as mock_bulk_opt:
            mock_bulk_opt.bulk_upsert = AsyncMock(return_value=2)
            with patch('nba_scraper.loaders.derived.get_performance_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__.return_value = mock_conn
                mock_get_conn.return_value.__aexit__.return_value = None
                
                result = await loader.upsert_q1_windows(sample_q1_windows)

                assert result == 2  # Two records were processed
                mock_validator.validate_before_insert.assert_called_once()
                mock_bulk_opt.bulk_upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_q1_windows_with_invalid_records(self, loader, sample_q1_windows):
        """Test Q1 window upsert with some invalid records."""
        # Mock validator to filter out invalid records
        mock_validator = AsyncMock()
        mock_validator.validate_before_insert.return_value = (
            [{'game_id': '0022400123'}],  # Only first record is valid
            ["Missing game_id references for q1_window_12_8: ['0022400999']"]
        )
        loader.validator = mock_validator

        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "INSERT 0 1"
        mock_conn.transaction.return_value.__aenter__.return_value = None
        mock_conn.transaction.return_value.__aexit__.return_value = None

        # Mock bulk_optimizer to return coroutine
        with patch('nba_scraper.loaders.derived.bulk_optimizer') as mock_bulk_opt:
            mock_bulk_opt.bulk_upsert = AsyncMock(return_value=1)
            with patch('nba_scraper.loaders.derived.get_performance_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__.return_value = mock_conn
                mock_get_conn.return_value.__aexit__.return_value = None
                
                result = await loader.upsert_q1_windows(sample_q1_windows)

                assert result == 1  # One record was processed
                mock_validator.validate_before_insert.assert_called_once()
                mock_bulk_opt.bulk_upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_q1_windows_all_invalid_records(self, loader, sample_q1_windows):
        """Test Q1 window upsert when all records are invalid."""
        # Mock validator to reject all records
        mock_validator = AsyncMock()
        mock_validator.validate_before_insert.return_value = (
            [],  # No valid records
            ["Missing game_id references for q1_window_12_8: ['0022400123', '0022400999']"]
        )
        loader.validator = mock_validator
        
        # Mock database connection (should not be called)
        mock_conn = AsyncMock()
        
        with patch('nba_scraper.loaders.derived.get_connection', return_value=mock_conn):
            result = await loader.upsert_q1_windows(sample_q1_windows)
            
            assert result == 0
            mock_validator.validate_before_insert.assert_called_once()
            assert mock_conn.execute.call_count == 0  # No records inserted
    
    @pytest.mark.asyncio
    async def test_upsert_early_shocks_with_validation(self, loader):
        """Test early shocks upsert with FK validation."""
        sample_shocks = [
            EarlyShockRow(
                game_id="0022400123",
                shock_seq=1,
                event_idx_start=25,
                shock_type=EarlyShockType.TECH,
                period=1,
                clock_hhmmss="00:10:30",
                player_slug="lebronjames",
                team_tricode="LAL",
                immediate_sub=False,
                notes="Technical foul",
                source="test",
                source_url="test://url"
            ),
            EarlyShockRow(
                game_id="0022400999",  # Invalid
                shock_seq=1,
                event_idx_start=18,
                shock_type=EarlyShockType.FLAGRANT,
                period=1,
                clock_hhmmss="00:08:15",
                player_slug="stephencurry",
                team_tricode="GSW",
                immediate_sub=True,
                notes="Flagrant 1",
                source="test",
                source_url="test://url"
            )
        ]

        # Mock validator to filter out invalid record
        mock_validator = AsyncMock()
        mock_validator.validate_before_insert.return_value = (
            [{'game_id': '0022400123'}],
            ["Missing game_id references for early_shocks: ['0022400999']"]
        )
        loader.validator = mock_validator

        # Mock database connection
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "INSERT 0 1"
        mock_conn.transaction.return_value.__aenter__.return_value = None
        mock_conn.transaction.return_value.__aexit__.return_value = None

        # Mock bulk_optimizer to return coroutine
        with patch('nba_scraper.loaders.derived.bulk_optimizer') as mock_bulk_opt:
            mock_bulk_opt.bulk_upsert = AsyncMock(return_value=1)
            with patch('nba_scraper.loaders.derived.get_performance_connection') as mock_get_conn:
                mock_get_conn.return_value.__aenter__.return_value = mock_conn
                mock_get_conn.return_value.__aexit__.return_value = None
                
                result = await loader.upsert_early_shocks(sample_shocks)

                assert result == 1  # One record was upserted
                mock_validator.validate_before_insert.assert_called_once()
                mock_bulk_opt.bulk_upsert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_early_shock_validation_with_missing_fields(self):
        """Test validation with missing required fields in EarlyShockRow."""
        from src.nba_scraper.loaders.derived import DerivedLoader
        from src.nba_scraper.models.derived_rows import EarlyShockRow, EarlyShockType
        from unittest.mock import AsyncMock, patch
        
        loader = DerivedLoader()
        
        # Create test data with all required fields
        shocks = [
            EarlyShockRow(
                game_id="0022400001", 
                shock_type=EarlyShockType.TWO_PF_EARLY,
                period=1,
                clock_hhmmss="00:10:30",
                player_slug="lebron_james",
                team_tricode="LAL",
                immediate_sub=True,
                notes="Two early fouls",
                source="test",
                source_url="http://test.com",
                shock_seq=1,  # Required field
                event_idx_start=15  # Required field
            )
        ]
        
        # Mock the bulk operations properly
        with patch('src.nba_scraper.loaders.derived.get_performance_connection') as mock_get_conn:
            with patch('src.nba_scraper.loaders.derived.bulk_optimizer') as mock_bulk_optimizer:
                # Set up the async context manager for the connection
                mock_conn = AsyncMock()
                mock_get_conn.return_value.__aenter__.return_value = mock_conn
                mock_get_conn.return_value.__aexit__.return_value = None
                
                # Mock the bulk_upsert to return an awaitable coroutine
                mock_bulk_optimizer.bulk_upsert = AsyncMock(return_value=1)
                
                # Mock the validator to return valid records
                with patch.object(loader.validator, 'validate_before_insert') as mock_validate:
                    mock_validate.return_value = ([{'game_id': "0022400001"}], [])
                    
                    result = await loader.upsert_early_shocks(shocks)
                    
                    # Verify the result
                    assert result == 1
                    
                    # Verify validation was called
                    mock_validate.assert_called_once_with('early_shocks', [{'game_id': '0022400001'}])
                    
                    # Verify bulk_upsert was called
                    mock_bulk_optimizer.bulk_upsert.assert_called_once()


class TestValidationIntegration:
    """End-to-end integration tests for validation system."""
    
    @pytest.mark.asyncio
    async def test_full_validation_workflow(self):
        """Test complete validation workflow from detection to reporting."""
        validator = DataQualityValidator()
        
        # Mock comprehensive database responses
        mock_conn = AsyncMock()
        
        # Mock responses for different validation checks
        mock_responses = [
            # Core FK validation
            {'total_count': 100, 'invalid_count': 2},
            [{'game_id': '0022400999'}, {'game_id': '0022401000'}],
            
            # Derived table FK validations (9 tables)
            *[{'total_count': 50, 'invalid_count': 0} for _ in range(9)],
            
            # Orphaned records
            [],
            
            # Uniqueness constraints
            [{'bref_game_id': 'TEST123', 'count': 2}],
            [{'game_id': '0022400123', 'referee_name_slug': 'johnsmith', 'count': 2, 'roles': 'CREW_CHIEF, REFEREE'}],
            
            # Cross-table consistency
            [],
            []
        ]
        
        # Set up mock to return different responses for each call
        mock_conn.fetchrow.side_effect = mock_responses[:10]  # fetchrow calls
        mock_conn.fetch.side_effect = mock_responses[10:]     # fetch calls
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            results = await validator.validate_all_tables(since_hours=24)
            
            # Verify we got results for all validation types
            assert len(results) > 10  # Should have multiple validation results
            
            # Check that we have different types of validations
            check_types = {r.check_type for r in results}
            expected_types = {
                'foreign_key_validation',
                'orphaned_records', 
                'uniqueness_validation',
                'cross_table_consistency'
            }
            assert expected_types.issubset(check_types)
            
            # Generate summary
            summary = await validator.get_validation_summary(results)
            
            assert 'validation_timestamp' in summary
            assert 'total_checks' in summary
            assert 'data_quality_score' in summary
            assert summary['total_checks'] == len(results)
    
    @pytest.mark.asyncio 
    async def test_validation_with_real_database_structure(self):
        """Test validation queries against realistic database structure."""
        # This test would use a real test database in a full implementation
        # For now, we'll mock the expected database responses
        
        validator = DataQualityValidator()
        mock_conn = AsyncMock()
        
        # Mock realistic database responses
        mock_conn.fetchrow.side_effect = [
            # game_id_crosswalk FK check
            {'total_count': 1000, 'invalid_count': 0},
            # Various derived table FK checks
            *[{'total_count': 200, 'invalid_count': 0} for _ in range(9)]
        ]
        
        mock_conn.fetch.side_effect = [
            # Orphaned records check
            [],
            # Uniqueness checks
            [],
            [],
            # Cross-table consistency
            [],
            []
        ]
        
        with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
            results = await validator.validate_all_tables(since_hours=1)
            
            # All checks should pass with clean data
            failed_results = [r for r in results if not r.is_valid]
            assert len(failed_results) == 0
            
            # Should have validated all expected tables
            table_names = {r.table_name for r in results}
            expected_tables = {
                'game_id_crosswalk', 'q1_window_12_8', 'early_shocks',
                'schedule_travel', 'outcomes', 'ref_assignments',
                'ref_alternates', 'starting_lineups', 'injury_status',
                'pbp_events', 'games'
            }
            assert expected_tables.issubset(table_names)