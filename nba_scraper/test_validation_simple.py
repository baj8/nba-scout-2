#!/usr/bin/env python3
"""Simple test for data quality validation functionality."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

async def test_validation_functionality():
    """Test the core validation functionality."""
    print("🧪 Testing data quality validation...")
    
    try:
        from nba_scraper.validation import DataQualityValidator, ValidationResult
        print("✅ Successfully imported validation classes")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    
    # Test ValidationResult dataclass
    result = ValidationResult(
        table_name="test_table",
        check_type="foreign_key_validation",
        is_valid=True,
        record_count=100,
        invalid_count=0,
        issues=[],
        details={}
    )
    
    assert result.table_name == "test_table"
    assert result.is_valid is True
    print("✅ ValidationResult dataclass works correctly")
    
    # Test DataQualityValidator initialization
    validator = DataQualityValidator()
    assert validator is not None
    print("✅ DataQualityValidator initializes correctly")
    
    # Test validate_before_insert with mocked database
    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = [
        {'game_id': '0022400123'},
        {'game_id': '0022400124'}
    ]
    
    with patch('nba_scraper.validation.get_connection', return_value=mock_conn):
        records = [
            {'game_id': '0022400123', 'some_field': 'value1'},
            {'game_id': '0022400124', 'some_field': 'value2'},
            {'game_id': '0022400999', 'some_field': 'value3'}  # Invalid
        ]
        
        valid_records, errors = await validator.validate_before_insert('test_table', records)
        
        # Should filter out the invalid game_id
        assert len(valid_records) == 2
        assert len(errors) == 1
        assert 'Missing game_id references' in errors[0]
        print("✅ Pre-insert validation works correctly")
    
    # Test validation summary generation
    sample_results = [
        ValidationResult(
            table_name="table1",
            check_type="foreign_key_validation", 
            is_valid=True,
            record_count=100,
            invalid_count=0,
            issues=[]
        ),
        ValidationResult(
            table_name="table2",
            check_type="orphaned_records",
            is_valid=False,
            record_count=10,
            invalid_count=3,
            issues=["Issue 1", "Issue 2"]
        )
    ]
    
    summary = await validator.get_validation_summary(sample_results)
    
    assert summary['total_checks'] == 2
    assert summary['failed_checks'] == 1
    assert summary['success_rate'] == 0.5
    assert summary['total_issues'] == 2
    assert summary['total_records_checked'] == 110
    assert summary['total_invalid_records'] == 3
    assert 'data_quality_score' in summary
    assert 'validation_timestamp' in summary
    
    print("✅ Validation summary generation works correctly")
    
    return True

async def test_derived_loader_integration():
    """Test validation integration in DerivedLoader."""
    print("\n🧪 Testing DerivedLoader validation integration...")
    
    try:
        from nba_scraper.loaders.derived import DerivedLoader
        print("✅ Successfully imported DerivedLoader")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    
    # Test loader initialization with validator
    loader = DerivedLoader()
    assert loader.validator is not None
    print("✅ DerivedLoader initializes with validator")
    
    # Test that validation is called during upsert (we'll mock the database parts)
    mock_validator = AsyncMock()
    mock_validator.validate_before_insert.return_value = ([], [])  # No valid records
    
    loader.validator = mock_validator
    
    # Create minimal Q1WindowRow data structure for testing
    from collections import namedtuple
    Q1WindowRow = namedtuple('Q1WindowRow', [
        'game_id', 'home_team_tricode', 'away_team_tricode',
        'possessions_elapsed', 'pace48_actual', 'pace48_expected',
        'home_efg_actual', 'home_efg_expected', 'away_efg_actual',
        'away_efg_expected', 'home_to_rate', 'away_to_rate',
        'home_ft_rate', 'away_ft_rate', 'home_orb_pct', 'home_drb_pct',
        'away_orb_pct', 'away_drb_pct', 'bonus_time_home_sec',
        'bonus_time_away_sec', 'transition_rate', 'early_clock_rate',
        'source', 'source_url'
    ])
    
    windows = [
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
        )
    ]
    
    # Test that upsert_q1_windows calls validation
    result = await loader.upsert_q1_windows(windows)
    
    # Should return 0 because no valid records after validation
    assert result == 0
    mock_validator.validate_before_insert.assert_called_once()
    
    print("✅ DerivedLoader calls validation during upsert")
    
    return True

async def main():
    """Run all validation tests."""
    print("🚀 Starting Data Quality Validation Tests")
    print("=" * 50)
    
    try:
        success1 = await test_validation_functionality()
        success2 = await test_derived_loader_integration()
        
        if success1 and success2:
            print("\n" + "=" * 50)
            print("🎉 All validation functionality tests passed!")
            print("\n📊 Summary of tested functionality:")
            print("✅ ValidationResult dataclass creation")
            print("✅ DataQualityValidator initialization")
            print("✅ Pre-insert FK validation with error detection")
            print("✅ Validation summary generation with metrics")
            print("✅ DerivedLoader integration with validation")
            print("✅ FK violation prevention in upsert operations")
            print("\n✨ Data quality validation system is ready for production!")
            return True
        else:
            print("\n❌ Some tests failed")
            return False
            
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)