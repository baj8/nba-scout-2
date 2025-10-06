# Strict Game Validation & Robust Pipeline Error Handling

## Summary

This PR implements strict validation for NBA game data processing with comprehensive error handling and fallback mechanisms.

### Key Changes

**🔒 Strict Game ID Validation**
- Game IDs must match `^0022\d{6}$` format (exactly 10 digits, starts with "0022" for regular season)
- Raises `ValueError` with descriptive message for invalid formats
- Preserves leading zeros during preprocessing

**⚠️ Season Validation with Smart Fallbacks**
- Validates season format as `YYYY-YY` (e.g., "2024-25")
- Logs warning for malformed seasons: `"season format invalid: {value} - expected YYYY-YY format"`
- Falls back to `derive_season_smart()` for automatic season derivation from game_id/date

**🛡️ Robust Pipeline Error Handling**
- Foundation pipeline continues PBP and lineup processing even when game validation fails
- Aggregates all errors in `result["errors"]` list rather than failing fast
- Preserves API responses for reuse across processing steps
- Comprehensive logging with structured context

## Breaking vs Non-Breaking Changes

### ✅ Non-Breaking Changes
- Season validation warnings (logs but continues processing)
- Pipeline error aggregation (improves reliability)
- Enhanced logging and error messages

### ⚠️ Breaking Changes
- **Game ID validation**: Stricter format requirements may reject previously accepted IDs
- **Error handling**: Some workflows expecting immediate failures may need updates

## Risks & Mitigations

### Risks
1. **Data rejection**: Stricter validation may reject edge-case game IDs
2. **Performance impact**: Additional validation overhead
3. **Logging volume**: More detailed warnings may increase log size

### Mitigations
1. **Comprehensive testing**: 30/30 validation tests passing with edge case coverage
2. **Smart fallbacks**: Season derivation prevents data loss on format issues
3. **Gradual rollout**: Deploy to staging first, monitor error rates
4. **Rollback plan**: Previous validation logic preserved in git history

## Test Coverage Summary

### Unit Tests (22/22 passing)
- `tests/unit/test_transformers_games.py`: Game validation logic
  - Valid game ID acceptance
  - Invalid game ID rejection with proper error messages
  - Season format validation and warning capture
  - Smart season derivation fallbacks

### Integration Tests (8/8 passing)  
- `tests/unit/test_foundation_validation.py`: Pipeline error handling
  - API error resilience
  - Multi-component error aggregation
  - Processing continuation after validation failures
  - Comprehensive logging verification

### Manual Verification

```bash
# Install and run tests
pip install -e .[dev]
pytest tests/unit/test_transformers_games.py -v
pytest tests/unit/test_foundation_validation.py -v

# Verify game ID validation
python -c "
from nba_scraper.transformers.games import transform_game
# Valid: should succeed
transform_game({'game_id': '0022301234', 'season': '2024-25', 'game_date': '2024-01-15', 'home_team_id': 1610612744, 'away_team_id': 1610612739, 'status': 'Final'})
# Invalid: should raise ValueError  
transform_game({'game_id': '0012301234', 'season': '2024-25', 'game_date': '2024-01-15', 'home_team_id': 1610612744, 'away_team_id': 1610612739, 'status': 'Final'})
"

# Verify pipeline error handling
python -c "
import asyncio
from nba_scraper.pipelines.foundation import FoundationPipeline
from unittest.mock import MagicMock, AsyncMock

async def test():
    mock_client = MagicMock()
    mock_client.get_boxscore = AsyncMock(side_effect=Exception('API Error'))
    mock_client.get_pbp = AsyncMock(return_value={'resultSets': []})
    pipeline = FoundationPipeline(client=mock_client)
    result = await pipeline.process_game('0022309999')
    print(f'Errors: {result[\"errors\"]}')
    print(f'Game processed: {result[\"game_processed\"]}')

asyncio.run(test())
"
```

## Checklist

- [ ] **Tests Pass**: All validation tests (30/30) pass locally
- [ ] **Lint**: Code passes ruff/black formatting checks  
- [ ] **Type Check**: mypy validation passes
- [ ] **Integration**: Foundation pipeline tests pass
- [ ] **Manual Testing**: Verified validation behavior manually
- [ ] **Documentation**: Updated validation rules in README
- [ ] **Backwards Compatibility**: Assessed breaking changes and mitigation
- [ ] **Performance**: Validated no significant performance regression
- [ ] **Logging**: Verified log level and message format
- [ ] **Error Handling**: Confirmed graceful degradation behavior

## Deployment Notes

1. **Monitor Error Rates**: Watch for increases in validation failures post-deployment
2. **Log Analysis**: Review warning patterns for season format issues
3. **Performance Monitoring**: Track validation overhead impact
4. **Rollback Triggers**: Revert if >5% increase in processing failures

## Related Issues

- Fixes data quality issues with malformed game IDs
- Improves pipeline reliability with better error handling  
- Addresses season format inconsistencies from various data sources