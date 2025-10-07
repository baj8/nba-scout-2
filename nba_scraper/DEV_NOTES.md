# NBA Scraper Development Notes

## CURRENT TASK
**Live API integration testing** - Create integration tests for NBA Stats API, BRef scraping, and gamebook downloads with proper mocking.
- Files: `tests/integration/test_live_api.py`
- Status: NEXT
- Goal: Comprehensive integration test suite for API clients with realistic mocking and error handling

## Project Overview

This is a production-grade NBA Historical Scraping & Ingestion Engine that fetches, normalizes, validates, and persists NBA historical datasets. It's built with async-first architecture, comprehensive rate limiting, and multi-source data integration.

## 🔒 STRICT REPOSITORY VALIDATION RULES

**These rules are MANDATORY and must be enforced in ALL code changes:**

### Game ID Validation
- **Format**: Must be exactly 10 characters, numeric, starts with "0022" (regular season)
- **Examples**: ✅ `"0022301234"` ❌ `"0012301234"` (wrong prefix) ❌ `"002230123"` (too short)
- **Error Handling**: If invalid → `raise ValueError(f"invalid game_id: {game_id}")`
- **Implementation**: Use regex `^0022\d{6}$` in `transform_game()`

### Season Validation  
- **Format**: Accept only "YYYY-YY" format (e.g., "2024-25")
- **Error Handling**: If invalid/missing → `logger.warning("season format invalid: {season}")`
- **Fallback**: Always use `derive_season_smart()` for automatic derivation
- **Return**: Always return as string, never fail processing

### Pipeline Integration Rules
- **Critical Failures**: Game ID validation failures → raise `ValueError`
- **Foundation Pipeline**: Must catch validation errors and set `game_processed=False`
- **Non-Critical Issues**: Bad season formats → log warning (not error), continue processing
- **Continuation**: Pipeline MUST continue PBP + lineups even if game metadata fails
- **Error Aggregation**: Collect all errors in `result["errors"]` list

### Logging Requirements
- **Logger**: Always use `logging.getLogger(__name__)`
- **Context**: Include game_id, season, error type in all log messages
- **Levels**: Critical validation → ERROR, format issues → WARNING
- **Format**: Structured logging with trace context

### Testing Requirements
- **Coverage**: ALL validation errors must have unit test coverage
- **Game Transformer Tests**: Must pass strict validation rules (22/22 tests)
- **Foundation Pipeline Tests**: Must handle validation errors gracefully (8/8 tests)
- **Guardrail Tests**: Ensure preprocessing never strips leading zeros from game IDs
- **Integration**: Test continuation of PBP/lineups after game validation failures

### Repository Hardening
- **CI Pipeline**: Must run pytest + ruff + mypy on all PRs
- **Pre-commit Hooks**: Enforce style/type checks before commits
- **PR Template**: Requires validation checklist completion
- **Version Control**: All validation rule changes require version bump

### Code Examples

**✅ CORRECT Game ID Validation:**
```python
import re
import logging

logger = logging.getLogger(__name__)

def transform_game(game_data: dict) -> Game:
    game_id = str(game_data.get("game_id", ""))
    
    # Strict validation - exactly 10 chars, numeric, starts with "0022"
    if not re.match(r"^0022\d{6}$", game_id):
        raise ValueError(f"invalid game_id: {game_id!r} - must be 10-char string matching ^0022\\d{{6}}$")
    
    # Season validation with fallback
    season = str(game_data.get("season", ""))
    if not re.match(r"^\d{4}-\d{2}$", season):
        logger.warning("season format invalid: %s - expected YYYY-YY format", season, 
                      extra={"game_id": game_id, "invalid_season": season})
        season = derive_season_smart(game_id, game_data.get("game_date"), season)
    
    return Game(game_id=game_id, season=season, ...)
```

**✅ CORRECT Pipeline Error Handling:**
```python
async def process_game(self, game_id: str) -> dict:
    results = {"game_id": game_id, "game_processed": False, "errors": []}
    
    try:
        game_meta = extract_game_from_boxscore(boxscore_resp)
        game_model = transform_game(game_meta)  # Can raise ValueError
        await upsert_game(conn, game_model)
        results["game_processed"] = True
    except ValueError as e:
        error_msg = f"Game metadata validation failed: {e}"
        results["errors"].append(error_msg)
        logger.error(error_msg, game_id=game_id, exc_info=True)
        # DON'T return early - continue with PBP processing
    
    # Continue PBP processing even if game validation failed
    try:
        pbp_events = process_pbp(game_id)
        results["pbp_events_processed"] = len(pbp_events)
    except Exception as e:
        results["errors"].append(f"PBP processing failed: {e}")
    
    return results
```

**❌ WRONG - Don't Do This:**
```python
# DON'T: Lenient game ID validation
if len(game_id) < 8:  # Too lenient!
    
# DON'T: Fail on season format issues  
if not season_valid:
    raise ValueError("Bad season")  # Should warn + fallback!
    
# DON'T: Stop processing on validation errors
if validation_error:
    return {"error": "Failed"}  # Should continue PBP/lineups!
```

# Executive Summary

This repository powers an async-first NBA data pipeline with strict validation rules to ensure data integrity and production readiness.  
The goal of this DEV_NOTES file is to give engineers a single source of truth for how to contribute safely, efficiently, and consistently.  

At the heart of this project is the **strict validation system**: every game, season, and pipeline event must meet exact rules before entering the database.  
This protects against silent data corruption and guarantees downstream analytics reliability.

---

## 🔒 Strict Validation Rules (Refined)

- **Game ID Validation**
  - ✅ Must be exactly 10 characters
  - ✅ Must be numeric
  - ✅ Must start with `0022` (regular season format)
  - ❌ Wrong: `"22301234"`, `"game1"`

- **Season Validation**
  - ✅ Accept only `"YYYY-YY"` format (e.g. `"2024-25"`)
  - ⚠️ Invalid formats (e.g. `"2024"`, `"2024-2025"`, `"24-25"`) will log a warning and fall back to `derive_season_smart()`

- **Pipeline Integration Rules**
  - Critical failures → `ValueError` raised  
  - Non-critical issues → logged as warnings but pipeline continues

- **Logging Requirements**
  - Use `logging.getLogger(__name__)`  
  - Always include game_id/season context in log messages

- **Testing Requirements**
  - Every validation error path must have at least one unit test
  - Edge cases (bad game_id, malformed season) must be covered

- **Repository Hardening**
  - CI must run: `pytest --maxfail=1 -q`, `ruff check .`, and `mypy .`

### Key Technologies
- **Python 3.11+** with async/await
- **PostgreSQL 12+** with asyncpg driver
- **httpx** for async HTTP requests
- **Pydantic** for data validation
- **SQLAlchemy** for database operations
- **Typer** for CLI interface
- **Rich** for beautiful terminal output

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Transformation │    │    PostgreSQL   │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ NBA Stats API   │───▶│ Pydantic Models  │───▶│ Core Tables     │
│ Basketball Ref  │    │ Rate Limiting    │    │ Derived Tables  │
│ NBA Game Books  │    │ Normalization    │    │ Indexes & FKs   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Core Components

1. **Extractors** (`src/nba_scraper/extractors/`) - Pure functions for data extraction
2. **IO Clients** (`src/nba_scraper/io_clients/`) - HTTP clients for each data source
3. **Transformers** (`src/nba_scraper/transformers/`) - Data transformation logic
4. **Loaders** (`src/nba_scraper/loaders/`) - Database upsert operations
5. **Pipelines** (`src/nba_scraper/pipelines/`) - Orchestration workflows
6. **Models** (`src/nba_scraper/models/`) - Pydantic data models

## Development Setup

### Prerequisites
```bash
# Python 3.11+
python --version

# PostgreSQL 12+
psql --version

# Git
git --version
```

### Initial Setup
```bash
# Clone and navigate
git clone <repository-url>
cd nba_scraper

# Install dependencies (recommended: uv)
pip install uv
uv sync

# Alternative: pip
pip install -e ".[dev]"

# Environment setup
cp .env.example .env
# Edit .env with your database credentials

# Database setup
createdb nba_scraper
psql nba_scraper < schema.sql
```

### Development Workflow

#### Code Quality Tools
```bash
# Format code
ruff format

# Lint code
ruff check

# Type checking
mypy src/nba_scraper

# Run all quality checks
ruff format && ruff check && mypy src/nba_scraper
```

#### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=nba_scraper

# Run specific test types
pytest tests/unit/
pytest tests/integration/
pytest -k validation

# Verbose output
pytest -v
```

## Key Conventions & Patterns

### Async-First Architecture
- All IO operations use `asyncio` and `httpx`
- Rate limiting with token bucket (45 req/min)
- Exponential backoff on errors
- Streaming parsers to avoid memory issues

### Data Normalization
- **Team tricodes**: Normalized via `team_aliases.yaml`, always uppercase
- **Player names**: PascalCase slugs (e.g., "LeBron James" → "LebronJames")
- **Timestamps**: All stored in UTC, arena timezone preserved
- **Enums**: Strict validation for event types, injury status, referee roles

### Database Patterns
- **Idempotent upserts**: `ON CONFLICT DO UPDATE SET ... WHERE excluded.col IS DISTINCT FROM target.col`
- **Provenance**: Every table has `source`, `source_url`, and `ingested_at_utc` columns
- **Natural PKs**: Composite keys using business identifiers
- **Foreign Keys**: Referential integrity with cascade deletes

### Code Style
- Type hints everywhere
- Pure functions for extractors and transformers
- Async context managers for resources
- Rich CLI with progress bars
- Defensive parsing with fallbacks and logging
- Structured logging with trace IDs

## Core Data Tables

### Primary Tables
- `games` - Game metadata with arena timezones
- `game_id_crosswalk` - ID mapping between data sources
- `ref_assignments` & `ref_alternates` - Referee crew information
- `starting_lineups` & `injury_status` - Player availability
- `pbp_events` - Normalized play-by-play with enrichment

### Derived Analytics Tables
- `q1_window_12_8` - First quarter analytics (12:00-8:00)
- `early_shocks` - Disruption events (fouls, technicals, injuries)
- `schedule_travel` - Travel fatigue and circadian metrics
- `outcomes` - Final scores and quarter breakdowns

## CLI Commands Reference

### Development & Testing
```bash
# Check system status
nba-scraper status

# Test with single day
nba-scraper daily --date-range 2024-01-15

# Validate data quality
nba-scraper validate --since 2024-01-01 --verbose
```

### Data Pipeline Operations
```bash
# Backfill historical data
nba-scraper backfill --seasons 2023-24 --dry-run
nba-scraper backfill --seasons 2023-24

# Daily incremental ingestion
nba-scraper daily
nba-scraper daily --date-range 2024-01-15..2024-01-20

# Derive analytics
nba-scraper derive --date-range 2024-01-01..2024-01-31
nba-scraper derive --date-range 2024-01-15 --tables q1_window,outcomes --force
```

## Configuration

### Environment Variables (.env)
```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/nba_scraper

# HTTP Client
USER_AGENT=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36
REQUESTS_PER_MIN=45
RETRY_MAX=5

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Pipeline
MAX_CONCURRENT_REQUESTS=5
BACKFILL_CHUNK_SIZE=10
CHECKPOINT_ENABLED=true
```

### Key Configuration Files
- `team_aliases.yaml` - Team tricode normalization mapping
- `venues.csv` - Arena coordinates and timezones
- `schema.sql` - Complete PostgreSQL DDL
- `pyproject.toml` - Project dependencies and tools

## Common Development Tasks

### Adding New Data Sources
1. Create client in `io_clients/`
2. Add extraction functions in `extractors/`
3. Create/update Pydantic models in `models/`
4. Add transformation logic in `transformers/`
5. Update database loader in `loaders/`
6. Add tests with golden files

### Adding New Analytics
1. Define derived table in `schema.sql`
2. Create transformation logic in `transformers/`
3. Add loader function in `loaders/derived.py`
4. Update analytics pipeline
5. Add validation tests

### Debugging Data Issues
```bash
# Check recent games
psql nba_scraper -c "SELECT * FROM games WHERE game_date_local >= CURRENT_DATE - INTERVAL '7 days';"

# Validate data completeness
psql nba_scraper -c "SELECT COUNT(*) FROM pbp_events WHERE game_id = 'specific_game_id';"

# Check for duplicates
psql nba_scraper -c "SELECT game_id, COUNT(*) FROM games GROUP BY game_id HAVING COUNT(*) > 1;"
```

## Troubleshooting Guide

### Common Issues

#### Database Connection Failed
```bash
# Check PostgreSQL status
pg_ctl status

# Test connection
psql nba_scraper -c "SELECT 1"

# Verify environment
echo $DATABASE_URL
```

#### Rate Limit Errors
```bash
# Check rate limit setting
grep REQUESTS_PER_MIN .env

# Monitor in logs
nba-scraper daily --date-range 2024-01-15 | grep "rate_limit"
```

#### Memory Issues
```bash
# Reduce batch sizes
export BACKFILL_CHUNK_SIZE=5
export MAX_CONCURRENT_REQUESTS=2

# Process smaller chunks
nba-scraper backfill --seasons 2023-24
```

#### Missing Dependencies
```bash
# Reinstall with dev dependencies
pip install -e ".[dev]"

# macOS: Install PostgreSQL client
brew install postgresql
```

### Data Quality Issues

#### Duplicate Detection
```sql
-- Check for duplicate games
SELECT game_id, COUNT(*) 
FROM games 
GROUP BY game_id 
HAVING COUNT(*) > 1;
```

#### Missing Crosswalk Entries
```sql
-- Games without B-Ref mapping
SELECT g.game_id, g.home_team_tricode, g.away_team_tricode 
FROM games g 
LEFT JOIN game_id_crosswalk c ON g.game_id = c.game_id 
WHERE c.game_id IS NULL;
```

#### PBP Validation
```sql
-- Check for PBP gaps
SELECT game_id, period, 
       LAG(event_idx) OVER (PARTITION BY game_id, period ORDER BY event_idx) as prev_idx,
       event_idx
FROM pbp_events 
WHERE event_idx - LAG(event_idx) OVER (PARTITION BY game_id, period ORDER BY event_idx) > 1;
```

## Testing Strategy

### Test Structure
```
tests/
├── unit/                 # Fast, isolated tests
├── integration/          # Database and API tests
└── data/                 # Golden test fixtures
    ├── expected/
    └── raw/
```

### Test Categories
- **Unit Tests**: Pure function testing with mocked dependencies
- **Integration Tests**: Database operations and API calls
- **Golden File Tests**: Deterministic parsing with known inputs
- **Validation Tests**: Data quality and completeness checks

### Best Practices
- No live API calls in CI
- Use fixtures for consistent test data
- Test error conditions and edge cases
- Validate data transformations thoroughly

# Development Best Practices

## 🔀 Branching & PR Strategy
- Create feature branches off `main`
- Use squash merges to keep history clean
- PRs must reference related issues/tickets

## 👥 Code Review
- All PRs require peer review
- Use the strict validation checklist in `.github/PULL_REQUEST_TEMPLATE`
- Block merges if validation rules, lint, or type checks fail

## 📦 Dependency Management
- Pin all dependencies in `pyproject.toml`
- Run `uv sync --frozen` before CI
- Regularly update with `uv pip upgrade` and review changelog
- Security scans must pass (Dependabot or equivalent)

## 🧪 Testing Expectations
- **Unit tests** for transformers, extractors, loaders
- **Integration tests** for full pipeline runs
- **Regression tests** for critical bugfixes
- Minimum 90% coverage required on PRs

## 📊 Logging & Monitoring
- Use structured logs (`game_id`, `season`, `event_type`)
- Log levels: INFO for success, WARNING for recoverable issues, ERROR for failures
- Monitor ETL runs with alerts on pipeline errors

## ⚙️ CI/CD Standards
- CI must include: tests, lint, type check, coverage
- CD must validate migrations and run smoke tests before deploy

## 🔑 Security Guidelines
- Never commit API keys or secrets
- Use `.env` with `dotenv` (ignored by git)
- Rotate keys regularly

## 📚 Documentation Standards
- Update README for new features
- Maintain CHANGELOG.md with every release
- All public functions require docstrings

## Deployment & Operations
```