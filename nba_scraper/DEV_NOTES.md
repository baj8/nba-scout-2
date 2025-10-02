# NBA Scraper Development Notes

## CURRENT TASK
**Live API integration testing** - Create integration tests for NBA Stats API, BRef scraping, and gamebook downloads with proper mocking.
- Files: `tests/integration/test_live_api.py`
- Status: NEXT
- Goal: Comprehensive integration test suite for API clients with realistic mocking and error handling

## Project Overview

This is a production-grade NBA Historical Scraping & Ingestion Engine that fetches, normalizes, validates, and persists NBA historical datasets. It's built with async-first architecture, comprehensive rate limiting, and multi-source data integration.

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

## Deployment & Operations

### Safe Backfill Process
```bash
# Start with recent season (faster feedback)
nba-scraper backfill --seasons 2024-25 --dry-run
nba-scraper backfill --seasons 2024-25

# Process earlier seasons individually
nba-scraper backfill --seasons 2023-24
nba-scraper backfill --seasons 2022-23

# Derive analytics for all seasons
nba-scraper derive --date-range 2021-10-01..2025-06-30

# Final validation
nba-scraper validate --since 2021-10-01
```

### Monitoring
```bash
# Watch logs in real-time
tail -f logs/nba_scraper.log | jq '.'

# Check database growth
psql nba_scraper -c "
SELECT schemaname, tablename, 
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"
```

## Contributing Guidelines

1. **Fork and Branch**: Create feature branches from main
2. **Code Quality**: Run `ruff format && ruff check && mypy src/nba_scraper`
3. **Testing**: Add tests for new functionality
4. **Documentation**: Update relevant docs and docstrings
5. **Pull Requests**: Include clear description and test results

## Useful Resources

- **Project Documentation**: `README.md`
- **Database Schema**: `schema.sql`
- **Configuration Examples**: `.env.example`
- **API Documentation**: Inline docstrings and type hints
- **Test Examples**: `tests/` directory

## Development Tips

1. **Use dry-run mode** for testing pipeline changes
2. **Start with small date ranges** when debugging
3. **Check logs frequently** - structured logging provides good visibility
4. **Validate data quality** after significant changes
5. **Use database constraints** to catch data issues early
6. **Profile memory usage** on large backfills
7. **Test rate limiting** - respect API limits