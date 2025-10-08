# NBA Historical Scraping & Ingestion Engine

A production-grade data pipeline for fetching, normalizing, validating, and persisting NBA historical datasets with async IO, rate limiting, and comprehensive analytics derivation.

## 📚 Documentation

- **[DEV_NOTES.md](DEV_NOTES.md)** - Engineering rules, hardening policies, and development checklists
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common failures, diagnostics, and step-by-step fixes
- **[MIGRATIONS_SETUP.md](MIGRATIONS_SETUP.md)** - Database migration procedures with Alembic
- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Contribution guidelines and code review process

## Dev quickstart

```bash
# Setup environment
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

# Raw harvest (no DB dependencies)
python -m nba_scraper.tools.raw_harvest_date --date 2023-10-27 --root "/tmp/nba_raw"

# Silver load (requires DB connection configured)
python -m nba_scraper.tools.silver_load_date --date 2023-10-27 --raw-root "/tmp/nba_raw"

# Run smoke tests
python tests/smoke/test_imports.py

# Test CLI (with all dependencies)
nba-scraper --help
```

## NBA Data Context

### Basketball Fundamentals for Developers

**NBA Season Structure:**
- **Regular Season**: October-April (~82 games per team, 1,230 total games)
- **Playoffs**: April-June (~90 games, best-of-7 series format)
- **Game Structure**: 4 quarters × 12 minutes, overtime periods if tied
- **Key Dates**: Trade deadline (~February), All-Star break (~February)

**Game Types & Scenarios:**
- **Regular Season**: Standard 82-game schedule with back-to-backs and travel
- **Playoffs**: Higher intensity, longer games, more timeouts and reviews
- **Preseason**: Exhibition games with different rules and lineups
- **Summer League**: Development games with modified rules

**Statistical Concepts:**
- **Box Score Stats**: Points, rebounds, assists, steals, blocks, turnovers, fouls
- **Advanced Metrics**: Plus/minus, true shooting %, usage rate, pace, efficiency
- **Team Aggregations**: Team totals, pace of play, defensive rating
- **Situational Stats**: Clutch time (last 5 minutes, ≤5 point difference)

**Data Quirks & Edge Cases:**
- **Suspended Games**: Rare but require special handling (e.g., Malice at the Palace)
- **Forfeitures**: Extremely rare, but games can be forfeited (recorded as 2-0)
- **Neutral Site Games**: London, Mexico City, Christmas games in non-home venues
- **COVID Adjustments**: 2019-20 bubble games, shortened 2020-21 season

## Features

- **Async-first architecture** with httpx and rate limiting (45 req/min)
- **Multi-source data integration**: NBA Stats API, Basketball Reference, NBA Game Books
- **Idempotent upserts** with diff-aware updates to minimize database churn
- **Comprehensive data validation** using Pydantic models with strict enums
- **UTC timestamp normalization** with arena timezone preservation
- **Streaming parsers** to avoid memory issues with large datasets
- **Rich CLI** with progress bars and structured logging
- **Resumable pipelines** with checkpoint support
- **Data quality validation** with comprehensive test suite

## Data Pipeline Documentation

### Data Flow Architecture

```mermaid
graph TD
    A[NBA Stats API] --> D[HTTP Client Layer]
    B[Basketball Reference] --> D
    C[NBA Game Books PDFs] --> D
    
    D --> E[Rate Limiter<br/>45 req/min]
    E --> F[Data Extractors]
    F --> G[Pydantic Validators]
    G --> H[Data Transformers]
    H --> I[PostgreSQL Loaders]
    
    I --> J[Core Tables]
    J --> K[Derived Analytics]
    K --> L[Data Quality Checks]
    
    M[Error Recovery] --> E
    N[Checkpointing] --> F
    O[Caching Layer] --> D
```

### API Rate Limiting Details

**NBA Stats API (`stats.nba.com`)**
- **Rate Limit**: 45 requests/minute (token bucket algorithm)
- **Key Endpoints**: `/leaguegamefinder`, `/playbyplay`, `/boxscoresummaryv2`
- **Headers Required**: User-Agent spoofing, Referer headers
- **Error Handling**: 429 → exponential backoff, 5xx → retry with jitter
- **Peak Hours**: Avoid 6-10 PM ET during season (high traffic)

**Basketball Reference (`basketball-reference.com`)**
- **Rate Limit**: Respectful crawling, ~30 requests/minute
- **Key Pages**: Game box scores, standings, schedule pages
- **Parsing**: BeautifulSoup HTML parsing with CSS selectors
- **Caching**: ETag/Last-Modified headers, 1-hour local cache
- **Challenges**: Dynamic class names, anti-bot measures

**NBA Game Books (Official PDFs)**
- **Source**: `official.nba.com/referee-assignments`
- **Rate Limit**: Conservative, 10 requests/minute
- **Processing**: PyPDF2 extraction → regex parsing → fuzzy name matching
- **Cache Strategy**: PDF files cached locally, parsed data cached for 24 hours
- **Reliability**: ~95% success rate, manual fallback for edge cases

### Error Recovery Scenarios

**API Downtime Recovery:**
```python
# Automatic retry with exponential backoff
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=1, max=60),
    retry=retry_if_exception_type((HTTPError, TimeoutError))
)
async def fetch_with_recovery(url: str) -> dict
```

**Common Failure Modes:**
1. **NBA Stats API Timeout** → Switch to Basketball Reference backup
2. **Rate Limit Exceeded** → Token bucket delay, then resume
3. **PDF Parsing Failure** → Skip referee data, continue pipeline
4. **Database Connection Loss** → Checkpoint current progress, reconnect
5. **Memory Exhaustion** → Reduce batch size, enable streaming mode

**Recovery Strategies:**
- **Checkpointing**: Every 10 games processed, resume from last successful batch
- **Graceful Degradation**: Skip non-critical data (refs, injuries) if sources fail
- **Data Backfill**: Daily cron job fills gaps from previous failures
- **Alert Mechanisms**: Slack notifications for sustained failures (>30 min)

### Performance Benchmarks

**Hardware Baseline**: MacBook Pro M2, 16GB RAM, SSD storage, 100 Mbps internet

**Daily Ingestion (10-15 games)**:
- **Runtime**: 3-5 minutes
- **API Calls**: ~150-200 requests
- **Database Writes**: ~2,000-3,000 rows
- **Memory Usage**: Peak 200MB
- **Disk I/O**: ~50MB cache writes

**Full Season Backfill (1,230 games)**:
- **Runtime**: 4-6 hours with rate limiting
- **API Calls**: ~15,000-20,000 requests
- **Database Writes**: ~500K-750K rows
- **Memory Usage**: Peak 1GB (streaming mode)
- **Final Database Size**: ~2-3GB per season

**4-Season Historical Backfill**:
- **Runtime**: 16-24 hours (spread across multiple days)
- **Total API Calls**: ~60,000-80,000 requests
- **Database Size**: ~8-12GB final size
- **Derived Analytics**: Additional 2-4 hours processing
- **Index Creation**: 15-30 minutes for full optimization

**Performance Bottlenecks:**
1. **API Rate Limits**: 45 req/min → ~1.3 req/sec maximum throughput
2. **PDF Processing**: Referee assignment parsing adds 30-60s per game
3. **Database Upserts**: Conflict resolution adds ~10ms per row
4. **Memory Growth**: Large PBP datasets require streaming parsers

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Transformation │    │    PostgreSQL   │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ NBA Stats API   │───▶│ Pydantic Models  │───▶│ Core Tables     │
│ Basketball Ref  │    │ Rate Limiting    │    │ Derived Tables  │
│ NBA Game Books  │    │ Normalization    │    │ Indexes & FKs   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Core Data Tables
- `games` - Game metadata with arena timezones
- `game_id_crosswalk` - ID mapping between data sources  
- `ref_assignments` & `ref_alternates` - Referee crew information
- `starting_lineups` & `injury_status` - Player availability
- `pbp_events` - Normalized play-by-play with enrichment
- `q1_window_12_8` - First quarter analytics (12:00-8:00)
- `early_shocks` - Disruption events (fouls, technicals, injuries)
- `schedule_travel` - Travel fatigue and circadian metrics
- `outcomes` - Final scores and quarter breakdowns

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 12+
- Git

### Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd nba_scraper
   ```

2. **Setup development environment**
   ```bash
   # Complete setup with virtual environment and dependencies
   make setup
   
   # Or manual setup:
   pip install -e ".[dev]"
   ```

3. **Configure environment**
   ```bash
   # Copy environment template and customize
   cp .env.example .env
   # Edit .env with your database credentials and preferences
   ```

4. **Create database and apply schema**
   ```bash
   # Create PostgreSQL database
   createdb nba_scraper
   
   # Apply schema
   psql nba_scraper < schema.sql
   ```

### Development Workflow

The project includes a comprehensive Makefile for development tasks:

```bash
# Quick development setup
make setup           # Create venv and install dependencies
make dev-setup       # Complete setup + lint + unit tests

# Code quality
make lint           # Run ruff linting
make typecheck      # Run mypy type checking  
make format         # Auto-format with ruff --fix
make precommit-install  # Install pre-commit hooks

# Testing
make test           # Run all tests
make test-unit      # Run unit tests only
make test-int       # Run integration tests only
make cov            # Run tests with coverage report

# Utilities
make clean          # Clean build artifacts
make ci-test        # Run full CI pipeline locally
make validate       # Quick validation check
```

### Environment Configuration

Copy `.env.example` to `.env` and configure your environment:

```bash
# Essential settings
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/nba_scraper
REQUESTS_PER_MIN=45
LOG_LEVEL=INFO

# Optional features
ENABLE_ASYNC_PROCESSING=true
MAX_CONCURRENT_REQUESTS=5
CACHE_ENABLE_HTTP_CACHE=true
```

### Basic Usage

1. **Check system status**
   ```bash
   nba-scraper status
   ```

2. **Run a sample daily ingestion**
   ```bash
   # Ingest data for yesterday
   nba-scraper daily
   
   # Ingest specific date
   nba-scraper daily --date-range 2024-01-15
   ```

3. **Validate data quality**
   ```bash
   nba-scraper validate --since 2024-01-01
   ```

---

## 🚀 Common Tasks

Ready-to-run commands for everyday workflows. See [examples/](examples/) for complete scripts.

### Process Two Specific Games

Perfect for testing, validation, or processing individual games:

```bash
# Fetch, transform, load, and derive analytics for 2 games
nba-scraper fetch --games 0022300123,0022300124
nba-scraper transform --game-ids 0022300123,0022300124
nba-scraper load --game-ids 0022300123,0022300124
nba-scraper derive --game-ids 0022300123,0022300124 --show

# Or use the convenience script:
cd examples && ./run_two_games.sh
```

### Daily Incremental Updates

Run these daily to keep your database current:

```bash
# Process yesterday's games
YESTERDAY=$(date -v-1d +%Y-%m-%d)  # On Linux: date -d "yesterday" +%Y-%m-%d
TODAY=$(date +%Y-%m-%d)

nba-scraper fetch --start-date $YESTERDAY --end-date $TODAY
nba-scraper transform
nba-scraper load
nba-scraper derive
```

### Backfill Historical Data

Load multiple seasons of historical data:

```bash
# Preview with dry run first
nba-scraper backfill --seasons 2023-24 --dry-run

# Run actual backfill
nba-scraper backfill --seasons 2023-24

# Backfill multiple seasons
nba-scraper backfill --seasons 2021-22,2022-23,2023-24,2024-25

# Backfill specific date range
nba-scraper backfill --start-date 2024-01-01 --end-date 2024-01-31
```

### Data Exploration & Debugging

View derived analytics and debug data issues:

```bash
# Show analytics in pretty tables
nba-scraper derive --game-ids 0022300123 --show

# Run with verbose logging
nba-scraper --verbose fetch --games 0022300123

# Preview operations without database writes
nba-scraper --dry-run derive --game-ids 0022300123

# Work offline with cached data
nba-scraper --offline transform
```

### Performance Testing

Benchmark your pipeline performance:

```bash
# Test with 10 games (adjust dates as needed)
cd examples && ./bench_10_games.sh

# Custom benchmark
time nba-scraper fetch --start-date 2024-01-15 --end-date 2024-01-16
time nba-scraper transform
time nba-scraper load --batch-size 100
time nba-scraper derive --metrics-only
```

### Data Quality Checks

Verify data integrity and completeness:

```bash
# Check recent data
nba-scraper derive --show

# Validate specific games
nba-scraper derive --game-ids 0022300123,0022300124 --show

# Check database completeness (PostgreSQL)
psql nba_scraper -c "
  SELECT 
    game_date_local,
    COUNT(*) as games,
    COUNT(DISTINCT game_id) as unique_ids,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed
  FROM games 
  WHERE game_date_local >= CURRENT_DATE - INTERVAL '7 days'
  GROUP BY game_date_local 
  ORDER BY game_date_local DESC;
"
```

### Retry Failed Operations

Resume after errors or retry quarantined items:

```bash
# Retry failed games from previous backfill
nba-scraper backfill --seasons 2023-24 --retry-quarantined

# Force reprocess specific games
nba-scraper fetch --games 0022300123 --force
nba-scraper derive --game-ids 0022300123 --force
```

### Development & Testing

Commands for local development:

```bash
# Test database connection
python -c "
import asyncio
from nba_scraper.db import get_connection

async def test():
    conn = await get_connection()
    print('✅ Database connected successfully')
    await conn.close()

asyncio.run(test())
"

# Check what's in raw cache
ls -lh raw/

# Inspect raw data
cat raw/games/0022300123.json | jq .

# Run full test suite
pytest -v

# Run with coverage
pytest --cov=nba_scraper --cov-report=html
```

### Maintenance Tasks

Keep your system healthy:

```bash
# Clean up old cache files (older than 7 days)
find raw/ -type f -mtime +7 -delete

# Vacuum database (PostgreSQL)
psql nba_scraper -c "VACUUM ANALYZE;"

# Check database size
psql nba_scraper -c "
  SELECT 
    pg_size_pretty(pg_database_size('nba_scraper')) as total_size;
"

# Check table sizes
psql nba_scraper -c "
  SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size
  FROM pg_tables 
  WHERE schemaname = 'public' 
  ORDER BY pg_total_relation_size('public.'||tablename) DESC 
  LIMIT 10;
"
```

### CI/CD Integration

Add to your continuous integration:

```bash
# GitHub Actions example (.github/workflows/daily.yml)
# See examples/README.md for complete workflow

# Cron job example
# Add to crontab: crontab -e
0 5 * * * cd /path/to/nba_scraper && source .venv/bin/activate && \
  nba-scraper fetch --start-date $(date -v-1d +\%Y-\%m-\%d) --end-date $(date +\%Y-\%m-\%d) && \
  nba-scraper transform && nba-scraper load && nba-scraper derive \
  >> logs/daily.log 2>&1
```

### Quick Reference

| Task | Command |
|------|---------|
| Fetch 2 games | `nba-scraper fetch --games 0022300123,0022300124` |
| Yesterday's games | `nba-scraper fetch --start-date $(date -v-1d +%Y-%m-%d) --end-date $(date +%Y-%m-%d)` |
| Full season | `nba-scraper backfill --seasons 2023-24` |
| Show analytics | `nba-scraper derive --show` |
| Dry run mode | `nba-scraper --dry-run COMMAND` |
| Verbose logging | `nba-scraper --verbose COMMAND` |
| Offline mode | `nba-scraper --offline COMMAND` |
| Help | `nba-scraper --help` or `nba-scraper COMMAND --help` |

💡 **Pro Tips:**
- Use `--dry-run` to preview operations before committing
- Use `--verbose` to debug issues and see detailed logs
- Use `--offline` during development to avoid API calls
- Check [examples/](examples/) for complete runnable scripts
- See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues

---

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

## CLI Commands

### Backfill Historical Data

```bash
# Backfill 4 seasons of data
nba-scraper backfill --seasons 2021-22,2022-23,2023-24,2024-25

# Resume from last checkpoint
nba-scraper backfill --seasons 2023-24 --resume

# Skip specific data types
nba-scraper backfill --seasons 2023-24 --skip-pbp --skip-refs

# Dry run to test without database writes
nba-scraper backfill --seasons 2023-24 --dry-run
```

### Daily Incremental Ingestion

```bash
# Process yesterday's games (default)
nba-scraper daily

# Process specific date
nba-scraper daily --date-range 2024-01-15

# Process date range
nba-scraper daily --date-range 2024-01-15..2024-01-20

# Force reprocessing
nba-scraper daily --date-range 2024-01-15 --force
```

### Derive Analytics

```bash
# Derive all analytics for date range
nba-scraper derive --date-range 2023-10-01..2024-06-30

# Derive specific tables only
nba-scraper derive --date-range 2024-01-01..2024-01-31 --tables q1_window,outcomes

# Force recomputation
nba-scraper derive --date-range 2024-01-15..2024-01-20 --force
```

### Data Quality Validation

```bash
# Run all validation checks
nba-scraper validate --since 2023-10-01

# Verbose output with details
nba-scraper validate --since 2024-01-01 --verbose
```

## Development

### Dev Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e .[dev]   # Install with development dependencies
ruff check .
mypy src
pytest -q --disable-warnings -W error
```

### Validation Rules

**Game ID Validation:**
- Must match `^0022\d{6}$` format (exactly 10 characters, numeric, starts with "0022")
- Examples: ✅ `"0022301234"` ❌ `"0012301234"` (wrong prefix) ❌ `"002230123"` (too short)
- Raises `ValueError` with descriptive message for invalid formats

**Season Validation:**
- Accepts `YYYY-YY` format (e.g., "2024-25")
- For malformed seasons: logs warning `"season format invalid: {value} - expected YYYY-YY format"`
- Falls back to `derive_season_smart()` for automatic season derivation from game_id/date
- Never fails processing - always produces a valid season string

**Pipeline Error Handling:**
- Foundation pipeline continues PBP and lineup processing even when game validation fails
- Aggregates all errors in `result["errors"]` list rather than failing fast
- Preserves API responses for reuse across processing steps
- Example: If game validation fails but PBP succeeds, `result["game_processed"] = False` but `result["pbp_events_processed"] > 0`

## Dev quickstart

### Project Structure

```
nba_scraper/
├── src/nba_scraper/           # Main package
│   ├── models/                # Pydantic data models
│   ├── io_clients/           # API clients (NBA Stats, B-Ref, Game Books)
│   ├── extractors/           # Pure extraction functions
│   ├── transformers/         # Analytics transformations
│   ├── loaders/              # Database upsert logic
│   ├── pipelines/            # Orchestration workflows
│   └── cli.py                # Typer CLI application
├── tests/                    # Test suite
│   ├── unit/                 # Unit tests
│   ├── integration/          # Integration tests
│   └── data/                 # Golden test fixtures
├── schema.sql                # PostgreSQL DDL
├── team_aliases.yaml         # Team tricode normalization
├── venues.csv               # Arena coordinates and timezones
└── .cache/                  # Runtime cache (Git ignored)
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=nba_scraper

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
pytest -k validation
```

### Code Quality

```bash
# Format code
ruff format

# Lint code  
ruff check

# Type checking
mypy src/nba_scraper
```

## Data Pipeline Details

### MVP Implementation Order

1. **Games + Crosswalk** (NBA Stats primary, B-Ref fallback)
2. **Play-by-Play + Outcomes** (NBA Stats PBP + B-Ref scores)
3. **Q1 Analytics + Early Shocks** (derived from PBP)
4. **Referee Assignments** (NBA Game Books PDFs)
5. **Lineups + Injuries** (B-Ref box notes)
6. **Schedule Travel** (venues.csv + haversine distance)

### Rate Limiting & Resilience

- Token bucket rate limiter: 45 requests/minute with jitter
- Exponential backoff on 429/5xx errors (max 60s)
- HTTP caching with ETag/Last-Modified support
- Automatic retry with tenacity library
- Graceful degradation and error logging

### Data Normalization

- **Team Tricodes**: Canonical mapping via `team_aliases.yaml`
- **Player Names**: PascalCase slugs (e.g., "LeBron James" → "LebronJames") 
- **Timestamps**: All stored in UTC, arena timezone preserved
- **Enums**: Strict validation for statuses, event types, positions
- **Referees**: Fuzzy name matching with rapidfuzz for consistency

### Database Design

- **Idempotent Upserts**: `ON CONFLICT DO UPDATE` only when values differ
- **Provenance**: Every row has `source`, `source_url`, `ingested_at_utc`
- **Natural PKs**: Composite keys using business identifiers
- **Foreign Keys**: Referential integrity with cascade deletes
- **Indexes**: Optimized for common query patterns

## Troubleshooting

### Common Issues

**Database Connection Failed**
```bash
# Check PostgreSQL is running
pg_ctl status

# Test connection manually
psql nba_scraper -c "SELECT 1"

# Verify DATABASE_URL in .env
echo $DATABASE_URL
```

**Rate Limit Errors**
```bash
# Check current rate limit setting
grep REQUESTS_PER_MIN .env

# Monitor rate limiting in logs
nba-scraper daily --date-range 2024-01-15 | grep "rate_limit"
```

**Memory Issues on Large Backfills**
```bash
# Reduce chunk size
export BACKFILL_CHUNK_SIZE=5

# Use smaller concurrency
export MAX_CONCURRENT_REQUESTS=2

# Process seasons individually
nba-scraper backfill --seasons 2023-24
```

**Missing Dependencies**
```bash
# Reinstall with dev dependencies
pip install -e ".[dev]"

# Check for missing system packages
# On Ubuntu/Debian: apt-get install postgresql-client
# On macOS: brew install postgresql
```

### Data Quality Issues

**Duplicate Games**
```sql
-- Check for duplicate games
SELECT game_id, COUNT(*) 
FROM games 
GROUP BY game_id 
HAVING COUNT(*) > 1;
```

**Missing Crosswalk Entries**
```sql
-- Games without B-Ref mapping
SELECT g.game_id, g.home_team_tricode, g.away_team_tricode 
FROM games g 
LEFT JOIN game_id_crosswalk c ON g.game_id = c.game_id 
WHERE c.game_id IS NULL;
```

**PBP Validation**
```sql
-- Check for PBP gaps
SELECT game_id, period, 
       LAG(event_idx) OVER (PARTITION BY game_id, period ORDER BY event_idx) as prev_idx,
       event_idx
FROM pbp_events 
WHERE event_idx - LAG(event_idx) OVER (PARTITION BY game_id, period ORDER BY event_idx) > 1;
```

## Runbook

### Setup PostgreSQL and Apply Schema

```bash
# Install PostgreSQL (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib

# Install PostgreSQL (macOS)
brew install postgresql
brew services start postgresql

# Create database and user
sudo -u postgres psql
CREATE DATABASE nba_scraper;
CREATE USER nba_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE nba_scraper TO nba_user;
\q

# Apply schema
psql -U nba_user -d nba_scraper -f schema.sql
```

### 1-Day Integration Test (Two Games Sample)

```bash
# Set test date with known games
export TEST_DATE="2024-01-15"

# Run daily pipeline for test date
nba-scraper daily --date-range $TEST_DATE --force

# Verify data was ingested
psql nba_scraper -c "
SELECT g.game_id, g.home_team_tricode, g.away_team_tricode, 
       COUNT(p.event_idx) as pbp_events,
       o.final_home_points, o.final_away_points
FROM games g
LEFT JOIN pbp_events p ON g.game_id = p.game_id  
LEFT JOIN outcomes o ON g.game_id = o.game_id
WHERE g.game_date_local = '$TEST_DATE'
GROUP BY g.game_id, g.home_team_tricode, g.away_team_tricode, 
         o.final_home_points, o.final_away_points;
"

# Derive analytics for test date
nba-scraper derive --date-range $TEST_DATE..$TEST_DATE

# Run validation
nba-scraper validate --since $TEST_DATE --verbose
```

### Data Quality Verification

```bash
# Run comprehensive validation
pytest tests/integration/validation_test.py -v

# Check data completeness for recent games
psql nba_scraper -c "
WITH game_completeness AS (
  SELECT g.game_id,
         g.game_date_local,
         EXISTS(SELECT 1 FROM game_id_crosswalk c WHERE c.game_id = g.game_id) as has_crosswalk,
         EXISTS(SELECT 1 FROM outcomes o WHERE o.game_id = g.game_id) as has_outcomes,
         EXISTS(SELECT 1 FROM pbp_events p WHERE p.game_id = g.game_id) as has_pbp,
         (SELECT COUNT(*) FROM starting_lineups s WHERE s.game_id = g.game_id) as lineup_count
  FROM games g 
  WHERE g.game_date_local >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT game_date_local,
       COUNT(*) as total_games,
       SUM(CASE WHEN has_crosswalk THEN 1 ELSE 0 END) as with_crosswalk,
       SUM(CASE WHEN has_outcomes THEN 1 ELSE 0 END) as with_outcomes,
       SUM(CASE WHEN has_pbp THEN 1 ELSE 0 END) as with_pbp,
       AVG(lineup_count) as avg_lineups
FROM game_completeness 
GROUP BY game_date_local 
ORDER BY game_date_local DESC;
"
```

### Safe 4-Season Backfill

```bash
# Start with most recent season (less data, faster feedback)
nba-scraper backfill --seasons 2024-25 --dry-run

# Run actual backfill with checkpoints enabled
nba-scraper backfill --seasons 2024-25

# Proceed with earlier seasons one at a time
nba-scraper backfill --seasons 2023-24
nba-scraper backfill --seasons 2022-23  
nba-scraper backfill --seasons 2021-22

# Derive analytics for all seasons
nba-scraper derive --date-range 2021-10-01..2025-06-30

# Final validation
nba-scraper validate --since 2021-10-01
```

**Monitor Progress:**
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

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run the test suite: `pytest`
5. Submit a pull request

## Releasing

### Release Process

The project uses automated release workflows triggered by Git tags. When you push a version tag, GitHub Actions will:

1. **Build Python wheel** from source
2. **Publish to PyPI** (when `PYPI_TOKEN` secret is configured)
3. **Build and push Docker image** to GitHub Container Registry (ghcr.io)
4. **Create GitHub Release** with changelog and artifacts

### Version Bumping

1. **Update version in `src/nba_scraper/version.py`**:
   ```python
   __version__ = "1.0.2"  # Bump from 1.0.1
   ```

2. **Update CHANGELOG.md** with release notes:
   ```markdown
   ## [1.0.2] - 2025-10-08
   
   ### Added
   - New feature X
   
   ### Fixed
   - Bug fix Y
   
   ### Changed
   - Improvement Z
   ```

3. **Commit the changes**:
   ```bash
   git add src/nba_scraper/version.py CHANGELOG.md
   git commit -m "chore: bump version to 1.0.2"
   ```

4. **Create and push the tag**:
   ```bash
   git tag v1.0.2
   git push origin main
   git push origin v1.0.2
   ```

5. **Monitor the release workflow**:
   - Go to GitHub Actions tab in your repository
   - Watch the "Release" workflow progress
   - Verify artifacts are published to PyPI and GHCR

### Semantic Versioning

Follow [Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH):

- **MAJOR** (v2.0.0): Breaking API changes
- **MINOR** (v1.1.0): New features, backward compatible
- **PATCH** (v1.0.1): Bug fixes, backward compatible

**Pre-release versions**:
- **Alpha**: `v1.1.0-alpha.1` (early development, unstable)
- **Beta**: `v1.1.0-beta.1` (feature complete, testing phase)
- **RC**: `v1.1.0-rc.1` (release candidate, near production)

### PyPI Configuration

**Option 1: Trusted Publishing (Recommended)**

Set up [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/) to publish without API tokens:

1. Go to PyPI project settings
2. Add GitHub Actions as a trusted publisher
3. Configure: `your-org/nba-scraper` repository, `release.yml` workflow
4. Set repository variable: `ENABLE_PYPI_PUBLISH=true`

**Option 2: API Token**

If trusted publishing isn't available:

1. Generate PyPI API token at https://pypi.org/manage/account/token/
2. Add as GitHub repository secret: `PYPI_TOKEN`
3. Set repository variable: `ENABLE_PYPI_PUBLISH=true`

### Docker Image Tags

The release workflow automatically creates multiple Docker tags:

- `ghcr.io/your-org/nba-scraper:1.0.2` (specific version)
- `ghcr.io/your-org/nba-scraper:1.0` (minor version)
- `ghcr.io/your-org/nba-scraper:1` (major version)
- `ghcr.io/your-org/nba-scraper:latest` (latest stable release)

Pre-releases (alpha/beta/rc) are marked as "prerelease" on GitHub and do not update the `latest` tag.

### Local Testing Before Release

**Test wheel building**:
```bash
# Build wheel locally
python -m build

# Install in clean virtualenv
python -m venv test_venv
source test_venv/bin/activate
pip install dist/nba_scraper-1.0.2-py3-none-any.whl

# Test CLI
nba-scraper --help
nba-scraper status

# Cleanup
deactivate
rm -rf test_venv dist build
```

**Test Docker image**:
```bash
# Build image locally (simulating CI)
docker build -t nba-scraper:test \
  --build-arg VERSION=1.0.2-test \
  --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
  --build-arg VCS_REF=$(git rev-parse --short HEAD) \
  .

# Test container
docker run --rm nba-scraper:test --help
docker run --rm nba-scraper:test --version

# Test with environment variables
docker run --rm \
  -e DATABASE_URL=postgresql://localhost/nba_scraper \
  nba-scraper:test status

# Cleanup
docker rmi nba-scraper:test
```

### Release Checklist

Before creating a release tag:

- [ ] All tests passing: `pytest`
- [ ] Linting clean: `ruff check .`
- [ ] Type checking clean: `mypy src`
- [ ] Version bumped in `src/nba_scraper/version.py`
- [ ] CHANGELOG.md updated with release notes
- [ ] Local wheel build works: `python -m build`
- [ ] Local Docker build works: `docker build .`
- [ ] Documentation updated (if API changed)
- [ ] Migration guide provided (if breaking changes)

After pushing the tag:

- [ ] GitHub Actions workflow completes successfully
- [ ] PyPI package published (if configured)
- [ ] Docker image available on GHCR
- [ ] GitHub Release created with artifacts
- [ ] Release announcement (if major version)

### Hotfix Releases

For urgent production fixes:

1. Create hotfix branch from the tag:
   ```bash
   git checkout -b hotfix/1.0.3 v1.0.2
   ```

2. Apply the fix and update version:
   ```bash
   # Fix the bug
   vim src/nba_scraper/...
   
   # Update version
   vim src/nba_scraper/version.py  # Change to 1.0.3
   
   # Commit
   git add .
   git commit -m "fix: critical bug in X"
   ```

3. Tag and push:
   ```bash
   git tag v1.0.3
   git push origin hotfix/1.0.3
   git push origin v1.0.3
   ```

4. Merge back to main:
   ```bash
   git checkout main
   git merge hotfix/1.0.3
   git push origin main
   ```

### Rollback Procedure

If a release has critical issues:

1. **Delete the PyPI release** (not possible - PyPI doesn't allow deletions)
   - Instead, publish a new patch version with the fix

2. **Mark Docker images as deprecated**:
   ```bash
   # Pull and retag as deprecated
   docker pull ghcr.io/your-org/nba-scraper:1.0.2
   docker tag ghcr.io/your-org/nba-scraper:1.0.2 \
              ghcr.io/your-org/nba-scraper:1.0.2-deprecated
   ```

3. **Mark GitHub Release as pre-release**:
   - Go to GitHub Releases
   - Edit the release
   - Check "Set as a pre-release"
   - Add warning to release notes

4. **Publish fixed version immediately**:
   ```bash
   # Fix and release patch
   git tag v1.0.3
   git push origin v1.0.3
   ```

## License

This project is licensed under the MIT License. See LICENSE file for details.