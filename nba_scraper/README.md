# NBA Historical Scraping & Ingestion Engine

A production-grade data pipeline for fetching, normalizing, validating, and persisting NBA historical datasets with async IO, rate limiting, and comprehensive analytics derivation.

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

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd nba_scraper
   ```

2. **Install dependencies**
   ```bash
   # Using uv (recommended)
   pip install uv
   uv sync
   
   # Or using pip
   pip install -e .
   ```

3. **Setup environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

4. **Create database and apply schema**
   ```bash
   # Create PostgreSQL database
   createdb nba_scraper
   
   # Apply schema
   psql nba_scraper < schema.sql
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

## License

This project is licensed under the MIT License. See LICENSE file for details.