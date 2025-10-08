# NBA Scraper Troubleshooting Guide

**Last Updated**: October 7, 2025  
**For**: Developers, Data Engineers, Operations  
**Related Docs**: [DEV_NOTES.md](DEV_NOTES.md) | [README.md](README.md)

This guide covers common failure scenarios, diagnostic approaches, and step-by-step fixes for the NBA Scraper pipeline.

---

## Table of Contents

1. [Common Pipeline Failures](#common-pipeline-failures)
2. [Database Issues](#database-issues)
3. [Network & API Issues](#network--api-issues)
4. [Data Quality Issues](#data-quality-issues)
5. [Development Environment Issues](#development-environment-issues)
6. [Quick Diagnostic Commands](#quick-diagnostic-commands)

---

## Common Pipeline Failures

### 1. Incomplete Play-by-Play Data

**Symptoms:**
- Log message: `"PBP event count below expected threshold"`
- Database query shows < 300 events for a completed game
- Missing quarters or large gaps in event sequences

**Log Keys to Search:**
```bash
grep "pbp_events_processed" logs/nba_scraper.log
grep "event_count.*[0-2][0-9]{2}" logs/nba_scraper.log  # Less than 300
```

**Diagnostic Steps:**
```bash
# 1. Check specific game's PBP count
psql nba_scraper -c "
SELECT game_id, COUNT(*) as event_count, 
       MAX(period) as max_period,
       MIN(period) as min_period
FROM pbp_events 
WHERE game_id = '0022400123'
GROUP BY game_id;
"

# 2. Check for period gaps
psql nba_scraper -c "
SELECT period, COUNT(*) as events_in_period
FROM pbp_events
WHERE game_id = '0022400123'
GROUP BY period
ORDER BY period;
"

# 3. Review API response logs
grep "0022400123" logs/nba_scraper.log | grep "api_response"
```

**Common Causes & Fixes:**

**Cause 1: API Rate Limiting**
```bash
# Check rate limit errors
grep "rate_limit.*exceeded" logs/nba_scraper.log

# Fix: Reduce request rate
export REQUESTS_PER_MIN=30  # From default 45
nba-scraper daily --date-range 2024-01-15 --force
```

**Cause 2: Partial API Response**
```bash
# Check response completeness
grep "incomplete_response" logs/nba_scraper.log

# Fix: Retry with longer timeout
export TIMEOUT_S=30  # From default 15
nba-scraper daily --date-range 2024-01-15 --force
```

**Cause 3: Game Still In Progress**
```bash
# Check game status
psql nba_scraper -c "
SELECT game_id, status, period, time_remaining
FROM games 
WHERE game_id = '0022400123';
"

# Fix: Wait for game to finish, then reprocess
# Games are typically finalized 2-3 hours after end
nba-scraper daily --date-range 2024-01-15 --force
```

**Preventive Measures:**
- Always check game status before expecting complete PBP
- Set up monitoring alerts for games with < 300 PBP events
- Implement automatic retry 4 hours after game start time

---

### 2. Incomplete Box Score Data

**Symptoms:**
- Missing player statistics
- Team totals don't match individual stats
- Log message: `"Box score validation failed"`

**Log Keys to Search:**
```bash
grep "box_score.*incomplete" logs/nba_scraper.log
grep "player_stats.*missing" logs/nba_scraper.log
```

**Diagnostic Steps:**
```bash
# 1. Check player stat completeness
psql nba_scraper -c "
SELECT g.game_id, 
       COUNT(DISTINCT p.player_id) as unique_players,
       SUM(p.points) as total_points,
       o.final_home_points + o.final_away_points as expected_points
FROM games g
LEFT JOIN player_game_stats p ON g.game_id = p.game_id
LEFT JOIN outcomes o ON g.game_id = o.game_id
WHERE g.game_id = '0022400123'
GROUP BY g.game_id, o.final_home_points, o.final_away_points;
"

# 2. Verify team totals match
psql nba_scraper -c "
SELECT team_tricode,
       SUM(points) as player_total_points,
       MAX(team_total) as team_stat_points
FROM player_game_stats
WHERE game_id = '0022400123'
GROUP BY team_tricode;
"
```

**Common Fixes:**

**Missing Players:**
```bash
# Re-fetch from Basketball Reference (backup source)
nba-scraper daily --date-range 2024-01-15 --source bref --force
```

**Stat Mismatch:**
```bash
# Clear cached data and re-fetch
rm -rf .cache/boxscores/0022400123*
nba-scraper daily --date-range 2024-01-15 --force
```

---

### 3. Season/Date Mismatch

**Symptoms:**
- Log warning: `"season format invalid"` or `"season derivation fallback"`
- Games appearing in wrong season in database
- Date-based queries returning unexpected results

**Log Keys to Search:**
```bash
grep "season.*invalid" logs/nba_scraper.log
grep "derive_season_smart" logs/nba_scraper.log
```

**Diagnostic Steps:**
```bash
# 1. Check for season mismatches
psql nba_scraper -c "
SELECT game_id, season, game_date_local,
       EXTRACT(YEAR FROM game_date_local) as date_year,
       SUBSTRING(season, 1, 4)::int as season_year
FROM games
WHERE SUBSTRING(season, 1, 4)::int != EXTRACT(YEAR FROM game_date_local)
  AND EXTRACT(MONTH FROM game_date_local) >= 10  -- October or later
ORDER BY game_date_local DESC
LIMIT 20;
"

# 2. Check for games with season = 'UNKNOWN'
psql nba_scraper -c "
SELECT COUNT(*), MIN(game_date_local), MAX(game_date_local)
FROM games 
WHERE season = 'UNKNOWN';
"
```

**Common Causes & Fixes:**

**Cause 1: Wrong Season Format from API**
```python
# The API returned "2024" instead of "2024-25"
# Fix: System auto-corrects via derive_season_smart()

# Manual verification:
python -c "
from nba_scraper.transformers.games import derive_season_smart
from datetime import date

# Test derivation from game date
game_date = date(2024, 10, 15)
season = derive_season_smart(game_date=game_date)
print(f'Season: {season}')  # Should print: 2024-25
"
```

**Cause 2: Game ID Missing or Malformed**
```bash
# Check for invalid game IDs
psql nba_scraper -c "
SELECT game_id, LENGTH(game_id) as id_length, season
FROM games
WHERE LENGTH(game_id) != 10 
   OR game_id !~ '^0022[0-9]{6}$'
LIMIT 10;
"

# Fix: These games need manual correction or re-fetch
# Note: Invalid game IDs should have been caught by validation
```

**Cause 3: Cross-Season Game (Oct 1 boundary)**
```bash
# Games in October belong to NEW season starting that year
# Games in Jan-Sep belong to PREVIOUS season

# Verify October games are assigned correctly:
psql nba_scraper -c "
SELECT game_id, season, game_date_local
FROM games
WHERE EXTRACT(MONTH FROM game_date_local) = 10
  AND EXTRACT(YEAR FROM game_date_local)::text || '-' || 
      LPAD((EXTRACT(YEAR FROM game_date_local)::int + 1)::text, 2, '0') != season
ORDER BY game_date_local;
"
```

**Manual Fix for Incorrect Seasons:**
```sql
-- Update specific games with correct season
UPDATE games 
SET season = '2024-25'
WHERE game_id IN ('0022400123', '0022400124')
  AND season != '2024-25';
```

---

### 4. Timezone Alias Issues

**Symptoms:**
- Log warning: `"No timezone found, using ET fallback"`
- Games in wrong local date buckets
- Incorrect `game_date_local` for West Coast games

**Log Keys to Search:**
```bash
grep "timezone.*fallback" logs/nba_scraper.log
grep "arena_tz.*None" logs/nba_scraper.log
```

**Diagnostic Steps:**
```bash
# 1. Find games missing venue timezone
psql nba_scraper -c "
SELECT game_id, arena_name, home_team_tricode, arena_tz
FROM games
WHERE arena_tz = 'America/New_York'  -- Default fallback
  AND home_team_tricode IN ('LAL', 'LAC', 'GSW', 'POR', 'SAC')  -- West coast teams
LIMIT 10;
"

# 2. Check venues.csv for missing entries
grep "Crypto.com Arena" venues.csv
# If missing, this is the issue
```

**Fixes:**

**Add Missing Venue to venues.csv:**
```bash
# 1. Check current venues.csv
cat venues.csv | head -5

# 2. Add missing venue (format: arena_name,city,state,timezone,latitude,longitude)
echo "Crypto.com Arena,Los Angeles,CA,America/Los_Angeles,34.043018,-118.267254" >> venues.csv

# 3. Test venue lookup
python -c "
from nba_scraper.utils.venues import get_venue_timezone
tz = get_venue_timezone('Crypto.com Arena', 'LAL')
print(f'Timezone: {tz}')  # Should print: America/Los_Angeles
"

# 4. Reprocess affected games
nba-scraper daily --date-range 2024-01-15 --force
```

**Add Team Timezone Fallback:**
```yaml
# Update team_aliases.yaml
LAL:
  - LAL
  - Lakers
  home_timezone: "America/Los_Angeles"  # Add this line
  full_name: "Los Angeles Lakers"
```

**Verify Fix:**
```bash
# Check that games now have correct timezone
psql nba_scraper -c "
SELECT game_id, home_team_tricode, arena_tz, 
       game_date_utc, game_date_local
FROM games
WHERE game_id = '0022400123';
"
# arena_tz should be 'America/Los_Angeles', not 'America/New_York'
```

---

## Database Issues

### 1. "No Such Table" Error

**Symptoms:**
```
sqlalchemy.exc.ProgrammingError: (psycopg2.ProgrammingError) relation "games" does not exist
```

**Diagnostic:**
```bash
# Check if database exists
psql -l | grep nba_scraper

# Check if tables exist
psql nba_scraper -c "\dt"
```

**Fixes:**

**If Database Doesn't Exist:**
```bash
# Create database
createdb nba_scraper

# Apply schema
psql nba_scraper < schema.sql
```

**If Database Exists But Tables Missing:**
```bash
# Option 1: Apply schema directly
psql nba_scraper < schema.sql

# Option 2: Use Alembic migrations
cd /path/to/nba_scraper
source .venv/bin/activate
alembic upgrade head
```

**For Test Environment:**
```bash
# Tests should use fixtures, but if needed:
export DATABASE_URL="sqlite+aiosqlite:///test.db"
pytest --create-test-db
```

**Common Test Fixture Issue:**
```python
# tests/conftest.py should have:
@pytest.fixture(scope="session")
async def test_db():
    """Create test database with schema."""
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    # Cleanup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

# If this is missing, add it to conftest.py
```

---

### 2. Database Connection Refused

**Symptoms:**
```
psycopg2.OperationalError: could not connect to server: Connection refused
```

**Diagnostic:**
```bash
# Check if PostgreSQL is running
pg_isready
# Should output: /tmp:5432 - accepting connections

# Check specific database
pg_isready -d nba_scraper
```

**Fixes:**

**PostgreSQL Not Running:**
```bash
# macOS (Homebrew)
brew services start postgresql

# Linux (systemd)
sudo systemctl start postgresql

# Check status
pg_isready
```

**Wrong Connection Parameters:**
```bash
# Check your .env file
cat .env | grep DB_URI

# Should be format:
# DB_URI=postgresql+asyncpg://user:password@localhost:5432/nba_scraper

# Test connection manually
psql postgresql://user:password@localhost:5432/nba_scraper -c "SELECT 1"
```

**Permission Issues:**
```bash
# Grant permissions to user
psql postgres -c "
GRANT ALL PRIVILEGES ON DATABASE nba_scraper TO nba_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO nba_user;
"
```

---

### 3. Migration Issues

**Symptoms:**
```
alembic.util.exc.CommandError: Can't locate revision identified by 'xxx'
```

**Diagnostic:**
```bash
# Check current migration state
alembic current

# Check migration history
alembic history

# Check for migration file issues
ls alembic/versions/
```

**Fixes:**

**Out of Sync Migrations:**
```bash
# Stamp database with current schema state
alembic stamp head

# Or stamp with specific revision
alembic stamp 001_baseline_schema
```

**Conflicting Migrations:**
```bash
# Check for duplicate revision IDs
python -c "
from pathlib import Path
import re

versions_dir = Path('alembic/versions')
revisions = {}

for file in versions_dir.glob('*.py'):
    content = file.read_text()
    match = re.search(r\"revision = ['\\\"]([^'\\\"]+)['\\\"]\", content)
    if match:
        rev = match.group(1)
        if rev in revisions:
            print(f'DUPLICATE: {rev} in {file.name} and {revisions[rev]}')
        revisions[rev] = file.name
"
```

---

## Network & API Issues

### 1. OFFLINE Mode Errors

**Symptoms:**
- Log message: `"OFFLINE mode enabled, skipping API call"`
- Empty datasets returned
- Tests pass but real data missing

**Diagnostic:**
```bash
# Check OFFLINE setting
env | grep OFFLINE

# Check in Python
python -c "
from nba_scraper.config import get_settings
settings = get_settings()
print(f'OFFLINE: {settings.OFFLINE}')
"
```

**Fixes:**

**For Development (Real API Calls):**
```bash
# Disable OFFLINE mode
export OFFLINE=false

# Or in .env file
echo "OFFLINE=false" >> .env

# Run with real API calls
nba-scraper daily --date-range 2024-01-15
```

**For Tests (Use Cassettes):**
```bash
# Tests should use VCR cassettes for repeatability
# Check if cassette directory exists
ls tests/cassettes/

# If cassettes missing, record them:
export OFFLINE=false
pytest tests/integration/test_nba_api.py --record-mode=once

# Future test runs will use cassettes (offline)
export OFFLINE=true
pytest tests/integration/test_nba_api.py
```

**Add New Test Cassettes:**
```python
# tests/integration/test_new_api.py

import vcr

@vcr.use_cassette('tests/cassettes/new_api_call.yaml')
def test_new_api_endpoint():
    """Test with VCR cassette for repeatability."""
    result = fetch_new_endpoint()
    assert result is not None

# First run with OFFLINE=false records cassette
# Subsequent runs use cassette (offline mode)
```

---

### 2. Rate Limit Exceeded

**Symptoms:**
```
HTTP 429: Too Many Requests
RateLimitExceeded: Token bucket exhausted
```

**Diagnostic:**
```bash
# Check rate limit configuration
grep REQUESTS_PER_MIN .env

# Check recent API calls
grep "rate_limit" logs/nba_scraper.log | tail -20
```

**Fixes:**

**Immediate (Reduce Rate):**
```bash
# Reduce requests per minute
export REQUESTS_PER_MIN=30  # From default 45

# Increase backoff
export RETRY_BACKOFF_FACTOR=3.0  # From default 2.0

# Retry failed batch
nba-scraper daily --date-range 2024-01-15 --force
```

**Long-term (Implement Better Queueing):**
```python
# src/nba_scraper/io_clients/rate_limiter.py

# Increase token bucket size
self.bucket_size = 60  # From 45
self.refill_rate = 30 / 60  # 30 per minute

# Add jitter to prevent thundering herd
import random
await asyncio.sleep(random.uniform(0.1, 0.5))
```

---

### 3. API Endpoint Changes

**Symptoms:**
```
KeyError: 'resultSets' in API response
ParseError: Unexpected response structure
```

**Diagnostic:**
```bash
# Check API response structure
grep "api_response.*resultSets" logs/nba_scraper.log

# Save raw response for analysis
curl -H "User-Agent: Mozilla/5.0..." \
     "https://stats.nba.com/stats/leaguegamefinder?..." \
     > api_response.json

# Inspect structure
jq '.' api_response.json | head -50
```

**Fixes:**

**Update Extractor:**
```python
# src/nba_scraper/extractors/nba_stats.py

def extract_games_from_response(response: dict) -> List[dict]:
    """Extract games, handling new API structure."""
    
    # Old structure
    if "resultSets" in response:
        return response["resultSets"][0]["rowSet"]
    
    # New structure (fallback)
    if "results" in response:
        return response["results"]
    
    # Log unexpected structure
    logger.error("Unexpected API response structure", 
                 keys=list(response.keys()))
    raise ValueError("Cannot parse API response")
```

---

## Data Quality Issues

### 1. Duplicate Game Records

**Symptoms:**
```sql
-- Query returns multiple rows for same game_id
SELECT game_id, COUNT(*) FROM games GROUP BY game_id HAVING COUNT(*) > 1;
```

**Diagnostic:**
```bash
# Find duplicates
psql nba_scraper -c "
SELECT game_id, COUNT(*) as count,
       STRING_AGG(DISTINCT source, ', ') as sources,
       STRING_AGG(DISTINCT ingested_at_utc::text, ', ') as ingestion_times
FROM games
GROUP BY game_id
HAVING COUNT(*) > 1
ORDER BY count DESC;
"
```

**Fix:**
```sql
-- Keep most recent record, delete older duplicates
WITH ranked_games AS (
    SELECT game_id, ingested_at_utc,
           ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY ingested_at_utc DESC) as rn
    FROM games
)
DELETE FROM games
WHERE (game_id, ingested_at_utc) IN (
    SELECT game_id, ingested_at_utc
    FROM ranked_games
    WHERE rn > 1
);
```

**Prevent Future Duplicates:**
```python
# Ensure upsert logic is correct
# src/nba_scraper/loaders/games.py

async def upsert_game(conn: AsyncConnection, game: Game) -> None:
    """Upsert game with proper conflict handling."""
    query = text("""
        INSERT INTO games (game_id, season, ...)
        VALUES (:game_id, :season, ...)
        ON CONFLICT (game_id) DO UPDATE SET
            season = excluded.season,
            ...
        WHERE games.game_id = excluded.game_id  -- Critical: ensure single match
    """)
```

---

## Quick Diagnostic Commands

### Pipeline Health Check
```bash
# Run full diagnostic suite
python -m nba_scraper.tools.diagnose

# Or manual checks:

# 1. Database connectivity
psql nba_scraper -c "SELECT COUNT(*) FROM games"

# 2. Recent ingestion
psql nba_scraper -c "
SELECT MAX(game_date_local) as latest_game, 
       MAX(ingested_at_utc) as latest_ingestion
FROM games"

# 3. Data completeness
psql nba_scraper -c "
SELECT 
    COUNT(DISTINCT g.game_id) as games,
    COUNT(DISTINCT p.game_id) as games_with_pbp,
    COUNT(DISTINCT o.game_id) as games_with_outcomes
FROM games g
LEFT JOIN pbp_events p ON g.game_id = p.game_id
LEFT JOIN outcomes o ON g.game_id = o.game_id
WHERE g.game_date_local >= CURRENT_DATE - INTERVAL '7 days'"
```

### Log Analysis
```bash
# Error summary
grep "ERROR" logs/nba_scraper.log | cut -d'"' -f4 | sort | uniq -c | sort -rn

# Warning summary
grep "WARNING" logs/nba_scraper.log | cut -d'"' -f4 | sort | uniq -c | sort -rn

# API failures
grep "api.*failed" logs/nba_scraper.log | tail -20

# Rate limit hits
grep "rate_limit" logs/nba_scraper.log | wc -l
```

### Test Specific Game
```bash
# Process single game with verbose logging
export LOG_LEVEL=DEBUG
nba-scraper process-game --game-id 0022400123 --force

# Check results
psql nba_scraper -c "
SELECT g.game_id, g.status,
       COUNT(DISTINCT p.event_idx) as pbp_events,
       o.final_home_points, o.final_away_points
FROM games g
LEFT JOIN pbp_events p ON g.game_id = p.game_id
LEFT JOIN outcomes o ON g.game_id = o.game_id
WHERE g.game_id = '0022400123'
GROUP BY g.game_id, g.status, o.final_home_points, o.final_away_points"
```

---

## Getting Help

If you've tried the above fixes and still have issues:

1. **Check Documentation:**
   - [DEV_NOTES.md](DEV_NOTES.md) - Engineering rules and policies
   - [README.md](README.md) - Project overview and setup
   - [MIGRATIONS_SETUP.md](MIGRATIONS_SETUP.md) - Database migrations

2. **Search Logs:**
   ```bash
   # Full-text search in logs
   grep -r "your_error_message" logs/
   ```

3. **Open GitHub Issue:**
   - Include error message, logs, and diagnostic output
   - Specify: game_id, date, environment (dev/prod)
   - Share relevant configuration (DB_URI redacted)

4. **Contact Team:**
   - Slack: #nba-scraper-support
   - Email: data-eng@example.com

---

**Last Resort: Nuclear Reset**

If all else fails and you need to start fresh:

```bash
# ⚠️  WARNING: This deletes ALL data ⚠️

# 1. Backup first!
pg_dump nba_scraper > backup_$(date +%Y%m%d).sql

# 2. Drop and recreate database
dropdb nba_scraper
createdb nba_scraper
psql nba_scraper < schema.sql

# 3. Clear caches
rm -rf .cache/

# 4. Reprocess critical data
nba-scraper backfill --seasons 2024-25 --start-date 2024-10-01
```
