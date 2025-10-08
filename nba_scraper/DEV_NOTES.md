# NBA Scraper Engineering Documentation

**Last Updated**: October 8, 2025  
**Maintainers**: Data Engineering Team  
**Status**: Production

This document contains critical engineering rules, data handling policies, and development checklists for the NBA Scraper project. All contributors must follow these guidelines to maintain data integrity and system reliability.

---

## Table of Contents

1. [Timezone Handling Policy](#timezone-handling-policy)
2. [Status Normalization](#status-normalization)
3. [Season Rules & Validation](#season-rules--validation)
4. [Team Alias Management](#team-alias-management)
5. [Performance & Benchmarking](#performance--benchmarking)
6. [Adding Derived Windows](#adding-derived-windows)
7. [Data Quality Checks](#data-quality-checks)
8. [Development Checklists](#development-checklists)

---

## Timezone Handling Policy

### Core Principle
**All timestamps are stored in UTC with venue timezone preserved for local time derivation.**

### Implementation Rules

#### 1. Venue Timezone Resolution
```python
# venues.csv lookup (primary source)
venue_tz = get_venue_timezone(arena_name, home_team_tricode)

# Fallback hierarchy:
# 1. Team home timezone from team_aliases.yaml
# 2. Eastern Time (league headquarters default)
# 3. UTC (absolute fallback)

if venue_tz is None:
    venue_tz = get_team_home_timezone(home_team_tricode)
if venue_tz is None:
    logger.warning("No timezone found, using ET fallback", 
                   game_id=game_id, team=home_team_tricode)
    venue_tz = "America/New_York"  # ET fallback
```

#### 2. Timestamp Storage Pattern
```python
from datetime import datetime, timezone
import pytz

# ALWAYS store in UTC
game_date_utc: datetime = datetime.now(timezone.utc)

# Preserve arena timezone string
arena_tz: str = "America/Los_Angeles"  # IANA timezone name

# Derive local date for joins/analytics
arena_tz_obj = pytz.timezone(arena_tz)
game_date_local: date = game_date_utc.astimezone(arena_tz_obj).date()
```

#### 3. Database Schema Pattern
```sql
-- Every time-based table follows this pattern:
CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    game_date_utc TIMESTAMPTZ NOT NULL,    -- Always UTC
    game_date_local DATE NOT NULL,          -- Arena local date for joins
    arena_tz TEXT NOT NULL,                 -- IANA timezone string
    ...
);
```

### Testing Requirements
- **Unit tests**: Verify all fallback paths (venue → team → ET → UTC)
- **Integration tests**: Test timezone edge cases (Hawaii, Puerto Rico, London games)
- **Validation**: Assert `arena_tz` is never NULL in production data

### Common Pitfalls
❌ **WRONG**: Using local time from API responses directly  
❌ **WRONG**: Assuming all games are in Eastern Time  
❌ **WRONG**: Storing naive datetime objects  
✅ **CORRECT**: Convert API times to UTC, derive local from arena_tz

---

## Status Normalization

### Game Status Mapping

All game statuses from external sources must be normalized to our canonical enum:

```python
from enum import Enum

class GameStatus(str, Enum):
    """Canonical game status values."""
    SCHEDULED = "SCHEDULED"       # Game not yet started
    IN_PROGRESS = "IN_PROGRESS"   # Game currently live
    FINAL = "FINAL"               # Game completed
    POSTPONED = "POSTPONED"       # Game delayed to future date
    CANCELLED = "CANCELLED"       # Game will not be played
    SUSPENDED = "SUSPENDED"       # Game stopped mid-play (rare)

# NBA Stats API → Canonical Mapping
NBA_STATS_STATUS_MAP = {
    "1": GameStatus.SCHEDULED,      # Not started
    "2": GameStatus.IN_PROGRESS,    # Live
    "3": GameStatus.FINAL,          # Final
    "Final": GameStatus.FINAL,
    "Scheduled": GameStatus.SCHEDULED,
    "PPD": GameStatus.POSTPONED,
    "Postponed": GameStatus.POSTPONED,
    "Cancelled": GameStatus.CANCELLED,
    "Suspended": GameStatus.SUSPENDED,
}

# Basketball Reference → Canonical Mapping
BREF_STATUS_MAP = {
    "Final": GameStatus.FINAL,
    "Final/OT": GameStatus.FINAL,
    "Final/2OT": GameStatus.FINAL,
    "": GameStatus.SCHEDULED,        # Empty = future game
    "Preview": GameStatus.SCHEDULED,
    "Postponed": GameStatus.POSTPONED,
}

def normalize_game_status(raw_status: str, source: str) -> GameStatus:
    """Normalize game status from any source."""
    status_map = NBA_STATS_STATUS_MAP if source == "nba_stats" else BREF_STATUS_MAP
    
    normalized = status_map.get(raw_status)
    if normalized is None:
        logger.warning(
            "Unknown game status encountered",
            raw_status=raw_status,
            source=source
        )
        # Safe fallback: assume completed if unknown
        return GameStatus.FINAL
    
    return normalized
```

### Usage in Transformers
```python
def transform_game(raw_data: dict, source: str) -> Game:
    raw_status = raw_data.get("game_status_text", "")
    status = normalize_game_status(raw_status, source)
    
    return Game(
        game_id=raw_data["game_id"],
        status=status,  # Always canonical enum
        ...
    )
```

### Testing Requirements
- Test all mapping keys have valid canonical values
- Test unknown status fallback behavior
- Validate no NULL statuses in production data

---

## Season Rules & Validation

### Season Format Standard

**Format**: `YYYY-YY` (e.g., "2024-25" for 2024-25 season)

### Season Derivation Logic

```python
import re
from datetime import date, datetime
from typing import Optional

def derive_season_smart(
    game_id: Optional[str] = None,
    game_date: Optional[date] = None,
    raw_season: Optional[str] = None
) -> str:
    """
    Smart season derivation with multiple fallback strategies.
    
    Priority:
    1. Valid raw_season if provided
    2. Derive from game_id if present
    3. Derive from game_date if present
    4. Return "UNKNOWN" (never fail)
    """
    # Try raw_season first if valid
    if raw_season and re.match(r'^\d{4}-\d{2}$', raw_season):
        return raw_season
    
    # Derive from game_id (format: 00SSGGGGGG where SS = season year suffix)
    if game_id and len(game_id) >= 4:
        try:
            season_suffix = game_id[2:4]  # Extract "24" from "0022401234"
            season_int = int(season_suffix)
            
            # Convert 2-digit year to 4-digit (handles century correctly)
            if season_int >= 90:  # 90-99 = 1990s
                start_year = 1900 + season_int
            else:  # 00-89 = 2000s
                start_year = 2000 + season_int
            
            end_year_suffix = (start_year + 1) % 100
            return f"{start_year}-{end_year_suffix:02d}"
        except (ValueError, IndexError):
            pass
    
    # Derive from game_date (season starts in October)
    if game_date:
        year = game_date.year
        # Games Oct-Dec belong to current season, Jan-Sep belong to previous season
        if game_date.month >= 10:
            start_year = year
        else:
            start_year = year - 1
        
        end_year_suffix = (start_year + 1) % 100
        return f"{start_year}-{end_year_suffix:02d}"
    
    # Absolute fallback
    logger.error(
        "Could not derive season from any source",
        game_id=game_id,
        game_date=game_date,
        raw_season=raw_season
    )
    return "UNKNOWN"
```

### Season Validation Window

```python
from datetime import datetime

def is_valid_season_year(season: str) -> bool:
    """Validate season is within reasonable bounds."""
    if not re.match(r'^\d{4}-\d{2}$', season):
        return False
    
    start_year = int(season[:4])
    
    # NBA founded in 1946, validate reasonable range
    min_year = 1946
    max_year = datetime.now().year + 2  # Allow 2 years in future
    
    return min_year <= start_year <= max_year

def validate_season(season: str) -> str:
    """Validate and return season or raise error."""
    if not is_valid_season_year(season):
        raise ValueError(
            f"Season {season!r} outside valid range (1946-present+2 years)"
        )
    return season
```

### Testing Requirements
- Test all derivation paths (game_id, game_date, raw_season)
- Test century boundary (1999-00, 2000-01)
- Test month boundaries (September → October cutoff)
- Test invalid formats (log warning, don't crash)

---

## Team Alias Management

### Adding a New Team Alias

**When to add**: When you encounter a team tricode variation that isn't recognized.

#### Step 1: Update `team_aliases.yaml`

```yaml
# team_aliases.yaml
# Format: CANONICAL_TRICODE is the key, aliases are the list

BOS:  # Boston Celtics (canonical)
  - BOS
  - BOSTON
  - Celtics
  home_timezone: "America/New_York"
  full_name: "Boston Celtics"

# Add new alias to existing team
LAL:  # Los Angeles Lakers
  - LAL
  - LAL_OLD  # Historical alias
  - LAKERS
  - "L.A. Lakers"  # Quote if contains special chars
  home_timezone: "America/Los_Angeles"
  full_name: "Los Angeles Lakers"

# Add completely new team (expansion scenario)
SEA:  # Seattle SuperSonics (if returning)
  - SEA
  - SEATTLE
  - SuperSonics
  home_timezone: "America/Los_Angeles"  # Pacific time
  full_name: "Seattle SuperSonics"
```

#### Step 2: Update Team Alias Tests

```python
# tests/unit/test_team_aliases.py

def test_new_team_alias_normalization():
    """Test that new alias normalizes correctly."""
    from nba_scraper.utils.teams import normalize_team_tricode
    
    # Test new alias
    assert normalize_team_tricode("LAL_OLD") == "LAL"
    assert normalize_team_tricode("L.A. Lakers") == "LAL"
    
    # Test canonical form
    assert normalize_team_tricode("LAL") == "LAL"

def test_all_teams_have_required_fields():
    """Validate team_aliases.yaml schema."""
    import yaml
    from pathlib import Path
    
    aliases_path = Path("team_aliases.yaml")
    with open(aliases_path) as f:
        teams = yaml.safe_load(f)
    
    for tricode, config in teams.items():
        # Required fields
        assert isinstance(config, list) or isinstance(config, dict)
        if isinstance(config, dict):
            assert "home_timezone" in config
            assert "full_name" in config
            assert isinstance(config.get("aliases", []), list)
```

#### Step 3: Validate with CLI

```bash
# Test alias resolution
python -c "
from nba_scraper.utils.teams import normalize_team_tricode
print(normalize_team_tricode('LAL_OLD'))  # Should print: LAL
print(normalize_team_tricode('LAKERS'))   # Should print: LAL
"

# Run all team-related tests
pytest tests/unit/test_team_aliases.py -v
```

#### Step 4: Integration Test

```bash
# Process a game with the new alias
nba-scraper daily --date-range 2024-01-15 --force

# Verify in database
psql nba_scraper -c "
SELECT DISTINCT home_team_tricode, away_team_tricode 
FROM games 
WHERE game_date_local = '2024-01-15'
ORDER BY home_team_tricode;
"
# All tricodes should be canonical (3 uppercase letters)
```

### Files to Touch Checklist
- [ ] `team_aliases.yaml` - Add alias mapping
- [ ] `tests/unit/test_team_aliases.py` - Add test for new alias
- [ ] Run `pytest tests/unit/test_team_aliases.py -v`
- [ ] Run integration test with real game data
- [ ] Verify database contains canonical tricodes only

---

## Performance & Benchmarking

### Performance Tuning Knobs

The scraper has several configurable parameters that affect throughput and resource utilization:

#### Database Connection Pool

```python
# Environment variables or .env file
DB_POOL_SIZE=20           # Connection pool size (default: 10)
DB_MAX_OVERFLOW=30        # Max overflow connections (default: 20)
DB_POOL_TIMEOUT=30        # Pool checkout timeout in seconds (default: 30)
DB_QUERY_TIMEOUT=30       # Query execution timeout in seconds (default: 30)
```

**Tuning Guidelines:**
- **Low concurrency (1-5 workers)**: Pool size = 10-15
- **Medium concurrency (5-10 workers)**: Pool size = 20-30
- **High concurrency (10-20 workers)**: Pool size = 40-60
- **Rule of thumb**: Pool size ≈ 2 × concurrency + safety margin

**Symptoms of undersized pool:**
- Warnings: `QueuePool limit of size X overflow Y reached`
- Increased latency due to connection waiting
- Timeouts on pool checkout

**Symptoms of oversized pool:**
- Database connection errors (too many clients)
- Wasted memory
- Increased connection overhead

#### Rate Limiting

```python
# API rate limits (requests per second)
NBA_API_RPS=4.0           # NBA Stats API (default: 4.0, conservative)
BREF_RPS=2.0              # Basketball Reference (default: 2.0)
MAX_CONCURRENT_REQUESTS=5  # Global concurrent limit (default: 5)
```

**Safe Defaults (No Ban Risk):**
- NBA Stats API: 3-4 RPS
- Basketball Reference: 1-2 RPS
- Max concurrent: 3-5

**Aggressive (Higher Ban Risk):**
- NBA Stats API: 6-8 RPS
- Basketball Reference: 3-4 RPS
- Max concurrent: 10-15

**Signs of rate limiting:**
- 429 (Too Many Requests) errors
- 503 (Service Unavailable) responses
- Exponential backoff retries triggered frequently

#### Concurrency Control

```python
# Pipeline-level concurrency
BACKFILL_CHUNK_SIZE=10    # Games per batch (default: 10)
```

**Recommendations:**
- **Initial backfill**: 5-10 games/batch, concurrency 3-5
- **Daily updates**: 20-50 games/batch, concurrency 5-10
- **Real-time live**: 1-2 games, concurrency 1-2

### Benchmark Harness

Use `scripts/bench_season.py` to measure pipeline performance:

#### Basic Usage

```bash
# Benchmark 10 games with default settings
python scripts/bench_season.py --games 10 --concurrency 5

# Output:
# ============================================================
# PERFORMANCE BENCHMARK SUMMARY
# ============================================================
# 
# TIMING:
#   Total Elapsed:        45.23s
#   Total Request Time:   38.42s
# 
# THROUGHPUT:
#   Requests Total:       30
#   Requests Success:     30
#   Requests Failed:      0
#   Requests/sec:         0.66
#   Success Rate:         100.0%
# 
# DATABASE:
#   Rows Inserted:        4,523
#   Rows Updated:         0
#   Rows/sec:             100.02
#   Inserts/sec:          100.02
# 
# LATENCY (milliseconds):
#   Average:              1,280.67ms
#   P50 (median):         1,245.32ms
#   P95:                  1,892.45ms
#   P99:                  2,103.21ms
#   Max:                  2,234.56ms
#   Min:                  892.14ms
# 
# ERRORS:
#   Total Errors:         0
# ============================================================
```

#### Advanced Usage

```bash
# Benchmark 50 games from specific season with high concurrency
python scripts/bench_season.py \
  --season 2024-25 \
  --games 50 \
  --concurrency 10 \
  --db-pool-size 25 \
  --max-rps 6.0

# Output JSON for parsing/automation
python scripts/bench_season.py \
  --games 20 \
  --format json > benchmark_results.json

# Output TSV for spreadsheet analysis
python scripts/bench_season.py \
  --games 30 \
  --format tsv > benchmark.tsv

# Run in offline mode (use cached data)
python scripts/bench_season.py \
  --games 10 \
  --offline

# Quiet mode (only show summary)
python scripts/bench_season.py \
  --games 100 \
  --concurrency 10 \
  --quiet
```

#### Interpreting Results

**Target Metrics (Production Quality):**
- **Requests/sec**: 0.5-1.0 (limited by rate limiting)
- **Rows/sec**: 50-150 (depends on PBP event density)
- **Success Rate**: >95%
- **P95 Latency**: <2000ms
- **Errors**: <5% of total requests

**Red Flags:**
- **Success rate <90%**: Check API availability, rate limits
- **P95 latency >3000ms**: Database bottleneck or network issues
- **Rows/sec <30**: Loader inefficiency or DB contention
- **Errors >10%**: Critical issue requiring investigation

#### TSV Format for Automation

```bash
# Run benchmark and parse TSV output
python scripts/bench_season.py --games 20 --format tsv --quiet | \
  awk -F'\t' '{print "Throughput:", $5, "req/s, Latency P95:", $11, "ms"}'

# TSV Column Order:
# 1.  elapsed_s      - Total elapsed time
# 2.  reqs_total     - Total requests
# 3.  reqs_ok        - Successful requests
# 4.  reqs_fail      - Failed requests
# 5.  req_per_s      - Requests per second
# 6.  rows_ins       - Rows inserted
# 7.  rows_upd       - Rows updated
# 8.  rows_per_s     - Rows per second
# 9.  ins_per_s      - Inserts per second
# 10. avg_lat_ms     - Average latency (ms)
# 11. p95_lat_ms     - P95 latency (ms)
# 12. errors         - Total errors
```

### Performance Testing Workflow

#### 1. Baseline Benchmark

```bash
# Establish baseline with conservative settings
python scripts/bench_season.py \
  --season 2024-25 \
  --games 20 \
  --concurrency 5 \
  --db-pool-size 15 \
  --max-rps 4.0 \
  --format json > baseline.json
```

#### 2. Tune Parameters

```bash
# Test higher concurrency
python scripts/bench_season.py \
  --games 20 \
  --concurrency 10 \
  --db-pool-size 25 \
  --max-rps 6.0 \
  --format json > tuned.json

# Compare results
python -c "
import json
with open('baseline.json') as f:
    baseline = json.load(f)
with open('tuned.json') as f:
    tuned = json.load(f)

print(f'Throughput improvement: {tuned[\"requests_per_sec\"] / baseline[\"requests_per_sec\"]:.2f}x')
print(f'Latency change: {tuned[\"p95_latency_ms\"] - baseline[\"p95_latency_ms\"]:.2f}ms')
print(f'Error rate: {tuned[\"total_errors\"]} vs {baseline[\"total_errors\"]}')
"
```

#### 3. Stress Test

```bash
# Push to limits to find breaking point
for concurrency in 5 10 15 20; do
  echo "Testing concurrency=$concurrency"
  python scripts/bench_season.py \
    --games 50 \
    --concurrency $concurrency \
    --db-pool-size $((concurrency * 2 + 10)) \
    --format tsv \
    --quiet >> stress_test_results.tsv
done

# Analyze results
cat stress_test_results.tsv | \
  awk -F'\t' '{print NR, $5, $11, $12}' | \
  column -t
```

### Database Performance

#### Connection Pool Monitoring

```python
# Add to monitoring dashboard
from nba_scraper.db import get_engine

engine = get_engine()
pool = engine.pool

print(f"Pool size: {pool.size()}")
print(f"Checked out: {pool.checkedout()}")
print(f"Overflow: {pool.overflow()}")
print(f"Queue size: {pool.queue.qsize()}")
```

#### Query Performance

```sql
-- Top 10 slowest queries
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    max_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

-- Index usage statistics
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND indexname NOT LIKE '%_pkey'
ORDER BY schemaname, tablename;

-- Table bloat check
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_live_tup,
    n_dead_tup,
    round(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY n_dead_tup DESC;
```

### Troubleshooting Performance Issues

#### Issue: Low Throughput (<0.3 req/s)

**Diagnosis:**
```bash
# Check if rate limiting is the bottleneck
python scripts/bench_season.py --games 10 --max-rps 8.0

# Check database pool
# Look for "QueuePool limit reached" in logs
```

**Solutions:**
- Increase `NBA_API_RPS` (cautiously)
- Increase `MAX_CONCURRENT_REQUESTS`
- Verify network latency to APIs

#### Issue: High Latency (P95 >3000ms)

**Diagnosis:**
```sql
-- Check for slow queries
SELECT * FROM pg_stat_activity WHERE state = 'active' AND query_start < NOW() - INTERVAL '2 seconds';

-- Check for lock contention
SELECT * FROM pg_locks WHERE NOT granted;
```

**Solutions:**
- Increase `DB_POOL_SIZE`
- Add missing indexes
- Optimize loader upsert queries
- Use `COPY` for bulk inserts

#### Issue: Database Deadlocks

**Symptoms:**
- Error: `deadlock detected`
- Transactions timing out
- Random failures during high concurrency

**Solutions:**
```python
# Use DEFERRABLE constraints in transactions
async with transaction():
    await conn.execute('SET CONSTRAINTS ALL DEFERRED')
    # ... your operations

# Reduce transaction scope
# Instead of: 1 transaction for entire game
# Use: Separate transactions for game, PBP, lineups

# Add explicit lock ordering
# Always acquire locks in same order: games → PBP → lineups
```

#### Issue: Memory Growth

**Diagnosis:**
```bash
# Monitor memory during benchmark
python scripts/bench_season.py --games 100 &
while true; do
  ps aux | grep bench_season.py | grep -v grep | awk '{print $6/1024 "MB"}'
  sleep 5
done
```

**Solutions:**
- Reduce `BACKFILL_CHUNK_SIZE`
- Process games in smaller batches
- Clear caches between batches
- Use streaming/pagination for large result sets

### Performance Best Practices

**DO:**
✅ Start with conservative settings and tune up  
✅ Monitor error rates and success percentage  
✅ Use connection pooling (never create connections per-request)  
✅ Batch similar operations together  
✅ Use prepared statements for repeated queries  
✅ Profile before optimizing  

**DON'T:**
❌ Disable rate limiting (risks API bans)  
❌ Create new connections for every request  
❌ Run high concurrency on shared/production databases  
❌ Ignore warnings about pool exhaustion  
❌ Skip benchmarking after changes  

---

## Adding Derived Windows

**Derived windows** are analytics tables computed from core data (e.g., `q1_window_12_8`, `early_shocks`).

### Checklist for New Derived Window

#### 1. Define Schema in `schema.sql`

```sql
-- Example: Add Q2 clutch window (6:00-2:00)
CREATE TABLE q2_clutch_6_2 (
    game_id TEXT PRIMARY KEY,
    home_team_tricode TEXT NOT NULL,
    away_team_tricode TEXT NOT NULL,
    
    -- Metrics
    possessions_elapsed INTEGER NOT NULL,
    home_clutch_fg_pct NUMERIC,
    away_clutch_fg_pct NUMERIC,
    home_turnovers INTEGER,
    away_turnovers INTEGER,
    
    -- Provenance
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign key
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- Indexes for common queries
CREATE INDEX idx_q2_clutch_teams ON q2_clutch_6_2 (home_team_tricode, away_team_tricode);
CREATE INDEX idx_q2_clutch_fg_pct ON q2_clutch_6_2 (home_clutch_fg_pct, away_clutch_fg_pct);

-- Table comment
COMMENT ON TABLE q2_clutch_6_2 IS 'Second quarter clutch analytics (6:00-2:00 window)';
```

#### 2. Create Transformer Logic

```python
# src/nba_scraper/transformers/derived/q2_clutch.py

from typing import List
from nba_scraper.models.pbp import PBPEvent
from nba_scraper.models.derived import Q2ClutchWindow

def compute_q2_clutch_window(
    game_id: str,
    pbp_events: List[PBPEvent]
) -> Q2ClutchWindow:
    """
    Compute Q2 clutch metrics for 6:00-2:00 window.
    
    Args:
        game_id: NBA game identifier
        pbp_events: List of play-by-play events for the game
    
    Returns:
        Q2ClutchWindow model with computed metrics
    """
    # Filter to Q2, 6:00-2:00 window
    q2_clutch_events = [
        e for e in pbp_events
        if e.period == 2 
        and 360 <= e.seconds_elapsed <= 600  # 6:00 to 10:00 mark
    ]
    
    # Compute metrics
    home_shots = [e for e in q2_clutch_events if e.event_type == "SHOT" and e.team_tricode == home_team]
    away_shots = [e for e in q2_clutch_events if e.event_type == "SHOT" and e.team_tricode == away_team]
    
    home_fg_pct = sum(1 for s in home_shots if s.shot_made) / len(home_shots) if home_shots else 0.0
    away_fg_pct = sum(1 for s in away_shots if s.shot_made) / len(away_shots) if away_shots else 0.0
    
    return Q2ClutchWindow(
        game_id=game_id,
        home_team_tricode=home_team,
        away_team_tricode=away_team,
        possessions_elapsed=len(q2_clutch_events),
        home_clutch_fg_pct=home_fg_pct,
        away_clutch_fg_pct=away_fg_pct,
        ...
    )
```

#### 3. Add Loader Function

```python
# src/nba_scraper/loaders/derived.py

async def upsert_q2_clutch_window(
    conn: AsyncConnection,
    window: Q2ClutchWindow
) -> None:
    """Upsert Q2 clutch window analytics."""
    query = text("""
        INSERT INTO q2_clutch_6_2 (
            game_id, home_team_tricode, away_team_tricode,
            possessions_elapsed, home_clutch_fg_pct, away_clutch_fg_pct,
            home_turnovers, away_turnovers,
            source, source_url
        ) VALUES (
            :game_id, :home_team_tricode, :away_team_tricode,
            :possessions_elapsed, :home_clutch_fg_pct, :away_clutch_fg_pct,
            :home_turnovers, :away_turnovers,
            :source, :source_url
        )
        ON CONFLICT (game_id) DO UPDATE SET
            home_clutch_fg_pct = excluded.home_clutch_fg_pct,
            away_clutch_fg_pct = excluded.away_clutch_fg_pct,
            home_turnovers = excluded.home_turnovers,
            away_turnovers = excluded.away_turnovers,
            ingested_at_utc = NOW()
        WHERE (
            q2_clutch_6_2.home_clutch_fg_pct IS DISTINCT FROM excluded.home_clutch_fg_pct
            OR q2_clutch_6_2.away_clutch_fg_pct IS DISTINCT FROM excluded.away_clutch_fg_pct
            OR q2_clutch_6_2.home_turnovers IS DISTINCT FROM excluded.home_turnovers
            OR q2_clutch_6_2.away_turnovers IS DISTINCT FROM excluded.away_turnovers
        )
    """)
    
    await conn.execute(query, window.model_dump())
```

#### 4. Update Analytics Pipeline

```python
# src/nba_scraper/pipelines/analytics.py

async def derive_analytics_for_game(game_id: str) -> None:
    """Derive all analytics for a single game."""
    # Fetch PBP data
    pbp_events = await fetch_pbp_events(game_id)
    
    # Compute Q1 window (existing)
    q1_window = compute_q1_window_12_8(game_id, pbp_events)
    await upsert_q1_window_12_8(conn, q1_window)
    
    # Compute NEW Q2 clutch window
    q2_clutch = compute_q2_clutch_window(game_id, pbp_events)
    await upsert_q2_clutch_window(conn, q2_clutch)
    
    # ... other analytics
```

#### 5. Create Unit Tests

```python
# tests/unit/test_q2_clutch_transformer.py

import pytest
from nba_scraper.transformers.derived import compute_q2_clutch_window
from nba_scraper.models.pbp import PBPEvent

def test_q2_clutch_basic_computation():
    """Test basic Q2 clutch window computation."""
    game_id = "0022400001"
    
    # Mock PBP events in Q2, 6:00-2:00 window
    pbp_events = [
        PBPEvent(
            game_id=game_id,
            period=2,
            event_idx=100,
            seconds_elapsed=420,  # 7:00 mark in Q2
            event_type="SHOT",
            team_tricode="BOS",
            shot_made=True,
            shot_value=2,
            ...
        ),
        # ... more test events
    ]
    
    result = compute_q2_clutch_window(game_id, pbp_events)
    
    assert result.game_id == game_id
    assert result.possessions_elapsed > 0
    assert 0.0 <= result.home_clutch_fg_pct <= 1.0
    assert 0.0 <= result.away_clutch_fg_pct <= 1.0

def test_q2_clutch_empty_window():
    """Test handling of empty Q2 clutch window."""
    game_id = "0022400002"
    pbp_events = []  # No events
    
    result = compute_q2_clutch_window(game_id, pbp_events)
    
    assert result.possessions_elapsed == 0
    assert result.home_clutch_fg_pct == 0.0
    assert result.away_clutch_fg_pct == 0.0
```

#### 6. Add Integration Tests

```python
# tests/integration/test_q2_clutch_integration.py

import pytest
from nba_scraper.pipelines.analytics import derive_analytics_for_game

@pytest.mark.asyncio
@pytest.mark.integration
async def test_q2_clutch_end_to_end(db_session):
    """Test Q2 clutch derivation end-to-end."""
    game_id = "0022400001"
    
    # Run analytics derivation
    await derive_analytics_for_game(game_id)
    
    # Verify data in database
    result = await db_session.execute(
        text("SELECT * FROM q2_clutch_6_2 WHERE game_id = :game_id"),
        {"game_id": game_id}
    )
    row = result.fetchone()
    
    assert row is not None
    assert row.game_id == game_id
    assert row.possessions_elapsed > 0
```

#### 7. Add Metrics & Monitoring

```python
# src/nba_scraper/metrics.py

from prometheus_client import Counter, Histogram

q2_clutch_computed = Counter(
    'q2_clutch_windows_computed_total',
    'Total Q2 clutch windows computed'
)

q2_clutch_computation_time = Histogram(
    'q2_clutch_computation_seconds',
    'Time to compute Q2 clutch window'
)

# Use in transformer
@q2_clutch_computation_time.time()
def compute_q2_clutch_window(...):
    ...
    q2_clutch_computed.inc()
    return result
```

### Complete Checklist

- [ ] Define schema in `schema.sql` with:
  - [ ] Table structure with required columns
  - [ ] Primary key (usually `game_id`)
  - [ ] Foreign key to `games` table
  - [ ] Indexes for common queries
  - [ ] Table comment describing purpose
  
- [ ] Create transformer in `src/nba_scraper/transformers/derived/`:
  - [ ] Pure function that takes PBP events
  - [ ] Returns Pydantic model
  - [ ] Handles edge cases (empty data, etc.)
  
- [ ] Add loader in `src/nba_scraper/loaders/derived.py`:
  - [ ] Idempotent upsert with conflict handling
  - [ ] Diff-aware UPDATE (only changed values)
  
- [ ] Update analytics pipeline:
  - [ ] Call transformer
  - [ ] Call loader
  - [ ] Add error handling
  
- [ ] Create unit tests:
  - [ ] Test basic computation
  - [ ] Test edge cases (empty, invalid data)
  - [ ] Test data validation
  
- [ ] Create integration tests:
  - [ ] Test end-to-end derivation
  - [ ] Verify database persistence
  - [ ] Test idempotency
  
- [ ] Add metrics:
  - [ ] Computation counter
  - [ ] Computation time histogram
  - [ ] Error counter
  
- [ ] Update documentation:
  - [ ] Add to README.md data tables section
  - [ ] Document metrics in DEV_NOTES.md
  - [ ] Add to CHANGELOG.md

---

## Data Quality Checks

### Pre-Deployment Validation

Before deploying any changes, run these checks:

```bash
# 1. Schema validation
psql nba_scraper -c "\d+ games"  # Verify table structure
psql nba_scraper -c "\di"        # Check indexes exist

# 2. Data completeness
psql nba_scraper -c "
SELECT 
    COUNT(*) as total_games,
    COUNT(DISTINCT game_id) as unique_games,
    SUM(CASE WHEN status = 'FINAL' THEN 1 ELSE 0 END) as final_games
FROM games
WHERE game_date_local >= CURRENT_DATE - INTERVAL '7 days';
"

# 3. Referential integrity
psql nba_scraper -c "
SELECT 'pbp_events' as table_name, COUNT(*) as orphaned_rows
FROM pbp_events p
LEFT JOIN games g ON p.game_id = g.game_id
WHERE g.game_id IS NULL
UNION ALL
SELECT 'outcomes', COUNT(*)
FROM outcomes o
LEFT JOIN games g ON o.game_id = g.game_id
WHERE g.game_id IS NULL;
"

# 4. Run validation suite
pytest tests/integration/test_data_quality.py -v
```

### Continuous Monitoring Queries

```sql
-- Daily data freshness check
SELECT MAX(game_date_local) as latest_game_date,
       MAX(ingested_at_utc) as latest_ingestion
FROM games;

-- PBP completeness (should be 400-500 events per game)
SELECT game_id, COUNT(*) as event_count
FROM pbp_events
GROUP BY game_id
HAVING COUNT(*) < 300 OR COUNT(*) > 700
ORDER BY event_count;

-- Status distribution
SELECT status, COUNT(*) as count
FROM games
WHERE game_date_local >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY status;
```

---

## Development Checklists

### Adding a New Data Source

- [ ] Create client in `src/nba_scraper/io_clients/`
  - [ ] Implement rate limiting
  - [ ] Add retry logic with exponential backoff
  - [ ] Include proper User-Agent headers
  
- [ ] Add extraction functions in `src/nba_scraper/extractors/`
  - [ ] Pure functions with no side effects
  - [ ] Defensive parsing with fallbacks
  - [ ] Structured error logging
  
- [ ] Create/update Pydantic models in `src/nba_scraper/models/`
  - [ ] Strict validation rules
  - [ ] Canonical enum values
  - [ ] Documentation strings
  
- [ ] Add transformation logic in `src/nba_scraper/transformers/`
  - [ ] Normalize team tricodes
  - [ ] Convert timestamps to UTC
  - [ ] Apply business rules
  
- [ ] Update database loader in `src/nba_scraper/loaders/`
  - [ ] Idempotent upserts
  - [ ] Diff-aware updates
  - [ ] Provenance tracking
  
- [ ] Add comprehensive tests
  - [ ] Unit tests with golden files
  - [ ] Integration tests with test database
  - [ ] Error condition coverage

### Pre-Commit Checklist

- [ ] Run code formatters: `ruff format`
- [ ] Run linters: `ruff check`
- [ ] Run type checker: `mypy src/nba_scraper`
- [ ] Run unit tests: `pytest tests/unit/ -v`
- [ ] Check test coverage: `pytest --cov=nba_scraper --cov-report=term-missing`
- [ ] Update CHANGELOG.md if needed
- [ ] Update documentation if APIs changed

### Pre-Deploy Checklist

- [ ] All tests passing in CI
- [ ] Database migrations tested in staging
- [ ] Data quality checks passing
- [ ] Performance benchmarks acceptable
- [ ] Error rates within SLOs
- [ ] Rollback plan documented
- [ ] Monitoring alerts configured

---

## Additional Resources

- **Main README**: [README.md](README.md) - Project overview and quickstart
- **Troubleshooting Guide**: [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and fixes
- **Migration Guide**: [MIGRATIONS_SETUP.md](MIGRATIONS_SETUP.md) - Database migration procedures
- **Contributing Guide**: [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- **Schema Documentation**: [schema.sql](schema.sql) - Complete database DDL with comments

---

**Questions or Issues?**  
Open a GitHub issue or contact the data engineering team on Slack (#nba-scraper-dev).