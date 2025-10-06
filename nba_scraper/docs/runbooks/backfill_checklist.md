# NBA Historical Backfill Checklist (Runbook)

## 1) Pre-flight Readiness (Go/No-Go)

- [ ] Loaders facade exports: `upsert_game`, `upsert_pbp`, `upsert_lineups`, `upsert_shots`, `upsert_adv_metrics`
- [ ] IO facade methods: `fetch_boxscore`, `fetch_pbp`, `fetch_lineups`, `fetch_shots`, `fetch_scoreboard`
- [ ] Game bridge used at all callsites (GameRow → Game; never pass GameRow to loaders)
- [ ] Crosswalk present with aliases (BKN/BRK, PHX/PHO, NOP/NOH, CHA/CHO)
- [ ] PBP timing: `clock_seconds` & `seconds_elapsed` derived; parser supports M:SS(.fff) and PTxMxS[.fff]
- [ ] DB constraints: FKs DEFERRABLE INITIALLY DEFERRED
- [ ] Upserts are idempotent (re-run a game; row counts unchanged)
- [ ] Rate limit configured ≤ 5 rps; retries with jitter; honors Retry-After
- [ ] Raw payload persistence configured (directory path documented)

## 2) Single-Game Smoke Test

**Command** (example; adjust module/flags to your repo):
```bash
python3 -m nba_scraper.tools.run_single_game --game-id <GAME_ID> --persist-raw ./raw
```

**SQL checks:**
```sql
SELECT * FROM games WHERE game_id = $1;
SELECT COUNT(*) FROM pbp_events WHERE game_id = $1;
SELECT 1.0 * SUM(CASE WHEN seconds_elapsed IS NOT NULL THEN 1 END) / COUNT(*)
FROM pbp_events WHERE game_id = $1;
```

**Pass if:**
- [ ] 1 row in games
- [ ] ≥ 400 rows in pbp_events
- [ ] seconds_elapsed non-NULL for ≥ 75% (or ≥ 300 rows)
- [ ] (If shots fetched) shot coverage ≥ 80%

## 3) Pilot Week (Dry-Run → Live)

**Dates** (suggested): 2024-01-15 → 2024-01-22

**Dry-run** (no inserts):
```bash
python3 -m nba_scraper.pipelines.backfill \
  --start 2024-01-15 --end 2024-01-22 \
  --dry-run true --rate-limit 5 --raw-dir ./raw
```

**Live:**
```bash
python3 -m nba_scraper.pipelines.backfill \
  --start 2024-01-15 --end 2024-01-22 \
  --dry-run false --rate-limit 5 --raw-dir ./raw --batch-size 100
```

**DQ Gates** (per date & per game):
- [ ] ≥ 95% of scoreboard games inserted into games
- [ ] Per game: ≥ 400 PBP rows
- [ ] Per game: seconds_elapsed coverage ≥ 75% (or ≥ 300 rows)
- [ ] If shots fetched: shot coverage ≥ 80%
- [ ] 0 FK violations, 0 duplicate PKs

## 4) Batch/Season Backfill (Scale-Out)

**Order:** newest season → older seasons  
**Batching:** by games, e.g., `--batch-size 100`  
**Between batches:** sleep/jitter; honor rate limit  
**Idempotency:** safe to re-run any game  

**Commands** (examples):
```bash
# One season (live)
python3 -m nba_scraper.pipelines.backfill \
  --season 2023-24 --dry-run false --rate-limit 5 --raw-dir ./raw --batch-size 100

# Date window
python3 -m nba_scraper.pipelines.backfill \
  --start 2019-10-01 --end 2020-04-15 --dry-run false --rate-limit 5 --raw-dir ./raw --batch-size 100
```

## 5) Raw Payload Persistence

**Layout:**
```
raw/
  2024-01-15/
    scoreboard.json
    0022400123_boxscore.json
    0022400123_pbp.json
    0022400123_shots.json
    0022400123_lineups.json
```

- [ ] Save raw on every successful/failed fetch
- [ ] Redact PII (if any) before sharing

## 6) Ops Log Template (per batch/date)

Copy & fill for each run:

```
Date range: <YYYY-MM-DD .. YYYY-MM-DD>   Batch size: <N>   Rate limit: <N rps>
Games from scoreboard: <N>   Inserted: <N>   Failed: <N>

PBP rows (sum): <N>   Avg/game: <N>   Games < 350 rows: <N>
seconds_elapsed coverage: <avg%> (min: <min%>, max: <max%>)
Shot coverage (if collected): <avg%>

Top endpoints by error:
- <endpoint> : <count> (statuses: 429=<n>, 500=<n>, ...)

Quarantined game_ids (retry later): <list or file path>
Raw dir: <path>
Notes:
- <brief notes / anomalies / API behavior>
```

## 7) Quick SQL Health Checks

```sql
-- Games loaded in a window
SELECT game_date, COUNT(*) FROM games
WHERE game_date BETWEEN $1 AND $2
GROUP BY 1 ORDER BY 1;

-- PBP density per game (<350 means investigate)
SELECT game_id, COUNT(*) AS events
FROM pbp_events
WHERE game_id IN (SELECT game_id FROM games WHERE game_date BETWEEN $1 AND $2)
GROUP BY game_id HAVING COUNT(*) < 350;

-- seconds_elapsed coverage per game
SELECT game_id,
  SUM(CASE WHEN seconds_elapsed IS NOT NULL THEN 1 ELSE 0 END)::float / COUNT(*) AS coverage
FROM pbp_events
WHERE game_id IN (SELECT game_id FROM games WHERE game_date BETWEEN $1 AND $2)
GROUP BY game_id
ORDER BY coverage ASC
LIMIT 20;
```

## 8) Rollback (per game_id)

Run in one transaction:
```sql
BEGIN;
DELETE FROM pbp_events   WHERE game_id = $1;
DELETE FROM lineup_stints WHERE game_id = $1;
DELETE FROM shots        WHERE game_id = $1;
DELETE FROM adv_metrics  WHERE game_id = $1;
DELETE FROM games        WHERE game_id = $1;
COMMIT;
```

## 9) Troubleshooting Quick Hits

- **429s/5xx:** lower rate limit (e.g., 3 rps), increase backoff cap (30s), honor Retry-After
- **Missing GameSummary:** derive game metadata from team box headers
- **Tricode mismatch:** add alias to crosswalk (BRK→BKN, PHO→PHX, NOH→NOP, CHO→CHA)
- **Clock anomalies:** log raw clock; parser handles fractional/ISO; skip rare unparseables and keep coverage above DQ threshold
- **Idempotency:** re-run a random sample of already-loaded games; counts must not increase

## 10) Sign-off Criteria (Backfill Complete)

- [ ] 100% of intended seasons backfilled
- [ ] Coverage KPIs met (PBP rows, seconds_elapsed %, shots % where applicable)
- [ ] Quarantine list empty or documented with reasons
- [ ] Monitoring dashboards green
- [ ] Raw payloads archived

**Deliverables:**
- [ ] Commit the new file `docs/runbooks/backfill_checklist.md`
- [ ] Ensure all code blocks are syntactically valid and copy-pasteable
- [ ] Keep the doc < 300 lines, no extraneous prose