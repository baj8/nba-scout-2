-- =====================================================================
-- NBA SCRAPER – HEALTH CHECKS (PostgreSQL)
-- Usage:
--   psql "$DATABASE_URL" -f sql/health_checks.sql \
--     -v start_date='2024-01-15' -v end_date='2024-01-22' -v game_id='0022300001'
-- Variables:
--   :start_date  -- inclusive (YYYY-MM-DD)
--   :end_date    -- inclusive (YYYY-MM-DD)
--   :game_id     -- optional, a single game to inspect
-- =====================================================================

\echo
\echo '=== PARAMETERS ====================================================='
\echo 'start_date = :start_date'
\echo 'end_date   = :end_date'
\echo 'game_id    = :game_id'
\echo '===================================================================='
\echo

-- 0) Sanity: how many games in window?
\echo '0) Games by date'
SELECT game_date::date AS day, COUNT(*) AS games
FROM games
WHERE game_date BETWEEN :'start_date' AND :'end_date'
GROUP BY 1
ORDER BY 1;

-- 1) Orphan checks (should return zero rows)
\echo
\echo '1) Orphans: children without parent games (expect 0 rows)'
WITH ids AS (
  SELECT game_id FROM games
)
SELECT 'pbp_events' AS table, pe.game_id, COUNT(*) AS cnt
FROM pbp_events pe LEFT JOIN ids i USING (game_id)
WHERE i.game_id IS NULL
GROUP BY 1,2
UNION ALL
SELECT 'lineup_stints', ls.game_id, COUNT(*)
FROM lineup_stints ls LEFT JOIN ids i USING (game_id)
WHERE i.game_id IS NULL
GROUP BY 1,2
UNION ALL
SELECT 'shots', s.game_id, COUNT(*)
FROM shots s LEFT JOIN ids i USING (game_id)
WHERE i.game_id IS NULL
GROUP BY 1,2
UNION ALL
SELECT 'adv_metrics', am.game_id, COUNT(*)
FROM adv_metrics am LEFT JOIN ids i USING (game_id)
WHERE i.game_id IS NULL
GROUP BY 1,2
ORDER BY 1,2;

-- 2) Duplicate PK checks (should return zero rows)
\echo
\echo '2) Duplicates (expect 0 rows)'
SELECT 'pbp_events' AS table, game_id, event_num, COUNT(*) AS dup_count
FROM pbp_events
GROUP BY 1,2,3 HAVING COUNT(*) > 1
UNION ALL
SELECT 'lineup_stints', game_id, period, COUNT(*)  -- adjust if you have a lineup hash PK
FROM lineup_stints
GROUP BY 1,2,3 HAVING COUNT(*) > 1
UNION ALL
SELECT 'shots', game_id, COALESCE(shot_id, -1), COUNT(*)
FROM shots
GROUP BY 1,2,3 HAVING COUNT(*) > 1
ORDER BY 1,2;

-- 3) PBP density per game (flag < 350 rows)
\echo
\echo '3) PBP density (rows per game) – flag < 350'
SELECT game_id, COUNT(*) AS events
FROM pbp_events
WHERE game_id IN (
  SELECT game_id FROM games
  WHERE game_date BETWEEN :'start_date' AND :'end_date'
)
GROUP BY 1
ORDER BY events ASC
LIMIT 50;

-- 4) seconds_elapsed coverage (bottom 20)
\echo
\echo '4) seconds_elapsed coverage (bottom 20 games)'
SELECT game_id,
       ROUND(100.0 * SUM(CASE WHEN seconds_elapsed IS NOT NULL THEN 1 ELSE 0 END)::numeric
             / NULLIF(COUNT(*),0), 2) AS coverage_pct,
       COUNT(*) AS events
FROM pbp_events
WHERE game_id IN (
  SELECT game_id FROM games
  WHERE game_date BETWEEN :'start_date' AND :'end_date'
)
GROUP BY 1
ORDER BY coverage_pct ASC, events DESC
LIMIT 20;

-- 5) Shot coverage per game (if shots collected)
\echo
\echo '5) Shot coverage per game (events with coordinates / total shot events)'
-- Adjust the WHERE clause if you need to filter by shot event types specifically.
SELECT s.game_id,
       COUNT(*) AS shot_rows,
       ROUND(100.0 * AVG(
         CASE WHEN s.x IS NOT NULL AND s.y IS NOT NULL THEN 1 ELSE 0 END
       )::numeric, 2) AS coord_coverage_pct
FROM shots s
WHERE s.game_id IN (
  SELECT game_id FROM games
  WHERE game_date BETWEEN :'start_date' AND :'end_date'
)
GROUP BY 1
ORDER BY coord_coverage_pct ASC
LIMIT 50;

-- 6) Errors by endpoint/status (if you log fetches to a table; optional)
-- Uncomment and adapt if you have an api_logs table.
-- \echo
-- \echo '6) API errors by endpoint/status in window'
-- SELECT endpoint, status_code, COUNT(*) AS cnt
-- FROM api_logs
-- WHERE ts::date BETWEEN :'start_date' AND :'end_date'
-- GROUP BY 1,2 ORDER BY cnt DESC;

-- 7) Single-game drill-down (only if :game_id is provided)
\echo
\echo '7) Single game detail (events, coverage) – optional'
SELECT 'pbp_events' AS table, COUNT(*) AS rows
FROM pbp_events WHERE game_id = :'game_id'
UNION ALL
SELECT 'shots', COUNT(*) FROM shots WHERE game_id = :'game_id'
UNION ALL
SELECT 'lineup_stints', COUNT(*) FROM lineup_stints WHERE game_id = :'game_id';

\echo '   seconds_elapsed coverage for :game_id'
SELECT
  SUM(CASE WHEN seconds_elapsed IS NOT NULL THEN 1 ELSE 0 END)::float
  / NULLIF(COUNT(*),0) AS coverage_ratio
FROM pbp_events WHERE game_id = :'game_id';

-- 8) Team ID sanity (tricodes resolved correctly) – spot-check
\echo
\echo '8) Team ID sanity (home/away ints, not tricodes)'
SELECT game_id, home_team_id, away_team_id
FROM games
WHERE game_date BETWEEN :'start_date' AND :'end_date'
ORDER BY game_date, game_id
LIMIT 50;

-- 9) Status casing consistency (e.g., "Final")
\echo
\echo '9) Status casing distribution'
SELECT status, COUNT(*) FROM games
WHERE game_date BETWEEN :'start_date' AND :'end_date'
GROUP BY 1 ORDER BY 2 DESC;

-- 10) Lineup sanity: exactly 5 starters rows per team for period=1 (if modeled that way)
\echo
\echo '10) Lineup period-1 sanity (expect 5 per team)'
SELECT game_id, team_id, period, COUNT(*) AS rows
FROM lineup_stints
WHERE period = 1
  AND game_id IN (
    SELECT game_id FROM games
    WHERE game_date BETWEEN :'start_date' AND :'end_date'
  )
GROUP BY 1,2,3
HAVING COUNT(*) <> 5
ORDER BY game_id, team_id;