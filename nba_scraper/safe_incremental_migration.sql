-- Safe Incremental Migration for NBA Stats Foundation
-- Following TL;DR order: guardrails → functions → tables → columns → constraints → indexes

-- =============================================================================
-- 0) GUARDRAILS & PREP
-- =============================================================================

-- Safety: make FKs deferrable
SET CONSTRAINTS ALL DEFERRED;

-- Create utility schema for helper functions
CREATE SCHEMA IF NOT EXISTS util;

-- =============================================================================
-- 1) CREATE HELPER FUNCTIONS (IMMUTABLE for generated columns)
-- =============================================================================

-- Create immutable clock parsing function
CREATE OR REPLACE FUNCTION util.parse_clock_to_seconds(clock_str TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
BEGIN
    -- Handle null/empty input
    IF clock_str IS NULL OR clock_str = '' THEN
        RETURN NULL;
    END IF;
    
    -- Parse MM:SS or MM:SS.fff format
    -- Extract minutes and seconds, ignore fractional part for now
    RETURN (split_part(clock_str, ':', 1))::int * 60 + 
           floor(split_part(split_part(clock_str, ':', 2), '.', 1)::numeric)::int;
EXCEPTION
    WHEN others THEN
        RETURN NULL; -- Invalid format returns NULL
END;
$$;

-- =============================================================================
-- 2) CREATE MISSING TABLES
-- =============================================================================

-- Create lineup_stints table (the main missing piece)
CREATE TABLE IF NOT EXISTS lineup_stints (
    stint_id BIGSERIAL PRIMARY KEY,
    game_id TEXT NOT NULL,
    team_id INT NOT NULL,
    period SMALLINT NOT NULL,
    start_event_num INT NOT NULL,
    end_event_num INT NOT NULL,
    lineup_player_ids INT[] NOT NULL CHECK (cardinality(lineup_player_ids) = 5),
    seconds_played INT NOT NULL,
    lineup_hash TEXT GENERATED ALWAYS AS (md5(array_to_string(lineup_player_ids, ','))) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- 3) ADD MISSING COLUMNS (IF NOT EXISTS)
-- =============================================================================

-- Add clock parsing columns to pbp_events
ALTER TABLE pbp_events 
    ADD COLUMN IF NOT EXISTS clock_seconds INT;

ALTER TABLE pbp_events 
    ADD COLUMN IF NOT EXISTS seconds_elapsed DOUBLE PRECISION;

-- Add shot coordinate columns to pbp_events (for Tranche 2 integration)
ALTER TABLE pbp_events 
    ADD COLUMN IF NOT EXISTS shot_x NUMERIC;

ALTER TABLE pbp_events 
    ADD COLUMN IF NOT EXISTS shot_y NUMERIC;

ALTER TABLE pbp_events 
    ADD COLUMN IF NOT EXISTS shot_distance_ft NUMERIC;

-- Ensure event_num exists in pbp_events
ALTER TABLE pbp_events 
    ADD COLUMN IF NOT EXISTS event_num INT;

-- =============================================================================
-- 4) ADD DEFERRABLE FOREIGN KEY CONSTRAINTS
-- =============================================================================

-- Add deferrable FK for lineup_stints -> games
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'fk_ls_game'
    ) THEN
        ALTER TABLE lineup_stints
            ADD CONSTRAINT fk_ls_game
            FOREIGN KEY (game_id) REFERENCES games(game_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END$$;

-- Make existing pbp_events FK deferrable if it exists
DO $$
BEGIN
    -- Drop existing FK if it exists and is not deferrable
    IF EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        WHERE t.relname = 'pbp_events' 
        AND c.contype = 'f' 
        AND c.condeferrable = false
    ) THEN
        ALTER TABLE pbp_events DROP CONSTRAINT IF EXISTS pbp_events_game_id_fkey;
        ALTER TABLE pbp_events DROP CONSTRAINT IF EXISTS fk_pbp_game;
    END IF;
    
    -- Add deferrable FK
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'fk_pbp_game'
    ) THEN
        ALTER TABLE pbp_events
            ADD CONSTRAINT fk_pbp_game
            FOREIGN KEY (game_id) REFERENCES games(game_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END$$;

-- Make shot_events FK deferrable
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        WHERE t.relname = 'shot_events' 
        AND c.contype = 'f' 
        AND c.condeferrable = false
    ) THEN
        ALTER TABLE shot_events DROP CONSTRAINT IF EXISTS shot_events_game_id_fkey;
        ALTER TABLE shot_events DROP CONSTRAINT IF EXISTS fk_shot_game;
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'fk_shot_game'
    ) THEN
        ALTER TABLE shot_events
            ADD CONSTRAINT fk_shot_game
            FOREIGN KEY (game_id) REFERENCES games(game_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END$$;

-- =============================================================================
-- 5) ADD UNIQUE CONSTRAINTS FOR IDEMPOTENT UPSERTS
-- =============================================================================

-- PBP events uniqueness
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_pbp_game_event'
    ) THEN
        ALTER TABLE pbp_events
            ADD CONSTRAINT uq_pbp_game_event 
            UNIQUE (game_id, event_num);
    END IF;
END$$;

-- Lineup stints uniqueness
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_ls_unique'
    ) THEN
        ALTER TABLE lineup_stints
            ADD CONSTRAINT uq_ls_unique
            UNIQUE (game_id, team_id, period, start_event_num, end_event_num, lineup_hash);
    END IF;
END$$;

-- =============================================================================
-- 6) CREATE PERFORMANCE INDEXES
-- =============================================================================

-- PBP events indexes
CREATE INDEX IF NOT EXISTS idx_pbp_game_period 
    ON pbp_events (game_id, period);

CREATE INDEX IF NOT EXISTS idx_pbp_clock_seconds 
    ON pbp_events (clock_seconds) 
    WHERE clock_seconds IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pbp_seconds_elapsed 
    ON pbp_events (seconds_elapsed) 
    WHERE seconds_elapsed IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pbp_shot_coords 
    ON pbp_events (shot_x, shot_y) 
    WHERE shot_x IS NOT NULL AND shot_y IS NOT NULL;

-- Lineup stints indexes
CREATE INDEX IF NOT EXISTS idx_ls_game 
    ON lineup_stints (game_id);

CREATE INDEX IF NOT EXISTS idx_ls_game_team_period 
    ON lineup_stints (game_id, team_id, period);

CREATE INDEX IF NOT EXISTS idx_ls_team_period 
    ON lineup_stints (team_id, period);

-- Shot events indexes
CREATE INDEX IF NOT EXISTS idx_shot_events_player 
    ON shot_events (player_id);

CREATE INDEX IF NOT EXISTS idx_shot_events_coords 
    ON shot_events (loc_x, loc_y);

-- =============================================================================
-- 7) ADD COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON TABLE lineup_stints IS 'Lineup stints with array-based player tracking and generated hash';
COMMENT ON COLUMN lineup_stints.lineup_player_ids IS 'Array of 5 player IDs in the lineup';
COMMENT ON COLUMN lineup_stints.lineup_hash IS 'MD5 hash of lineup for uniqueness';

COMMENT ON COLUMN pbp_events.clock_seconds IS 'Clock time parsed to seconds (supports fractional seconds)';
COMMENT ON COLUMN pbp_events.seconds_elapsed IS 'Total seconds elapsed from start of game';
COMMENT ON COLUMN pbp_events.shot_x IS 'Shot X coordinate (from Tranche 2 integration)';
COMMENT ON COLUMN pbp_events.shot_y IS 'Shot Y coordinate (from Tranche 2 integration)';

-- =============================================================================
-- 8) VALIDATION QUERIES
-- =============================================================================

-- Verify new table exists
SELECT 'lineup_stints created' AS status, to_regclass('public.lineup_stints') IS NOT NULL AS exists;

-- Check pbp_events columns
SELECT 'pbp_events enhanced' AS status,
       EXISTS(SELECT 1 FROM information_schema.columns 
              WHERE table_name='pbp_events' AND column_name='clock_seconds') AS has_clock_seconds,
       EXISTS(SELECT 1 FROM information_schema.columns 
              WHERE table_name='pbp_events' AND column_name='shot_x') AS has_shot_coords;

-- Check constraints
SELECT 'constraints added' AS status,
       COUNT(*) AS constraint_count
FROM pg_constraint 
WHERE conname IN ('fk_pbp_game', 'fk_ls_game', 'fk_shot_game', 'uq_pbp_game_event', 'uq_ls_unique');

\echo 'Migration completed successfully! Ready for clock parsing backfill.'