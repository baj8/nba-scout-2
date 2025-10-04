-- Database migrations for NBA Stats foundation tables with deferrable FK constraints
-- This ensures transactions can load parent and child records in any order

-- Create foundation tables if they don't exist
CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    season TEXT NOT NULL,
    game_date DATE NOT NULL,
    home_team_id INT NOT NULL,
    away_team_id INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Final',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- PBP events table with enhanced clock support
CREATE TABLE IF NOT EXISTS pbp_events (
    game_id TEXT NOT NULL,
    event_num INT NOT NULL,
    period INT NOT NULL,
    clock TEXT NOT NULL,
    clock_seconds DOUBLE PRECISION,
    seconds_elapsed DOUBLE PRECISION,
    team_id INT,
    player1_id INT,
    action_type INT,
    action_subtype INT,
    description TEXT,
    
    -- Shot coordinate fields for Tranche 2
    shot_x NUMERIC,
    shot_y NUMERIC,
    shot_distance_ft NUMERIC,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, event_num)
);

-- Lineup stints with array-based player tracking
CREATE TABLE IF NOT EXISTS lineup_stints (
    game_id TEXT NOT NULL,
    team_id INT NOT NULL,
    period INT NOT NULL,
    lineup_player_ids INT[] NOT NULL,
    seconds_played INT NOT NULL,
    lineup_hash TEXT GENERATED ALWAYS AS (md5(array_to_string(lineup_player_ids, ','))) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, team_id, period, lineup_hash)
);

-- Shot events table for coordinate data
CREATE TABLE IF NOT EXISTS shot_events (
    game_id TEXT NOT NULL,
    player_id INT NOT NULL,
    team_id INT,
    period INT NOT NULL,
    shot_made_flag INT NOT NULL,
    loc_x INT NOT NULL,
    loc_y INT NOT NULL,
    event_num INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, player_id, period, loc_x, loc_y)
);

-- Add deferrable foreign key constraints
-- These allow loading child records before parent records within a transaction
ALTER TABLE pbp_events 
DROP CONSTRAINT IF EXISTS fk_pbp_game,
ADD CONSTRAINT fk_pbp_game 
    FOREIGN KEY (game_id) REFERENCES games(game_id) 
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE lineup_stints 
DROP CONSTRAINT IF EXISTS fk_lineup_game,
ADD CONSTRAINT fk_lineup_game 
    FOREIGN KEY (game_id) REFERENCES games(game_id) 
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE shot_events 
DROP CONSTRAINT IF EXISTS fk_shot_game,
ADD CONSTRAINT fk_shot_game 
    FOREIGN KEY (game_id) REFERENCES games(game_id) 
    DEFERRABLE INITIALLY DEFERRED;

-- Ensure advanced metrics tables also have deferrable FKs if they exist
DO $$
BEGIN
    -- Advanced player stats
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'advanced_player_stats') THEN
        ALTER TABLE advanced_player_stats 
        DROP CONSTRAINT IF EXISTS fk_advanced_player_game,
        ADD CONSTRAINT fk_advanced_player_game 
            FOREIGN KEY (game_id) REFERENCES games(game_id) 
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
    
    -- Advanced team stats
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'advanced_team_stats') THEN
        ALTER TABLE advanced_team_stats 
        DROP CONSTRAINT IF EXISTS fk_advanced_team_game,
        ADD CONSTRAINT fk_advanced_team_game 
            FOREIGN KEY (game_id) REFERENCES games(game_id) 
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
    
    -- Other metrics tables
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'misc_player_stats') THEN
        ALTER TABLE misc_player_stats 
        DROP CONSTRAINT IF EXISTS fk_misc_player_game,
        ADD CONSTRAINT fk_misc_player_game 
            FOREIGN KEY (game_id) REFERENCES games(game_id) 
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'usage_player_stats') THEN
        ALTER TABLE usage_player_stats 
        DROP CONSTRAINT IF EXISTS fk_usage_player_game,
        ADD CONSTRAINT fk_usage_player_game 
            FOREIGN KEY (game_id) REFERENCES games(game_id) 
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_pbp_game_period ON pbp_events (game_id, period);
CREATE INDEX IF NOT EXISTS idx_pbp_clock_seconds ON pbp_events (clock_seconds) WHERE clock_seconds IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pbp_seconds_elapsed ON pbp_events (seconds_elapsed) WHERE seconds_elapsed IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pbp_shot_coords ON pbp_events (shot_x, shot_y) WHERE shot_x IS NOT NULL AND shot_y IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lineup_team_period ON lineup_stints (team_id, period);
CREATE INDEX IF NOT EXISTS idx_shot_events_player ON shot_events (player_id);
CREATE INDEX IF NOT EXISTS idx_shot_events_coords ON shot_events (loc_x, loc_y);

-- Add comments for documentation
COMMENT ON TABLE pbp_events IS 'Play-by-play events with enhanced clock parsing and shot coordinate support';
COMMENT ON COLUMN pbp_events.clock_seconds IS 'Clock time parsed to seconds (supports fractional seconds)';
COMMENT ON COLUMN pbp_events.seconds_elapsed IS 'Total seconds elapsed from start of game';
COMMENT ON COLUMN pbp_events.shot_x IS 'Shot X coordinate (from Tranche 2 integration)';
COMMENT ON COLUMN pbp_events.shot_y IS 'Shot Y coordinate (from Tranche 2 integration)';

COMMENT ON TABLE lineup_stints IS 'Lineup stints with array-based player tracking';
COMMENT ON COLUMN lineup_stints.lineup_player_ids IS 'Array of 5 player IDs in the lineup';
COMMENT ON COLUMN lineup_stints.lineup_hash IS 'MD5 hash of lineup for uniqueness';

COMMENT ON TABLE shot_events IS 'Shot chart coordinate data for Tranche 2 analytics';
COMMENT ON COLUMN shot_events.event_num IS 'Links to pbp_events.event_num when available';