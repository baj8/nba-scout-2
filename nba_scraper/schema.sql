-- NBA Historical Scraping & Ingestion Engine Database Schema
-- PostgreSQL DDL with complete table definitions, indexes, and constraints

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Games table - core game information
CREATE TABLE games (
    game_id TEXT PRIMARY KEY,
    bref_game_id TEXT,
    season TEXT NOT NULL,
    game_date_utc TIMESTAMPTZ NOT NULL,
    game_date_local DATE NOT NULL,
    arena_tz TEXT NOT NULL, -- IANA timezone
    home_team_tricode TEXT NOT NULL,
    away_team_tricode TEXT NOT NULL,
    home_team_id TEXT,
    away_team_id TEXT,
    odds_join_key TEXT, -- computed from arena local date
    status TEXT NOT NULL DEFAULT 'SCHEDULED',
    period INTEGER DEFAULT 0,
    time_remaining TEXT,
    arena_name TEXT,
    attendance INTEGER,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW()
);

-- Game ID crosswalk for mapping between data sources
CREATE TABLE game_id_crosswalk (
    game_id TEXT NOT NULL,
    bref_game_id TEXT NOT NULL,
    nba_stats_game_id TEXT,
    espn_game_id TEXT,
    yahoo_game_id TEXT,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, bref_game_id)
);

-- Referee assignments
CREATE TABLE ref_assignments (
    game_id TEXT NOT NULL,
    referee_name_slug TEXT NOT NULL,
    referee_display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('CREW_CHIEF', 'REFEREE', 'UMPIRE', 'OFFICIAL')),
    crew_position INTEGER,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, referee_name_slug)
);

-- Referee alternates
CREATE TABLE ref_alternates (
    game_id TEXT NOT NULL,
    referee_name_slug TEXT NOT NULL,
    referee_display_name TEXT NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, referee_name_slug)
);

-- Starting lineups
CREATE TABLE starting_lineups (
    game_id TEXT NOT NULL,
    team_tricode TEXT NOT NULL,
    player_name_slug TEXT NOT NULL,
    player_display_name TEXT NOT NULL,
    player_id TEXT,
    position TEXT,
    jersey_number INTEGER,
    final_pre_tip BOOLEAN NOT NULL DEFAULT TRUE,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, team_tricode, player_name_slug)
);

-- Injury status tracking
CREATE TABLE injury_status (
    game_id TEXT NOT NULL,
    team_tricode TEXT NOT NULL,
    player_name_slug TEXT NOT NULL,
    player_display_name TEXT NOT NULL,
    player_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('OUT', 'QUESTIONABLE', 'PROBABLE', 'ACTIVE', 'DNP', 'INACTIVE')),
    reason TEXT,
    snapshot_utc TIMESTAMPTZ,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, team_tricode, player_name_slug, snapshot_utc)
);

-- Play-by-play events
CREATE TABLE pbp_events (
    game_id TEXT NOT NULL,
    period INTEGER NOT NULL,
    event_idx INTEGER NOT NULL,
    event_id TEXT,
    time_remaining TEXT,
    seconds_elapsed NUMERIC,
    score_home INTEGER,
    score_away INTEGER,
    event_type TEXT NOT NULL,
    event_subtype TEXT,
    description TEXT,
    team_tricode TEXT,
    player1_name_slug TEXT,
    player1_display_name TEXT,
    player1_id TEXT,
    player2_name_slug TEXT,
    player2_display_name TEXT,
    player2_id TEXT,
    player3_name_slug TEXT,
    player3_display_name TEXT,
    player3_id TEXT,
    -- Shot-specific fields
    shot_made BOOLEAN,
    shot_value INTEGER,
    shot_type TEXT,
    shot_zone TEXT,
    shot_distance_ft NUMERIC,
    shot_x NUMERIC,
    shot_y NUMERIC,
    -- Game situation
    shot_clock_seconds NUMERIC,
    possession_team TEXT,
    -- Enrichment flags
    is_transition BOOLEAN DEFAULT FALSE,
    is_early_clock BOOLEAN DEFAULT FALSE,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, period, event_idx)
);

-- Q1 window analysis (12:00 to 8:00)
CREATE TABLE q1_window_12_8 (
    game_id TEXT PRIMARY KEY,
    home_team_tricode TEXT NOT NULL,
    away_team_tricode TEXT NOT NULL,
    possessions_elapsed INTEGER NOT NULL,
    pace48_actual NUMERIC,
    pace48_expected NUMERIC,
    home_efg_actual NUMERIC,
    home_efg_expected NUMERIC,
    away_efg_actual NUMERIC,
    away_efg_expected NUMERIC,
    home_to_rate NUMERIC,
    away_to_rate NUMERIC,
    home_ft_rate NUMERIC,
    away_ft_rate NUMERIC,
    home_orb_pct NUMERIC,
    home_drb_pct NUMERIC,
    away_orb_pct NUMERIC,
    away_drb_pct NUMERIC,
    bonus_time_home_sec NUMERIC DEFAULT 0,
    bonus_time_away_sec NUMERIC DEFAULT 0,
    transition_rate NUMERIC,
    early_clock_rate NUMERIC,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW()
);

-- Early shock events detection
CREATE TABLE early_shocks (
    game_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('EARLY_FOUL_TROUBLE', 'TECHNICAL', 'FLAGRANT', 'INJURY_EXIT')),
    period INTEGER NOT NULL,
    time_remaining TEXT NOT NULL,
    seconds_elapsed NUMERIC NOT NULL,
    player_name_slug TEXT,
    player_display_name TEXT,
    team_tricode TEXT,
    severity TEXT, -- LOW, MEDIUM, HIGH
    immediate_sub BOOLEAN DEFAULT FALSE,
    description TEXT,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (game_id, event_type, period, seconds_elapsed, player_name_slug)
);

-- Schedule and travel analysis
CREATE TABLE schedule_travel (
    game_id TEXT PRIMARY KEY,
    team_tricode TEXT NOT NULL,
    is_back_to_back BOOLEAN NOT NULL DEFAULT FALSE,
    is_3_in_4 BOOLEAN NOT NULL DEFAULT FALSE,
    is_5_in_7 BOOLEAN NOT NULL DEFAULT FALSE,
    days_rest INTEGER NOT NULL,
    timezone_shift_hours NUMERIC DEFAULT 0,
    circadian_index NUMERIC, -- Composite metric
    altitude_change_m NUMERIC DEFAULT 0,
    travel_distance_km NUMERIC DEFAULT 0,
    prev_game_date DATE,
    prev_arena_tz TEXT,
    prev_lat NUMERIC,
    prev_lon NUMERIC,
    prev_altitude_m NUMERIC,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW()
);

-- Game outcomes summary
CREATE TABLE outcomes (
    game_id TEXT PRIMARY KEY,
    home_team_tricode TEXT NOT NULL,
    away_team_tricode TEXT NOT NULL,
    q1_home_points INTEGER,
    q1_away_points INTEGER,
    final_home_points INTEGER NOT NULL,
    final_away_points INTEGER NOT NULL,
    total_points INTEGER NOT NULL,
    home_win BOOLEAN NOT NULL,
    margin INTEGER NOT NULL,
    overtime_periods INTEGER DEFAULT 0,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW()
);

-- Pipeline state tracking for resumability
CREATE TABLE pipeline_state (
    pipeline_name TEXT NOT NULL,
    game_id TEXT,
    date_key DATE,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    metadata JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, game_id, date_key, step_name)
);

-- TRANCHE 1: NBA API ADVANCED METRICS TABLES

-- Advanced player statistics (per game)
CREATE TABLE advanced_player_stats (
    game_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team_id TEXT,
    team_abbreviation TEXT NOT NULL,
    
    -- Core efficiency metrics
    offensive_rating NUMERIC,
    defensive_rating NUMERIC,
    net_rating NUMERIC,
    
    -- Advanced percentages
    assist_percentage NUMERIC,
    assist_to_turnover NUMERIC,
    assist_ratio NUMERIC,
    offensive_rebound_pct NUMERIC,
    defensive_rebound_pct NUMERIC,
    rebound_pct NUMERIC,
    turnover_ratio NUMERIC,
    effective_fg_pct NUMERIC,
    true_shooting_pct NUMERIC,
    usage_pct NUMERIC,
    
    -- Pace and impact
    pace NUMERIC,
    pie NUMERIC, -- Player Impact Estimate
    
    -- Metadata
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- Miscellaneous player statistics (per game)
CREATE TABLE misc_player_stats (
    game_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team_id TEXT,
    team_abbreviation TEXT NOT NULL,
    
    -- Plus/minus and impact
    plus_minus NUMERIC,
    nba_fantasy_pts NUMERIC,
    
    -- Achievement tracking
    dd2 INTEGER, -- Double-doubles
    td3 INTEGER, -- Triple-doubles
    
    -- Ranking metrics
    fg_pct_rank INTEGER,
    ft_pct_rank INTEGER,
    fg3_pct_rank INTEGER,
    pts_rank INTEGER,
    reb_rank INTEGER,
    ast_rank INTEGER,
    
    -- Additional fantasy metrics
    wnba_fantasy_pts NUMERIC,
    
    -- Metadata
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- Usage player statistics (per game)
CREATE TABLE usage_player_stats (
    game_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team_id TEXT,
    team_abbreviation TEXT NOT NULL,
    
    -- Core usage metrics
    usage_pct NUMERIC,
    
    -- Percentage breakdowns
    pct_fgm NUMERIC,  -- % of team's FG made
    pct_fga NUMERIC,  -- % of team's FG attempted
    pct_fg3m NUMERIC, -- % of team's 3PT made
    pct_fg3a NUMERIC, -- % of team's 3PT attempted
    pct_ftm NUMERIC,  -- % of team's FT made
    pct_fta NUMERIC,  -- % of team's FT attempted
    pct_oreb NUMERIC, -- % of team's offensive rebounds
    pct_dreb NUMERIC, -- % of team's defensive rebounds
    pct_reb NUMERIC,  -- % of team's total rebounds
    pct_ast NUMERIC,  -- % of team's assists
    pct_tov NUMERIC,  -- % of team's turnovers
    pct_stl NUMERIC,  -- % of team's steals
    pct_blk NUMERIC,  -- % of team's blocks
    pct_blka NUMERIC, -- % of team's blocked attempts
    pct_pf NUMERIC,   -- % of team's personal fouls
    pct_pfd NUMERIC,  -- % of team's fouls drawn
    pct_pts NUMERIC,  -- % of team's points
    
    -- Metadata
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- Advanced team statistics (per game)
CREATE TABLE advanced_team_stats (
    game_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    team_abbreviation TEXT NOT NULL,
    team_name TEXT,
    
    -- Core efficiency metrics
    offensive_rating NUMERIC,
    defensive_rating NUMERIC,
    net_rating NUMERIC,
    
    -- Advanced percentages
    assist_percentage NUMERIC,
    assist_to_turnover NUMERIC,
    assist_ratio NUMERIC,
    offensive_rebound_pct NUMERIC,
    defensive_rebound_pct NUMERIC,
    rebound_pct NUMERIC,
    turnover_ratio NUMERIC,
    effective_fg_pct NUMERIC,
    true_shooting_pct NUMERIC,
    
    -- Pace and impact
    pace NUMERIC,
    pie NUMERIC, -- Team aggregate Player Impact Estimate
    
    -- Metadata
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- Team game statistics - comprehensive per-game team metrics
CREATE TABLE team_game_stats (
    game_id TEXT NOT NULL,
    team_id TEXT,
    team_tricode TEXT NOT NULL,
    
    -- Basic box score stats
    points INTEGER DEFAULT 0,
    fgm INTEGER DEFAULT 0,
    fga INTEGER DEFAULT 0,
    fg_pct NUMERIC,
    fg3m INTEGER DEFAULT 0,
    fg3a INTEGER DEFAULT 0,
    fg3_pct NUMERIC,
    ftm INTEGER DEFAULT 0,
    fta INTEGER DEFAULT 0,
    ft_pct NUMERIC,
    oreb INTEGER DEFAULT 0,
    dreb INTEGER DEFAULT 0,
    reb INTEGER DEFAULT 0,
    ast INTEGER DEFAULT 0,
    stl INTEGER DEFAULT 0,
    blk INTEGER DEFAULT 0,
    tov INTEGER DEFAULT 0,
    pf INTEGER DEFAULT 0,
    
    -- Possession and pace metrics
    possessions_estimated NUMERIC,
    pace NUMERIC,
    
    -- Advanced efficiency metrics
    offensive_rating NUMERIC,
    defensive_rating NUMERIC,
    net_rating NUMERIC,
    effective_fg_pct NUMERIC,
    true_shooting_pct NUMERIC,
    
    -- Four factors
    efg_pct NUMERIC,
    tov_rate NUMERIC,
    orb_pct NUMERIC,
    ft_rate NUMERIC, -- FT attempts per FG attempt
    
    -- League-relative z-scores
    pace_z_score NUMERIC,
    offensive_efficiency_z NUMERIC,
    defensive_efficiency_z NUMERIC,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (game_id, team_tricode),
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- Player game statistics - basic box score stats per game
CREATE TABLE player_game_stats (
    game_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    player_name TEXT NOT NULL,
    team_id TEXT,
    team_abbreviation TEXT NOT NULL,
    
    -- Basic box score statistics
    minutes_played TEXT, -- Format: "MM:SS"
    points INTEGER DEFAULT 0,
    field_goals_made INTEGER DEFAULT 0,
    field_goals_attempted INTEGER DEFAULT 0,
    field_goal_percentage NUMERIC,
    three_pointers_made INTEGER DEFAULT 0,
    three_pointers_attempted INTEGER DEFAULT 0,
    three_point_percentage NUMERIC,
    free_throws_made INTEGER DEFAULT 0,
    free_throws_attempted INTEGER DEFAULT 0,
    free_throw_percentage NUMERIC,
    offensive_rebounds INTEGER DEFAULT 0,
    defensive_rebounds INTEGER DEFAULT 0,
    total_rebounds INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    steals INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    turnovers INTEGER DEFAULT 0,
    personal_fouls INTEGER DEFAULT 0,
    
    -- Additional metrics
    plus_minus INTEGER,
    starter BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    ingested_at_utc TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE
);

-- PERFORMANCE OPTIMIZED INDEXES
-- Basic single-column indexes for common queries
CREATE INDEX idx_games_date_local ON games (game_date_local);
CREATE INDEX idx_games_date_utc ON games (game_date_utc);
CREATE INDEX idx_games_season ON games (season);
CREATE INDEX idx_games_teams ON games (home_team_tricode, away_team_tricode);
CREATE INDEX idx_games_status ON games (status);

-- Composite indexes for common analytical queries
CREATE INDEX idx_games_season_date_teams ON games (season, game_date_local, home_team_tricode, away_team_tricode);
CREATE INDEX idx_games_date_status ON games (game_date_local, status);
CREATE INDEX idx_games_season_teams ON games (season, home_team_tricode) INCLUDE (away_team_tricode, game_date_local);

-- Crosswalk optimization
CREATE INDEX idx_crosswalk_bref_id ON game_id_crosswalk (bref_game_id);
CREATE INDEX idx_crosswalk_nba_stats_id ON game_id_crosswalk (nba_stats_game_id);
CREATE INDEX idx_crosswalk_game_bref_composite ON game_id_crosswalk (game_id, bref_game_id);

-- Referee analysis indexes
CREATE INDEX idx_ref_assignments_referee ON ref_assignments (referee_name_slug);
CREATE INDEX idx_ref_assignments_role ON ref_assignments (role);
CREATE INDEX idx_ref_assignments_referee_role ON ref_assignments (referee_name_slug, role);
CREATE INDEX idx_ref_alternates_referee ON ref_alternates (referee_name_slug);

-- Lineup analysis indexes
CREATE INDEX idx_lineups_team ON starting_lineups (team_tricode);
CREATE INDEX idx_lineups_player ON starting_lineups (player_name_slug);
CREATE INDEX idx_lineups_team_player ON starting_lineups (team_tricode, player_name_slug);
CREATE INDEX idx_lineups_game_team ON starting_lineups (game_id, team_tricode);

-- Injury tracking indexes
CREATE INDEX idx_injury_status_team ON injury_status (team_tricode);
CREATE INDEX idx_injury_status_player ON injury_status (player_name_slug);
CREATE INDEX idx_injury_status_snapshot ON injury_status (snapshot_utc);
CREATE INDEX idx_injury_status_team_status ON injury_status (team_tricode, status);
CREATE INDEX idx_injury_status_game_team ON injury_status (game_id, team_tricode);

-- PBP PERFORMANCE INDEXES - Critical for analytics
-- Core PBP indexes
CREATE INDEX idx_pbp_period_time ON pbp_events (game_id, period, seconds_elapsed);
CREATE INDEX idx_pbp_event_type ON pbp_events (event_type);
CREATE INDEX idx_pbp_team ON pbp_events (team_tricode);
CREATE INDEX idx_pbp_players ON pbp_events (player1_name_slug, player2_name_slug);

-- Advanced PBP composite indexes for common analytical queries
CREATE INDEX idx_pbp_game_period_event ON pbp_events (game_id, period, event_type);
CREATE INDEX idx_pbp_team_event_type ON pbp_events (team_tricode, event_type) INCLUDE (game_id, period);
CREATE INDEX idx_pbp_shot_analysis ON pbp_events (game_id, shot_made, shot_value) WHERE shot_made IS NOT NULL;
CREATE INDEX idx_pbp_transition_analysis ON pbp_events (game_id, is_transition) WHERE is_transition = true;
CREATE INDEX idx_pbp_early_clock_analysis ON pbp_events (game_id, is_early_clock) WHERE is_early_clock = true;

-- Player-focused PBP indexes
CREATE INDEX idx_pbp_player1_events ON pbp_events (player1_name_slug, event_type) INCLUDE (game_id, team_tricode);
CREATE INDEX idx_pbp_player_shots ON pbp_events (player1_name_slug, shot_made, shot_value) WHERE shot_made IS NOT NULL;

-- Time-based PBP indexes
CREATE INDEX idx_pbp_time_sequencing ON pbp_events (game_id, period, event_idx);
CREATE INDEX idx_pbp_seconds_elapsed ON pbp_events (seconds_elapsed, event_type);

-- Q1 Window Analytics indexes
CREATE INDEX idx_q1_window_teams ON q1_window_12_8 (home_team_tricode, away_team_tricode);
CREATE INDEX idx_q1_window_pace ON q1_window_12_8 (pace48_actual, pace48_expected);
CREATE INDEX idx_q1_window_efficiency ON q1_window_12_8 (home_efg_actual, away_efg_actual);

-- Early Shocks analysis indexes
CREATE INDEX idx_early_shocks_type ON early_shocks (event_type);
CREATE INDEX idx_early_shocks_severity ON early_shocks (severity);
CREATE INDEX idx_early_shocks_player ON early_shocks (player_name_slug);
CREATE INDEX idx_early_shocks_team ON early_shocks (team_tricode);
CREATE INDEX idx_early_shocks_game_type ON early_shocks (game_id, event_type);
CREATE INDEX idx_early_shocks_team_severity ON early_shocks (team_tricode, severity);

-- Schedule/Travel optimization indexes
CREATE INDEX idx_schedule_travel_team ON schedule_travel (team_tricode);
CREATE INDEX idx_schedule_travel_b2b ON schedule_travel (is_back_to_back);
CREATE INDEX idx_schedule_travel_rest_patterns ON schedule_travel (team_tricode, is_back_to_back, is_3_in_4);
CREATE INDEX idx_schedule_travel_circadian ON schedule_travel (team_tricode, circadian_index) WHERE circadian_index IS NOT NULL;
CREATE INDEX idx_schedule_travel_distance ON schedule_travel (travel_distance_km) WHERE travel_distance_km > 0;

-- Outcomes analysis indexes
CREATE INDEX idx_outcomes_teams ON outcomes (home_team_tricode, away_team_tricode);
CREATE INDEX idx_outcomes_margin ON outcomes (margin);
CREATE INDEX idx_outcomes_total_points ON outcomes (total_points);
CREATE INDEX idx_outcomes_q1_performance ON outcomes (q1_home_points, q1_away_points);
CREATE INDEX idx_outcomes_overtime ON outcomes (overtime_periods) WHERE overtime_periods > 0;

-- Pipeline state tracking indexes
CREATE INDEX idx_pipeline_state_status ON pipeline_state (status);
CREATE INDEX idx_pipeline_state_date ON pipeline_state (date_key);
CREATE INDEX idx_pipeline_state_pipeline_status ON pipeline_state (pipeline_name, status);
CREATE INDEX idx_pipeline_state_updated ON pipeline_state (updated_at);

-- Advanced Player Stats indexes
CREATE INDEX idx_advanced_player_stats_player ON advanced_player_stats (player_id);
CREATE INDEX idx_advanced_player_stats_team ON advanced_player_stats (team_abbreviation);
CREATE INDEX idx_advanced_player_stats_game_team ON advanced_player_stats (game_id, team_abbreviation);
CREATE INDEX idx_advanced_player_stats_efficiency ON advanced_player_stats (offensive_rating, defensive_rating);
CREATE INDEX idx_advanced_player_stats_usage ON advanced_player_stats (usage_pct) WHERE usage_pct IS NOT NULL;
CREATE INDEX idx_advanced_player_stats_impact ON advanced_player_stats (pie) WHERE pie IS NOT NULL;

-- Misc Player Stats indexes
CREATE INDEX idx_misc_player_stats_player ON misc_player_stats (player_id);
CREATE INDEX idx_misc_player_stats_team ON misc_player_stats (team_abbreviation);
CREATE INDEX idx_misc_player_stats_plus_minus ON misc_player_stats (plus_minus) WHERE plus_minus IS NOT NULL;
CREATE INDEX idx_misc_player_stats_achievements ON misc_player_stats (dd2, td3) WHERE dd2 > 0 OR td3 > 0;

-- Usage Player Stats indexes
CREATE INDEX idx_usage_player_stats_player ON usage_player_stats (player_id);
CREATE INDEX idx_usage_player_stats_team ON usage_player_stats (team_abbreviation);
CREATE INDEX idx_usage_player_stats_usage_pct ON usage_player_stats (usage_pct) WHERE usage_pct IS NOT NULL;
CREATE INDEX idx_usage_player_stats_involvement ON usage_player_stats (pct_pts, pct_reb, pct_ast);

-- Advanced Team Stats indexes
CREATE INDEX idx_advanced_team_stats_team ON advanced_team_stats (team_abbreviation);
CREATE INDEX idx_advanced_team_stats_efficiency ON advanced_team_stats (offensive_rating, defensive_rating);
CREATE INDEX idx_advanced_team_stats_pace ON advanced_team_stats (pace) WHERE pace IS NOT NULL;

-- Covering indexes for common queries
CREATE INDEX idx_advanced_player_stats_covering ON advanced_player_stats (game_id, player_id) 
    INCLUDE (offensive_rating, defensive_rating, usage_pct, plus_minus);
CREATE INDEX idx_advanced_team_stats_covering ON advanced_team_stats (game_id, team_id) 
    INCLUDE (offensive_rating, defensive_rating, net_rating, pace);

-- PARTIAL INDEXES for specific use cases
CREATE INDEX idx_games_completed ON games (game_date_local, season) WHERE status = 'COMPLETED';
CREATE INDEX idx_games_in_progress ON games (game_date_utc) WHERE status IN ('IN_PROGRESS', 'LIVE');
CREATE INDEX idx_pbp_scoring_events ON pbp_events (game_id, period, seconds_elapsed) WHERE event_type IN ('SHOT_MADE', 'FREE_THROW_MADE');
CREATE INDEX idx_pbp_turnovers ON pbp_events (game_id, team_tricode, period) WHERE event_type = 'TURNOVER';
CREATE INDEX idx_injury_out_players ON injury_status (game_id, team_tricode, player_name_slug) WHERE status = 'OUT';

-- EXPRESSION INDEXES for computed queries
CREATE INDEX idx_games_team_participation ON games ((
    CASE 
        WHEN COALESCE(home_team_tricode::text, '') = '' OR COALESCE(away_team_tricode::text, '') = '' THEN 
            COALESCE(NULLIF(home_team_tricode::text, ''), 'UNK') || '_' || COALESCE(NULLIF(away_team_tricode::text, ''), 'UNK')
        WHEN COALESCE(home_team_tricode::text, '') < COALESCE(away_team_tricode::text, '') THEN 
            COALESCE(home_team_tricode::text, '') || '_' || COALESCE(away_team_tricode::text, '') 
        ELSE 
            COALESCE(away_team_tricode::text, '') || COALESCE(home_team_tricode::text, '') 
    END
));

CREATE INDEX idx_pbp_shot_efficiency ON pbp_events ((CASE WHEN shot_made THEN 1.0 ELSE 0.0 END)) WHERE shot_made IS NOT NULL;

-- GIN indexes for text search and JSONB
CREATE INDEX idx_pbp_description_search ON pbp_events USING GIN (to_tsvector('english', description));
CREATE INDEX idx_pipeline_metadata_gin ON pipeline_state USING GIN (metadata);

-- COVERING INDEXES (PostgreSQL INCLUDE syntax) for read-heavy queries
CREATE INDEX idx_games_lookup_covering ON games (game_id) INCLUDE (home_team_tricode, away_team_tricode, game_date_local, status);
CREATE INDEX idx_pbp_game_covering ON pbp_events (game_id) INCLUDE (period, event_idx, event_type, team_tricode);

-- Foreign key constraints
ALTER TABLE game_id_crosswalk ADD CONSTRAINT fk_crosswalk_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE ref_assignments ADD CONSTRAINT fk_ref_assignments_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE ref_alternates ADD CONSTRAINT fk_ref_alternates_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE starting_lineups ADD CONSTRAINT fk_lineups_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE injury_status ADD CONSTRAINT fk_injury_status_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE pbp_events ADD CONSTRAINT fk_pbp_events_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE q1_window_12_8 ADD CONSTRAINT fk_q1_window_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE early_shocks ADD CONSTRAINT fk_early_shocks_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE schedule_travel ADD CONSTRAINT fk_schedule_travel_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

ALTER TABLE outcomes ADD CONSTRAINT fk_outcomes_game_id 
    FOREIGN KEY (game_id) REFERENCES games (game_id) ON DELETE CASCADE;

-- Comments for advanced metrics tables
COMMENT ON TABLE advanced_player_stats IS 'Advanced player efficiency metrics from NBA Stats API per game';
COMMENT ON TABLE misc_player_stats IS 'Miscellaneous player statistics including plus/minus and achievements';
COMMENT ON TABLE usage_player_stats IS 'Player usage rates and team involvement percentages';
COMMENT ON TABLE advanced_team_stats IS 'Advanced team efficiency and pace metrics per game';

COMMENT ON COLUMN advanced_player_stats.pie IS 'Player Impact Estimate - NBA proprietary impact metric';
COMMENT ON COLUMN advanced_player_stats.usage_pct IS 'Percentage of team plays used by player while on court';
COMMENT ON COLUMN misc_player_stats.plus_minus IS 'Point differential while player was on court';
COMMENT ON COLUMN usage_player_stats.pct_pts IS 'Percentage of team total points scored by player';

-- Table and column comments for documentation
COMMENT ON TABLE games IS 'Core game information with scheduling and venue details';
COMMENT ON TABLE game_id_crosswalk IS 'Mapping between game IDs across different data sources';
COMMENT ON TABLE ref_assignments IS 'Official referee crew assignments per game';
COMMENT ON TABLE ref_alternates IS 'Alternate referees available for each game';
COMMENT ON TABLE starting_lineups IS 'Starting five players for each team per game';
COMMENT ON TABLE injury_status IS 'Player injury/availability status with snapshot tracking';
COMMENT ON TABLE pbp_events IS 'Normalized play-by-play events with enrichment fields';
COMMENT ON TABLE q1_window_12_8 IS 'First quarter 12:00-8:00 window analytics';
COMMENT ON TABLE early_shocks IS 'Early game disruption events (fouls, technicals, injuries)';
COMMENT ON TABLE schedule_travel IS 'Team schedule difficulty and travel impact metrics';
COMMENT ON TABLE outcomes IS 'Final game results and quarter scores';
COMMENT ON TABLE pipeline_state IS 'Pipeline execution state for resumability';

COMMENT ON COLUMN games.odds_join_key IS 'Computed key based on arena local calendar date for joining with odds data';
COMMENT ON COLUMN injury_status.snapshot_utc IS 'When status was captured; NULL for game-time status';
COMMENT ON COLUMN pbp_events.is_transition IS 'Event occurred in transition (first 6 seconds of possession)';
COMMENT ON COLUMN pbp_events.is_early_clock IS 'Event occurred in early shot clock (first 8 seconds)';
COMMENT ON COLUMN schedule_travel.circadian_index IS 'Composite metric of travel fatigue and circadian disruption';