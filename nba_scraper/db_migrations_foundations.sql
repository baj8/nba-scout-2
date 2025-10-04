-- Foundation tables for Tranche 0 and Tranche 2 integration
-- All foreign keys are DEFERRABLE INITIALLY DEFERRED for transaction safety

-- Parent table: games
CREATE TABLE IF NOT EXISTS games (
  game_id TEXT PRIMARY KEY,
  season TEXT NOT NULL,
  game_date DATE NOT NULL,
  home_team_id INT NOT NULL,
  away_team_id INT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- PBP events with shot coordinate columns for Tranche 2 and clock_seconds
CREATE TABLE IF NOT EXISTS pbp_events (
  game_id TEXT NOT NULL,
  event_num INT NOT NULL,
  period INT NOT NULL,
  clock TEXT NOT NULL,
  team_id INT,
  player1_id INT,
  action_type INT,
  action_subtype INT,
  description TEXT,
  clock_seconds DOUBLE PRECISION,
  -- Tranche 2 shot coordinate columns
  loc_x INT,
  loc_y INT,
  shot_distance INT,
  shot_zone TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (game_id, event_num),
  CONSTRAINT fk_pbp_game FOREIGN KEY (game_id)
    REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED
);

-- Lineup stints - use array as primary key component
CREATE TABLE IF NOT EXISTS lineup_stints (
  game_id TEXT NOT NULL,
  team_id INT NOT NULL,
  period INT NOT NULL,
  lineup_player_ids INT[] NOT NULL,
  seconds_played INT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (game_id, team_id, period, lineup_player_ids),
  CONSTRAINT fk_ls_game FOREIGN KEY (game_id)
    REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED
);

-- Optional: separate shots table if not enriching PBP directly
CREATE TABLE IF NOT EXISTS shot_events (
  game_id TEXT NOT NULL,
  player_id INT NOT NULL,
  team_id INT,
  period INT NOT NULL,
  shot_made_flag INT NOT NULL,
  loc_x INT NOT NULL,
  loc_y INT NOT NULL,
  event_num INT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  PRIMARY KEY (game_id, player_id, period, loc_x, loc_y),
  CONSTRAINT fk_shots_game FOREIGN KEY (game_id)
    REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED
);

-- Update existing advanced metrics tables to have deferrable FKs
-- (This assumes they exist from Tranche 1)
DO $$
BEGIN
  -- Drop existing FK constraints if they exist
  IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
             WHERE constraint_name = 'advanced_player_stats_game_id_fkey') THEN
    ALTER TABLE advanced_player_stats DROP CONSTRAINT advanced_player_stats_game_id_fkey;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
             WHERE constraint_name = 'advanced_team_stats_game_id_fkey') THEN
    ALTER TABLE advanced_team_stats DROP CONSTRAINT advanced_team_stats_game_id_fkey;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
             WHERE constraint_name = 'misc_player_stats_game_id_fkey') THEN
    ALTER TABLE misc_player_stats DROP CONSTRAINT misc_player_stats_game_id_fkey;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.table_constraints 
             WHERE constraint_name = 'usage_player_stats_game_id_fkey') THEN
    ALTER TABLE usage_player_stats DROP CONSTRAINT usage_player_stats_game_id_fkey;
  END IF;
  
  -- Add deferrable FK constraints
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'advanced_player_stats') THEN
    ALTER TABLE advanced_player_stats 
    ADD CONSTRAINT advanced_player_stats_game_id_fkey 
    FOREIGN KEY (game_id) REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'advanced_team_stats') THEN
    ALTER TABLE advanced_team_stats 
    ADD CONSTRAINT advanced_team_stats_game_id_fkey 
    FOREIGN KEY (game_id) REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'misc_player_stats') THEN
    ALTER TABLE misc_player_stats 
    ADD CONSTRAINT misc_player_stats_game_id_fkey 
    FOREIGN KEY (game_id) REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'usage_player_stats') THEN
    ALTER TABLE usage_player_stats 
    ADD CONSTRAINT usage_player_stats_game_id_fkey 
    FOREIGN KEY (game_id) REFERENCES games(game_id) DEFERRABLE INITIALLY DEFERRED;
  END IF;
END
$$;

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_pbp_events_action_type ON pbp_events(action_type);
CREATE INDEX IF NOT EXISTS idx_pbp_events_period ON pbp_events(period);
CREATE INDEX IF NOT EXISTS idx_pbp_events_team_id ON pbp_events(team_id);
CREATE INDEX IF NOT EXISTS idx_pbp_events_coordinates ON pbp_events(loc_x, loc_y) WHERE loc_x IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pbp_events_clock_seconds ON pbp_events(clock_seconds) WHERE clock_seconds IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lineup_stints_team_period ON lineup_stints(team_id, period);
CREATE INDEX IF NOT EXISTS idx_shot_events_player ON shot_events(player_id);
CREATE INDEX IF NOT EXISTS idx_shot_events_coordinates ON shot_events(loc_x, loc_y);