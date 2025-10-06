-- Roll back a single game (delete children first, then parent)
-- Usage:
--   psql "$DATABASE_URL" -f sql/rollback_game.sql -v game_id='0022300001'

BEGIN;

-- Children
DELETE FROM pbp_events     WHERE game_id = :'game_id';
DELETE FROM lineup_stints  WHERE game_id = :'game_id';
DELETE FROM shots          WHERE game_id = :'game_id';
DELETE FROM adv_metrics    WHERE game_id = :'game_id';

-- Parent
DELETE FROM games          WHERE game_id = :'game_id';

COMMIT;