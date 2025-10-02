"""Game data loader with idempotent upserts."""

from datetime import datetime
from typing import List

import asyncpg

from ..models import GameRow, GameIdCrosswalkRow, OutcomesRow
from ..db import get_connection
from ..logging import get_logger

logger = get_logger(__name__)


class GameLoader:
    """Loader for game-related data with diff-aware upserts."""
    
    async def upsert_games(self, games: List[GameRow]) -> int:
        """Upsert games with diff-aware updates.
        
        Args:
            games: List of GameRow instances to upsert
            
        Returns:
            Number of rows actually updated (not just touched)
        """
        if not games:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with conn.transaction():
                for game in games:
                    query = """
                    INSERT INTO games (
                        game_id, bref_game_id, season, game_date_utc, game_date_local,
                        arena_tz, home_team_tricode, away_team_tricode, home_team_id,
                        away_team_id, odds_join_key, status, period, time_remaining,
                        arena_name, attendance, source, source_url, ingested_at_utc
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19
                    )
                    ON CONFLICT (game_id) DO UPDATE SET
                        bref_game_id = CASE 
                            WHEN excluded.bref_game_id IS DISTINCT FROM games.bref_game_id 
                            THEN excluded.bref_game_id ELSE games.bref_game_id END,
                        season = CASE 
                            WHEN excluded.season IS DISTINCT FROM games.season 
                            THEN excluded.season ELSE games.season END,
                        game_date_utc = CASE 
                            WHEN excluded.game_date_utc IS DISTINCT FROM games.game_date_utc 
                            THEN excluded.game_date_utc ELSE games.game_date_utc END,
                        game_date_local = CASE 
                            WHEN excluded.game_date_local IS DISTINCT FROM games.game_date_local 
                            THEN excluded.game_date_local ELSE games.game_date_local END,
                        arena_tz = CASE 
                            WHEN excluded.arena_tz IS DISTINCT FROM games.arena_tz 
                            THEN excluded.arena_tz ELSE games.arena_tz END,
                        home_team_tricode = CASE 
                            WHEN excluded.home_team_tricode IS DISTINCT FROM games.home_team_tricode 
                            THEN excluded.home_team_tricode ELSE games.home_team_tricode END,
                        away_team_tricode = CASE 
                            WHEN excluded.away_team_tricode IS DISTINCT FROM games.away_team_tricode 
                            THEN excluded.away_team_tricode ELSE games.away_team_tricode END,
                        home_team_id = CASE 
                            WHEN excluded.home_team_id IS DISTINCT FROM games.home_team_id 
                            THEN excluded.home_team_id ELSE games.home_team_id END,
                        away_team_id = CASE 
                            WHEN excluded.away_team_id IS DISTINCT FROM games.away_team_id 
                            THEN excluded.away_team_id ELSE games.away_team_id END,
                        odds_join_key = CASE 
                            WHEN excluded.odds_join_key IS DISTINCT FROM games.odds_join_key 
                            THEN excluded.odds_join_key ELSE games.odds_join_key END,
                        status = CASE 
                            WHEN excluded.status IS DISTINCT FROM games.status 
                            THEN excluded.status ELSE games.status END,
                        period = CASE 
                            WHEN excluded.period IS DISTINCT FROM games.period 
                            THEN excluded.period ELSE games.period END,
                        time_remaining = CASE 
                            WHEN excluded.time_remaining IS DISTINCT FROM games.time_remaining 
                            THEN excluded.time_remaining ELSE games.time_remaining END,
                        arena_name = CASE 
                            WHEN excluded.arena_name IS DISTINCT FROM games.arena_name 
                            THEN excluded.arena_name ELSE games.arena_name END,
                        attendance = CASE 
                            WHEN excluded.attendance IS DISTINCT FROM games.attendance 
                            THEN excluded.attendance ELSE games.attendance END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.bref_game_id IS DISTINCT FROM games.bref_game_id OR
                        excluded.season IS DISTINCT FROM games.season OR
                        excluded.game_date_utc IS DISTINCT FROM games.game_date_utc OR
                        excluded.game_date_local IS DISTINCT FROM games.game_date_local OR
                        excluded.arena_tz IS DISTINCT FROM games.arena_tz OR
                        excluded.home_team_tricode IS DISTINCT FROM games.home_team_tricode OR
                        excluded.away_team_tricode IS DISTINCT FROM games.away_team_tricode OR
                        excluded.home_team_id IS DISTINCT FROM games.home_team_id OR
                        excluded.away_team_id IS DISTINCT FROM games.away_team_id OR
                        excluded.odds_join_key IS DISTINCT FROM games.odds_join_key OR
                        excluded.status IS DISTINCT FROM games.status OR
                        excluded.period IS DISTINCT FROM games.period OR
                        excluded.time_remaining IS DISTINCT FROM games.time_remaining OR
                        excluded.arena_name IS DISTINCT FROM games.arena_name OR
                        excluded.attendance IS DISTINCT FROM games.attendance
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        game.game_id,
                        game.bref_game_id,
                        game.season,
                        game.game_date_utc,
                        game.game_date_local,
                        game.arena_tz,
                        game.home_team_tricode,
                        game.away_team_tricode,
                        game.home_team_id,
                        game.away_team_id,
                        game.odds_join_key,
                        game.status.value,
                        game.period,
                        game.time_remaining,
                        game.arena_name,
                        game.attendance,
                        game.source,
                        game.source_url,
                        datetime.utcnow(),
                    )
                    
                    # Check if row was actually updated
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted games", total=len(games), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert games", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_crosswalk(self, crosswalks: List[GameIdCrosswalkRow]) -> int:
        """Upsert game ID crosswalk entries.
        
        Args:
            crosswalks: List of GameIdCrosswalkRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not crosswalks:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with conn.transaction():
                for crosswalk in crosswalks:
                    query = """
                    INSERT INTO game_id_crosswalk (
                        game_id, bref_game_id, nba_stats_game_id, espn_game_id,
                        yahoo_game_id, source, source_url, ingested_at_utc
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (game_id, bref_game_id) DO UPDATE SET
                        nba_stats_game_id = CASE 
                            WHEN excluded.nba_stats_game_id IS DISTINCT FROM game_id_crosswalk.nba_stats_game_id 
                            THEN excluded.nba_stats_game_id ELSE game_id_crosswalk.nba_stats_game_id END,
                        espn_game_id = CASE 
                            WHEN excluded.espn_game_id IS DISTINCT FROM game_id_crosswalk.espn_game_id 
                            THEN excluded.espn_game_id ELSE game_id_crosswalk.espn_game_id END,
                        yahoo_game_id = CASE 
                            WHEN excluded.yahoo_game_id IS DISTINCT FROM game_id_crosswalk.yahoo_game_id 
                            THEN excluded.yahoo_game_id ELSE game_id_crosswalk.yahoo_game_id END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.nba_stats_game_id IS DISTINCT FROM game_id_crosswalk.nba_stats_game_id OR
                        excluded.espn_game_id IS DISTINCT FROM game_id_crosswalk.espn_game_id OR
                        excluded.yahoo_game_id IS DISTINCT FROM game_id_crosswalk.yahoo_game_id
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        crosswalk.game_id,
                        crosswalk.bref_game_id,
                        crosswalk.nba_stats_game_id,
                        crosswalk.espn_game_id,
                        crosswalk.yahoo_game_id,
                        crosswalk.source,
                        crosswalk.source_url,
                        datetime.utcnow(),
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted crosswalk entries", total=len(crosswalks), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert crosswalk entries", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_outcomes(self, outcomes: List[OutcomesRow]) -> int:
        """Upsert game outcomes.
        
        Args:
            outcomes: List of OutcomesRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not outcomes:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with conn.transaction():
                for outcome in outcomes:
                    query = """
                    INSERT INTO outcomes (
                        game_id, home_team_tricode, away_team_tricode, q1_home_points,
                        q1_away_points, final_home_points, final_away_points, total_points,
                        home_win, margin, overtime_periods, source, source_url, ingested_at_utc
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (game_id) DO UPDATE SET
                        home_team_tricode = CASE 
                            WHEN excluded.home_team_tricode IS DISTINCT FROM outcomes.home_team_tricode 
                            THEN excluded.home_team_tricode ELSE outcomes.home_team_tricode END,
                        away_team_tricode = CASE 
                            WHEN excluded.away_team_tricode IS DISTINCT FROM outcomes.away_team_tricode 
                            THEN excluded.away_team_tricode ELSE outcomes.away_team_tricode END,
                        q1_home_points = CASE 
                            WHEN excluded.q1_home_points IS DISTINCT FROM outcomes.q1_home_points 
                            THEN excluded.q1_home_points ELSE outcomes.q1_home_points END,
                        q1_away_points = CASE 
                            WHEN excluded.q1_away_points IS DISTINCT FROM outcomes.q1_away_points 
                            THEN excluded.q1_away_points ELSE outcomes.q1_away_points END,
                        final_home_points = CASE 
                            WHEN excluded.final_home_points IS DISTINCT FROM outcomes.final_home_points 
                            THEN excluded.final_home_points ELSE outcomes.final_home_points END,
                        final_away_points = CASE 
                            WHEN excluded.final_away_points IS DISTINCT FROM outcomes.final_away_points 
                            THEN excluded.final_away_points ELSE outcomes.final_away_points END,
                        total_points = CASE 
                            WHEN excluded.total_points IS DISTINCT FROM outcomes.total_points 
                            THEN excluded.total_points ELSE outcomes.total_points END,
                        home_win = CASE 
                            WHEN excluded.home_win IS DISTINCT FROM outcomes.home_win 
                            THEN excluded.home_win ELSE outcomes.home_win END,
                        margin = CASE 
                            WHEN excluded.margin IS DISTINCT FROM outcomes.margin 
                            THEN excluded.margin ELSE outcomes.margin END,
                        overtime_periods = CASE 
                            WHEN excluded.overtime_periods IS DISTINCT FROM outcomes.overtime_periods 
                            THEN excluded.overtime_periods ELSE outcomes.overtime_periods END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.home_team_tricode IS DISTINCT FROM outcomes.home_team_tricode OR
                        excluded.away_team_tricode IS DISTINCT FROM outcomes.away_team_tricode OR
                        excluded.q1_home_points IS DISTINCT FROM outcomes.q1_home_points OR
                        excluded.q1_away_points IS DISTINCT FROM outcomes.q1_away_points OR
                        excluded.final_home_points IS DISTINCT FROM outcomes.final_home_points OR
                        excluded.final_away_points IS DISTINCT FROM outcomes.final_away_points OR
                        excluded.total_points IS DISTINCT FROM outcomes.total_points OR
                        excluded.home_win IS DISTINCT FROM outcomes.home_win OR
                        excluded.margin IS DISTINCT FROM outcomes.margin OR
                        excluded.overtime_periods IS DISTINCT FROM outcomes.overtime_periods
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        outcome.game_id,
                        outcome.home_team_tricode,
                        outcome.away_team_tricode,
                        outcome.q1_home_points,
                        outcome.q1_away_points,
                        outcome.final_home_points,
                        outcome.final_away_points,
                        outcome.total_points,
                        outcome.home_win,
                        outcome.margin,
                        outcome.overtime_periods,
                        outcome.source,
                        outcome.source_url,
                        datetime.utcnow(),
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted outcomes", total=len(outcomes), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert outcomes", error=str(e))
            raise
        
        return updated_count