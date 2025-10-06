"""Advanced metrics data loader with idempotent upserts for Tranche 1."""

from datetime import datetime, UTC
from typing import List, Dict, Any

import asyncpg

from ..db import get_connection
from ..nba_logging import get_logger
from ..pipelines.foundation import _maybe_transaction

logger = get_logger(__name__)


class AdvancedMetricsLoader:
    """Loader for advanced metrics data from NBA Stats API with diff-aware upserts."""
    
    async def upsert_advanced_player_stats(self, stats: List[Dict[str, Any]]) -> int:
        """Upsert advanced player statistics with diff-aware updates.
        
        Args:
            stats: List of advanced player stats dictionaries
            
        Returns:
            Number of rows actually updated (not just touched)
        """
        if not stats:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with _maybe_transaction(conn):
                for stat in stats:
                    query = """
                    INSERT INTO advanced_player_stats (
                        game_id, player_id, player_name, team_id, team_abbreviation,
                        offensive_rating, defensive_rating, net_rating,
                        assist_percentage, assist_to_turnover, assist_ratio,
                        offensive_rebound_pct, defensive_rebound_pct, rebound_pct,
                        turnover_ratio, effective_fg_pct, true_shooting_pct, usage_pct,
                        pace, pie, source, source_url, ingested_at_utc
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21, $22, $23
                    )
                    ON CONFLICT (game_id, player_id) DO UPDATE SET
                        player_name = CASE 
                            WHEN excluded.player_name IS DISTINCT FROM advanced_player_stats.player_name 
                            THEN excluded.player_name ELSE advanced_player_stats.player_name END,
                        team_id = CASE 
                            WHEN excluded.team_id IS DISTINCT FROM advanced_player_stats.team_id 
                            THEN excluded.team_id ELSE advanced_player_stats.team_id END,
                        team_abbreviation = CASE 
                            WHEN excluded.team_abbreviation IS DISTINCT FROM advanced_player_stats.team_abbreviation 
                            THEN excluded.team_abbreviation ELSE advanced_player_stats.team_abbreviation END,
                        offensive_rating = CASE 
                            WHEN excluded.offensive_rating IS DISTINCT FROM advanced_player_stats.offensive_rating 
                            THEN excluded.offensive_rating ELSE advanced_player_stats.offensive_rating END,
                        defensive_rating = CASE 
                            WHEN excluded.defensive_rating IS DISTINCT FROM advanced_player_stats.defensive_rating 
                            THEN excluded.defensive_rating ELSE advanced_player_stats.defensive_rating END,
                        net_rating = CASE 
                            WHEN excluded.net_rating IS DISTINCT FROM advanced_player_stats.net_rating 
                            THEN excluded.net_rating ELSE advanced_player_stats.net_rating END,
                        assist_percentage = CASE 
                            WHEN excluded.assist_percentage IS DISTINCT FROM advanced_player_stats.assist_percentage 
                            THEN excluded.assist_percentage ELSE advanced_player_stats.assist_percentage END,
                        assist_to_turnover = CASE 
                            WHEN excluded.assist_to_turnover IS DISTINCT FROM advanced_player_stats.assist_to_turnover 
                            THEN excluded.assist_to_turnover ELSE advanced_player_stats.assist_to_turnover END,
                        assist_ratio = CASE 
                            WHEN excluded.assist_ratio IS DISTINCT FROM advanced_player_stats.assist_ratio 
                            THEN excluded.assist_ratio ELSE advanced_player_stats.assist_ratio END,
                        offensive_rebound_pct = CASE 
                            WHEN excluded.offensive_rebound_pct IS DISTINCT FROM advanced_player_stats.offensive_rebound_pct 
                            THEN excluded.offensive_rebound_pct ELSE advanced_player_stats.offensive_rebound_pct END,
                        defensive_rebound_pct = CASE 
                            WHEN excluded.defensive_rebound_pct IS DISTINCT FROM advanced_player_stats.defensive_rebound_pct 
                            THEN excluded.defensive_rebound_pct ELSE advanced_player_stats.defensive_rebound_pct END,
                        rebound_pct = CASE 
                            WHEN excluded.rebound_pct IS DISTINCT FROM advanced_player_stats.rebound_pct 
                            THEN excluded.rebound_pct ELSE advanced_player_stats.rebound_pct END,
                        turnover_ratio = CASE 
                            WHEN excluded.turnover_ratio IS DISTINCT FROM advanced_player_stats.turnover_ratio 
                            THEN excluded.turnover_ratio ELSE advanced_player_stats.turnover_ratio END,
                        effective_fg_pct = CASE 
                            WHEN excluded.effective_fg_pct IS DISTINCT FROM advanced_player_stats.effective_fg_pct 
                            THEN excluded.effective_fg_pct ELSE advanced_player_stats.effective_fg_pct END,
                        true_shooting_pct = CASE 
                            WHEN excluded.true_shooting_pct IS DISTINCT FROM advanced_player_stats.true_shooting_pct 
                            THEN excluded.true_shooting_pct ELSE advanced_player_stats.true_shooting_pct END,
                        usage_pct = CASE 
                            WHEN excluded.usage_pct IS DISTINCT FROM advanced_player_stats.usage_pct 
                            THEN excluded.usage_pct ELSE advanced_player_stats.usage_pct END,
                        pace = CASE 
                            WHEN excluded.pace IS DISTINCT FROM advanced_player_stats.pace 
                            THEN excluded.pace ELSE advanced_player_stats.pace END,
                        pie = CASE 
                            WHEN excluded.pie IS DISTINCT FROM advanced_player_stats.pie 
                            THEN excluded.pie ELSE advanced_player_stats.pie END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.player_name IS DISTINCT FROM advanced_player_stats.player_name OR
                        excluded.team_id IS DISTINCT FROM advanced_player_stats.team_id OR
                        excluded.team_abbreviation IS DISTINCT FROM advanced_player_stats.team_abbreviation OR
                        excluded.offensive_rating IS DISTINCT FROM advanced_player_stats.offensive_rating OR
                        excluded.defensive_rating IS DISTINCT FROM advanced_player_stats.defensive_rating OR
                        excluded.net_rating IS DISTINCT FROM advanced_player_stats.net_rating OR
                        excluded.assist_percentage IS DISTINCT FROM advanced_player_stats.assist_percentage OR
                        excluded.assist_to_turnover IS DISTINCT FROM advanced_player_stats.assist_to_turnover OR
                        excluded.assist_ratio IS DISTINCT FROM advanced_player_stats.assist_ratio OR
                        excluded.offensive_rebound_pct IS DISTINCT FROM advanced_player_stats.offensive_rebound_pct OR
                        excluded.defensive_rebound_pct IS DISTINCT FROM advanced_player_stats.defensive_rebound_pct OR
                        excluded.rebound_pct IS DISTINCT FROM advanced_player_stats.rebound_pct OR
                        excluded.turnover_ratio IS DISTINCT FROM advanced_player_stats.turnover_ratio OR
                        excluded.effective_fg_pct IS DISTINCT FROM advanced_player_stats.effective_fg_pct OR
                        excluded.true_shooting_pct IS DISTINCT FROM advanced_player_stats.true_shooting_pct OR
                        excluded.usage_pct IS DISTINCT FROM advanced_player_stats.usage_pct OR
                        excluded.pace IS DISTINCT FROM advanced_player_stats.pace OR
                        excluded.pie IS DISTINCT FROM advanced_player_stats.pie
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        stat.get('game_id'),
                        stat.get('player_id'), 
                        stat.get('player_name'),
                        stat.get('team_id'),
                        stat.get('team_abbreviation'),
                        stat.get('offensive_rating'),
                        stat.get('defensive_rating'),
                        stat.get('net_rating'),
                        stat.get('assist_percentage'),
                        stat.get('assist_to_turnover'),
                        stat.get('assist_ratio'),
                        stat.get('offensive_rebound_pct'),
                        stat.get('defensive_rebound_pct'),
                        stat.get('rebound_pct'),
                        stat.get('turnover_ratio'),
                        stat.get('effective_fg_pct'),
                        stat.get('true_shooting_pct'),
                        stat.get('usage_pct'),
                        stat.get('pace'),
                        stat.get('pie'),
                        stat.get('source', 'nba_stats'),
                        stat.get('source_url'),
                        stat.get('ingested_at_utc', datetime.now(UTC))
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted advanced player stats", total=len(stats), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert advanced player stats", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_misc_player_stats(self, stats: List[Dict[str, Any]]) -> int:
        """Upsert miscellaneous player statistics with diff-aware updates.
        
        Args:
            stats: List of misc player stats dictionaries
            
        Returns:
            Number of rows actually updated
        """
        if not stats:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with _maybe_transaction(conn):
                for stat in stats:
                    query = """
                    INSERT INTO misc_player_stats (
                        game_id, player_id, player_name, team_id, team_abbreviation,
                        plus_minus, nba_fantasy_pts, dd2, td3,
                        fg_pct_rank, ft_pct_rank, fg3_pct_rank, pts_rank, reb_rank, ast_rank,
                        wnba_fantasy_pts, source, source_url, ingested_at_utc
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
                    )
                    ON CONFLICT (game_id, player_id) DO UPDATE SET
                        player_name = CASE 
                            WHEN excluded.player_name IS DISTINCT FROM misc_player_stats.player_name 
                            THEN excluded.player_name ELSE misc_player_stats.player_name END,
                        team_id = CASE 
                            WHEN excluded.team_id IS DISTINCT FROM misc_player_stats.team_id 
                            THEN excluded.team_id ELSE misc_player_stats.team_id END,
                        team_abbreviation = CASE 
                            WHEN excluded.team_abbreviation IS DISTINCT FROM misc_player_stats.team_abbreviation 
                            THEN excluded.team_abbreviation ELSE misc_player_stats.team_abbreviation END,
                        plus_minus = CASE 
                            WHEN excluded.plus_minus IS DISTINCT FROM misc_player_stats.plus_minus 
                            THEN excluded.plus_minus ELSE misc_player_stats.plus_minus END,
                        nba_fantasy_pts = CASE 
                            WHEN excluded.nba_fantasy_pts IS DISTINCT FROM misc_player_stats.nba_fantasy_pts 
                            THEN excluded.nba_fantasy_pts ELSE misc_player_stats.nba_fantasy_pts END,
                        dd2 = CASE 
                            WHEN excluded.dd2 IS DISTINCT FROM misc_player_stats.dd2 
                            THEN excluded.dd2 ELSE misc_player_stats.dd2 END,
                        td3 = CASE 
                            WHEN excluded.td3 IS DISTINCT FROM misc_player_stats.td3 
                            THEN excluded.td3 ELSE misc_player_stats.td3 END,
                        fg_pct_rank = CASE 
                            WHEN excluded.fg_pct_rank IS DISTINCT FROM misc_player_stats.fg_pct_rank 
                            THEN excluded.fg_pct_rank ELSE misc_player_stats.fg_pct_rank END,
                        ft_pct_rank = CASE 
                            WHEN excluded.ft_pct_rank IS DISTINCT FROM misc_player_stats.ft_pct_rank 
                            THEN excluded.ft_pct_rank ELSE misc_player_stats.ft_pct_rank END,
                        fg3_pct_rank = CASE 
                            WHEN excluded.fg3_pct_rank IS DISTINCT FROM misc_player_stats.fg3_pct_rank 
                            THEN excluded.fg3_pct_rank ELSE misc_player_stats.fg3_pct_rank END,
                        pts_rank = CASE 
                            WHEN excluded.pts_rank IS DISTINCT FROM misc_player_stats.pts_rank 
                            THEN excluded.pts_rank ELSE misc_player_stats.pts_rank END,
                        reb_rank = CASE 
                            WHEN excluded.reb_rank IS DISTINCT FROM misc_player_stats.reb_rank 
                            THEN excluded.reb_rank ELSE misc_player_stats.reb_rank END,
                        ast_rank = CASE 
                            WHEN excluded.ast_rank IS DISTINCT FROM misc_player_stats.ast_rank 
                            THEN excluded.ast_rank ELSE misc_player_stats.ast_rank END,
                        wnba_fantasy_pts = CASE 
                            WHEN excluded.wnba_fantasy_pts IS DISTINCT FROM misc_player_stats.wnba_fantasy_pts 
                            THEN excluded.wnba_fantasy_pts ELSE misc_player_stats.wnba_fantasy_pts END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.player_name IS DISTINCT FROM misc_player_stats.player_name OR
                        excluded.team_id IS DISTINCT FROM misc_player_stats.team_id OR
                        excluded.team_abbreviation IS DISTINCT FROM misc_player_stats.team_abbreviation OR
                        excluded.plus_minus IS DISTINCT FROM misc_player_stats.plus_minus OR
                        excluded.nba_fantasy_pts IS DISTINCT FROM misc_player_stats.nba_fantasy_pts OR
                        excluded.dd2 IS DISTINCT FROM misc_player_stats.dd2 OR
                        excluded.td3 IS DISTINCT FROM misc_player_stats.td3 OR
                        excluded.fg_pct_rank IS DISTINCT FROM misc_player_stats.fg_pct_rank OR
                        excluded.ft_pct_rank IS DISTINCT FROM misc_player_stats.ft_pct_rank OR
                        excluded.fg3_pct_rank IS DISTINCT FROM misc_player_stats.fg3_pct_rank OR
                        excluded.pts_rank IS DISTINCT FROM misc_player_stats.pts_rank OR
                        excluded.reb_rank IS DISTINCT FROM misc_player_stats.reb_rank OR
                        excluded.ast_rank IS DISTINCT FROM misc_player_stats.ast_rank OR
                        excluded.wnba_fantasy_pts IS DISTINCT FROM misc_player_stats.wnba_fantasy_pts
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        stat.get('game_id'),
                        stat.get('player_id'),
                        stat.get('player_name'),
                        stat.get('team_id'),
                        stat.get('team_abbreviation'),
                        stat.get('plus_minus'),
                        stat.get('nba_fantasy_pts'),
                        stat.get('dd2'),
                        stat.get('td3'),
                        stat.get('fg_pct_rank'),
                        stat.get('ft_pct_rank'),
                        stat.get('fg3_pct_rank'),
                        stat.get('pts_rank'),
                        stat.get('reb_rank'),
                        stat.get('ast_rank'),
                        stat.get('wnba_fantasy_pts'),
                        stat.get('source', 'nba_stats'),
                        stat.get('source_url'),
                        stat.get('ingested_at_utc', datetime.now(UTC))
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted misc player stats", total=len(stats), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert misc player stats", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_usage_player_stats(self, stats: List[Dict[str, Any]]) -> int:
        """Upsert usage player statistics with diff-aware updates.
        
        Args:
            stats: List of usage player stats dictionaries
            
        Returns:
            Number of rows actually updated
        """
        if not stats:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with _maybe_transaction(conn):
                for stat in stats:
                    query = """
                    INSERT INTO usage_player_stats (
                        game_id, player_id, player_name, team_id, team_abbreviation,
                        usage_pct, pct_fgm, pct_fga, pct_fg3m, pct_fg3a, pct_ftm, pct_fta,
                        pct_oreb, pct_dreb, pct_reb, pct_ast, pct_tov, pct_stl, pct_blk, 
                        pct_blka, pct_pf, pct_pfd, pct_pts, source, source_url, ingested_at_utc
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
                        $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
                    )
                    ON CONFLICT (game_id, player_id) DO UPDATE SET
                        player_name = CASE 
                            WHEN excluded.player_name IS DISTINCT FROM usage_player_stats.player_name 
                            THEN excluded.player_name ELSE usage_player_stats.player_name END,
                        team_id = CASE 
                            WHEN excluded.team_id IS DISTINCT FROM usage_player_stats.team_id 
                            THEN excluded.team_id ELSE usage_player_stats.team_id END,
                        team_abbreviation = CASE 
                            WHEN excluded.team_abbreviation IS DISTINCT FROM usage_player_stats.team_abbreviation 
                            THEN excluded.team_abbreviation ELSE usage_player_stats.team_abbreviation END,
                        usage_pct = CASE 
                            WHEN excluded.usage_pct IS DISTINCT FROM usage_player_stats.usage_pct 
                            THEN excluded.usage_pct ELSE usage_player_stats.usage_pct END,
                        pct_fgm = CASE 
                            WHEN excluded.pct_fgm IS DISTINCT FROM usage_player_stats.pct_fgm 
                            THEN excluded.pct_fgm ELSE usage_player_stats.pct_fgm END,
                        pct_fga = CASE 
                            WHEN excluded.pct_fga IS DISTINCT FROM usage_player_stats.pct_fga 
                            THEN excluded.pct_fga ELSE usage_player_stats.pct_fga END,
                        pct_fg3m = CASE 
                            WHEN excluded.pct_fg3m IS DISTINCT FROM usage_player_stats.pct_fg3m 
                            THEN excluded.pct_fg3m ELSE usage_player_stats.pct_fg3m END,
                        pct_fg3a = CASE 
                            WHEN excluded.pct_fg3a IS DISTINCT FROM usage_player_stats.pct_fg3a 
                            THEN excluded.pct_fg3a ELSE usage_player_stats.pct_fg3a END,
                        pct_ftm = CASE 
                            WHEN excluded.pct_ftm IS DISTINCT FROM usage_player_stats.pct_ftm 
                            THEN excluded.pct_ftm ELSE usage_player_stats.pct_ftm END,
                        pct_fta = CASE 
                            WHEN excluded.pct_fta IS DISTINCT FROM usage_player_stats.pct_fta 
                            THEN excluded.pct_fta ELSE usage_player_stats.pct_fta END,
                        pct_oreb = CASE 
                            WHEN excluded.pct_oreb IS DISTINCT FROM usage_player_stats.pct_oreb 
                            THEN excluded.pct_oreb ELSE usage_player_stats.pct_oreb END,
                        pct_dreb = CASE 
                            WHEN excluded.pct_dreb IS DISTINCT FROM usage_player_stats.pct_dreb 
                            THEN excluded.pct_dreb ELSE usage_player_stats.pct_dreb END,
                        pct_reb = CASE 
                            WHEN excluded.pct_reb IS DISTINCT FROM usage_player_stats.pct_reb 
                            THEN excluded.pct_reb ELSE usage_player_stats.pct_reb END,
                        pct_ast = CASE 
                            WHEN excluded.pct_ast IS DISTINCT FROM usage_player_stats.pct_ast 
                            THEN excluded.pct_ast ELSE usage_player_stats.pct_ast END,
                        pct_tov = CASE 
                            WHEN excluded.pct_tov IS DISTINCT FROM usage_player_stats.pct_tov 
                            THEN excluded.pct_tov ELSE usage_player_stats.pct_tov END,
                        pct_stl = CASE 
                            WHEN excluded.pct_stl IS DISTINCT FROM usage_player_stats.pct_stl 
                            THEN excluded.pct_stl ELSE usage_player_stats.pct_stl END,
                        pct_blk = CASE 
                            WHEN excluded.pct_blk IS DISTINCT FROM usage_player_stats.pct_blk 
                            THEN excluded.pct_blk ELSE usage_player_stats.pct_blk END,
                        pct_blka = CASE 
                            WHEN excluded.pct_blka IS DISTINCT FROM usage_player_stats.pct_blka 
                            THEN excluded.pct_blka ELSE usage_player_stats.pct_blka END,
                        pct_pf = CASE 
                            WHEN excluded.pct_pf IS DISTINCT FROM usage_player_stats.pct_pf 
                            THEN excluded.pct_pf ELSE usage_player_stats.pct_pf END,
                        pct_pfd = CASE 
                            WHEN excluded.pct_pfd IS DISTINCT FROM usage_player_stats.pct_pfd 
                            THEN excluded.pct_pfd ELSE usage_player_stats.pct_pfd END,
                        pct_pts = CASE 
                            WHEN excluded.pct_pts IS DISTINCT FROM usage_player_stats.pct_pts 
                            THEN excluded.pct_pts ELSE usage_player_stats.pct_pts END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.player_name IS DISTINCT FROM usage_player_stats.player_name OR
                        excluded.team_id IS DISTINCT FROM usage_player_stats.team_id OR
                        excluded.team_abbreviation IS DISTINCT FROM usage_player_stats.team_abbreviation OR
                        excluded.usage_pct IS DISTINCT FROM usage_player_stats.usage_pct OR
                        excluded.pct_fgm IS DISTINCT FROM usage_player_stats.pct_fgm OR
                        excluded.pct_fga IS DISTINCT FROM usage_player_stats.pct_fga OR
                        excluded.pct_fg3m IS DISTINCT FROM usage_player_stats.pct_fg3m OR
                        excluded.pct_fg3a IS DISTINCT FROM usage_player_stats.pct_fg3a OR
                        excluded.pct_ftm IS DISTINCT FROM usage_player_stats.pct_ftm OR
                        excluded.pct_fta IS DISTINCT FROM usage_player_stats.pct_fta OR
                        excluded.pct_oreb IS DISTINCT FROM usage_player_stats.pct_oreb OR
                        excluded.pct_dreb IS DISTINCT FROM usage_player_stats.pct_dreb OR
                        excluded.pct_reb IS DISTINCT FROM usage_player_stats.pct_reb OR
                        excluded.pct_ast IS DISTINCT FROM usage_player_stats.pct_ast OR
                        excluded.pct_tov IS DISTINCT FROM usage_player_stats.pct_tov OR
                        excluded.pct_stl IS DISTINCT FROM usage_player_stats.pct_stl OR
                        excluded.pct_blk IS DISTINCT FROM usage_player_stats.pct_blk OR
                        excluded.pct_blka IS DISTINCT FROM usage_player_stats.pct_blka OR
                        excluded.pct_pf IS DISTINCT FROM usage_player_stats.pct_pf OR
                        excluded.pct_pfd IS DISTINCT FROM usage_player_stats.pct_pfd OR
                        excluded.pct_pts IS DISTINCT FROM usage_player_stats.pct_pts
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        stat.get('game_id'),
                        stat.get('player_id'),
                        stat.get('player_name'),
                        stat.get('team_id'),
                        stat.get('team_abbreviation'),
                        stat.get('usage_pct'),
                        stat.get('pct_fgm'),
                        stat.get('pct_fga'),
                        stat.get('pct_fg3m'),
                        stat.get('pct_fg3a'),
                        stat.get('pct_ftm'),
                        stat.get('pct_fta'),
                        stat.get('pct_oreb'),
                        stat.get('pct_dreb'),
                        stat.get('pct_reb'),
                        stat.get('pct_ast'),
                        stat.get('pct_tov'),
                        stat.get('pct_stl'),
                        stat.get('pct_blk'),
                        stat.get('pct_blka'),
                        stat.get('pct_pf'),
                        stat.get('pct_pfd'),
                        stat.get('pct_pts'),
                        stat.get('source', 'nba_stats'),
                        stat.get('source_url'),
                        stat.get('ingested_at_utc', datetime.now(UTC))
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted usage player stats", total=len(stats), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert usage player stats", error=str(e))
            raise
        
        return updated_count
    
    async def upsert_advanced_team_stats(self, stats: List[Dict[str, Any]]) -> int:
        """Upsert advanced team statistics with diff-aware updates.
        
        Args:
            stats: List of advanced team stats dictionaries
            
        Returns:
            Number of rows actually updated
        """
        if not stats:
            return 0
        
        conn = await get_connection()
        updated_count = 0
        
        try:
            async with _maybe_transaction(conn):
                for stat in stats:
                    query = """
                    INSERT INTO advanced_team_stats (
                        game_id, team_id, team_abbreviation, team_name,
                        offensive_rating, defensive_rating, net_rating,
                        assist_percentage, assist_to_turnover, assist_ratio,
                        offensive_rebound_pct, defensive_rebound_pct, rebound_pct,
                        turnover_ratio, effective_fg_pct, true_shooting_pct,
                        pace, pie, source, source_url, ingested_at_utc
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
                    )
                    ON CONFLICT (game_id, team_id) DO UPDATE SET
                        team_abbreviation = CASE 
                            WHEN excluded.team_abbreviation IS DISTINCT FROM advanced_team_stats.team_abbreviation 
                            THEN excluded.team_abbreviation ELSE advanced_team_stats.team_abbreviation END,
                        team_name = CASE 
                            WHEN excluded.team_name IS DISTINCT FROM advanced_team_stats.team_name 
                            THEN excluded.team_name ELSE advanced_team_stats.team_name END,
                        offensive_rating = CASE 
                            WHEN excluded.offensive_rating IS DISTINCT FROM advanced_team_stats.offensive_rating 
                            THEN excluded.offensive_rating ELSE advanced_team_stats.offensive_rating END,
                        defensive_rating = CASE 
                            WHEN excluded.defensive_rating IS DISTINCT FROM advanced_team_stats.defensive_rating 
                            THEN excluded.defensive_rating ELSE advanced_team_stats.defensive_rating END,
                        net_rating = CASE 
                            WHEN excluded.net_rating IS DISTINCT FROM advanced_team_stats.net_rating 
                            THEN excluded.net_rating ELSE advanced_team_stats.net_rating END,
                        assist_percentage = CASE 
                            WHEN excluded.assist_percentage IS DISTINCT FROM advanced_team_stats.assist_percentage 
                            THEN excluded.assist_percentage ELSE advanced_team_stats.assist_percentage END,
                        assist_to_turnover = CASE 
                            WHEN excluded.assist_to_turnover IS DISTINCT FROM advanced_team_stats.assist_to_turnover 
                            THEN excluded.assist_to_turnover ELSE advanced_team_stats.assist_to_turnover END,
                        assist_ratio = CASE 
                            WHEN excluded.assist_ratio IS DISTINCT FROM advanced_team_stats.assist_ratio 
                            THEN excluded.assist_ratio ELSE advanced_team_stats.assist_ratio END,
                        offensive_rebound_pct = CASE 
                            WHEN excluded.offensive_rebound_pct IS DISTINCT FROM advanced_team_stats.offensive_rebound_pct 
                            THEN excluded.offensive_rebound_pct ELSE advanced_team_stats.offensive_rebound_pct END,
                        defensive_rebound_pct = CASE 
                            WHEN excluded.defensive_rebound_pct IS DISTINCT FROM advanced_team_stats.defensive_rebound_pct 
                            THEN excluded.defensive_rebound_pct ELSE advanced_team_stats.defensive_rebound_pct END,
                        rebound_pct = CASE 
                            WHEN excluded.rebound_pct IS DISTINCT FROM advanced_team_stats.rebound_pct 
                            THEN excluded.rebound_pct ELSE advanced_team_stats.rebound_pct END,
                        turnover_ratio = CASE 
                            WHEN excluded.turnover_ratio IS DISTINCT FROM advanced_team_stats.turnover_ratio 
                            THEN excluded.turnover_ratio ELSE advanced_team_stats.turnover_ratio END,
                        effective_fg_pct = CASE 
                            WHEN excluded.effective_fg_pct IS DISTINCT FROM advanced_team_stats.effective_fg_pct 
                            THEN excluded.effective_fg_pct ELSE advanced_team_stats.effective_fg_pct END,
                        true_shooting_pct = CASE 
                            WHEN excluded.true_shooting_pct IS DISTINCT FROM advanced_team_stats.true_shooting_pct 
                            THEN excluded.true_shooting_pct ELSE advanced_team_stats.true_shooting_pct END,
                        pace = CASE 
                            WHEN excluded.pace IS DISTINCT FROM advanced_team_stats.pace 
                            THEN excluded.pace ELSE advanced_team_stats.pace END,
                        pie = CASE 
                            WHEN excluded.pie IS DISTINCT FROM advanced_team_stats.pie 
                            THEN excluded.pie ELSE advanced_team_stats.pie END,
                        source = excluded.source,
                        source_url = excluded.source_url,
                        ingested_at_utc = excluded.ingested_at_utc
                    WHERE (
                        excluded.team_abbreviation IS DISTINCT FROM advanced_team_stats.team_abbreviation OR
                        excluded.team_name IS DISTINCT FROM advanced_team_stats.team_name OR
                        excluded.offensive_rating IS DISTINCT FROM advanced_team_stats.offensive_rating OR
                        excluded.defensive_rating IS DISTINCT FROM advanced_team_stats.defensive_rating OR
                        excluded.net_rating IS DISTINCT FROM advanced_team_stats.net_rating OR
                        excluded.assist_percentage IS DISTINCT FROM advanced_team_stats.assist_percentage OR
                        excluded.assist_to_turnover IS DISTINCT FROM advanced_team_stats.assist_to_turnover OR
                        excluded.assist_ratio IS DISTINCT FROM advanced_team_stats.assist_ratio OR
                        excluded.offensive_rebound_pct IS DISTINCT FROM advanced_team_stats.offensive_rebound_pct OR
                        excluded.defensive_rebound_pct IS DISTINCT FROM advanced_team_stats.defensive_rebound_pct OR
                        excluded.rebound_pct IS DISTINCT FROM advanced_team_stats.rebound_pct OR
                        excluded.turnover_ratio IS DISTINCT FROM advanced_team_stats.turnover_ratio OR
                        excluded.effective_fg_pct IS DISTINCT FROM advanced_team_stats.effective_fg_pct OR
                        excluded.true_shooting_pct IS DISTINCT FROM advanced_team_stats.true_shooting_pct OR
                        excluded.pace IS DISTINCT FROM advanced_team_stats.pace OR
                        excluded.pie IS DISTINCT FROM advanced_team_stats.pie
                    )
                    """
                    
                    result = await conn.execute(
                        query,
                        stat.get('game_id'),
                        stat.get('team_id'),
                        stat.get('team_abbreviation'),
                        stat.get('team_name'),
                        stat.get('offensive_rating'),
                        stat.get('defensive_rating'),
                        stat.get('net_rating'),
                        stat.get('assist_percentage'),
                        stat.get('assist_to_turnover'),
                        stat.get('assist_ratio'),
                        stat.get('offensive_rebound_pct'),
                        stat.get('defensive_rebound_pct'),
                        stat.get('rebound_pct'),
                        stat.get('turnover_ratio'),
                        stat.get('effective_fg_pct'),
                        stat.get('true_shooting_pct'),
                        stat.get('pace'),
                        stat.get('pie'),
                        stat.get('source', 'nba_stats'),
                        stat.get('source_url'),
                        stat.get('ingested_at_utc', datetime.now(UTC))
                    )
                    
                    if result.startswith('UPDATE'):
                        updated_count += 1
            
            logger.info("Upserted advanced team stats", total=len(stats), updated=updated_count)
            
        except Exception as e:
            logger.error("Failed to upsert advanced team stats", error=str(e))
            raise
        
        return updated_count

async def upsert_adv_metrics(conn, metrics_data):
    """Standalone adapter function for advanced metrics upserts.
    
    Args:
        conn: Database connection (unused, loader manages its own connection)
        metrics_data: List of metrics dictionaries or empty list
        
    Returns:
        None (for compatibility with pipeline expectations)
    """
    if not metrics_data:
        return None
    
    loader = AdvancedMetricsLoader()
    
    # Route to appropriate upsert method based on data structure
    if isinstance(metrics_data, list) and len(metrics_data) > 0:
        sample = metrics_data[0]
        
        # Check which type of metrics based on available fields
        if 'offensive_rating' in sample:
            if 'player_id' in sample:
                await loader.upsert_advanced_player_stats(metrics_data)
            else:
                await loader.upsert_advanced_team_stats(metrics_data)
        elif 'plus_minus' in sample:
            await loader.upsert_misc_player_stats(metrics_data)
        elif 'usage_pct' in sample:
            await loader.upsert_usage_player_stats(metrics_data)
    
    return None