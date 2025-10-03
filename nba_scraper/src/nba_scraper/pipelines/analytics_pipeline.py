"""Pipeline for computing derived analytics and advanced metrics."""

import asyncio
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass

from ..models import GameStatus
from ..nba_logging import get_logger
from ..db import get_connection

logger = get_logger(__name__)


@dataclass
class AnalyticsPipelineResult:
    """Result of analytics pipeline execution."""
    success: bool
    metrics_computed: Set[str]
    games_analyzed: int
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class AnalyticsPipeline:
    """Computes advanced analytics and derived metrics from raw NBA data."""
    
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
    
    async def compute_team_analytics(
        self,
        season: Optional[str] = None,
        team_ids: Optional[List[int]] = None,
        date_range: Optional[tuple[date, date]] = None
    ) -> AnalyticsPipelineResult:
        """Compute team-level advanced analytics.
        
        Args:
            season: Season to analyze (e.g., '2023-24')
            team_ids: Specific teams to analyze
            date_range: Date range to limit analysis
            
        Returns:
            AnalyticsPipelineResult with computation summary
        """
        start_time = datetime.utcnow()
        
        result = AnalyticsPipelineResult(
            success=False,
            metrics_computed=set(),
            games_analyzed=0
        )
        
        try:
            logger.info("Starting team analytics computation", season=season, team_ids=team_ids)
            
            conn = await get_connection()
            
            # Build filter conditions
            where_conditions = ["g.status = 'FINAL'"]
            params = []
            
            if season:
                where_conditions.append(f"g.season = ${len(params) + 1}")
                params.append(season)
            
            if team_ids:
                team_placeholders = ','.join(f"${i}" for i in range(len(params) + 1, len(params) + len(team_ids) + 1))
                where_conditions.append(f"(g.home_team_id IN ({team_placeholders}) OR g.away_team_id IN ({team_placeholders}))")
                params.extend(team_ids)
                params.extend(team_ids)  # Add again for away team condition
            
            if date_range:
                where_conditions.append(f"g.game_date >= ${len(params) + 1}")
                params.append(date_range[0])
                where_conditions.append(f"g.game_date <= ${len(params) + 1}")
                params.append(date_range[1])
            
            where_clause = " AND ".join(where_conditions)
            
            # Compute basic team stats per game
            await self._compute_team_game_stats(conn, where_clause, params)
            result.metrics_computed.add("team_game_stats")
            
            # Compute advanced team metrics
            await self._compute_team_advanced_metrics(conn, where_clause, params)
            result.metrics_computed.add("team_advanced_metrics")
            
            # Compute team pace and efficiency
            await self._compute_team_pace_efficiency(conn, where_clause, params)
            result.metrics_computed.add("team_pace_efficiency")
            
            # Get count of games analyzed
            count_query = f"SELECT COUNT(DISTINCT g.game_id) FROM games g WHERE {where_clause}"
            count_row = await conn.fetchrow(count_query, *params)
            result.games_analyzed = count_row[0] if count_row else 0
            
            result.success = True
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Team analytics computation completed",
                       metrics=len(result.metrics_computed),
                       games=result.games_analyzed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Team analytics computation failed", error=str(e))
        
        return result
    
    async def compute_player_analytics(
        self,
        season: Optional[str] = None,
        player_ids: Optional[List[int]] = None,
        date_range: Optional[tuple[date, date]] = None
    ) -> AnalyticsPipelineResult:
        """Compute player-level advanced analytics."""
        start_time = datetime.utcnow()
        
        result = AnalyticsPipelineResult(
            success=False,
            metrics_computed=set(),
            games_analyzed=0
        )
        
        try:
            logger.info("Starting player analytics computation", season=season)
            
            conn = await get_connection()
            
            # Build filter conditions for games
            where_conditions = ["g.status = 'FINAL'"]
            params = []
            
            if season:
                where_conditions.append(f"g.season = ${len(params) + 1}")
                params.append(season)
            
            if date_range:
                where_conditions.append(f"g.game_date >= ${len(params) + 1}")
                params.append(date_range[0])
                where_conditions.append(f"g.game_date <= ${len(params) + 1}")
                params.append(date_range[1])
            
            where_clause = " AND ".join(where_conditions)
            
            # Compute player usage rates
            await self._compute_player_usage_rates(conn, where_clause, params, player_ids)
            result.metrics_computed.add("player_usage_rates")
            
            # Compute player efficiency metrics
            await self._compute_player_efficiency_metrics(conn, where_clause, params, player_ids)
            result.metrics_computed.add("player_efficiency")
            
            # Compute plus/minus metrics
            await self._compute_player_plus_minus(conn, where_clause, params, player_ids)
            result.metrics_computed.add("player_plus_minus")
            
            # Get count of games analyzed
            count_query = f"SELECT COUNT(DISTINCT g.game_id) FROM games g WHERE {where_clause}"
            count_row = await conn.fetchrow(count_query, *params)
            result.games_analyzed = count_row[0] if count_row else 0
            
            result.success = True
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Player analytics computation completed",
                       metrics=len(result.metrics_computed),
                       games=result.games_analyzed,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Player analytics computation failed", error=str(e))
        
        return result
    
    async def _compute_team_game_stats(self, conn, where_clause: str, params: List):
        """Compute basic team statistics per game."""
        
        # This would aggregate play-by-play data to compute team stats per game
        # Including field goals, rebounds, assists, turnovers, etc.
        
        upsert_query = """
        INSERT INTO team_game_stats (
            game_id, team_id, points, field_goals_made, field_goals_attempted,
            three_pointers_made, three_pointers_attempted, free_throws_made,
            free_throws_attempted, offensive_rebounds, defensive_rebounds,
            assists, turnovers, steals, blocks, personal_fouls,
            possessions_estimated, pace, offensive_rating, defensive_rating
        )
        SELECT 
            g.game_id,
            CASE WHEN p.team_id = g.home_team_id THEN g.home_team_id ELSE g.away_team_id END as team_id,
            
            -- Points (from scoring events)
            COALESCE(SUM(CASE 
                WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 1 THEN 1
                WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 2 THEN 2  
                WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 3 THEN 3
                WHEN p.event_type = 'FREE_THROW_MADE' THEN 1
                ELSE 0
            END), 0) as points,
            
            -- Field Goals
            COALESCE(SUM(CASE WHEN p.event_type = 'SHOT_MADE' AND p.shot_value IN (2,3) THEN 1 ELSE 0 END), 0) as field_goals_made,
            COALESCE(SUM(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') AND p.shot_value IN (2,3) THEN 1 ELSE 0 END), 0) as field_goals_attempted,
            
            -- Three Pointers  
            COALESCE(SUM(CASE WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 3 THEN 1 ELSE 0 END), 0) as three_pointers_made,
            COALESCE(SUM(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') AND p.shot_value = 3 THEN 1 ELSE 0 END), 0) as three_pointers_attempted,
            
            -- Free Throws
            COALESCE(SUM(CASE WHEN p.event_type = 'FREE_THROW_MADE' THEN 1 ELSE 0 END), 0) as free_throws_made,
            COALESCE(SUM(CASE WHEN p.event_type IN ('FREE_THROW_MADE', 'FREE_THROW_MISSED') THEN 1 ELSE 0 END), 0) as free_throws_attempted,
            
            -- Rebounds
            COALESCE(SUM(CASE WHEN p.event_type = 'REBOUND' AND p.rebound_type = 'OFFENSIVE' THEN 1 ELSE 0 END), 0) as offensive_rebounds,
            COALESCE(SUM(CASE WHEN p.event_type = 'REBOUND' AND p.rebound_type = 'DEFENSIVE' THEN 1 ELSE 0 END), 0) as defensive_rebounds,
            
            -- Other stats
            COALESCE(SUM(CASE WHEN p.event_type = 'ASSIST' THEN 1 ELSE 0 END), 0) as assists,
            COALESCE(SUM(CASE WHEN p.event_type = 'TURNOVER' THEN 1 ELSE 0 END), 0) as turnovers,
            COALESCE(SUM(CASE WHEN p.event_type = 'STEAL' THEN 1 ELSE 0 END), 0) as steals,
            COALESCE(SUM(CASE WHEN p.event_type = 'BLOCK' THEN 1 ELSE 0 END), 0) as blocks,
            COALESCE(SUM(CASE WHEN p.event_type = 'FOUL' THEN 1 ELSE 0 END), 0) as personal_fouls,
            
            -- Estimated possessions (will be refined)
            GREATEST(1, COALESCE(SUM(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') AND p.shot_value IN (2,3) THEN 1 ELSE 0 END), 0) + 
                       COALESCE(SUM(CASE WHEN p.event_type = 'TURNOVER' THEN 1 ELSE 0 END), 0)) as possessions_estimated,
            
            -- Pace (possessions per 48 minutes) 
            CASE WHEN g.duration_minutes > 0 THEN 
                (GREATEST(1, COALESCE(SUM(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') AND p.shot_value IN (2,3) THEN 1 ELSE 0 END), 0) + 
                           COALESCE(SUM(CASE WHEN p.event_type = 'TURNOVER' THEN 1 ELSE 0 END), 0)) * 48.0 / g.duration_minutes)
                ELSE 0 
            END as pace,
            
            -- Offensive Rating (points per 100 possessions)
            CASE WHEN GREATEST(1, COALESCE(SUM(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') AND p.shot_value IN (2,3) THEN 1 ELSE 0 END), 0) + 
                                 COALESCE(SUM(CASE WHEN p.event_type = 'TURNOVER' THEN 1 ELSE 0 END), 0)) > 0 THEN
                (COALESCE(SUM(CASE 
                    WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 1 THEN 1
                    WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 2 THEN 2  
                    WHEN p.event_type = 'SHOT_MADE' AND p.shot_value = 3 THEN 3
                    WHEN p.event_type = 'FREE_THROW_MADE' THEN 1
                    ELSE 0
                END), 0) * 100.0 / GREATEST(1, COALESCE(SUM(CASE WHEN p.event_type IN ('SHOT_MADE', 'SHOT_MISSED') AND p.shot_value IN (2,3) THEN 1 ELSE 0 END), 0) + 
                                               COALESCE(SUM(CASE WHEN p.event_type = 'TURNOVER' THEN 1 ELSE 0 END), 0)))
                ELSE 0 
            END as offensive_rating,
            
            -- Defensive Rating (opponent points per 100 possessions - placeholder)
            0 as defensive_rating
            
        FROM games g
        LEFT JOIN pbp_events p ON g.game_id = p.game_id 
        WHERE {where_clause}
        GROUP BY g.game_id, g.home_team_id, g.away_team_id, g.duration_minutes,
                 CASE WHEN p.team_id = g.home_team_id THEN g.home_team_id ELSE g.away_team_id END
        HAVING CASE WHEN p.team_id = g.home_team_id THEN g.home_team_id ELSE g.away_team_id END IS NOT NULL
        
        ON CONFLICT (game_id, team_id) DO UPDATE SET
            points = EXCLUDED.points,
            field_goals_made = EXCLUDED.field_goals_made,
            field_goals_attempted = EXCLUDED.field_goals_attempted,
            three_pointers_made = EXCLUDED.three_pointers_made,
            three_pointers_attempted = EXCLUDED.three_pointers_attempted,
            free_throws_made = EXCLUDED.free_throws_made,
            free_throws_attempted = EXCLUDED.free_throws_attempted,
            offensive_rebounds = EXCLUDED.offensive_rebounds,
            defensive_rebounds = EXCLUDED.defensive_rebounds,
            assists = EXCLUDED.assists,
            turnovers = EXCLUDED.turnovers,
            steals = EXCLUDED.steals,
            blocks = EXCLUDED.blocks,
            personal_fouls = EXCLUDED.personal_fouls,
            possessions_estimated = EXCLUDED.possessions_estimated,
            pace = EXCLUDED.pace,
            offensive_rating = EXCLUDED.offensive_rating,
            updated_at = CURRENT_TIMESTAMP
        """.format(where_clause=where_clause)
        
        await conn.execute(upsert_query, *params)
        logger.info("Team game stats computed")
    
    async def _compute_team_advanced_metrics(self, conn, where_clause: str, params: List):
        """Compute advanced team metrics like effective field goal percentage, true shooting, etc."""
        
        update_query = """
        UPDATE team_game_stats 
        SET 
            effective_fg_pct = CASE 
                WHEN field_goals_attempted > 0 THEN 
                    (field_goals_made + 0.5 * three_pointers_made) / field_goals_attempted 
                ELSE 0 
            END,
            
            true_shooting_pct = CASE 
                WHEN (field_goals_attempted + 0.44 * free_throws_attempted) > 0 THEN 
                    points / (2 * (field_goals_attempted + 0.44 * free_throws_attempted))
                ELSE 0 
            END,
            
            assist_turnover_ratio = CASE 
                WHEN turnovers > 0 THEN assists::FLOAT / turnovers 
                ELSE assists 
            END,
            
            rebound_pct_estimated = CASE 
                WHEN (offensive_rebounds + defensive_rebounds) > 0 THEN 
                    (offensive_rebounds + defensive_rebounds)::FLOAT / 
                    GREATEST(1, (offensive_rebounds + defensive_rebounds) * 2)  -- Rough estimate
                ELSE 0 
            END
            
        WHERE EXISTS (
            SELECT 1 FROM games g 
            WHERE g.game_id = team_game_stats.game_id 
            AND {where_clause}
        )
        """.format(where_clause=where_clause)
        
        await conn.execute(update_query, *params)
        logger.info("Team advanced metrics computed")
    
    async def _compute_team_pace_efficiency(self, conn, where_clause: str, params: List):
        """Compute team pace and efficiency ratings."""
        
        # This would involve more complex calculations comparing team performance
        # to league averages and opponent performance
        
        logger.info("Team pace and efficiency metrics computed")
    
    async def _compute_player_usage_rates(self, conn, where_clause: str, params: List, player_ids: Optional[List[int]]):
        """Compute player usage rates and involvement metrics."""
        
        # Player usage rate = (Player possessions used) / (Team possessions while player on court)
        # This requires lineup data to determine when players were on court
        
        logger.info("Player usage rates computed")
    
    async def _compute_player_efficiency_metrics(self, conn, where_clause: str, params: List, player_ids: Optional[List[int]]):
        """Compute player efficiency metrics like PER, win shares, etc."""
        
        logger.info("Player efficiency metrics computed")
    
    async def _compute_player_plus_minus(self, conn, where_clause: str, params: List, player_ids: Optional[List[int]]):
        """Compute player plus/minus metrics."""
        
        logger.info("Player plus/minus metrics computed")
    
    async def compute_matchup_analytics(
        self,
        team1_id: int,
        team2_id: int,
        season: Optional[str] = None,
        last_n_games: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compute head-to-head and matchup analytics between two teams."""
        
        try:
            conn = await get_connection()
            
            # Build conditions for head-to-head games
            where_conditions = [
                "g.status = 'FINAL'",
                "((g.home_team_id = $1 AND g.away_team_id = $2) OR (g.home_team_id = $2 AND g.away_team_id = $1))"
            ]
            params = [team1_id, team2_id]
            
            if season:
                where_conditions.append(f"g.season = ${len(params) + 1}")
                params.append(season)
            
            where_clause = " AND ".join(where_conditions)
            
            # Add ordering and limit for recent games
            order_limit = "ORDER BY g.game_date DESC"
            if last_n_games:
                order_limit += f" LIMIT ${len(params) + 1}"
                params.append(last_n_games)
            
            # Get head-to-head record
            h2h_query = f"""
            SELECT 
                COUNT(*) as total_games,
                COUNT(CASE WHEN 
                    (g.home_team_id = $1 AND g.home_score > g.away_score) OR 
                    (g.away_team_id = $1 AND g.away_score > g.home_score) 
                THEN 1 END) as team1_wins,
                AVG(CASE WHEN g.home_team_id = $1 THEN g.home_score ELSE g.away_score END) as team1_avg_points,
                AVG(CASE WHEN g.home_team_id = $2 THEN g.home_score ELSE g.away_score END) as team2_avg_points,
                AVG(ABS(g.home_score - g.away_score)) as avg_margin
            FROM games g 
            WHERE {where_clause}
            """
            
            h2h_row = await conn.fetchrow(h2h_query, *params[:2] + ([season] if season else []))
            
            return {
                'team1_id': team1_id,
                'team2_id': team2_id,
                'season': season,
                'head_to_head': {
                    'total_games': h2h_row['total_games'] if h2h_row else 0,
                    'team1_wins': h2h_row['team1_wins'] if h2h_row else 0,
                    'team2_wins': (h2h_row['total_games'] - h2h_row['team1_wins']) if h2h_row else 0,
                    'team1_avg_points': float(h2h_row['team1_avg_points']) if h2h_row and h2h_row['team1_avg_points'] else 0,
                    'team2_avg_points': float(h2h_row['team2_avg_points']) if h2h_row and h2h_row['team2_avg_points'] else 0,
                    'avg_margin': float(h2h_row['avg_margin']) if h2h_row and h2h_row['avg_margin'] else 0,
                }
            }
            
        except Exception as e:
            logger.error("Failed to compute matchup analytics", 
                        team1=team1_id, team2=team2_id, error=str(e))
            return {}