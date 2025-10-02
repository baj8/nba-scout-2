"""Pipeline for deriving analytics tables from raw NBA data."""

import asyncio
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from ..transformers.q1_window import Q1WindowTransformer
from ..transformers.early_shocks import EarlyShocksTransformer
from ..transformers.schedule_travel import ScheduleTravelTransformer
from ..loaders.derived import DerivedLoader
from ..logging import get_logger
from ..db import get_connection

logger = get_logger(__name__)


@dataclass
class DerivePipelineResult:
    """Result of a derive pipeline execution."""
    success: bool
    start_date: date
    end_date: date
    tables_processed: List[str]
    tables_failed: List[str]
    records_updated: Dict[str, int]
    duration_seconds: float
    error: Optional[str] = None


class DerivePipeline:
    """Pipeline for deriving analytics tables from raw NBA data."""
    
    def __init__(self):
        """Initialize derive pipeline."""
        self.q1_transformer = Q1WindowTransformer()
        self.early_shocks_transformer = EarlyShocksTransformer()
        self.schedule_travel_transformer = ScheduleTravelTransformer()
        self.derived_loader = DerivedLoader()
        
    async def derive_all(
        self,
        start_date: date,
        end_date: date,
        tables: Optional[List[str]] = None,
        force: bool = False,
        dry_run: bool = False
    ) -> DerivePipelineResult:
        """Derive all analytics tables for a date range.
        
        Args:
            start_date: Start date for derivation
            end_date: End date for derivation
            tables: Specific tables to derive (None for all)
            force: Whether to force recomputation of existing data
            dry_run: Whether to run in dry-run mode (no database writes)
            
        Returns:
            DerivePipelineResult with execution details
        """
        start_time = datetime.utcnow()
        
        # Default to all available tables
        available_tables = ['q1_window', 'early_shocks', 'schedule_travel', 'outcomes']
        tables_to_process = tables if tables else available_tables
        
        result = DerivePipelineResult(
            success=False,
            start_date=start_date,
            end_date=end_date,
            tables_processed=[],
            tables_failed=[],
            records_updated={},
            duration_seconds=0
        )
        
        try:
            logger.info("Starting analytics derivation",
                       start_date=start_date,
                       end_date=end_date,
                       tables=tables_to_process,
                       force=force,
                       dry_run=dry_run)
            
            # Process each table
            for table in tables_to_process:
                try:
                    if table == 'q1_window':
                        count = await self._derive_q1_window(start_date, end_date, force, dry_run)
                        result.records_updated['q1_window_12_8'] = count
                        result.tables_processed.append(table)
                        
                    elif table == 'early_shocks':
                        count = await self._derive_early_shocks(start_date, end_date, force, dry_run)
                        result.records_updated['early_shocks'] = count
                        result.tables_processed.append(table)
                        
                    elif table == 'schedule_travel':
                        count = await self._derive_schedule_travel(start_date, end_date, force, dry_run)
                        result.records_updated['schedule_travel'] = count
                        result.tables_processed.append(table)
                        
                    elif table == 'outcomes':
                        count = await self._derive_outcomes(start_date, end_date, force, dry_run)
                        result.records_updated['outcomes'] = count
                        result.tables_processed.append(table)
                        
                    else:
                        logger.warning("Unknown table for derivation", table=table)
                        result.tables_failed.append(table)
                        
                except Exception as e:
                    logger.error("Failed to derive table", table=table, error=str(e))
                    result.tables_failed.append(table)
            
            result.success = len(result.tables_processed) > 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Analytics derivation completed",
                       tables_processed=len(result.tables_processed),
                       tables_failed=len(result.tables_failed),
                       total_records=sum(result.records_updated.values()),
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Analytics derivation failed", error=str(e))
        
        return result
    
    async def _derive_q1_window(
        self,
        start_date: date,
        end_date: date,
        force: bool,
        dry_run: bool
    ) -> int:
        """Derive Q1 window analytics."""
        try:
            # Get games in date range
            games = await self._get_games_in_range(start_date, end_date, force)
            
            if not games:
                logger.info("No games found for Q1 window derivation",
                           start_date=start_date, end_date=end_date)
                return 0
            
            logger.info("Deriving Q1 window analytics", game_count=len(games))
            
            total_updated = 0
            
            # Process games in batches
            batch_size = 50
            for i in range(0, len(games), batch_size):
                batch = games[i:i + batch_size]
                
                # Transform batch
                q1_windows = []
                for game in batch:
                    try:
                        windows = await self.q1_transformer.transform_game_q1_window(game['game_id'])
                        q1_windows.extend(windows)
                    except Exception as e:
                        logger.error("Failed to transform Q1 window",
                                   game_id=game['game_id'], error=str(e))
                
                # Load batch
                if q1_windows and not dry_run:
                    batch_updated = await self.derived_loader.upsert_q1_windows(q1_windows)
                    total_updated += batch_updated
                elif q1_windows and dry_run:
                    logger.info("Dry run: would upsert Q1 windows", count=len(q1_windows))
                    total_updated += len(q1_windows)
            
            return total_updated
            
        except Exception as e:
            logger.error("Failed to derive Q1 window analytics", error=str(e))
            raise
    
    async def _derive_early_shocks(
        self,
        start_date: date,
        end_date: date,
        force: bool,
        dry_run: bool
    ) -> int:
        """Derive early shocks analytics."""
        try:
            games = await self._get_games_in_range(start_date, end_date, force)
            
            if not games:
                logger.info("No games found for early shocks derivation",
                           start_date=start_date, end_date=end_date)
                return 0
            
            logger.info("Deriving early shocks analytics", game_count=len(games))
            
            total_updated = 0
            batch_size = 50
            
            for i in range(0, len(games), batch_size):
                batch = games[i:i + batch_size]
                
                # Transform batch
                early_shocks = []
                for game in batch:
                    try:
                        shocks = await self.early_shocks_transformer.transform_game_early_shocks(game['game_id'])
                        early_shocks.extend(shocks)
                    except Exception as e:
                        logger.error("Failed to transform early shocks",
                                   game_id=game['game_id'], error=str(e))
                
                # Load batch
                if early_shocks and not dry_run:
                    batch_updated = await self.derived_loader.upsert_early_shocks(early_shocks)
                    total_updated += batch_updated
                elif early_shocks and dry_run:
                    logger.info("Dry run: would upsert early shocks", count=len(early_shocks))
                    total_updated += len(early_shocks)
            
            return total_updated
            
        except Exception as e:
            logger.error("Failed to derive early shocks analytics", error=str(e))
            raise
    
    async def _derive_schedule_travel(
        self,
        start_date: date,
        end_date: date,
        force: bool,
        dry_run: bool
    ) -> int:
        """Derive schedule travel analytics."""
        try:
            games = await self._get_games_in_range(start_date, end_date, force)
            
            if not games:
                logger.info("No games found for schedule travel derivation",
                           start_date=start_date, end_date=end_date)
                return 0
            
            logger.info("Deriving schedule travel analytics", game_count=len(games))
            
            total_updated = 0
            batch_size = 50
            
            for i in range(0, len(games), batch_size):
                batch = games[i:i + batch_size]
                
                # Transform batch
                travel_rows = []
                for game in batch:
                    try:
                        travel = await self.schedule_travel_transformer.transform_game_travel(game['game_id'])
                        travel_rows.extend(travel)
                    except Exception as e:
                        logger.error("Failed to transform schedule travel",
                                   game_id=game['game_id'], error=str(e))
                
                # Load batch
                if travel_rows and not dry_run:
                    batch_updated = await self.derived_loader.upsert_schedule_travel(travel_rows)
                    total_updated += batch_updated
                elif travel_rows and dry_run:
                    logger.info("Dry run: would upsert schedule travel", count=len(travel_rows))
                    total_updated += len(travel_rows)
            
            return total_updated
            
        except Exception as e:
            logger.error("Failed to derive schedule travel analytics", error=str(e))
            raise
    
    async def _derive_outcomes(
        self,
        start_date: date,
        end_date: date,
        force: bool,
        dry_run: bool
    ) -> int:
        """Derive game outcomes."""
        try:
            conn = await get_connection()
            
            # Query to upsert outcomes from games table
            if not dry_run:
                query = """
                INSERT INTO outcomes (
                    game_id, q1_home_points, q1_away_points, 
                    final_home_points, final_away_points, total_points,
                    home_win, margin, overtime_periods,
                    source, source_url, ingested_at_utc
                )
                SELECT 
                    g.game_id,
                    g.q1_home_points,
                    g.q1_away_points,
                    g.home_score as final_home_points,
                    g.away_score as final_away_points,
                    g.home_score + g.away_score as total_points,
                    g.home_score > g.away_score as home_win,
                    ABS(g.home_score - g.away_score) as margin,
                    COALESCE(g.overtime_periods, 0) as overtime_periods,
                    'derived_pipeline' as source,
                    'nba_scraper://outcomes' as source_url,
                    NOW() as ingested_at_utc
                FROM games g
                WHERE g.game_date_local >= $1 
                  AND g.game_date_local <= $2
                  AND g.status = 'FINAL'
                  AND g.home_score IS NOT NULL 
                  AND g.away_score IS NOT NULL
                ON CONFLICT (game_id) DO UPDATE SET
                    q1_home_points = CASE 
                        WHEN excluded.q1_home_points IS DISTINCT FROM outcomes.q1_home_points 
                        THEN excluded.q1_home_points ELSE outcomes.q1_home_points END,
                    q1_away_points = CASE 
                        WHEN excluded.q1_away_points IS DISTINCT FROM outcomes.q1_away_points 
                        THEN excluded.q1_away_points ELSE outcomes.q1_away_points END,
                    final_home_points = excluded.final_home_points,
                    final_away_points = excluded.final_away_points,
                    total_points = excluded.total_points,
                    home_win = excluded.home_win,
                    margin = excluded.margin,
                    overtime_periods = excluded.overtime_periods,
                    source = excluded.source,
                    source_url = excluded.source_url,
                    ingested_at_utc = excluded.ingested_at_utc
                WHERE (
                    excluded.q1_home_points IS DISTINCT FROM outcomes.q1_home_points OR
                    excluded.q1_away_points IS DISTINCT FROM outcomes.q1_away_points OR
                    excluded.final_home_points IS DISTINCT FROM outcomes.final_home_points OR
                    excluded.final_away_points IS DISTINCT FROM outcomes.final_away_points OR
                    excluded.overtime_periods IS DISTINCT FROM outcomes.overtime_periods
                )
                """
                
                result = await conn.execute(query, start_date, end_date)
                
                # Extract count from result string like "INSERT 0 5" or "UPDATE 3"
                if result.startswith('INSERT'):
                    return int(result.split()[-1])
                elif result.startswith('UPDATE'):
                    return int(result.split()[1])
                else:
                    return 0
            else:
                # Dry run: just count potential updates
                count_query = """
                SELECT COUNT(*)
                FROM games g
                WHERE g.game_date_local >= $1 
                  AND g.game_date_local <= $2
                  AND g.status = 'FINAL'
                  AND g.home_score IS NOT NULL 
                  AND g.away_score IS NOT NULL
                """
                
                row = await conn.fetchrow(count_query, start_date, end_date)
                count = row[0] if row else 0
                logger.info("Dry run: would upsert outcomes", count=count)
                return count
                
        except Exception as e:
            logger.error("Failed to derive outcomes", error=str(e))
            raise
    
    async def _get_games_in_range(
        self,
        start_date: date,
        end_date: date,
        force: bool
    ) -> List[Dict[str, Any]]:
        """Get games in the specified date range."""
        try:
            conn = await get_connection()
            
            # Base query for games in range
            query = """
            SELECT game_id, game_date_local, status, season
            FROM games 
            WHERE game_date_local >= $1 
              AND game_date_local <= $2
            """
            
            # If not forcing, only get games that might need processing
            if not force:
                query += " AND status = 'FINAL'"
            
            query += " ORDER BY game_date_local, game_id"
            
            rows = await conn.fetch(query, start_date, end_date)
            
            games = []
            for row in rows:
                games.append({
                    'game_id': row['game_id'],
                    'game_date_local': row['game_date_local'],
                    'status': row['status'],
                    'season': row['season']
                })
            
            return games
            
        except Exception as e:
            logger.error("Failed to get games in range", 
                        start_date=start_date, end_date=end_date, error=str(e))
            return []