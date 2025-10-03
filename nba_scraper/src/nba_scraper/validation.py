"""Data quality validation with FK checks and orphaned record detection."""

from datetime import datetime, timedelta, UTC
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass

from .db import get_connection
from .nba_logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of a data validation check."""
    table_name: str
    check_type: str
    is_valid: bool
    record_count: int
    invalid_count: int
    issues: List[str]
    details: Dict[str, Any] = None


class DataQualityValidator:
    """Validates data quality with FK checks and orphaned record detection."""
    
    async def validate_all_tables(self, since_hours: int = 24) -> List[ValidationResult]:
        """Run all validation checks on recently ingested data.
        
        Args:
            since_hours: Only validate data ingested in the last N hours
            
        Returns:
            List of validation results
        """
        results = []
        cutoff_time = datetime.now(UTC) - timedelta(hours=since_hours)
        
        logger.info("Starting comprehensive data validation", since_hours=since_hours)
        
        # Core table FK validations
        results.extend(await self._validate_core_foreign_keys(cutoff_time))
        
        # Derived table FK validations  
        results.extend(await self._validate_derived_foreign_keys(cutoff_time))
        
        # Orphaned record detection
        results.extend(await self._detect_orphaned_records(cutoff_time))
        
        # Uniqueness constraint validation
        results.extend(await self._validate_uniqueness_constraints(cutoff_time))
        
        # Cross-table consistency checks
        results.extend(await self._validate_cross_table_consistency(cutoff_time))
        
        # PBP monotonicity validation (NEW)
        results.extend(await self._validate_pbp_monotonicity(cutoff_time))
        
        # Summarize results
        total_issues = sum(len(r.issues) for r in results)
        invalid_tables = sum(1 for r in results if not r.is_valid)
        
        logger.info("Data validation completed", 
                   total_checks=len(results),
                   invalid_tables=invalid_tables,
                   total_issues=total_issues)
        
        return results
    
    async def _validate_core_foreign_keys(self, cutoff_time: datetime) -> List[ValidationResult]:
        """Validate FK relationships in core tables."""
        results = []
        conn = await get_connection()
        
        # Check game_id_crosswalk references to games
        query = """
        SELECT COUNT(*) as total_count,
               COALESCE(SUM(CASE WHEN g.game_id IS NULL THEN 1 ELSE 0 END), 0) as invalid_count
        FROM game_id_crosswalk c
        LEFT JOIN games g ON c.game_id = g.game_id
        WHERE c.ingested_at_utc >= $1
        """
        
        row = await conn.fetchrow(query, cutoff_time)
        invalid_games = []
        
        if row['invalid_count'] > 0:
            # Get specific invalid game IDs
            invalid_query = """
            SELECT DISTINCT c.game_id
            FROM game_id_crosswalk c
            LEFT JOIN games g ON c.game_id = g.game_id
            WHERE g.game_id IS NULL AND c.ingested_at_utc >= $1
            LIMIT 10
            """
            invalid_rows = await conn.fetch(invalid_query, cutoff_time)
            invalid_games = [row['game_id'] for row in invalid_rows]
        
        results.append(ValidationResult(
            table_name="game_id_crosswalk",
            check_type="foreign_key_validation",
            is_valid=row['invalid_count'] == 0,
            record_count=row['total_count'],
            invalid_count=row['invalid_count'],
            issues=[f"Invalid game_id references: {invalid_games[:5]}"] if invalid_games else [],
            details={"invalid_game_ids": invalid_games}
        ))
        
        return results
    
    async def _validate_derived_foreign_keys(self, cutoff_time: datetime) -> List[ValidationResult]:
        """Validate FK relationships in derived analytics tables."""
        results = []
        conn = await get_connection()
        
        # Define derived tables that reference games
        derived_tables = [
            "q1_window_12_8",
            "early_shocks", 
            "schedule_travel",
            "outcomes",
            "ref_assignments",
            "ref_alternates",
            "starting_lineups",
            "injury_status",
            "pbp_events"
        ]
        
        for table in derived_tables:
            query = f"""
            SELECT COUNT(*) as total_count,
                   COALESCE(SUM(CASE WHEN g.game_id IS NULL THEN 1 ELSE 0 END), 0) as invalid_count
            FROM {table} t
            LEFT JOIN games g ON t.game_id = g.game_id
            WHERE t.ingested_at_utc >= $1
            """
            
            row = await conn.fetchrow(query, cutoff_time)
            invalid_games = []
            
            if row['invalid_count'] > 0:
                # Get specific invalid game IDs
                invalid_query = f"""
                SELECT DISTINCT t.game_id
                FROM {table} t
                LEFT JOIN games g ON t.game_id = g.game_id
                WHERE g.game_id IS NULL AND t.ingested_at_utc >= $1
                LIMIT 10
                """
                invalid_rows = await conn.fetch(invalid_query, cutoff_time)
                invalid_games = [row['game_id'] for row in invalid_rows]
            
            results.append(ValidationResult(
                table_name=table,
                check_type="foreign_key_validation",
                is_valid=row['invalid_count'] == 0,
                record_count=row['total_count'],
                invalid_count=row['invalid_count'],
                issues=[f"Invalid game_id references: {invalid_games[:5]}"] if invalid_games else [],
                details={"invalid_game_ids": invalid_games}
            ))
        
        return results
    
    async def _detect_orphaned_records(self, cutoff_time: datetime) -> List[ValidationResult]:
        """Detect orphaned records across the database."""
        results = []
        conn = await get_connection()
        
        # Check for games without any related data
        query = """
        SELECT g.game_id, g.season, g.game_date_local,
               CASE WHEN o.game_id IS NOT NULL THEN 1 ELSE 0 END as has_outcome,
               CASE WHEN p.game_id IS NOT NULL THEN 1 ELSE 0 END as has_pbp,
               CASE WHEN r.game_id IS NOT NULL THEN 1 ELSE 0 END as has_refs,
               CASE WHEN l.game_id IS NOT NULL THEN 1 ELSE 0 END as has_lineups
        FROM games g
        LEFT JOIN outcomes o ON g.game_id = o.game_id
        LEFT JOIN (SELECT DISTINCT game_id FROM pbp_events) p ON g.game_id = p.game_id
        LEFT JOIN (SELECT DISTINCT game_id FROM ref_assignments) r ON g.game_id = r.game_id
        LEFT JOIN (SELECT DISTINCT game_id FROM starting_lineups) l ON g.game_id = l.game_id
        WHERE g.ingested_at_utc >= $1
          AND g.status NOT IN ('SCHEDULED', 'POSTPONED')
          AND (o.game_id IS NULL OR p.game_id IS NULL OR r.game_id IS NULL OR l.game_id IS NULL)
        ORDER BY g.game_date_local DESC
        LIMIT 20
        """
        
        orphaned_games = await conn.fetch(query, cutoff_time)
        issues = []
        
        for game in orphaned_games:
            missing_data = []
            if not game['has_outcome']:
                missing_data.append('outcomes')
            if not game['has_pbp']:
                missing_data.append('pbp_events')
            if not game['has_refs']:
                missing_data.append('ref_assignments')
            if not game['has_lineups']:
                missing_data.append('starting_lineups')
            
            issues.append(f"Game {game['game_id']} ({game['game_date_local']}) missing: {', '.join(missing_data)}")
        
        results.append(ValidationResult(
            table_name="games",
            check_type="orphaned_records",
            is_valid=len(orphaned_games) == 0,
            record_count=len(orphaned_games),
            invalid_count=len(orphaned_games),
            issues=issues[:10],  # Limit to first 10 issues
            details={"orphaned_games": [dict(game) for game in orphaned_games]}
        ))
        
        return results
    
    async def _validate_uniqueness_constraints(self, cutoff_time: datetime) -> List[ValidationResult]:
        """Validate uniqueness constraints beyond primary keys."""
        results = []
        conn = await get_connection()
        
        # Check for duplicate bref_game_ids in games table
        query = """
        SELECT bref_game_id, COUNT(*) as count
        FROM games
        WHERE bref_game_id IS NOT NULL 
          AND ingested_at_utc >= $1
        GROUP BY bref_game_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
        """
        
        duplicates = await conn.fetch(query, cutoff_time)
        duplicate_issues = [f"Duplicate bref_game_id: {row['bref_game_id']} ({row['count']} occurrences)" 
                           for row in duplicates]
        
        results.append(ValidationResult(
            table_name="games",
            check_type="uniqueness_validation",
            is_valid=len(duplicates) == 0,
            record_count=len(duplicates),
            invalid_count=len(duplicates),
            issues=duplicate_issues,
            details={"duplicate_bref_ids": [dict(row) for row in duplicates]}
        ))
        
        # Check for duplicate referee assignments (same referee, different roles)
        query = """
        SELECT game_id, referee_name_slug, COUNT(*) as count,
               STRING_AGG(role, ', ') as roles
        FROM ref_assignments
        WHERE ingested_at_utc >= $1
        GROUP BY game_id, referee_name_slug
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
        """
        
        ref_duplicates = await conn.fetch(query, cutoff_time)
        ref_issues = [f"Referee {row['referee_name_slug']} has multiple roles in game {row['game_id']}: {row['roles']}" 
                     for row in ref_duplicates]
        
        results.append(ValidationResult(
            table_name="ref_assignments",
            check_type="uniqueness_validation",
            is_valid=len(ref_duplicates) == 0,
            record_count=len(ref_duplicates),
            invalid_count=len(ref_duplicates),
            issues=ref_issues,
            details={"duplicate_ref_assignments": [dict(row) for row in ref_duplicates]}
        ))
        
        return results
    
    async def _validate_cross_table_consistency(self, cutoff_time: datetime) -> List[ValidationResult]:
        """Validate consistency across related tables."""
        results = []
        conn = await get_connection()
        
        # Check team tricode consistency between games and derived tables
        query = """
        SELECT g.game_id, g.home_team_tricode as game_home, g.away_team_tricode as game_away,
               q.home_team_tricode as q1_home, q.away_team_tricode as q1_away
        FROM games g
        INNER JOIN q1_window_12_8 q ON g.game_id = q.game_id
        WHERE g.ingested_at_utc >= $1
          AND (g.home_team_tricode != q.home_team_tricode 
               OR g.away_team_tricode != q.away_team_tricode)
        LIMIT 10
        """
        
        inconsistent_teams = await conn.fetch(query, cutoff_time)
        team_issues = [f"Game {row['game_id']}: teams mismatch - games({row['game_home']} vs {row['game_away']}) vs q1_window({row['q1_home']} vs {row['q1_away']})" 
                      for row in inconsistent_teams]
        
        results.append(ValidationResult(
            table_name="q1_window_12_8",
            check_type="cross_table_consistency",
            is_valid=len(inconsistent_teams) == 0,
            record_count=len(inconsistent_teams),
            invalid_count=len(inconsistent_teams),
            issues=team_issues,
            details={"inconsistent_teams": [dict(row) for row in inconsistent_teams]}
        ))
        
        # Check game date consistency between games and outcomes
        query = """
        SELECT g.game_id, g.game_date_local, g.status,
               CASE WHEN o.game_id IS NULL THEN 'missing' ELSE 'present' END as outcome_status
        FROM games g
        LEFT JOIN outcomes o ON g.game_id = o.game_id
        WHERE g.ingested_at_utc >= $1
          AND g.status = 'FINAL'
          AND o.game_id IS NULL
        LIMIT 10
        """
        
        missing_outcomes = await conn.fetch(query, cutoff_time)
        outcome_issues = [f"Final game {row['game_id']} ({row['game_date_local']}) missing outcome data" 
                         for row in missing_outcomes]
        
        results.append(ValidationResult(
            table_name="outcomes",
            check_type="cross_table_consistency", 
            is_valid=len(missing_outcomes) == 0,
            record_count=len(missing_outcomes),
            invalid_count=len(missing_outcomes),
            issues=outcome_issues,
            details={"missing_outcomes": [dict(row) for row in missing_outcomes]}
        ))
        
        return results
    
    async def _validate_pbp_monotonicity(self, cutoff_time: datetime) -> List[ValidationResult]:
        """Validate play-by-play event sequence monotonicity and clock progression.
        
        Checks for:
        1. Event index gaps (missing event_idx values)
        2. Event index overlaps (duplicate event_idx values)
        3. Clock progression issues (time moving backwards)
        4. Period boundary violations
        """
        results = []
        conn = await get_connection()
        
        logger.info("Starting PBP monotonicity validation")
        
        # 1. Check for event index gaps within periods
        gap_query = """
        WITH event_gaps AS (
            SELECT 
                game_id,
                period,
                event_idx,
                LAG(event_idx) OVER (PARTITION BY game_id, period ORDER BY event_idx) as prev_event_idx,
                event_idx - LAG(event_idx) OVER (PARTITION BY game_id, period ORDER BY event_idx) as gap_size
            FROM pbp_events
            WHERE ingested_at_utc >= $1
        )
        SELECT 
            game_id,
            period,
            prev_event_idx,
            event_idx,
            gap_size
        FROM event_gaps
        WHERE gap_size > 1  -- Gap larger than 1 indicates missing events
        ORDER BY game_id, period, event_idx
        LIMIT 50
        """
        
        gaps = await conn.fetch(gap_query, cutoff_time)
        gap_issues = []
        
        for gap in gaps:
            gap_issues.append(
                f"Game {gap['game_id']} Q{gap['period']}: Missing events between idx {gap['prev_event_idx']} and {gap['event_idx']} (gap of {gap['gap_size']-1})"
            )
        
        results.append(ValidationResult(
            table_name="pbp_events",
            check_type="pbp_monotonicity_gaps",
            is_valid=len(gaps) == 0,
            record_count=len(gaps),
            invalid_count=len(gaps),
            issues=gap_issues[:10],  # Limit to first 10 issues
            details={"gaps": [dict(gap) for gap in gaps]}
        ))
        
        # 2. Check for duplicate event indices (overlaps)
        overlap_query = """
        SELECT 
            game_id,
            period, 
            event_idx,
            COUNT(*) as duplicate_count,
            STRING_AGG(DISTINCT event_type, ', ') as event_types
        FROM pbp_events
        WHERE ingested_at_utc >= $1
        GROUP BY game_id, period, event_idx
        HAVING COUNT(*) > 1
        ORDER BY game_id, period, event_idx
        LIMIT 50
        """
        
        overlaps = await conn.fetch(overlap_query, cutoff_time)
        overlap_issues = []
        
        for overlap in overlaps:
            overlap_issues.append(
                f"Game {overlap['game_id']} Q{overlap['period']}: Duplicate event_idx {overlap['event_idx']} ({overlap['duplicate_count']} occurrences) - types: {overlap['event_types']}"
            )
        
        results.append(ValidationResult(
            table_name="pbp_events",
            check_type="pbp_monotonicity_overlaps",
            is_valid=len(overlaps) == 0,
            record_count=len(overlaps),
            invalid_count=len(overlaps),
            issues=overlap_issues[:10],
            details={"overlaps": [dict(overlap) for overlap in overlaps]}
        ))
        
        # 3. Check for clock progression issues (time moving backwards)
        clock_query = """
        WITH clock_progression AS (
            SELECT 
                game_id,
                period,
                event_idx,
                seconds_elapsed,
                LAG(seconds_elapsed) OVER (PARTITION BY game_id, period ORDER BY event_idx) as prev_seconds,
                seconds_elapsed - LAG(seconds_elapsed) OVER (PARTITION BY game_id, period ORDER BY event_idx) as time_diff,
                time_remaining,
                LAG(time_remaining) OVER (PARTITION BY game_id, period ORDER BY event_idx) as prev_time_remaining
            FROM pbp_events
            WHERE ingested_at_utc >= $1
              AND seconds_elapsed IS NOT NULL
        )
        SELECT 
            game_id,
            period,
            event_idx,
            seconds_elapsed,
            prev_seconds,
            time_diff,
            time_remaining,
            prev_time_remaining
        FROM clock_progression
        WHERE time_diff < -5  -- Allow small negative values for simultaneous events, but flag large backward jumps
        ORDER BY game_id, period, event_idx
        LIMIT 50
        """
        
        clock_issues_raw = await conn.fetch(clock_query, cutoff_time)
        clock_issues = []
        
        for issue in clock_issues_raw:
            clock_issues.append(
                f"Game {issue['game_id']} Q{issue['period']}: Clock moved backwards at event {issue['event_idx']} - from {issue['prev_seconds']}s to {issue['seconds_elapsed']}s (diff: {issue['time_diff']}s)"
            )
        
        results.append(ValidationResult(
            table_name="pbp_events",
            check_type="pbp_clock_progression",
            is_valid=len(clock_issues_raw) == 0,
            record_count=len(clock_issues_raw),
            invalid_count=len(clock_issues_raw),
            issues=clock_issues[:10],
            details={"clock_issues": [dict(issue) for issue in clock_issues_raw]}
        ))
        
        # 4. Check for period boundary issues
        period_query = """
        WITH period_boundaries AS (
            SELECT 
                game_id,
                period,
                MIN(event_idx) as first_event_idx,
                MAX(event_idx) as last_event_idx,
                MIN(seconds_elapsed) as period_start_seconds,
                MAX(seconds_elapsed) as period_end_seconds,
                COUNT(*) as event_count
            FROM pbp_events
            WHERE ingested_at_utc >= $1
              AND period BETWEEN 1 AND 4  -- Regular periods only
            GROUP BY game_id, period
        ),
        period_issues AS (
            SELECT 
                pb1.game_id,
                pb1.period,
                pb1.last_event_idx,
                pb1.period_end_seconds,
                pb2.period as next_period,
                pb2.first_event_idx as next_first_idx,
                pb2.period_start_seconds as next_start_seconds
            FROM period_boundaries pb1
            LEFT JOIN period_boundaries pb2 ON pb1.game_id = pb2.game_id AND pb2.period = pb1.period + 1
            WHERE pb2.period IS NOT NULL  -- Only check when next period exists
              AND (
                pb2.first_event_idx <= pb1.last_event_idx  -- Next period starts before current ends
                OR pb2.period_start_seconds < pb1.period_end_seconds  -- Time overlap
              )
        )
        SELECT * FROM period_issues
        ORDER BY game_id, period
        LIMIT 20
        """
        
        period_issues_raw = await conn.fetch(period_query, cutoff_time)
        period_issues = []
        
        for issue in period_issues_raw:
            period_issues.append(
                f"Game {issue['game_id']}: Period boundary issue between Q{issue['period']} and Q{issue['next_period']} - event indices overlap or time regression"
            )
        
        results.append(ValidationResult(
            table_name="pbp_events",
            check_type="pbp_period_boundaries",
            is_valid=len(period_issues_raw) == 0,
            record_count=len(period_issues_raw),
            invalid_count=len(period_issues_raw),
            issues=period_issues[:10],
            details={"period_issues": [dict(issue) for issue in period_issues_raw]}
        ))
        
        # 5. Summary statistics for PBP completeness
        stats_query = """
        SELECT 
            COUNT(DISTINCT game_id) as games_with_pbp,
            COUNT(*) as total_events,
            AVG(CASE WHEN period = 1 THEN 1.0 ELSE 0.0 END) as q1_coverage,
            AVG(CASE WHEN period = 4 THEN 1.0 ELSE 0.0 END) as q4_coverage,
            COUNT(CASE WHEN seconds_elapsed IS NULL THEN 1 END) as events_missing_time
        FROM pbp_events
        WHERE ingested_at_utc >= $1
        """
        
        stats = await conn.fetchrow(stats_query, cutoff_time)
        
        completeness_issues = []
        if stats['events_missing_time'] > 0:
            completeness_issues.append(f"{stats['events_missing_time']} events missing seconds_elapsed timestamps")
        
        if stats['q1_coverage'] < 0.8:  # Less than 80% of games have Q1 events
            completeness_issues.append(f"Low Q1 coverage: {stats['q1_coverage']:.2%} of games")
        
        if stats['q4_coverage'] < 0.8:  # Less than 80% of games have Q4 events  
            completeness_issues.append(f"Low Q4 coverage: {stats['q4_coverage']:.2%} of games")
        
        results.append(ValidationResult(
            table_name="pbp_events",
            check_type="pbp_completeness",
            is_valid=len(completeness_issues) == 0,
            record_count=stats['total_events'] if stats['total_events'] else 0,
            invalid_count=stats['events_missing_time'] if stats['events_missing_time'] else 0,
            issues=completeness_issues,
            details={
                "stats": dict(stats) if stats else {},
                "games_analyzed": stats['games_with_pbp'] if stats else 0
            }
        ))
        
        logger.info("PBP monotonicity validation completed", 
                   gaps=len(gaps),
                   overlaps=len(overlaps), 
                   clock_issues=len(clock_issues_raw),
                   period_issues=len(period_issues_raw))
        
        return results
    
    async def validate_before_insert(self, table_name: str, records: List[Dict]) -> Tuple[List[Dict], List[str]]:
        """Validate records before insertion to catch FK violations early.
        
        Args:
            table_name: Name of table to insert into
            records: List of record dictionaries to validate
            
        Returns:
            Tuple of (valid_records, error_messages)
        """
        if not records:
            return [], []
        
        conn = await get_connection()
        valid_records = []
        errors = []
        
        # Extract unique game_ids from the records
        game_ids = set()
        for record in records:
            if 'game_id' in record and record['game_id']:
                game_ids.add(record['game_id'])
        
        if not game_ids:
            # No game_ids to validate, return all records
            return records, []
        
        # Check which game_ids actually exist
        query = "SELECT game_id FROM games WHERE game_id = ANY($1)"
        existing_games = await conn.fetch(query, list(game_ids))
        existing_game_ids = {row['game_id'] for row in existing_games}
        
        missing_game_ids = game_ids - existing_game_ids
        
        if missing_game_ids:
            errors.append(f"Missing game_id references for {table_name}: {list(missing_game_ids)[:5]}")
        
        # Filter records to only include those with valid game_ids
        for record in records:
            game_id = record.get('game_id')
            if not game_id or game_id in existing_game_ids:
                valid_records.append(record)
            else:
                logger.warning("Skipping record with invalid game_id", 
                             table=table_name, 
                             game_id=game_id)
        
        logger.info("Pre-insert validation completed",
                   table=table_name,
                   total_records=len(records),
                   valid_records=len(valid_records),
                   invalid_records=len(records) - len(valid_records))
        
        return valid_records, errors
    
    def get_validation_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate a summary of validation results.
        
        Args:
            results: List of validation results
            
        Returns:
            Summary dictionary with key metrics
        """
        if not results:
            return {
                'validation_timestamp': datetime.now(UTC).isoformat(),
                'total_checks': 0,
                'failed_checks': 0,
                'success_rate': 1.0,
                'total_issues': 0,
                'total_records_checked': 0,
                'total_invalid_records': 0,
                'data_quality_score': 1.0,
                'failed_tables': []
            }
        
        failed_results = [r for r in results if not r.is_valid]
        total_issues = sum(len(r.issues) for r in results)
        total_records = sum(r.record_count for r in results)
        total_invalid = sum(r.invalid_count for r in results)
        
        success_rate = (len(results) - len(failed_results)) / len(results)
        
        # Calculate data quality score (0-1, higher is better)
        if total_records == 0:
            data_quality_score = 1.0
        else:
            data_quality_score = max(0.0, 1.0 - (total_invalid / total_records))
        
        return {
            'validation_timestamp': datetime.now(UTC).isoformat(),
            'total_checks': len(results),
            'failed_checks': len(failed_results),
            'success_rate': success_rate,
            'total_issues': total_issues,
            'total_records_checked': total_records,
            'total_invalid_records': total_invalid,
            'data_quality_score': data_quality_score,
            'failed_tables': [r.table_name for r in failed_results]
        }