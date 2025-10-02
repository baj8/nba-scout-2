"""Pipeline for data quality validation and integrity checks."""

import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass

from ..validation import DataQualityValidator
from ..logging import get_logger
from ..db import get_connection

logger = get_logger(__name__)


@dataclass
class ValidationPipelineResult:
    """Result of a validation pipeline execution."""
    success: bool
    checks_run: int
    checks_passed: int
    checks_failed: int
    validation_results: Dict[str, Dict[str, Any]]
    duration_seconds: float
    error: Optional[str] = None


class ValidationPipeline:
    """Pipeline for comprehensive data quality validation."""
    
    def __init__(self):
        """Initialize validation pipeline."""
        self.validator = DataQualityValidator()
    
    async def validate_all(
        self, 
        since_date: date, 
        verbose: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """Run all validation checks since a given date.
        
        Args:
            since_date: Date to start validation from
            verbose: Whether to include detailed validation output
            
        Returns:
            Dictionary of validation results by check name
        """
        start_time = datetime.utcnow()
        results = {}
        
        try:
            logger.info("Starting comprehensive data validation", since_date=since_date, verbose=verbose)
            
            # Convert date to datetime for validation functions
            cutoff_time = datetime.combine(since_date, datetime.min.time())
            
            # Run FK validation checks
            logger.info("Running FK validation checks")
            fk_results = await self.validator.validate_foreign_keys(cutoff_time)
            for check_name, result in fk_results.items():
                results[f"fk_{check_name}"] = {
                    'passed': result.passed,
                    'details': f"{len(result.issues)} issues found" if result.issues else "No issues",
                    'issues': result.issues if verbose else []
                }
            
            # Run uniqueness validation checks
            logger.info("Running uniqueness validation checks")
            uniqueness_results = await self.validator.validate_uniqueness_constraints(cutoff_time)
            for check_name, result in uniqueness_results.items():
                results[f"uniqueness_{check_name}"] = {
                    'passed': result.passed,
                    'details': f"{len(result.issues)} issues found" if result.issues else "No issues",
                    'issues': result.issues if verbose else []
                }
            
            # Run PBP monotonicity validation
            logger.info("Running PBP monotonicity validation")
            pbp_results = await self.validator._validate_pbp_monotonicity(cutoff_time)
            for i, result in enumerate(pbp_results):
                check_name = f"pbp_{['gaps', 'overlaps', 'clock', 'periods', 'completeness'][i]}"
                results[check_name] = {
                    'passed': result.passed,
                    'details': f"{len(result.issues)} issues found" if result.issues else "No issues",
                    'issues': result.issues if verbose else []
                }
            
            # Run crosswalk validation
            logger.info("Running crosswalk validation")
            try:
                crosswalk_result = await self.validator.validate_crosswalk_completeness(cutoff_time)
                results['crosswalk_completeness'] = {
                    'passed': crosswalk_result.passed,
                    'details': f"{len(crosswalk_result.issues)} issues found" if crosswalk_result.issues else "No issues",
                    'issues': crosswalk_result.issues if verbose else []
                }
            except Exception as e:
                results['crosswalk_completeness'] = {
                    'passed': False,
                    'details': f"Validation failed: {str(e)}",
                    'issues': [str(e)]
                }
            
            # Run data completeness checks
            logger.info("Running data completeness checks")
            completeness_results = await self._validate_data_completeness(since_date)
            results.update(completeness_results)
            
            # Run data freshness checks
            logger.info("Running data freshness checks")
            freshness_results = await self._validate_data_freshness()
            results.update(freshness_results)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info("Data validation completed",
                       total_checks=len(results),
                       passed_checks=sum(1 for r in results.values() if r['passed']),
                       failed_checks=sum(1 for r in results.values() if not r['passed']),
                       duration=duration)
            
        except Exception as e:
            logger.error("Data validation failed", error=str(e))
            results['validation_error'] = {
                'passed': False,
                'details': f"Validation pipeline failed: {str(e)}",
                'issues': [str(e)]
            }
        
        return results
    
    async def _validate_data_completeness(self, since_date: date) -> Dict[str, Dict[str, Any]]:
        """Validate data completeness for recent games."""
        results = {}
        
        try:
            conn = await get_connection()
            
            # Check game completeness
            game_query = """
            WITH game_stats AS (
                SELECT 
                    COUNT(*) as total_games,
                    COUNT(CASE WHEN status = 'FINAL' THEN 1 END) as final_games,
                    COUNT(CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 1 END) as games_with_scores,
                    COUNT(CASE WHEN q1_home_points IS NOT NULL AND q1_away_points IS NOT NULL THEN 1 END) as games_with_q1_scores
                FROM games 
                WHERE game_date_local >= $1
            )
            SELECT *,
                   CASE WHEN total_games > 0 THEN games_with_scores::float / total_games ELSE 0 END as score_completeness,
                   CASE WHEN final_games > 0 THEN games_with_q1_scores::float / final_games ELSE 0 END as q1_completeness
            FROM game_stats
            """
            
            row = await conn.fetchrow(game_query, since_date)
            
            # Game completeness check
            score_completeness = float(row['score_completeness']) if row else 0
            q1_completeness = float(row['q1_completeness']) if row else 0
            
            results['game_score_completeness'] = {
                'passed': score_completeness >= 0.95,  # 95% threshold
                'details': f"Score completeness: {score_completeness:.1%} ({row['games_with_scores']}/{row['total_games']} games)" if row else "No games found",
                'issues': [] if score_completeness >= 0.95 else [f"Low score completeness: {score_completeness:.1%}"]
            }
            
            results['game_q1_completeness'] = {
                'passed': q1_completeness >= 0.80,  # 80% threshold for Q1 scores
                'details': f"Q1 completeness: {q1_completeness:.1%} ({row['games_with_q1_scores']}/{row['final_games']} final games)" if row else "No final games found",
                'issues': [] if q1_completeness >= 0.80 else [f"Low Q1 completeness: {q1_completeness:.1%}"]
            }
            
            # Check PBP completeness
            pbp_query = """
            WITH pbp_stats AS (
                SELECT 
                    COUNT(DISTINCT g.game_id) as total_games,
                    COUNT(DISTINCT p.game_id) as games_with_pbp,
                    COUNT(p.event_idx) as total_events,
                    COUNT(CASE WHEN p.clock_time IS NOT NULL THEN 1 END) as events_with_time
                FROM games g
                LEFT JOIN pbp_events p ON g.game_id = p.game_id 
                WHERE g.game_date_local >= $1 AND g.status = 'FINAL'
            )
            SELECT *,
                   CASE WHEN total_games > 0 THEN games_with_pbp::float / total_games ELSE 0 END as pbp_game_coverage,
                   CASE WHEN total_events > 0 THEN events_with_time::float / total_events ELSE 0 END as pbp_time_coverage
            FROM pbp_stats
            """
            
            pbp_row = await conn.fetchrow(pbp_query, since_date)
            
            pbp_game_coverage = float(pbp_row['pbp_game_coverage']) if pbp_row else 0
            pbp_time_coverage = float(pbp_row['pbp_time_coverage']) if pbp_row else 0
            
            results['pbp_game_coverage'] = {
                'passed': pbp_game_coverage >= 0.90,  # 90% threshold
                'details': f"PBP game coverage: {pbp_game_coverage:.1%} ({pbp_row['games_with_pbp']}/{pbp_row['total_games']} games)" if pbp_row else "No games found",
                'issues': [] if pbp_game_coverage >= 0.90 else [f"Low PBP game coverage: {pbp_game_coverage:.1%}"]
            }
            
            results['pbp_time_coverage'] = {
                'passed': pbp_time_coverage >= 0.95,  # 95% threshold for event timestamps
                'details': f"PBP time coverage: {pbp_time_coverage:.1%} ({pbp_row['events_with_time']}/{pbp_row['total_events']} events)" if pbp_row else "No events found",
                'issues': [] if pbp_time_coverage >= 0.95 else [f"Low PBP time coverage: {pbp_time_coverage:.1%}"]
            }
            
        except Exception as e:
            logger.error("Failed to validate data completeness", error=str(e))
            results['completeness_check_error'] = {
                'passed': False,
                'details': f"Completeness check failed: {str(e)}",
                'issues': [str(e)]
            }
        
        return results
    
    async def _validate_data_freshness(self) -> Dict[str, Dict[str, Any]]:
        """Validate data freshness - check if recent data is being ingested."""
        results = {}
        
        try:
            conn = await get_connection()
            
            # Check when data was last ingested
            freshness_query = """
            WITH freshness_stats AS (
                SELECT 
                    'games' as table_name,
                    MAX(ingested_at_utc) as last_ingested,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM games
                UNION ALL
                SELECT 
                    'pbp_events' as table_name,
                    MAX(ingested_at_utc) as last_ingested,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM pbp_events
                UNION ALL
                SELECT 
                    'outcomes' as table_name,
                    MAX(ingested_at_utc) as last_ingested,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN ingested_at_utc >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM outcomes
            )
            SELECT * FROM freshness_stats ORDER BY table_name
            """
            
            rows = await conn.fetch(freshness_query)
            
            for row in rows:
                table_name = row['table_name']
                last_ingested = row['last_ingested']
                recent_records = row['recent_records']
                
                if last_ingested:
                    hours_since_last = (datetime.utcnow() - last_ingested).total_seconds() / 3600
                    is_fresh = hours_since_last <= 48  # 48 hour threshold
                    
                    results[f'{table_name}_freshness'] = {
                        'passed': is_fresh,
                        'details': f"Last ingested: {hours_since_last:.1f} hours ago, Recent records: {recent_records}",
                        'issues': [] if is_fresh else [f"Data not fresh: {hours_since_last:.1f} hours since last ingestion"]
                    }
                else:
                    results[f'{table_name}_freshness'] = {
                        'passed': False,
                        'details': "No data found in table",
                        'issues': [f"No data found in {table_name} table"]
                    }
                    
        except Exception as e:
            logger.error("Failed to validate data freshness", error=str(e))
            results['freshness_check_error'] = {
                'passed': False,
                'details': f"Freshness check failed: {str(e)}",
                'issues': [str(e)]
            }
        
        return results
    
    async def run_pipeline_validation(
        self,
        since_date: date,
        verbose: bool = False
    ) -> ValidationPipelineResult:
        """Run the full validation pipeline and return structured results.
        
        Args:
            since_date: Date to start validation from
            verbose: Whether to include detailed output
            
        Returns:
            ValidationPipelineResult with execution summary
        """
        start_time = datetime.utcnow()
        
        result = ValidationPipelineResult(
            success=False,
            checks_run=0,
            checks_passed=0,
            checks_failed=0,
            validation_results={},
            duration_seconds=0
        )
        
        try:
            # Run all validations
            validation_results = await self.validate_all(since_date, verbose)
            
            # Aggregate results
            result.validation_results = validation_results
            result.checks_run = len(validation_results)
            result.checks_passed = sum(1 for r in validation_results.values() if r['passed'])
            result.checks_failed = result.checks_run - result.checks_passed
            result.success = result.checks_failed == 0
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("Validation pipeline completed",
                       checks_run=result.checks_run,
                       checks_passed=result.checks_passed,
                       checks_failed=result.checks_failed,
                       success=result.success,
                       duration=result.duration_seconds)
            
        except Exception as e:
            result.error = str(e)
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            logger.error("Validation pipeline failed", error=str(e))
        
        return result