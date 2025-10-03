"""Derived analytics data loader with idempotent upserts."""

from datetime import datetime
from typing import List

from ..models.derived_rows import Q1WindowRow, EarlyShockRow, ScheduleTravelRow
from ..db import get_connection
from ..nba_logging import get_logger
from ..validation import DataQualityValidator
from ..performance import (
    get_performance_connection, bulk_optimizer, parallel_processor,
    optimize_query, PerformanceOptimizedConnection
)

logger = get_logger(__name__)


class DerivedLoader:
    """Loader for derived analytics data with diff-aware upserts and performance optimization."""
    
    def __init__(self):
        """Initialize the loader with validation."""
        self.validator = DataQualityValidator()
    
    @optimize_query("bulk_upsert")
    async def upsert_q1_windows(self, windows: List[Q1WindowRow]) -> int:
        """Upsert Q1 window analytics (12:00 to 8:00) with FK validation and bulk optimization.
        
        Args:
            windows: List of Q1WindowRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not windows:
            return 0
        
        # Convert to dict format for validation
        records = [
            {
                'game_id': window.game_id,
                'home_team_tricode': window.home_team_tricode,
                'away_team_tricode': window.away_team_tricode,
            }
            for window in windows
        ]
        
        # Validate before insert
        valid_records, errors = await self.validator.validate_before_insert('q1_window_12_8', records)
        
        if errors:
            logger.warning("FK validation errors for Q1 windows", errors=errors)
        
        if len(valid_records) < len(records):
            logger.warning("Some Q1 window records failed validation",
                          total=len(records),
                          valid=len(valid_records),
                          invalid=len(records) - len(valid_records))
            
            # Filter original windows to match valid records
            valid_game_ids = {record['game_id'] for record in valid_records}
            windows = [w for w in windows if w.game_id in valid_game_ids]
        
        if not windows:
            logger.warning("No valid Q1 window records to insert after FK validation")
            return 0
        
        # Use performance-optimized connection and bulk operations
        async with get_performance_connection() as conn:
            # Prepare data for bulk upsert
            columns = [
                'game_id', 'home_team_tricode', 'away_team_tricode', 'possessions_elapsed',
                'pace48_actual', 'pace48_expected', 'home_efg_actual', 'home_efg_expected',
                'away_efg_actual', 'away_efg_expected', 'home_to_rate', 'away_to_rate',
                'home_ft_rate', 'away_ft_rate', 'home_orb_pct', 'home_drb_pct',
                'away_orb_pct', 'away_drb_pct', 'bonus_time_home_sec', 'bonus_time_away_sec',
                'transition_rate', 'early_clock_rate', 'source', 'source_url', 'ingested_at_utc'
            ]
            
            data = []
            for window in windows:
                data.append((
                    window.game_id,
                    window.home_team_tricode,
                    window.away_team_tricode,
                    window.possessions_elapsed,
                    window.pace48_actual,
                    window.pace48_expected,
                    window.home_efg_actual,
                    window.home_efg_expected,
                    window.away_efg_actual,
                    window.away_efg_expected,
                    window.home_to_rate,
                    window.away_to_rate,
                    window.home_ft_rate,
                    window.away_ft_rate,
                    window.home_orb_pct,
                    window.home_drb_pct,
                    window.away_orb_pct,
                    window.away_drb_pct,
                    window.bonus_time_home_sec,
                    window.bonus_time_away_sec,
                    window.transition_rate,
                    window.early_clock_rate,
                    window.source,
                    window.source_url,
                    datetime.utcnow(),
                ))
            
            # Use bulk optimizer for efficient upserts
            updated_count = await bulk_optimizer.bulk_upsert(
                connection=conn,
                table_name='q1_window_12_8',
                columns=columns,
                data=data,
                conflict_columns=['game_id'],
                update_columns=[col for col in columns if col not in ['game_id', 'ingested_at_utc']]
            )
            
            logger.info("Upserted Q1 window analytics", total=len(windows), updated=updated_count)
            return updated_count

    @optimize_query("bulk_upsert")
    async def upsert_early_shocks(self, shocks: List[EarlyShockRow]) -> int:
        """Upsert early shock events with FK validation and bulk optimization.
        
        Args:
            shocks: List of EarlyShockRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not shocks:
            return 0
        
        # Convert to dict format for validation
        records = [{'game_id': shock.game_id} for shock in shocks]
        
        # Validate before insert
        valid_records, errors = await self.validator.validate_before_insert('early_shocks', records)
        
        if errors:
            logger.warning("FK validation errors for early shocks", errors=errors)
        
        if len(valid_records) < len(records):
            logger.warning("Some early shock records failed validation",
                          total=len(records),
                          valid=len(valid_records),
                          invalid=len(records) - len(valid_records))
            
            # Filter original shocks to match valid records
            valid_game_ids = {record['game_id'] for record in valid_records}
            shocks = [s for s in shocks if s.game_id in valid_game_ids]
        
        if not shocks:
            logger.warning("No valid early shock records to insert after FK validation")
            return 0
        
        # Process shocks in parallel if dataset is large
        if len(shocks) > 500:
            async def process_shock_chunk(shock_chunk: List[EarlyShockRow]) -> List[tuple]:
                """Process a chunk of shocks into database tuples."""
                chunk_data = []
                for shock in shock_chunk:
                    # Map our EarlyShockType enum to database values
                    event_type_map = {
                        "TWO_PF_EARLY": "EARLY_FOUL_TROUBLE",
                        "TECH": "TECHNICAL",
                        "FLAGRANT": "FLAGRANT", 
                        "INJURY_LEAVE": "INJURY_EXIT"
                    }
                    
                    # Convert clock format back to time_remaining (MM:SS)
                    time_remaining = self._convert_clock_to_time_remaining(shock.clock_hhmmss)
                    
                    # Convert clock to seconds_elapsed 
                    seconds_elapsed = self._convert_clock_to_seconds_elapsed(shock.clock_hhmmss)
                    
                    # Determine severity based on shock type
                    severity = self._determine_severity(shock.shock_type, shock.notes)
                    
                    chunk_data.append((
                        shock.game_id,
                        event_type_map.get(shock.shock_type.value, shock.shock_type.value),
                        shock.period,
                        time_remaining,
                        seconds_elapsed,
                        shock.player_slug,
                        shock.player_slug.replace('_', ' ').title() if shock.player_slug else None,
                        shock.team_tricode,
                        severity,
                        shock.immediate_sub,
                        shock.notes,
                        shock.source,
                        shock.source_url,
                        datetime.utcnow(),
                    ))
                
                return chunk_data
            
            # Process in parallel
            processed_chunks = await parallel_processor.process_parallel(
                items=shocks,
                processor_func=process_shock_chunk,
                chunk_size=100
            )
            
            # Flatten results
            data = []
            for chunk in processed_chunks:
                data.extend(chunk)
        else:
            # Process serially for smaller datasets
            data = []
            for shock in shocks:
                # Map our EarlyShockType enum to database values
                event_type_map = {
                    "TWO_PF_EARLY": "EARLY_FOUL_TROUBLE",
                    "TECH": "TECHNICAL",
                    "FLAGRANT": "FLAGRANT", 
                    "INJURY_LEAVE": "INJURY_EXIT"
                }
                
                # Convert clock format back to time_remaining (MM:SS)
                time_remaining = self._convert_clock_to_time_remaining(shock.clock_hhmmss)
                
                # Convert clock to seconds_elapsed 
                seconds_elapsed = self._convert_clock_to_seconds_elapsed(shock.clock_hhmmss)
                
                # Determine severity based on shock type
                severity = self._determine_severity(shock.shock_type, shock.notes)
                
                data.append((
                    shock.game_id,
                    event_type_map.get(shock.shock_type.value, shock.shock_type.value),
                    shock.period,
                    time_remaining,
                    seconds_elapsed,
                    shock.player_slug,
                    shock.player_slug.replace('_', ' ').title() if shock.player_slug else None,
                    shock.team_tricode,
                    severity,
                    shock.immediate_sub,
                    shock.notes,
                    shock.source,
                    shock.source_url,
                    datetime.utcnow(),
                ))
        
        # Use performance-optimized connection and bulk operations
        async with get_performance_connection() as conn:
            columns = [
                'game_id', 'event_type', 'period', 'time_remaining', 'seconds_elapsed',
                'player_name_slug', 'player_display_name', 'team_tricode', 'severity',
                'immediate_sub', 'description', 'source', 'source_url', 'ingested_at_utc'
            ]
            
            # Use bulk optimizer for efficient upserts
            # Note: early_shocks has a composite primary key
            updated_count = await bulk_optimizer.bulk_upsert(
                connection=conn,
                table_name='early_shocks',
                columns=columns,
                data=data,
                conflict_columns=['game_id', 'event_type', 'period', 'seconds_elapsed', 'player_name_slug'],
                update_columns=[
                    'time_remaining', 'player_display_name', 'team_tricode', 'severity',
                    'immediate_sub', 'description', 'source', 'source_url', 'ingested_at_utc'
                ]
            )
            
            logger.info("Upserted early shocks", total=len(shocks), updated=updated_count)
            return updated_count
    
    @optimize_query("bulk_upsert")
    async def upsert_schedule_travel(self, travel_rows: List[ScheduleTravelRow]) -> int:
        """Upsert schedule travel analytics with bulk optimization.
        
        Args:
            travel_rows: List of ScheduleTravelRow instances
            
        Returns:
            Number of rows actually updated
        """
        if not travel_rows:
            return 0
        
        # Use performance-optimized connection and bulk operations
        async with get_performance_connection() as conn:
            columns = [
                'game_id', 'team_tricode', 'is_back_to_back', 'is_3_in_4', 'is_5_in_7',
                'days_rest', 'timezone_shift_hours', 'circadian_index', 'altitude_change_m',
                'travel_distance_km', 'prev_game_date', 'prev_arena_tz', 'prev_lat',
                'prev_lon', 'prev_altitude_m', 'source', 'source_url', 'ingested_at_utc'
            ]
            
            data = []
            for travel in travel_rows:
                data.append((
                    travel.game_id,
                    travel.team_tricode,
                    travel.is_back_to_back,
                    travel.is_3_in_4,
                    travel.is_5_in_7,
                    travel.days_rest,
                    travel.timezone_shift_hours,
                    travel.circadian_index,
                    travel.altitude_change_m,
                    travel.travel_distance_km,
                    travel.prev_game_date,
                    travel.prev_arena_tz,
                    travel.prev_lat,
                    travel.prev_lon,
                    travel.prev_altitude_m,
                    travel.source,
                    travel.source_url,
                    datetime.utcnow(),
                ))
            
            # Use bulk optimizer for efficient upserts
            updated_count = await bulk_optimizer.bulk_upsert(
                connection=conn,
                table_name='schedule_travel',
                columns=columns,
                data=data,
                conflict_columns=['game_id'],
                update_columns=[col for col in columns if col not in ['game_id', 'ingested_at_utc']]
            )
            
            logger.info("Upserted schedule travel analytics", total=len(travel_rows), updated=updated_count)
            return updated_count
    
    def _convert_clock_to_time_remaining(self, clock_hhmmss: str) -> str:
        """Convert HH:MM:SS clock format to MM:SS time remaining format."""
        try:
            # Extract minutes and seconds from HH:MM:SS
            parts = clock_hhmmss.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = parts
                # For basketball, we only care about MM:SS within the quarter
                return f"{minutes}:{seconds}"
            return "12:00"  # Fallback
        except (ValueError, IndexError):
            return "12:00"
    
    def _convert_clock_to_seconds_elapsed(self, clock_hhmmss: str) -> float:
        """Convert HH:MM:SS clock format to seconds elapsed in quarter."""
        try:
            parts = clock_hhmmss.split(':')
            if len(parts) == 3:
                hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
                # Q1 is 12 minutes total, so elapsed = 720 - (minutes*60 + seconds)
                total_remaining = minutes * 60 + seconds
                return 720.0 - total_remaining
            return 0.0
        except (ValueError, IndexError):
            return 0.0
    
    def _determine_severity(self, shock_type: str, notes: str) -> str:
        """Determine severity level based on shock type and notes."""
        if shock_type == "FLAGRANT":
            if notes and "Flagrant 2" in notes:
                return "HIGH"
            return "MEDIUM"
        elif shock_type == "INJURY_LEAVE":
            return "HIGH"
        elif shock_type == "TWO_PF_EARLY":
            return "MEDIUM"
        elif shock_type == "TECH":
            return "LOW"
        return "LOW"