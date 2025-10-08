"""Facade-visible exports for derived-data loaders.

Tests monkeypatch these symbols. Provide minimal signatures so imports work,
and leave the body for the actual implementations or for test-time patches.
"""

import logging
from datetime import UTC, datetime
from typing import Any, Dict, Optional, Sequence

__all__ = [
    "bulk_upsert",
    "upsert_q1_windows",
    "upsert_early_shocks",
    "upsert_schedule_travel",
    "DerivedLoader",
    "get_connection",
    "get_performance_connection",
    "bulk_optimizer",
]

# Set up logging
logger = logging.getLogger(__name__)

# ---- Database connection functions (tests patch these) ----------------------


async def get_connection():
    """Placeholder database connection function for test mocking."""
    pass


async def get_performance_connection():
    """Placeholder performance database connection function for test mocking."""
    pass


# ---- Bulk optimization function (tests patch this) -------------------------


class BulkOptimizer:
    """Placeholder bulk optimizer class for test mocking."""

    async def bulk_upsert(self, conn: Any, table: str, rows: list[dict]) -> int:
        """Placeholder bulk upsert method. Tests will patch this."""
        return len(rows)


bulk_optimizer = BulkOptimizer()

# ---- Bulk upsert primitive (tests usually monkeypatch this) -----------------


async def bulk_upsert(
    conn: Any,
    table: str,
    rows: Sequence[Dict[str, Any]],
    *,
    conflict_keys: Sequence[str] = (),
    update_cols: Optional[Sequence[str]] = None,
) -> int:
    """Minimal placeholder. Tests will patch this function."""
    return len(rows)


# ---- High-level helpers (tests patch or assert calls happen) ---------------


async def upsert_q1_windows(records: Sequence[Dict[str, Any]], *, conn: Any = None) -> int:
    """Insert or update Q1 window analytics."""
    if not records:
        return 0
    return await bulk_upsert(
        conn,
        "q1_windows",
        list(records),
        conflict_keys=("game_id",),
        update_cols=None,
    )


async def upsert_early_shocks(records: Sequence[Dict[str, Any]], *, conn: Any = None) -> int:
    """Insert or update Early Shocks analytics."""
    if not records:
        return 0
    return await bulk_upsert(
        conn,
        "early_shocks",
        list(records),
        conflict_keys=("game_id", "sequence"),
        update_cols=None,
    )


async def upsert_schedule_travel(records: Sequence[Dict[str, Any]], *, conn: Any = None) -> int:
    """Insert or update schedule travel summaries."""
    if not records:
        return 0
    return await bulk_upsert(
        conn,
        "schedule_travel",
        list(records),
        conflict_keys=("team_id", "date"),
        update_cols=None,
    )


# ---- DerivedLoader class (tests expect this) -------------------------------

try:
    from ..pipelines.foundation import _maybe_transaction
except ImportError:
    # Fallback for testing - proper async context manager
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _maybe_transaction(conn):
        yield conn


class DerivedLoader:
    """Hardened loader class for derived analytics with comprehensive validation and error handling."""

    def __init__(self):
        self.validator = None
        self._metrics = {"inserts": 0, "updates": 0, "errors": 0, "validations": 0}

    def _validate_input_records(
        self, records: list[dict], table_name: str, required_fields: list[str]
    ) -> tuple[list[dict], list[str]]:
        """Validate input records before processing.

        Accepts both dictionaries and model objects (will convert models to dicts).

        Returns:
            Tuple of (valid_records, error_messages)
        """
        if not records:
            return [], []

        valid_records = []
        errors = []

        for idx, record in enumerate(records):
            # Convert model objects to dict if needed
            if hasattr(record, "model_dump"):
                # Pydantic v2 model
                record_dict = record.model_dump()
            elif hasattr(record, "dict"):
                # Pydantic v1 model
                record_dict = record.dict()
            elif isinstance(record, dict):
                # Already a dict
                record_dict = record
            else:
                errors.append(f"Record {idx} is not a dict or model object: {type(record)}")
                continue

            # Check required fields
            missing_fields = [field for field in required_fields if field not in record_dict]
            if missing_fields:
                errors.append(
                    f"Record {idx} missing required fields for {table_name}: {missing_fields}"
                )
                continue

            # Check for None values in required fields
            none_fields = [field for field in required_fields if record_dict.get(field) is None]
            if none_fields:
                errors.append(
                    f"Record {idx} has None values in required fields for {table_name}: {none_fields}"
                )
                continue

            valid_records.append(record_dict)

        if errors:
            logger.warning(
                f"Input validation found {len(errors)} invalid records for {table_name} "
                f"(valid: {len(valid_records)}, invalid: {len(errors)})"
            )

        return valid_records, errors

    async def _validate_foreign_keys(
        self, records: list[dict], table_name: str
    ) -> tuple[list[dict], list[str]]:
        """Validate foreign key references if validator is available.

        Returns:
            Tuple of (valid_records, warning_messages)
        """
        if not self.validator:
            # No validator configured - skip FK validation (test mode)
            return records, []

        if not records:
            return [], []

        try:
            self._metrics["validations"] += 1
            valid_records, warnings = await self.validator.validate_before_insert(records)

            if warnings:
                logger.warning(
                    f"FK validation warnings for {table_name}: {len(warnings)} warnings, "
                    f"{len(valid_records)} valid records, {len(records) - len(valid_records)} filtered"
                )

            return valid_records, warnings
        except Exception as e:
            logger.error(f"FK validation failed for {table_name}: {str(e)}", exc_info=True)
            # On validation error, return original records but log the issue
            return records, [f"Validation error: {str(e)}"]

    async def _safe_bulk_upsert(self, conn: Any, table: str, rows: list[dict]) -> int:
        """Safely execute bulk upsert with error handling and logging.

        Returns:
            Number of records processed
        """
        if not rows:
            return 0

        try:
            start_time = datetime.now(UTC)
            result = await bulk_optimizer.bulk_upsert(conn, table, rows)
            duration = (datetime.now(UTC) - start_time).total_seconds()

            processed = result if result is not None else 0
            self._metrics["inserts"] += processed

            logger.info(
                f"Bulk upsert completed for {table}: {processed} records in {duration:.3f}s "
                f"({processed / duration if duration > 0 else 0:.1f} records/sec)"
            )

            return processed
        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                f"Bulk upsert failed for {table} ({len(rows)} records): {str(e)}", exc_info=True
            )
            raise RuntimeError(f"Failed to upsert {len(rows)} records to {table}: {str(e)}") from e

    async def upsert_q1_windows(self, rows: list[dict], *, conn: Any = None) -> int:
        """Insert or update Q1 window analytics with comprehensive validation.

        Args:
            rows: List of Q1 window records
            conn: Optional database connection (will be created if not provided)

        Returns:
            Number of records successfully processed

        Raises:
            RuntimeError: If upsert operation fails
            ValueError: If input validation fails critically
        """
        table_name = "q1_windows"
        required_fields = ["game_id"]

        if not rows:
            logger.debug(f"No records to upsert for {table_name}")
            return 0

        logger.info(f"Starting upsert for {table_name}", record_count=len(rows))

        # Step 1: Input validation
        valid_rows, input_errors = self._validate_input_records(rows, table_name, required_fields)
        if input_errors and not valid_rows:
            error_msg = f"All records failed input validation for {table_name}: {input_errors[:3]}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Step 1.5: Completeness validation - filter out incomplete games
        complete_rows = []
        for row in valid_rows:
            game_id = row.get("game_id")
            if not game_id:
                continue
                
            # Import here to avoid circular imports
            from ..quality.completeness import game_is_complete, get_skip_reason_description
            
            try:
                is_complete, reasons = await game_is_complete(game_id)
                if is_complete:
                    complete_rows.append(row)
                else:
                    # Log structured skip with reasons
                    for reason in reasons:
                        logger.info(
                            "derived_loader.skip",
                            extra={
                                "game_id": game_id,
                                "reason": reason,
                                "table": table_name,
                                "description": get_skip_reason_description(reason)
                            }
                        )
            except Exception as e:
                logger.warning(
                    "Completeness check failed, proceeding with caution",
                    game_id=game_id,
                    error=str(e)
                )
                complete_rows.append(row)  # Include row if completeness check fails

        if not complete_rows:
            logger.info(f"No complete games found for {table_name}")
            return 0
        
        logger.info(
            f"Completeness filtering for {table_name}",
            original_count=len(valid_rows),
            complete_count=len(complete_rows),
            filtered_count=len(valid_rows) - len(complete_rows)
        )

        # Step 2: FK validation (if validator available)
        validated_rows, fk_warnings = await self._validate_foreign_keys(complete_rows, table_name)
        if not validated_rows:
            logger.warning(f"No valid records after FK validation for {table_name}")
            return 0

        # Step 3: Get connection if not provided
        if conn is None:
            conn_func = get_connection()
            if hasattr(conn_func, "__await__"):
                conn = await conn_func
            else:
                conn = conn_func

        # Step 4: Execute upsert within transaction
        async with _maybe_transaction(conn):
            result = await self._safe_bulk_upsert(conn, table_name, validated_rows)
            return result

    async def upsert_early_shocks(self, rows: list[dict], *, conn: Any = None) -> int:
        """Insert or update Early Shocks analytics with comprehensive validation.

        Args:
            rows: List of early shock records
            conn: Optional database connection (will be created if not provided)

        Returns:
            Number of records successfully processed

        Raises:
            RuntimeError: If upsert operation fails
            ValueError: If input validation fails critically
        """
        table_name = "early_shocks"
        required_fields = ["game_id", "sequence"]

        if not rows:
            logger.debug(f"No records to upsert for {table_name}")
            return 0

        logger.info(f"Starting upsert for {table_name}", record_count=len(rows))

        # Step 1: Input validation
        valid_rows, input_errors = self._validate_input_records(rows, table_name, required_fields)
        if input_errors and not valid_rows:
            error_msg = f"All records failed input validation for {table_name}: {input_errors[:3]}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Step 1.5: Completeness validation - filter out incomplete games
        complete_rows = []
        for row in valid_rows:
            game_id = row.get("game_id")
            if not game_id:
                continue
                
            # Import here to avoid circular imports
            from ..quality.completeness import game_is_complete, get_skip_reason_description
            
            try:
                is_complete, reasons = await game_is_complete(game_id)
                if is_complete:
                    complete_rows.append(row)
                else:
                    # Log structured skip with reasons
                    for reason in reasons:
                        logger.info(
                            "derived_loader.skip",
                            extra={
                                "game_id": game_id,
                                "reason": reason,
                                "table": table_name,
                                "description": get_skip_reason_description(reason)
                            }
                        )
            except Exception as e:
                logger.warning(
                    "Completeness check failed, proceeding with caution",
                    game_id=game_id,
                    error=str(e)
                )
                complete_rows.append(row)  # Include row if completeness check fails

        if not complete_rows:
            logger.info(f"No complete games found for {table_name}")
            return 0
        
        logger.info(
            f"Completeness filtering for {table_name}",
            original_count=len(valid_rows),
            complete_count=len(complete_rows),
            filtered_count=len(valid_rows) - len(complete_rows)
        )

        # Step 2: FK validation (if validator available)
        validated_rows, fk_warnings = await self._validate_foreign_keys(complete_rows, table_name)
        if not validated_rows:
            logger.warning(f"No valid records after FK validation for {table_name}")
            return 0

        # Step 3: Get connection if not provided
        if conn is None:
            conn_func = get_connection()
            if hasattr(conn_func, "__await__"):
                conn = await conn_func
            else:
                conn = conn_func

        # Step 4: Execute upsert within transaction
        async with _maybe_transaction(conn):
            result = await self._safe_bulk_upsert(conn, table_name, validated_rows)
            return result

    async def upsert_schedule_travel(self, rows: list[dict], *, conn: Any = None) -> int:
        """Insert or update schedule travel summaries with comprehensive validation.

        Args:
            rows: List of schedule travel records
            conn: Optional database connection (will be created if not provided)

        Returns:
            Number of records successfully processed

        Raises:
            RuntimeError: If upsert operation fails
            ValueError: If input validation fails critically
        """
        table_name = "schedule_travel"
        required_fields = ["team_id", "date"]

        if not rows:
            logger.debug(f"No records to upsert for {table_name}")
            return 0

        logger.info(f"Starting upsert for {table_name}", record_count=len(rows))

        # Step 1: Input validation
        valid_rows, input_errors = self._validate_input_records(rows, table_name, required_fields)
        if input_errors and not valid_rows:
            error_msg = f"All records failed input validation for {table_name}: {input_errors[:3]}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Step 2: FK validation (if validator available)
        validated_rows, fk_warnings = await self._validate_foreign_keys(valid_rows, table_name)
        if not validated_rows:
            logger.warning(f"No valid records after FK validation for {table_name}")
            return 0

        # Step 3: Get connection if not provided
        if conn is None:
            conn_func = get_connection()
            if hasattr(conn_func, "__await__"):
                conn = await conn_func
            else:
                conn = conn_func

        # Step 4: Execute upsert within transaction
        async with _maybe_transaction(conn):
            result = await self._safe_bulk_upsert(conn, table_name, validated_rows)
            return result

    def get_metrics(self) -> dict[str, int]:
        """Get loader metrics for monitoring and debugging."""
        return dict(self._metrics)

    def reset_metrics(self):
        """Reset metrics counters."""
        self._metrics = {"inserts": 0, "updates": 0, "errors": 0, "validations": 0}

    def _convert_clock_to_time_remaining(self, clock_hhmmss: str) -> str:
        """Convert HH:MM:SS to MM:SS format with defensive error handling."""
        if not clock_hhmmss or not isinstance(clock_hhmmss, str):
            logger.debug("Invalid clock format, using default", value=clock_hhmmss)
            return "12:00"

        try:
            parts = clock_hhmmss.split(":")
            if len(parts) == 3:
                return f"{parts[1]}:{parts[2]}"
            elif len(parts) == 2:
                return clock_hhmmss  # Already in MM:SS format
        except (ValueError, IndexError) as e:
            logger.debug("Clock conversion failed", value=clock_hhmmss, error=str(e))

        return "12:00"  # Default fallback

    def _convert_clock_to_seconds_elapsed(self, clock_hhmmss: str) -> float:
        """Convert HH:MM:SS to seconds elapsed in quarter with defensive error handling."""
        if not clock_hhmmss or not isinstance(clock_hhmmss, str):
            logger.debug("Invalid clock format for seconds calculation", value=clock_hhmmss)
            return 0.0

        try:
            parts = clock_hhmmss.split(":")
            if len(parts) == 3:
                minutes = int(parts[1])
                seconds = int(parts[2])
                time_remaining = minutes * 60 + seconds
                return 720.0 - time_remaining  # 12 minutes = 720 seconds
            elif len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                time_remaining = minutes * 60 + seconds
                return 720.0 - time_remaining
        except (ValueError, IndexError) as e:
            logger.debug("Clock to seconds conversion failed", value=clock_hhmmss, error=str(e))

        return 0.0  # Default fallback

    def _determine_severity(self, shock_type: str, notes: str = "") -> str:
        """Determine severity level for early shocks with defensive error handling."""
        if not shock_type or not isinstance(shock_type, str):
            logger.debug("Invalid shock_type for severity determination", value=shock_type)
            return "LOW"

        shock_type_upper = shock_type.upper()
        notes_str = str(notes) if notes else ""

        if shock_type_upper == "FLAGRANT":
            if "Flagrant 2" in notes_str or "FLAGRANT 2" in notes_str:
                return "HIGH"
            return "MEDIUM"
        elif shock_type_upper == "INJURY_LEAVE":
            return "HIGH"
        elif shock_type_upper == "TWO_PF_EARLY":
            return "MEDIUM"
        elif shock_type_upper == "TECH":
            return "LOW"

        return "LOW"  # Default fallback
