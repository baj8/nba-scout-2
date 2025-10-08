"""E2E test utilities for database queries and snapshot validation."""

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def fetch_table_as_sorted_json(
    conn: Any,
    table: str,
    order_by_cols: List[str],
    where_clause: Optional[str] = None,
    params: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch table data as sorted JSON-serializable dictionaries.

    Args:
        conn: Database connection (asyncpg or sqlite3)
        table: Table name to query
        order_by_cols: Columns to sort by for deterministic ordering
        where_clause: Optional WHERE clause (without the WHERE keyword)
        params: Parameters for the WHERE clause

    Returns:
        List of dictionaries representing table rows
    """
    # Build query
    query = f"SELECT * FROM {table}"

    if where_clause:
        query += f" WHERE {where_clause}"

    if order_by_cols:
        order_clause = ", ".join(order_by_cols)
        query += f" ORDER BY {order_clause}"

    # Execute query based on connection type
    if hasattr(conn, "fetch"):
        # AsyncPG connection
        import asyncio

        async def _fetch():
            if params:
                rows = await conn.fetch(query, *params)
            else:
                rows = await conn.fetch(query)

            # Convert asyncpg.Record objects to dictionaries
            return [dict(row) for row in rows]

        # If we're in an async context, run directly, otherwise create event loop
        try:
            asyncio.get_running_loop()
            # We're already in an async context, can't use asyncio.run
            # This should be called from an async test function
            raise RuntimeError("fetch_table_as_sorted_json should be awaited in async context")
        except RuntimeError:
            # No running loop, we can use asyncio.run
            return asyncio.run(_fetch())

    else:
        # SQLite connection
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Get column names
        columns = [description[0] for description in cursor.description]

        # Convert rows to dictionaries
        rows = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            rows.append(row_dict)

        return rows


def fetch_table_as_json(
    conn: Any,
    table: str,
    columns: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch table data as JSON-serializable list of dictionaries.
    
    This is a synchronous wrapper that handles both async and sync connections.
    For async code, use fetch_table_as_sorted_json_async directly.
    
    Args:
        conn: Database connection (SQLAlchemy session or connection)
        table: Table name to query
        columns: Specific columns to select (None = SELECT *)
        order_by: Columns to order by for deterministic results
        where_clause: Optional WHERE clause (without WHERE keyword)
        params: Parameters for WHERE clause
    
    Returns:
        List of dictionaries with JSON-serializable values
    """
    import asyncio
    from sqlalchemy import text
    
    # Build query
    if columns:
        cols = ", ".join(columns)
        query = f"SELECT {cols} FROM {table}"
    else:
        query = f"SELECT * FROM {table}"
    
    if where_clause:
        query += f" WHERE {where_clause}"
    
    if order_by:
        query += f" ORDER BY {', '.join(order_by)}"
    
    # Execute based on connection type
    try:
        # Try async execution
        async def _fetch():
            result = await conn.execute(text(query), params or {})
            rows = result.fetchall()
            return [_serialize_row(dict(row)) for row in rows]
        
        # Check if we're in an async context
        try:
            loop = asyncio.get_running_loop()
            # We're in async context, this shouldn't be called
            raise RuntimeError(
                "fetch_table_as_json is synchronous. "
                "Use fetch_table_as_sorted_json_async in async context."
            )
        except RuntimeError:
            # No running loop, we can use asyncio.run
            return asyncio.run(_fetch())
    except AttributeError:
        # Synchronous connection
        from sqlalchemy import create_engine
        result = conn.execute(text(query), params or {})
        rows = result.fetchall()
        return [_serialize_row(dict(row)) for row in rows]


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a database row to JSON-serializable format.
    
    Args:
        row: Dictionary from database row
    
    Returns:
        Dictionary with JSON-serializable values
    """
    serialized = {}
    for key, value in row.items():
        # Handle datetime/date objects
        if hasattr(value, "isoformat"):
            serialized[key] = value.isoformat()
        # Handle Decimal/numeric types
        elif hasattr(value, "__float__"):
            serialized[key] = float(value)
        # Handle bytes
        elif isinstance(value, bytes):
            serialized[key] = value.hex()
        # Handle booleans
        elif isinstance(value, bool):
            serialized[key] = value
        # Handle None
        elif value is None:
            serialized[key] = None
        # Handle primitives
        elif isinstance(value, (int, float, str)):
            serialized[key] = value
        # Fallback: convert to string
        else:
            serialized[key] = str(value)
    
    return serialized


async def fetch_table_as_sorted_json_async(
    conn: Any,
    table: str,
    order_by_cols: List[str],
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Async version of fetch_table_as_json for use in async test functions.
    
    Args:
        conn: Async database connection/session
        table: Table name to query
        order_by_cols: Columns to sort by for deterministic ordering
        columns: Specific columns to select (None = SELECT *)
        where_clause: Optional WHERE clause (without WHERE keyword)
        params: Parameters for WHERE clause
    
    Returns:
        List of JSON-serializable dictionaries
    """
    from sqlalchemy import text
    
    # Build query
    if columns:
        cols = ", ".join(columns)
        query = f"SELECT {cols} FROM {table}"
    else:
        query = f"SELECT * FROM {table}"
    
    if where_clause:
        query += f" WHERE {where_clause}"
    
    if order_by_cols:
        order_clause = ", ".join(order_by_cols)
        query += f" ORDER BY {order_clause}"
    
    # Execute query
    result = await conn.execute(text(query), params or {})
    rows = result.fetchall()
    
    # Convert to JSON-serializable format
    return [_serialize_row(dict(row)) for row in rows]


def normalize_for_comparison(data: Any) -> Any:
    """Normalize data for comparison by handling floating point precision and sorting."""
    if isinstance(data, dict):
        return {k: normalize_for_comparison(v) for k, v in sorted(data.items())}
    elif isinstance(data, list):
        return [normalize_for_comparison(item) for item in data]
    elif isinstance(data, float):
        # Round to 6 decimal places to handle floating point precision
        return round(data, 6)
    else:
        return data


def assert_json_close(
    actual: Union[Dict, List],
    expected: Union[Dict, List],
    float_tol: float = 1e-6,
    ignore_keys: Optional[List[str]] = None,
    path: str = "",
) -> None:
    """Compare JSON data structures with tolerance for floating point differences.

    Args:
        actual: Actual data structure
        expected: Expected data structure
        float_tol: Tolerance for floating point comparisons (relative and absolute)
        ignore_keys: List of keys to ignore in comparison (e.g., timestamps)
        path: Current path in data structure for error reporting

    Raises:
        AssertionError: If structures don't match within tolerance
    """
    ignore_keys = ignore_keys or []
    
    if type(actual) != type(expected):
        raise AssertionError(
            f"Type mismatch at {path or 'root'}: "
            f"{type(actual).__name__} vs {type(expected).__name__}"
        )

    if isinstance(actual, dict):
        # Check keys match (excluding ignored keys)
        actual_keys = set(k for k in actual.keys() if k not in ignore_keys)
        expected_keys = set(k for k in expected.keys() if k not in ignore_keys)

        if actual_keys != expected_keys:
            missing = expected_keys - actual_keys
            extra = actual_keys - expected_keys
            msg_parts = []
            if missing:
                msg_parts.append(f"missing: {sorted(missing)}")
            if extra:
                msg_parts.append(f"extra: {sorted(extra)}")
            raise AssertionError(
                f"Key mismatch at {path or 'root'}: {', '.join(msg_parts)}"
            )

        # Recursively compare values
        for key in actual_keys:
            assert_json_close(
                actual[key],
                expected[key],
                float_tol,
                ignore_keys,
                f"{path}.{key}" if path else key,
            )

    elif isinstance(actual, list):
        if len(actual) != len(expected):
            raise AssertionError(
                f"Length mismatch at {path or 'root'}: "
                f"{len(actual)} vs {len(expected)}"
            )

        # Compare each element
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert_json_close(
                a, e, float_tol, ignore_keys, f"{path}[{i}]" if path else f"[{i}]"
            )

    elif isinstance(actual, float) and isinstance(expected, float):
        # Use relative and absolute tolerance for floating point comparison
        if math.isnan(actual) and math.isnan(expected):
            # Both NaN is considered equal
            pass
        elif math.isnan(actual) or math.isnan(expected):
            raise AssertionError(
                f"NaN mismatch at {path or 'root'}: {actual} vs {expected}"
            )
        elif not math.isclose(actual, expected, rel_tol=float_tol, abs_tol=float_tol):
            raise AssertionError(
                f"Float mismatch at {path or 'root'}: "
                f"{actual} vs {expected} (tol={float_tol})"
            )

    elif isinstance(actual, (int, str, bool, type(None))):
        if actual != expected:
            raise AssertionError(
                f"Value mismatch at {path or 'root'}: {actual!r} vs {expected!r}"
            )

    else:
        # For other types, convert to string and compare
        if str(actual) != str(expected):
            raise AssertionError(
                f"String representation mismatch at {path or 'root'}: "
                f"{actual} vs {expected}"
            )


def save_golden_snapshot(data: Any, filepath: Union[str, Path]) -> None:
    """Save data as a golden snapshot JSON file.

    Args:
        data: Data to save (must be JSON serializable)
        filepath: Path to save the JSON file
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Normalize data before saving
    normalized_data = normalize_for_comparison(data)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(normalized_data, f, indent=2, sort_keys=True, ensure_ascii=False)


def load_golden_snapshot(filepath: Union[str, Path]) -> Any:
    """Load golden snapshot data from JSON file.

    Args:
        filepath: Path to the JSON file

    Returns:
        Loaded data structure

    Raises:
        FileNotFoundError: If snapshot file doesn't exist
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"Golden snapshot not found: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def create_test_game_data() -> List[Dict[str, Any]]:
    """Create test game data for known games used in E2E testing.

    Returns:
        List of dictionaries representing test games with known outcomes
    """
    return [
        {
            "game_id": "0022400001",
            "home_team": "LAL",
            "away_team": "BOS",
            "game_date": "2024-10-19",
            "season": "2024-25",
            "expected_pbp_events": 450,  # Approximate
            "expected_derived_rows": True,
        },
        {
            "game_id": "0022400002",
            "home_team": "GSW",
            "away_team": "PHX",
            "game_date": "2024-10-19",
            "season": "2024-25",
            "expected_pbp_events": 420,
            "expected_derived_rows": True,
        },
    ]


def validate_database_state(conn: Any, expected_counts: Dict[str, int]) -> Dict[str, Any]:
    """Validate database state after pipeline execution.

    Args:
        conn: Database connection
        expected_counts: Expected row counts by table

    Returns:
        Dictionary with validation results
    """
    import asyncio

    async def _validate():
        results = {
            "valid": True,
            "errors": [],
            "actual_counts": {},
            "expected_counts": expected_counts,
        }

        for table, expected_count in expected_counts.items():
            try:
                actual_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                results["actual_counts"][table] = actual_count

                if actual_count != expected_count:
                    results["valid"] = False
                    results["errors"].append(
                        f"{table}: expected {expected_count}, got {actual_count}"
                    )
            except Exception as e:
                results["valid"] = False
                results["errors"].append(f"Failed to count {table}: {str(e)}")

        return results

    # Handle both sync and async contexts
    try:
        asyncio.get_running_loop()
        # We're in an async context, this should be called with await
        raise RuntimeError("validate_database_state should be awaited in async context")
    except RuntimeError:
        return asyncio.run(_validate())


async def validate_database_state_async(
    conn: Any, expected_counts: Dict[str, int]
) -> Dict[str, Any]:
    """Async version of validate_database_state for use in async test functions."""
    results = {"valid": True, "errors": [], "actual_counts": {}, "expected_counts": expected_counts}

    for table, expected_count in expected_counts.items():
        try:
            actual_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            results["actual_counts"][table] = actual_count

            if actual_count != expected_count:
                results["valid"] = False
                results["errors"].append(f"{table}: expected {expected_count}, got {actual_count}")
        except Exception as e:
            results["valid"] = False
            results["errors"].append(f"Failed to count {table}: {str(e)}")

    return results


def setup_test_database_schema(conn: Any) -> None:
    """Setup minimal test database schema for E2E testing.

    Args:
        conn: Database connection
    """
    import asyncio

    async def _setup():
        # Create minimal tables for testing
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                game_id TEXT PRIMARY KEY,
                season TEXT NOT NULL,
                game_date DATE NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                status TEXT DEFAULT 'Final'
            )
        """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS early_shocks (
                game_id TEXT PRIMARY KEY,
                home_team_tricode TEXT NOT NULL,
                away_team_tricode TEXT NOT NULL,
                shock_magnitude REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            )
        """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS q1_window_12_8 (
                game_id TEXT PRIMARY KEY,
                home_team_tricode TEXT NOT NULL,
                away_team_tricode TEXT NOT NULL,
                home_points_window INTEGER DEFAULT 0,
                away_points_window INTEGER DEFAULT 0,
                total_possessions INTEGER DEFAULT 0,
                pace_factor REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            )
        """
        )

    # Handle both sync and async contexts
    try:
        asyncio.get_running_loop()
        raise RuntimeError("setup_test_database_schema should be awaited in async context")
    except RuntimeError:
        asyncio.run(_setup())


async def setup_test_database_schema_async(conn: Any) -> None:
    """Async version of setup_test_database_schema for use in async test functions."""
    # Create minimal tables for testing
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            season TEXT NOT NULL,
            game_date DATE NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            status TEXT DEFAULT 'Final'
        )
    """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS early_shocks (
            game_id TEXT PRIMARY KEY,
            home_team_tricode TEXT NOT NULL,
            away_team_tricode TEXT NOT NULL,
            shock_magnitude REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS q1_window_12_8 (
            game_id TEXT PRIMARY KEY,
            home_team_tricode TEXT NOT NULL,
            away_team_tricode TEXT NOT NULL,
            home_points_window INTEGER DEFAULT 0,
            away_points_window INTEGER DEFAULT 0,
            total_possessions INTEGER DEFAULT 0,
            pace_factor REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (game_id) REFERENCES games(game_id)
        )
    """
    )
