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


async def fetch_table_as_sorted_json_async(
    conn: Any,
    table: str,
    order_by_cols: List[str],
    where_clause: Optional[str] = None,
    params: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    """Async version of fetch_table_as_sorted_json for use in async test functions."""
    # Build query
    query = f"SELECT * FROM {table}"

    if where_clause:
        query += f" WHERE {where_clause}"

    if order_by_cols:
        order_clause = ", ".join(order_by_cols)
        query += f" ORDER BY {order_clause}"

    # Execute query
    if params:
        rows = await conn.fetch(query, *params)
    else:
        rows = await conn.fetch(query)

    # Convert asyncpg.Record objects to dictionaries and handle JSON serialization
    result = []
    for row in rows:
        row_dict = {}
        for key, value in dict(row).items():
            # Convert non-JSON-serializable types
            if hasattr(value, "isoformat"):  # datetime/date objects
                row_dict[key] = value.isoformat()
            elif isinstance(value, (int, float, str, bool, type(None))):
                row_dict[key] = value
            else:
                # Convert other types to string
                row_dict[key] = str(value)
        result.append(row_dict)

    return result


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
    actual: Union[Dict, List], expected: Union[Dict, List], tol: float = 1e-6, path: str = ""
) -> None:
    """Compare JSON data structures with tolerance for floating point differences.

    Args:
        actual: Actual data structure
        expected: Expected data structure
        tol: Tolerance for floating point comparisons
        path: Current path in data structure for error reporting

    Raises:
        AssertionError: If structures don't match within tolerance
    """
    if type(actual) != type(expected):
        raise AssertionError(f"Type mismatch at {path}: {type(actual)} vs {type(expected)}")

    if isinstance(actual, dict):
        # Check keys match
        actual_keys = set(actual.keys())
        expected_keys = set(expected.keys())

        if actual_keys != expected_keys:
            missing = expected_keys - actual_keys
            extra = actual_keys - expected_keys
            raise AssertionError(f"Key mismatch at {path}: missing {missing}, extra {extra}")

        # Recursively compare values
        for key in actual_keys:
            assert_json_close(actual[key], expected[key], tol, f"{path}.{key}" if path else key)

    elif isinstance(actual, list):
        if len(actual) != len(expected):
            raise AssertionError(f"Length mismatch at {path}: {len(actual)} vs {len(expected)}")

        # Compare each element
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert_json_close(a, e, tol, f"{path}[{i}]")

    elif isinstance(actual, float) and isinstance(expected, float):
        # Use relative tolerance for floating point comparison
        if not math.isclose(actual, expected, rel_tol=tol, abs_tol=tol):
            raise AssertionError(f"Float mismatch at {path}: {actual} vs {expected} (tol={tol})")

    elif isinstance(actual, (int, str, bool, type(None))):
        if actual != expected:
            raise AssertionError(f"Value mismatch at {path}: {actual} vs {expected}")

    else:
        # For other types, convert to string and compare
        if str(actual) != str(expected):
            raise AssertionError(
                f"String representation mismatch at {path}: {actual} vs {expected}"
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
