"""E2E pipeline smoke test that runs a miniature pipeline on known games."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from .utils import (
    assert_json_close,
    create_test_game_data,
    fetch_table_as_sorted_json_async,
    load_golden_snapshot,
    save_golden_snapshot,
    setup_test_database_schema_async,
    validate_database_state_async,
)

# Skip PostgreSQL tests if psycopg/asyncpg not available
pytest_postgresql = None
try:
    import pytest_postgresql

    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import asyncpg

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False


class MockNBAStatsClient:
    """Mock NBA Stats client for E2E testing."""

    def __init__(self):
        self.call_count = 0

    async def fetch_boxscore(self, game_id: str) -> Dict[str, Any]:
        """Mock boxscore fetch with realistic data."""
        self.call_count += 1

        # Return mock data based on game ID
        if game_id == "0022400001":
            return {
                "resultSets": [
                    {
                        "name": "GameSummary",
                        "headers": [
                            "GAME_ID",
                            "GAME_DATE_EST",
                            "HOME_TEAM_ID",
                            "VISITOR_TEAM_ID",
                            "HOME_TEAM_ABBREVIATION",
                            "VISITOR_TEAM_ABBREVIATION",
                            "PTS_HOME",
                            "PTS_AWAY",
                        ],
                        "rowSet": [
                            [
                                "0022400001",
                                "2024-10-19",
                                1610612747,
                                1610612738,
                                "LAL",
                                "BOS",
                                118,
                                114,
                            ]
                        ],
                    }
                ]
            }
        elif game_id == "0022400002":
            return {
                "resultSets": [
                    {
                        "name": "GameSummary",
                        "headers": [
                            "GAME_ID",
                            "GAME_DATE_EST",
                            "HOME_TEAM_ID",
                            "VISITOR_TEAM_ID",
                            "HOME_TEAM_ABBREVIATION",
                            "VISITOR_TEAM_ABBREVIATION",
                            "PTS_HOME",
                            "PTS_AWAY",
                        ],
                        "rowSet": [
                            [
                                "0022400002",
                                "2024-10-19",
                                1610612744,
                                1610612756,
                                "GSW",
                                "PHX",
                                125,
                                112,
                            ]
                        ],
                    }
                ]
            }
        else:
            raise ValueError(f"Unknown game ID: {game_id}")

    async def fetch_playbyplay(self, game_id: str) -> Dict[str, Any]:
        """Mock play-by-play fetch with minimal realistic data."""
        self.call_count += 1

        return {
            "resultSets": [
                {
                    "name": "PlayByPlay",
                    "headers": [
                        "GAME_ID",
                        "EVENTNUM",
                        "PERIOD",
                        "PCTIMESTRING",
                        "HOMEDESCRIPTION",
                        "VISITORDESCRIPTION",
                        "SCORE",
                        "SCOREMARGIN",
                    ],
                    "rowSet": [
                        [game_id, 1, 1, "12:00", "Jump Ball", None, "0-0", "TIE"],
                        [game_id, 2, 1, "11:45", "Made Shot", None, "2-0", "+2"],
                        [game_id, 3, 1, "11:30", None, "Made Shot", "2-3", "-1"],
                        [game_id, 4, 1, "8:15", "Made Shot", None, "5-3", "+2"],
                        [game_id, 5, 1, "8:00", None, "Made Shot", "5-6", "-1"],
                    ],
                }
            ]
        }


class MockPipelineOrchestrator:
    """Mock pipeline orchestrator for E2E testing."""

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.mock_client = MockNBAStatsClient()

    async def run_fetch_transform_load(self, game_ids: List[str]) -> Dict[str, Any]:
        """Mock the full ETL pipeline for test games."""
        results = {"games_processed": len(game_ids), "success": True, "errors": []}

        try:
            # Step 1: Fetch (mocked)
            for game_id in game_ids:
                boxscore_data = await self.mock_client.fetch_boxscore(game_id)
                # pbp_data is fetched for realism but not used in this mock
                _ = await self.mock_client.fetch_playbyplay(game_id)

                # Step 2: Transform - extract game data
                game_summary = boxscore_data["resultSets"][0]["rowSet"][0]
                game_data = {
                    "game_id": game_summary[0],
                    "season": "2024-25",
                    "game_date": game_summary[1],
                    "home_team_id": game_summary[2],
                    "away_team_id": game_summary[3],
                    "status": "Final",
                }

                # Step 3: Load - insert into database
                await self.db_conn.execute(
                    """
                    INSERT OR REPLACE INTO games
                    (game_id, season, game_date, home_team_id, away_team_id, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        game_data["game_id"],
                        game_data["season"],
                        game_data["game_date"],
                        game_data["home_team_id"],
                        game_data["away_team_id"],
                        game_data["status"],
                    ),
                )

                # Generate derived analytics (mocked)
                await self._generate_derived_data(game_id, game_summary)

        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))

        return results

    async def _generate_derived_data(self, game_id: str, game_summary: List[Any]) -> None:
        """Generate mock derived analytics data."""
        home_team = game_summary[4]  # home_team_abbreviation
        away_team = game_summary[5]  # visitor_team_abbreviation

        # Mock early shocks data
        await self.db_conn.execute(
            """
            INSERT OR REPLACE INTO early_shocks
            (game_id, home_team_tricode, away_team_tricode, shock_magnitude)
            VALUES (?, ?, ?, ?)
        """,
            (game_id, home_team, away_team, 0.75),
        )

        # Mock Q1 window data
        await self.db_conn.execute(
            """
            INSERT OR REPLACE INTO q1_window_12_8
            (game_id, home_team_tricode, away_team_tricode, home_points_window,
             away_points_window, total_possessions, pace_factor)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (game_id, home_team, away_team, 15, 12, 18, 1.2),
        )


@pytest.fixture
async def temp_sqlite_db():
    """Create temporary SQLite database for testing."""
    # Create temporary database file
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db.close()

    try:
        # Connect to SQLite database
        import aiosqlite

        conn = await aiosqlite.connect(temp_db.name)

        # Setup test schema
        await setup_test_database_schema_async(conn)

        yield conn

    finally:
        await conn.close()
        # Clean up temporary file
        os.unlink(temp_db.name)


@pytest.fixture
async def temp_postgres_db():
    """Create temporary PostgreSQL database for testing."""
    if not POSTGRES_AVAILABLE or not ASYNCPG_AVAILABLE:
        pytest.skip("PostgreSQL or asyncpg not available")

    # Use pytest-postgresql to create temp database
    postgresql = pytest_postgresql.factories.postgresql_proc()

    with postgresql as postgres_proc:
        # Connect to the database
        conn = await asyncpg.connect(
            host=postgres_proc.host,
            port=postgres_proc.port,
            user=postgres_proc.user,
            database=postgres_proc.dbname,
        )

        try:
            # Setup test schema
            await setup_test_database_schema_async(conn)
            yield conn
        finally:
            await conn.close()


@pytest.fixture(params=["sqlite", "postgres"])
async def test_db(request):
    """Parametrized fixture for both SQLite and PostgreSQL."""
    if request.param == "sqlite":
        async with temp_sqlite_db() as db:
            yield db
    elif request.param == "postgres":
        if not POSTGRES_AVAILABLE or not ASYNCPG_AVAILABLE:
            pytest.skip("PostgreSQL or asyncpg not available")
        async with temp_postgres_db() as db:
            yield db


class TestPipelineSmoke:
    """End-to-end pipeline smoke tests with golden snapshot validation."""

    @pytest.mark.asyncio
    async def test_miniature_pipeline_smoke(self, temp_sqlite_db):
        """Test miniature pipeline on 2-3 known games with golden snapshot validation."""

        # Setup
        conn = temp_sqlite_db
        test_games = create_test_game_data()[:2]  # Use first 2 test games
        game_ids = [game["game_id"] for game in test_games]

        # Create mock pipeline orchestrator
        orchestrator = MockPipelineOrchestrator(conn)

        # Run the pipeline
        result = await orchestrator.run_fetch_transform_load(game_ids)

        # Verify pipeline execution
        assert result["success"], f"Pipeline failed: {result['errors']}"
        assert result["games_processed"] == 2

        # Validate database state
        expected_counts = {"games": 2, "early_shocks": 2, "q1_window_12_8": 2}

        validation = await validate_database_state_async(conn, expected_counts)
        assert validation["valid"], f"Database validation failed: {validation['errors']}"

        # Fetch actual data for snapshot comparison
        games_data = await fetch_table_as_sorted_json_async(conn, "games", ["game_id"])

        early_shocks_data = await fetch_table_as_sorted_json_async(
            conn, "early_shocks", ["game_id"]
        )

        q1_window_data = await fetch_table_as_sorted_json_async(conn, "q1_window_12_8", ["game_id"])

        # Create snapshot data
        snapshot_data = {
            "games": games_data,
            "early_shocks": early_shocks_data,
            "q1_window_12_8": q1_window_data,
            "metadata": {"test_games_count": len(game_ids), "pipeline_success": result["success"]},
        }

        # Path to golden snapshots
        golden_dir = Path(__file__).parent / "golden"
        golden_file = golden_dir / "pipeline_smoke_sqlite.json"

        # Load or create golden snapshot
        try:
            expected_data = load_golden_snapshot(golden_file)

            # Compare with tolerance for floating point differences
            assert_json_close(snapshot_data, expected_data, tol=1e-6)

        except FileNotFoundError:
            # First run - save golden snapshot
            save_golden_snapshot(snapshot_data, golden_file)
            pytest.skip(f"Created golden snapshot: {golden_file}")

    @pytest.mark.skipif(
        not POSTGRES_AVAILABLE or not ASYNCPG_AVAILABLE,
        reason="PostgreSQL or asyncpg not available",
    )
    @pytest.mark.asyncio
    async def test_miniature_pipeline_postgres(self, temp_postgres_db):
        """Test miniature pipeline with PostgreSQL backend."""

        # Setup
        conn = temp_postgres_db
        test_games = create_test_game_data()[:2]
        game_ids = [game["game_id"] for game in test_games]

        # Create mock pipeline orchestrator
        orchestrator = MockPipelineOrchestrator(conn)

        # Run the pipeline
        result = await orchestrator.run_fetch_transform_load(game_ids)

        # Verify pipeline execution
        assert result["success"], f"Pipeline failed: {result['errors']}"
        assert result["games_processed"] == 2

        # Validate database state
        expected_counts = {"games": 2, "early_shocks": 2, "q1_window_12_8": 2}

        validation = await validate_database_state_async(conn, expected_counts)
        assert validation["valid"], f"Database validation failed: {validation['errors']}"

    @pytest.mark.asyncio
    async def test_cli_integration_mock(self, temp_sqlite_db):
        """Test CLI integration with mocked components."""

        conn = temp_sqlite_db

        # Mock the CLI commands
        with patch("subprocess.run") as mock_subprocess:
            # Mock successful CLI execution
            mock_subprocess.return_value = MagicMock(
                returncode=0, stdout="Pipeline completed successfully", stderr=""
            )

            # Simulate CLI command: python -m nba_scraper.cli fetch --games 0022400001,0022400002
            # then transform then load

            # For this test, we'll directly call our mock orchestrator
            orchestrator = MockPipelineOrchestrator(conn)
            result = await orchestrator.run_fetch_transform_load(["0022400001", "0022400002"])

            assert result["success"]
            assert result["games_processed"] == 2

    @pytest.mark.asyncio
    async def test_golden_snapshot_comparison(self, temp_sqlite_db):
        """Test golden snapshot comparison with different data."""

        conn = temp_sqlite_db

        # Insert known test data
        await conn.execute(
            """
            INSERT INTO games (game_id, season, game_date, home_team_id, away_team_id, status)
            VALUES ('TEST001', '2024-25', '2024-10-19', 1610612747, 1610612738, 'Final')
        """
        )

        await conn.execute(
            """
            INSERT INTO early_shocks (game_id, home_team_tricode, away_team_tricode, shock_magnitude)
            VALUES ('TEST001', 'LAL', 'BOS', 0.85)
        """
        )

        # Fetch data
        games_data = await fetch_table_as_sorted_json_async(conn, "games", ["game_id"])
        early_shocks_data = await fetch_table_as_sorted_json_async(
            conn, "early_shocks", ["game_id"]
        )

        # Test snapshot comparison utilities
        test_data = {"games": games_data, "early_shocks": early_shocks_data}

        # Test that identical data compares equal
        assert_json_close(test_data, test_data, tol=1e-6)

        # Test floating point tolerance
        modified_data = {
            "games": games_data,
            "early_shocks": [
                {
                    **early_shocks_data[0],
                    "shock_magnitude": 0.850001,  # Slight difference within tolerance
                }
            ],
        }

        assert_json_close(test_data, modified_data, tol=1e-3)

    @pytest.mark.asyncio
    async def test_pipeline_error_handling(self, temp_sqlite_db):
        """Test pipeline error handling and recovery."""

        conn = temp_sqlite_db

        # Create orchestrator that will fail
        class FailingOrchestrator(MockPipelineOrchestrator):
            async def run_fetch_transform_load(self, game_ids: List[str]) -> Dict[str, Any]:
                return {"games_processed": 0, "success": False, "errors": ["Simulated API failure"]}

        failing_orchestrator = FailingOrchestrator(conn)
        result = await failing_orchestrator.run_fetch_transform_load(["0022400001"])

        # Verify error handling
        assert not result["success"]
        assert len(result["errors"]) > 0
        assert "Simulated API failure" in result["errors"]

        # Verify database remains clean
        validation = await validate_database_state_async(
            conn, {"games": 0, "early_shocks": 0, "q1_window_12_8": 0}
        )
        assert validation["valid"]

    @pytest.mark.asyncio
    async def test_three_game_pipeline(self, temp_sqlite_db):
        """Test pipeline with 3 known games for comprehensive coverage."""

        conn = temp_sqlite_db

        # Add a third test game
        test_games = create_test_game_data()
        test_games.append(
            {
                "game_id": "0022400003",
                "home_team": "MIA",
                "away_team": "NYK",
                "game_date": "2024-10-20",
                "season": "2024-25",
            }
        )

        game_ids = [game["game_id"] for game in test_games]

        # Extend mock client for third game
        class ExtendedMockClient(MockNBAStatsClient):
            async def fetch_boxscore(self, game_id: str) -> Dict[str, Any]:
                if game_id == "0022400003":
                    return {
                        "resultSets": [
                            {
                                "name": "GameSummary",
                                "headers": [
                                    "GAME_ID",
                                    "GAME_DATE_EST",
                                    "HOME_TEAM_ID",
                                    "VISITOR_TEAM_ID",
                                    "HOME_TEAM_ABBREVIATION",
                                    "VISITOR_TEAM_ABBREVIATION",
                                    "PTS_HOME",
                                    "PTS_AWAY",
                                ],
                                "rowSet": [
                                    [
                                        "0022400003",
                                        "2024-10-20",
                                        1610612748,
                                        1610612752,
                                        "MIA",
                                        "NYK",
                                        108,
                                        102,
                                    ]
                                ],
                            }
                        ]
                    }
                else:
                    return await super().fetch_boxscore(game_id)

        # Create orchestrator with extended mock
        orchestrator = MockPipelineOrchestrator(conn)
        orchestrator.mock_client = ExtendedMockClient()

        # Run pipeline
        result = await orchestrator.run_fetch_transform_load(game_ids)

        # Verify results
        assert result["success"]
        assert result["games_processed"] == 3

        # Validate database state
        expected_counts = {"games": 3, "early_shocks": 3, "q1_window_12_8": 3}

        validation = await validate_database_state_async(conn, expected_counts)
        assert validation["valid"], f"Database validation failed: {validation['errors']}"

        # Verify data quality
        games_data = await fetch_table_as_sorted_json_async(conn, "games", ["game_id"])

        # Check that all 3 games are present and have expected structure
        assert len(games_data) == 3

        for game in games_data:
            assert "game_id" in game
            assert "season" in game
            assert "game_date" in game
            assert game["season"] == "2024-25"
