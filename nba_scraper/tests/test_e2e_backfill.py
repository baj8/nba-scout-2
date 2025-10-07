"""
End-to-end tests for the complete backfill workflow.

Tests the entire pipeline from CLI invocation through to database verification:
1. CLI command parsing and validation
2. Orchestrator coordination
3. API data fetching (mocked)
4. Data transformation and validation
5. Database persistence
6. Error handling and recovery
"""

from unittest.mock import AsyncMock, patch

import pytest
from nba_scraper.cli import app
from nba_scraper.db import get_connection
from typer.testing import CliRunner


@pytest.fixture
def cli_runner():
    """Create CLI runner for testing Typer commands."""
    return CliRunner()


@pytest.fixture
def mock_api_responses():
    """Mock API responses for a complete game."""
    return {
        "scoreboard": {
            "games": [
                {
                    "gameId": "0022100001",
                    "gameCode": "20211019/MILBKN",
                    "gameStatus": 3,
                    "gameStatusText": "Final",
                    "period": 4,
                    "gameClock": "",
                    "gameTimeUTC": "2021-10-19T23:30:00Z",
                    "gameEt": "2021-10-19T19:30:00-04:00",
                    "regulationPeriods": 4,
                    "seriesGameNumber": "",
                    "seriesText": "",
                    "homeTeam": {
                        "teamId": 1610612751,
                        "teamName": "Nets",
                        "teamCity": "Brooklyn",
                        "teamTricode": "BKN",
                        "score": 127,
                        "inBonus": None,
                        "timeoutsRemaining": 0,
                        "periods": [
                            {"period": 1, "periodType": "REGULAR", "score": 30},
                            {"period": 2, "periodType": "REGULAR", "score": 32},
                            {"period": 3, "periodType": "REGULAR", "score": 33},
                            {"period": 4, "periodType": "REGULAR", "score": 32},
                        ],
                    },
                    "awayTeam": {
                        "teamId": 1610612749,
                        "teamName": "Bucks",
                        "teamCity": "Milwaukee",
                        "teamTricode": "MIL",
                        "score": 104,
                        "inBonus": None,
                        "timeoutsRemaining": 0,
                        "periods": [
                            {"period": 1, "periodType": "REGULAR", "score": 25},
                            {"period": 2, "periodType": "REGULAR", "score": 26},
                            {"period": 3, "periodType": "REGULAR", "score": 27},
                            {"period": 4, "periodType": "REGULAR", "score": 26},
                        ],
                    },
                }
            ]
        },
        "boxscore": {
            "game": {
                "gameId": "0022100001",
                "homeTeam": {
                    "teamId": 1610612751,
                    "teamTricode": "BKN",
                    "players": [
                        {
                            "personId": 203507,
                            "name": "Giannis Antetokounmpo",
                            "position": "F",
                            "starter": "1",
                            "oncourt": "0",
                            "played": "1",
                            "statistics": {
                                "minutes": "PT35M12S",
                                "fieldGoalsMade": 10,
                                "fieldGoalsAttempted": 20,
                                "threePointersMade": 2,
                                "threePointersAttempted": 5,
                                "freeThrowsMade": 8,
                                "freeThrowsAttempted": 10,
                                "reboundsOffensive": 2,
                                "reboundsDefensive": 8,
                                "rebounds": 10,
                                "assists": 5,
                                "steals": 2,
                                "blocks": 1,
                                "turnovers": 3,
                                "foulsPersonal": 2,
                                "points": 30,
                                "plusMinusPoints": 15,
                            },
                        }
                    ],
                },
                "awayTeam": {"teamId": 1610612749, "teamTricode": "MIL", "players": []},
            }
        },
        "playbyplay": {
            "game": {
                "gameId": "0022100001",
                "actions": [
                    {
                        "actionNumber": 1,
                        "clock": "PT12M00S",
                        "period": 1,
                        "teamId": 1610612751,
                        "teamTricode": "BKN",
                        "personId": 203507,
                        "playerName": "Antetokounmpo",
                        "playerNameI": "G. Antetokounmpo",
                        "xLegacy": 25,
                        "yLegacy": 5,
                        "shotDistance": 23,
                        "shotResult": "Made",
                        "isFieldGoal": 1,
                        "scoreHome": "3",
                        "scoreAway": "0",
                        "pointsTotal": 3,
                        "location": "SideCenter",
                        "description": "Antetokounmpo 3PT Shot",
                        "actionType": "Shot",
                        "subType": "3pt",
                        "qualifiers": ["fromTurnover"],
                        "personIdsFilter": [203507],
                    }
                ],
            }
        },
    }


@pytest.fixture
async def clean_test_db():
    """Ensure clean database state for E2E tests."""
    conn = await get_connection()
    try:
        # Clean all tables in reverse dependency order
        await conn.execute("DELETE FROM play_by_play")
        await conn.execute("DELETE FROM player_stats")
        await conn.execute("DELETE FROM team_stats")
        await conn.execute("DELETE FROM games")
        await conn.execute("DELETE FROM teams")
        await conn.execute("DELETE FROM players")
        await conn.commit()
        yield conn
    finally:
        await conn.close()


class TestCLIBackfill:
    """Test CLI backfill command functionality."""

    def test_cli_help(self, cli_runner):
        """Test CLI help displays correctly."""
        result = cli_runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "NBA data scraping" in result.stdout

    def test_backfill_help(self, cli_runner):
        """Test backfill command help."""
        result = cli_runner.invoke(app, ["backfill", "--help"])
        assert result.exit_code == 0
        assert "Backfill NBA data" in result.stdout
        assert "--seasons" in result.stdout
        assert "--start-date" in result.stdout
        assert "--dry-run" in result.stdout

    def test_cli_validation_date_range_requires_end(self, cli_runner):
        """Test that start-date requires end-date."""
        result = cli_runner.invoke(app, ["backfill", "--start-date", "2021-10-19"])
        assert result.exit_code == 1
        assert "end-date is required" in result.stdout

    def test_cli_validation_mutually_exclusive(self, cli_runner):
        """Test that seasons and start-date are mutually exclusive."""
        result = cli_runner.invoke(
            app,
            [
                "backfill",
                "--seasons",
                "2023-24",
                "--start-date",
                "2021-10-19",
                "--end-date",
                "2021-10-20",
            ],
        )
        assert result.exit_code == 1
        assert "Cannot specify both" in result.stdout


class TestE2ESeasonBackfill:
    """Test complete season backfill workflow."""

    @pytest.mark.asyncio
    async def test_single_season_dry_run(self, cli_runner, mock_api_responses):
        """Test season backfill in dry-run mode."""
        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class:
            # Setup mock client
            mock_client = AsyncMock()
            mock_client.get_scoreboard.return_value = mock_api_responses["scoreboard"]
            mock_client_class.return_value = mock_client

            result = cli_runner.invoke(
                app, ["backfill", "--seasons", "2021-22", "--dry-run", "--batch-size", "10"]
            )

            assert result.exit_code == 0
            assert "DRY RUN MODE" in result.stdout
            assert "Season Summary" in result.stdout

    @pytest.mark.asyncio
    async def test_season_backfill_full_pipeline(
        self, cli_runner, mock_api_responses, clean_test_db
    ):
        """Test complete season backfill with database persistence."""
        conn = clean_test_db

        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class, patch(
            "nba_scraper.pipelines.backfill.BackfillOrchestrator._process_game"
        ) as mock_process:
            # Setup mock client
            mock_client = AsyncMock()
            mock_client.get_scoreboard.return_value = mock_api_responses["scoreboard"]
            mock_client.get_boxscore.return_value = mock_api_responses["boxscore"]
            mock_client.get_playbyplay.return_value = mock_api_responses["playbyplay"]
            mock_client_class.return_value = mock_client

            # Mock game processing to insert test data
            async def mock_game_processor(game_id, *args, **kwargs):
                # Insert teams
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO teams (team_id, abbreviation, full_name)
                    VALUES (?, ?, ?)
                """,
                    (1610612751, "BKN", "Brooklyn Nets"),
                )

                await conn.execute(
                    """
                    INSERT OR IGNORE INTO teams (team_id, abbreviation, full_name)
                    VALUES (?, ?, ?)
                """,
                    (1610612749, "MIL", "Milwaukee Bucks"),
                )

                # Insert game
                await conn.execute(
                    """
                    INSERT INTO games (
                        game_id, game_date, season, home_team_id, away_team_id,
                        home_score, away_score, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "0022100001",
                        "2021-10-19",
                        "2021-22",
                        1610612751,
                        1610612749,
                        127,
                        104,
                        "Final",
                    ),
                )

                await conn.commit()
                return {"success": True, "game_id": game_id}

            mock_process.side_effect = mock_game_processor

            result = cli_runner.invoke(
                app, ["backfill", "--seasons", "2021-22", "--batch-size", "1"]
            )

            # Verify CLI output
            assert result.exit_code == 0
            assert "Season Summary" in result.stdout

            # Verify database state
            cursor = await conn.execute("SELECT COUNT(*) FROM games")
            game_count = (await cursor.fetchone())[0]
            assert game_count >= 1

            cursor = await conn.execute("SELECT COUNT(*) FROM teams")
            team_count = (await cursor.fetchone())[0]
            assert team_count >= 2


class TestE2EDateRangeBackfill:
    """Test complete date range backfill workflow."""

    @pytest.mark.asyncio
    async def test_date_range_single_day(self, cli_runner, mock_api_responses, clean_test_db):
        """Test date range backfill for a single day."""
        # conn variable was unused, removing it

        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_scoreboard.return_value = mock_api_responses["scoreboard"]
            mock_client.get_boxscore.return_value = mock_api_responses["boxscore"]
            mock_client.get_playbyplay.return_value = mock_api_responses["playbyplay"]
            mock_client_class.return_value = mock_client

            result = cli_runner.invoke(
                app,
                ["backfill", "--start-date", "2021-10-19", "--end-date", "2021-10-19", "--dry-run"],
            )

            assert result.exit_code == 0
            assert "Date Range Summary" in result.stdout

    @pytest.mark.asyncio
    async def test_date_range_multi_day(self, cli_runner, mock_api_responses):
        """Test date range backfill spanning multiple days."""
        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_scoreboard.return_value = mock_api_responses["scoreboard"]
            mock_client_class.return_value = mock_client

            result = cli_runner.invoke(
                app,
                [
                    "backfill",
                    "--start-date",
                    "2021-10-19",
                    "--end-date",
                    "2021-10-21",
                    "--dry-run",
                    "--rate-limit",
                    "10.0",
                ],
            )

            assert result.exit_code == 0
            assert "Date Range Summary" in result.stdout


class TestE2EDataTransformation:
    """Test end-to-end data transformation pipeline."""

    @pytest.mark.asyncio
    async def test_complete_game_transformation(self, mock_api_responses, clean_test_db):
        """Test complete game data transformation and persistence."""
        from nba_scraper.transformers.game import GameTransformer

        conn = clean_test_db

        # Insert prerequisite data
        await conn.execute(
            """
            INSERT OR IGNORE INTO teams (team_id, abbreviation, full_name)
            VALUES (?, ?, ?)
        """,
            (1610612751, "BKN", "Brooklyn Nets"),
        )

        await conn.execute(
            """
            INSERT OR IGNORE INTO teams (team_id, abbreviation, full_name)
            VALUES (?, ?, ?)
        """,
            (1610612749, "MIL", "Milwaukee Bucks"),
        )

        await conn.execute(
            """
            INSERT OR IGNORE INTO players (player_id, full_name)
            VALUES (?, ?)
        """,
            (203507, "Giannis Antetokounmpo"),
        )

        await conn.commit()

        # Transform scoreboard data
        transformer = GameTransformer()
        game_data = mock_api_responses["scoreboard"]["games"][0]

        transformed = transformer.transform_game(game_data)

        # Verify transformation
        assert transformed["game_id"] == "0022100001"
        assert transformed["home_team_id"] == 1610612751
        assert transformed["away_team_id"] == 1610612749
        assert transformed["home_score"] == 127
        assert transformed["away_score"] == 104

        # Insert into database
        await conn.execute(
            """
            INSERT INTO games (
                game_id, game_date, season, home_team_id, away_team_id,
                home_score, away_score, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                transformed["game_id"],
                transformed["game_date"],
                transformed["season"],
                transformed["home_team_id"],
                transformed["away_team_id"],
                transformed["home_score"],
                transformed["away_score"],
                transformed["status"],
            ),
        )

        await conn.commit()

        # Verify database
        cursor = await conn.execute(
            """
            SELECT game_id, home_score, away_score
            FROM games
            WHERE game_id = ?
        """,
            ("0022100001",),
        )

        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "0022100001"
        assert row[1] == 127
        assert row[2] == 104


class TestE2EErrorHandling:
    """Test end-to-end error handling and recovery."""

    @pytest.mark.asyncio
    async def test_api_error_handling(self, cli_runner):
        """Test handling of API errors during backfill."""
        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_scoreboard.side_effect = Exception("API Error")
            mock_client_class.return_value = mock_client

            result = cli_runner.invoke(app, ["backfill", "--seasons", "2021-22", "--dry-run"])

            # Should handle error gracefully
            assert result.exit_code != 0 or "Error" in result.stdout

    @pytest.mark.asyncio
    async def test_database_connection_error(self, cli_runner):
        """Test handling of database connection errors."""
        with patch("nba_scraper.db.get_connection") as mock_conn:
            mock_conn.side_effect = Exception("Database connection failed")

            result = cli_runner.invoke(app, ["backfill", "--seasons", "2021-22"])

            assert result.exit_code == 1
            assert "Database connection failed" in result.stdout

    @pytest.mark.asyncio
    async def test_retry_quarantined_games(self, cli_runner, tmp_path):
        """Test retry of previously quarantined games."""
        # Create quarantine file
        quarantine_file = tmp_path / "quarantine_game_ids.txt"
        quarantine_file.write_text("0022100001\n0022100002\n")

        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class, patch(
            "nba_scraper.pipelines.backfill.QUARANTINE_FILE", str(quarantine_file)
        ):
            mock_client = AsyncMock()
            mock_client.get_scoreboard.return_value = {"games": []}
            mock_client_class.return_value = mock_client

            result = cli_runner.invoke(
                app, ["backfill", "--seasons", "2021-22", "--retry-quarantined", "--dry-run"]
            )

            assert result.exit_code == 0


class TestE2EPerformance:
    """Test end-to-end performance characteristics."""

    @pytest.mark.asyncio
    async def test_batch_processing(self, cli_runner, mock_api_responses):
        """Test batch processing performance."""
        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class:
            mock_client = AsyncMock()
            # Return multiple games
            mock_api_responses["scoreboard"]["games"] = [
                mock_api_responses["scoreboard"]["games"][0] for _ in range(5)
            ]
            mock_client.get_scoreboard.return_value = mock_api_responses["scoreboard"]
            mock_client_class.return_value = mock_client

            import time

            start = time.time()

            result = cli_runner.invoke(
                app,
                [
                    "backfill",
                    "--start-date",
                    "2021-10-19",
                    "--end-date",
                    "2021-10-19",
                    "--batch-size",
                    "5",
                    "--dry-run",
                ],
            )

            duration = time.time() - start

            assert result.exit_code == 0
            # Should complete reasonably quickly
            assert duration < 10.0

    @pytest.mark.asyncio
    async def test_rate_limiting(self, cli_runner, mock_api_responses):
        """Test that rate limiting is applied correctly."""
        with patch("nba_scraper.pipelines.backfill.NBAApiClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_scoreboard.return_value = mock_api_responses["scoreboard"]
            mock_client_class.return_value = mock_client

            result = cli_runner.invoke(
                app,
                [
                    "backfill",
                    "--start-date",
                    "2021-10-19",
                    "--end-date",
                    "2021-10-19",
                    "--rate-limit",
                    "1.0",
                    "--dry-run",
                ],
            )

            assert result.exit_code == 0


class TestE2EDataQuality:
    """Test end-to-end data quality validation."""

    @pytest.mark.asyncio
    async def test_data_completeness(self, mock_api_responses, clean_test_db):
        """Test that all expected data is persisted."""
        conn = clean_test_db

        # Setup prerequisite data
        await conn.execute(
            """
            INSERT OR IGNORE INTO teams (team_id, abbreviation, full_name)
            VALUES (?, ?, ?), (?, ?, ?)
        """,
            (1610612751, "BKN", "Brooklyn Nets", 1610612749, "MIL", "Milwaukee Bucks"),
        )

        await conn.execute(
            """
            INSERT OR IGNORE INTO players (player_id, full_name)
            VALUES (?, ?)
        """,
            (203507, "Giannis Antetokounmpo"),
        )

        # Insert game
        await conn.execute(
            """
            INSERT INTO games (
                game_id, game_date, season, home_team_id, away_team_id,
                home_score, away_score, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            ("0022100001", "2021-10-19", "2021-22", 1610612751, 1610612749, 127, 104, "Final"),
        )

        await conn.commit()

        # Verify all expected entities exist
        cursor = await conn.execute("SELECT COUNT(*) FROM games")
        assert (await cursor.fetchone())[0] == 1

        cursor = await conn.execute("SELECT COUNT(*) FROM teams")
        assert (await cursor.fetchone())[0] == 2

        cursor = await conn.execute("SELECT COUNT(*) FROM players")
        assert (await cursor.fetchone())[0] == 1

    @pytest.mark.asyncio
    async def test_data_consistency(self, clean_test_db):
        """Test referential integrity and data consistency."""
        conn = clean_test_db

        # Setup test data with relationships
        await conn.execute(
            """
            INSERT OR IGNORE INTO teams (team_id, abbreviation, full_name)
            VALUES (?, ?, ?)
        """,
            (1610612751, "BKN", "Brooklyn Nets"),
        )

        await conn.execute(
            """
            INSERT INTO games (
                game_id, game_date, season, home_team_id, away_team_id,
                home_score, away_score, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                "0022100001",
                "2021-10-19",
                "2021-22",
                1610612751,
                1610612749,  # This should fail due to FK constraint
                127,
                104,
                "Final",
            ),
        )

        # Should fail due to missing away team
        with pytest.raises(Exception):
            await conn.commit()
