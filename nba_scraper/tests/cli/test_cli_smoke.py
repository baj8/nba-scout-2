"""Smoke tests for the NBA Scraper CLI to ensure it works without network/DB calls."""

import pytest
from typer.testing import CliRunner
from nba_scraper.cli import app

runner = CliRunner()


class FakeOrchestrator:
    """Mock orchestrator that returns deterministic results without doing real work."""
    
    def __init__(self, rate_limit: float, batch_size: int, raw_root: str):
        """Initialize fake orchestrator with same signature as real one."""
        self.rate_limit = rate_limit
        self.batch_size = batch_size
        self.raw_root = raw_root

    async def backfill_date_range(
        self, 
        start_date: str, 
        end_date: str, 
        dry_run: bool, 
        retry_quarantined: bool
    ):
        """Mock date range backfill that returns deterministic results."""
        return {
            "start_date": start_date,
            "end_date": end_date,
            "dates_processed": 1,
            "total_games": 11,
            "errors": []
        }

    async def backfill_seasons(
        self, 
        seasons, 
        dry_run: bool, 
        retry_quarantined: bool
    ):
        """Mock season backfill that returns deterministic results."""
        return {
            "seasons_processed": [
                {
                    "season": seasons[0] if seasons else "2023-24",
                    "games": 1230,
                    "status": "completed",
                    "dates": 170
                }
            ],
            "total_dates": 170,
            "total_games": 1230,
            "errors": []
        }


def test_backfill_smoke_date_range_dry_run(monkeypatch):
    """Test that CLI date range backfill works with dry run and stubbed orchestrator."""
    # Patch the BackfillOrchestrator class at the source module where it's defined
    monkeypatch.setattr("nba_scraper.pipelines.backfill.BackfillOrchestrator", FakeOrchestrator)
    
    # Run the CLI command
    result = runner.invoke(app, [
        "--start-date", "2023-10-27", 
        "--end-date", "2023-10-27", 
        "--dry-run", 
        "--raw-root", "raw"
    ])
    
    # Verify successful execution
    assert result.exit_code == 0, f"CLI failed with output: {result.output}"
    
    # Verify expected output content
    assert "Starting date range backfill: 2023-10-27 to 2023-10-27" in result.output
    assert "DRY RUN MODE" in result.output
    assert "Dates processed: 1" in result.output
    assert "Total games: 11" in result.output
    assert "Backfill completed successfully" in result.output
    
    # Ensure no error messages
    assert "failed" not in result.output.lower()
    assert "error" not in result.output.lower()


def test_backfill_smoke_seasons_dry_run(monkeypatch):
    """Test that CLI season backfill works with dry run and stubbed orchestrator."""
    # Patch the BackfillOrchestrator class at the source module where it's defined
    monkeypatch.setattr("nba_scraper.pipelines.backfill.BackfillOrchestrator", FakeOrchestrator)
    
    # Run the CLI command
    result = runner.invoke(app, [
        "--seasons", "2023-24", 
        "--dry-run", 
        "--raw-root", "raw"
    ])
    
    # Verify successful execution
    assert result.exit_code == 0, f"CLI failed with output: {result.output}"
    
    # Verify expected output content
    assert "Starting season backfill: 2023-24" in result.output
    assert "DRY RUN MODE" in result.output
    assert "2023-24: 1230 games" in result.output
    assert "Total dates: 170" in result.output
    assert "Total games: 1230" in result.output
    assert "Backfill completed successfully" in result.output
    
    # Ensure no error messages
    assert "failed" not in result.output.lower()
    assert "error" not in result.output.lower()


def test_backfill_smoke_help_command():
    """Test that CLI help command works without errors."""
    result = runner.invoke(app, ["--help"])
    
    # Verify successful execution
    assert result.exit_code == 0, f"Help command failed: {result.output}"
    
    # Verify help content
    assert "Backfill NBA data for seasons or date ranges" in result.output
    assert "--start-date" in result.output
    assert "--seasons" in result.output
    assert "--dry-run" in result.output
    assert "--rate-limit" in result.output


def test_backfill_smoke_input_validation():
    """Test that CLI properly validates mutually exclusive options."""
    # Test that providing both start-date and custom seasons fails
    result = runner.invoke(app, [
        "--seasons", "2022-23", 
        "--start-date", "2023-10-27", 
        "--end-date", "2023-10-27"
    ])
    
    # Should fail with validation error
    assert result.exit_code == 1
    assert "Cannot specify both --seasons and --start-date" in result.output
    
    # Test that providing start-date without end-date fails
    result = runner.invoke(app, [
        "--start-date", "2023-10-27"
    ])
    
    # Should fail with validation error
    assert result.exit_code == 1
    assert "--end-date is required when using --start-date" in result.output