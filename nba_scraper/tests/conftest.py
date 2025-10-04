"""Test configuration and fixtures for NBA scraper test suite."""

import sys
import pathlib
from datetime import datetime

# Ensure tests can import from src/
ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pytest
import asyncpg
from nba_scraper.db import get_connection
from nba_scraper.io_clients.nba_stats import NBAStatsClient


@pytest.fixture
async def db():
    """Database connection fixture with transaction rollback."""
    conn = await get_connection()
    async with conn.transaction():
        yield conn
        # Transaction is automatically rolled back after test


@pytest.fixture
def io_impl():
    """Mock or real IO implementation for testing."""
    return NBAStatsClient()


@pytest.fixture
def test_game_id():
    """Standard test game ID for consistent testing."""
    return "0022300001"