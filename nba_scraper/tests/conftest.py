"""Test configuration and fixtures for NBA scraper test suite."""

import os
import sys
import pathlib

# Force test-safe defaults before any other imports
os.environ.setdefault('ENV', 'TEST')
os.environ.setdefault('DB_URI', 'sqlite:///:memory:')
os.environ.setdefault('OFFLINE', 'true')

# Ensure tests can import from src/
ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from datetime import datetime

import pytest
import asyncpg

# Import database fixtures for easy access
from tests.fixtures.db import (
    db_session,
    db_connection,
    asyncpg_connection,
    test_db_engine,
    test_session_factory,
    test_game,
    multiple_test_games,
    clean_tables,
)

from nba_scraper.db import get_connection
from nba_scraper.io_clients.nba_stats import NBAStatsClient
from nba_scraper.utils.db import maybe_transaction


@pytest.fixture
async def db():
    """Database connection fixture with transaction rollback."""
    conn = await get_connection()
    async with maybe_transaction(conn):
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