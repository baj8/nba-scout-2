"""Pytest configuration and fixtures."""

import os
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator, Dict, Any

# Set test environment before importing our modules
os.environ['ENVIRONMENT'] = 'test'
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

from src.nba_scraper.config import get_settings
from src.nba_scraper.models.pbp_rows import PbpEventRow
from src.nba_scraper.models.enums import EventType
from src.nba_scraper.models.game_rows import GameRow
from datetime import datetime, date


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_settings():
    """Get test-specific settings."""
    return get_settings()


@pytest.fixture
def mock_db_connection():
    """Mock database connection for unit tests."""
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value="INSERT 0 1")
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_conn.fetchval = AsyncMock(return_value=True)
    mock_conn.transaction = AsyncMock()
    
    # Mock the context manager behavior
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    
    return mock_conn


@pytest.fixture
def mock_performance_pool(mock_db_connection):
    """Mock performance connection pool."""
    mock_pool = AsyncMock()
    mock_pool.acquire = AsyncMock(return_value=mock_db_connection)
    mock_pool.release = AsyncMock()
    
    # Mock the context manager behavior
    mock_pool.__aenter__ = AsyncMock(return_value=mock_db_connection)
    mock_pool.__aexit__ = AsyncMock(return_value=None)
    
    return mock_pool


@pytest.fixture
def mock_validator():
    """Mock data quality validator."""
    mock_val = AsyncMock()
    
    # Mock validation methods to always return success
    # Return the input records as valid instead of empty lists
    async def mock_validate_before_insert(table_name, records):
        return records, []  # (valid_records, errors) - return all records as valid
    
    mock_val.validate_before_insert = mock_validate_before_insert
    mock_val.validate_foreign_keys = AsyncMock(return_value=True)
    return mock_val


@pytest.fixture(autouse=True)
def mock_database_connections(monkeypatch, mock_performance_pool, mock_db_connection, mock_validator):
    """Auto-mock all database connections for all tests."""
    
    # Mock all the different connection methods
    async def mock_get_connection():
        return mock_db_connection
    
    async def mock_get_performance_pool():
        return mock_performance_pool
    
    async def mock_get_session():
        yield mock_db_connection
    
    # Apply mocks to all the different import paths
    monkeypatch.setattr("src.nba_scraper.db.get_connection", mock_get_connection)
    monkeypatch.setattr("src.nba_scraper.db.get_performance_pool", mock_get_performance_pool)
    monkeypatch.setattr("src.nba_scraper.db.get_session", mock_get_session)
    
    # Mock imports from different modules
    monkeypatch.setattr("nba_scraper.db.get_connection", mock_get_connection)
    monkeypatch.setattr("nba_scraper.db.get_performance_pool", mock_get_performance_pool)
    monkeypatch.setattr("nba_scraper.db.get_session", mock_get_session)
    
    # Mock the validation system's database access
    monkeypatch.setattr("nba_scraper.validation.get_connection", mock_get_connection)
    monkeypatch.setattr("src.nba_scraper.validation.get_connection", mock_get_connection)
    
    # Mock the loaders' database access
    monkeypatch.setattr("nba_scraper.loaders.derived.get_connection", mock_get_connection)
    monkeypatch.setattr("src.nba_scraper.loaders.derived.get_connection", mock_get_connection)
    
    # Mock DataQualityValidator completely to avoid any database calls
    with patch('src.nba_scraper.validation.DataQualityValidator', return_value=mock_validator):
        with patch('nba_scraper.validation.DataQualityValidator', return_value=mock_validator):
            yield


@pytest.fixture
def sample_pbp_events():
    """Sample PBP events for testing."""
    return [
        PbpEventRow(
            game_id="0022300001",
            event_idx=1,
            period=1,
            seconds_elapsed=10.0,
            team_tricode="BOS",
            event_type=EventType.SHOT_MADE,
            shot_made=True,
            shot_value=2,
            description="Made 2PT field goal",
            source="test",
            source_url="https://test.com"
        ),
        PbpEventRow(
            game_id="0022300001",
            event_idx=2,
            period=1,
            seconds_elapsed=30.0,
            team_tricode="LAL",
            event_type=EventType.SHOT_MISSED,
            shot_made=False,
            shot_value=3,
            description="Missed 3PT field goal",
            source="test",
            source_url="https://test.com"
        )
    ]


@pytest.fixture
def sample_game_row():
    """Sample game row for testing."""
    return GameRow(
        game_id="0022300001",
        season="2023-24",
        game_date_utc=datetime(2023, 10, 17, 19, 30),
        game_date_local=date(2023, 10, 17),
        arena_tz="America/New_York",
        home_team_tricode="BOS",
        away_team_tricode="LAL",
        source="test",
        source_url="https://test.com"
    )


@pytest.fixture
def sample_parsed_gamebook():
    """Sample parsed gamebook data for testing."""
    return {
        'game_id': '0022400123',
        'refs': [
            {'name': 'John Smith', 'role': 'CREW_CHIEF', 'position': 1},
            {'name': 'Jane Doe', 'role': 'REFEREE', 'position': 2},
            {'name': 'Bob Johnson', 'role': 'OFFICIAL', 'position': 3}
        ],
        'alternates': ['Mike Wilson', 'Sarah Davis'],
        'parsing_confidence': 0.85,
        'arena': 'Madison Square Garden',
        'technical_fouls': [
            {'player': 'Jayson Tatum', 'time': '10:30', 'reason': 'Arguing call'},
            {'player': None, 'time': '5:15', 'reason': 'Bench Technical'}
        ],
        'teams': {'home': 'New York Knicks', 'away': 'Boston Celtics'}
    }


# Integration test markers
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as requiring actual database/API connections"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """Automatically mark integration tests."""
    for item in items:
        # Mark tests in integration directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Mark tests that use real databases
        if any(fixture in item.fixturenames for fixture in ["real_db", "live_api"]):
            item.add_marker(pytest.mark.integration)