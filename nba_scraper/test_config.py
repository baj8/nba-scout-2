"""Test configuration to override database settings for testing."""

import os
from unittest.mock import Mock

# Mock database connection for tests
def get_test_db():
    """Return a mock database connection for testing."""
    mock_db = Mock()
    mock_db.execute = Mock(return_value=Mock())
    mock_db.fetchall = Mock(return_value=[])
    mock_db.fetchone = Mock(return_value=None)
    mock_db.commit = Mock()
    mock_db.rollback = Mock()
    mock_db.close = Mock()
    return mock_db

# Set test environment variables
os.environ.setdefault('NBA_SCRAPER_ENV', 'test')
os.environ.setdefault('NBA_SCRAPER_DB_HOST', 'localhost')
os.environ.setdefault('NBA_SCRAPER_DB_NAME', 'test_nba_scraper')
os.environ.setdefault('NBA_SCRAPER_DB_USER', 'test_user')
os.environ.setdefault('NBA_SCRAPER_DB_PASSWORD', 'test_password')
os.environ.setdefault('NBA_SCRAPER_LOG_LEVEL', 'WARNING')