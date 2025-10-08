"""Configuration management for NBA scraper with safe test defaults."""

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Application environment types."""
    TEST = "TEST"
    DEV = "DEV"
    PROD = "PROD"


class AppSettings(BaseSettings):
    """Application settings with safe test defaults and dotenv support.
    
    Environment variables can be set directly or via .env file.
    Nested settings use double underscore: DB__URL, CACHE__DIR, etc.
    """
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        case_sensitive=False,
        extra='ignore'  # Ignore extra fields for flexibility
    )

    # ===================
    # Environment
    # ===================
    ENV: Environment = Field(
        default=Environment.TEST,
        description='Application environment: TEST, DEV, or PROD'
    )

    # ===================
    # Database
    # ===================
    DB_URI: str = Field(
        default='sqlite:///:memory:',
        description='Database connection URI. Safe default for tests.'
    )
    DB_POOL_SIZE: int = Field(default=10, description='Database connection pool size')
    DB_MAX_OVERFLOW: int = Field(default=20, description='Maximum pool overflow')
    DB_POOL_TIMEOUT: int = Field(default=30, description='Pool timeout in seconds')
    DB_QUERY_TIMEOUT: int = Field(default=30, description='Query timeout in seconds')

    # ===================
    # HTTP Client
    # ===================
    TIMEOUT_S: float = Field(default=15.0, description='HTTP request timeout in seconds')
    OFFLINE: bool = Field(
        default=True,
        description='Offline mode - safe default for unit tests'
    )
    USER_AGENT: str = Field(
        default='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        description='User agent for HTTP requests'
    )
    
    # ===================
    # Rate Limits
    # ===================
    NBA_API_RPS: float = Field(
        default=4.0,
        description='NBA API requests per second (conservative default)'
    )
    BREF_RPS: float = Field(
        default=2.0,
        description='Basketball Reference requests per second'
    )
    MAX_CONCURRENT_REQUESTS: int = Field(
        default=5,
        description='Maximum concurrent HTTP requests'
    )
    
    # ===================
    # Retry & Backoff
    # ===================
    RETRY_MAX: int = Field(default=3, description='Maximum retry attempts')
    RETRY_BACKOFF_FACTOR: float = Field(default=2.0, description='Exponential backoff factor')
    
    # ===================
    # API Endpoints
    # ===================
    NBA_STATS_BASE_URL: str = Field(
        default='https://stats.nba.com/stats',
        description='NBA Stats API base URL'
    )
    BREF_BASE_URL: str = Field(
        default='https://www.basketball-reference.com',
        description='Basketball Reference base URL'
    )
    
    # ===================
    # Logging
    # ===================
    LOG_LEVEL: str = Field(default='INFO', description='Logging level')
    LOG_FORMAT: str = Field(default='json', description='Log format: json, text, or structured')
    LOG_FILE: Optional[Path] = Field(default=None, description='Log file path')
    
    # ===================
    # Cache & Storage
    # ===================
    CACHE_DIR: Path = Field(default=Path('.cache'), description='Local cache directory')
    CACHE_TTL: int = Field(default=3600, description='Cache TTL in seconds')
    ENABLE_HTTP_CACHE: bool = Field(default=True, description='Enable HTTP caching')
    
    # ===================
    # Pipeline
    # ===================
    BACKFILL_CHUNK_SIZE: int = Field(
        default=10,
        description='Number of games to process per chunk'
    )
    CHECKPOINT_ENABLED: bool = Field(
        default=True,
        description='Enable pipeline checkpointing'
    )
    
    # ===================
    # Feature Flags
    # ===================
    DEBUG: bool = Field(default=False, description='Debug mode')
    ENABLE_METRICS: bool = Field(default=False, description='Enable metrics collection')
    ENABLE_EXPERIMENTAL: bool = Field(default=False, description='Enable experimental features')
    
    # ===================
    # Data Paths
    # ===================
    TEAM_ALIASES_PATH: Path = Field(
        default=Path('team_aliases.yaml'),
        description='Path to team aliases file'
    )
    VENUES_PATH: Path = Field(
        default=Path('venues.csv'),
        description='Path to venues file'
    )

    @field_validator('LOG_LEVEL')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of: {', '.join(valid_levels)}")
        return v_upper
    
    @field_validator('ENV', mode='before')
    @classmethod
    def validate_env(cls, v) -> Environment:
        """Validate and normalize environment value."""
        if isinstance(v, Environment):
            return v
        if isinstance(v, str):
            v_upper = v.upper()
            # Map common variations to standard values
            if v_upper in ('TEST', 'TESTING'):
                return Environment.TEST
            elif v_upper in ('DEV', 'DEVELOPMENT', 'LOCAL'):
                return Environment.DEV
            elif v_upper in ('PROD', 'PRODUCTION'):
                return Environment.PROD
        raise ValueError(f"ENV must be TEST, DEV, or PROD (got: {v})")

    def is_test(self) -> bool:
        """Check if running in test environment."""
        return self.ENV == Environment.TEST
    
    def is_dev(self) -> bool:
        """Check if running in development environment."""
        return self.ENV == Environment.DEV
    
    def is_prod(self) -> bool:
        """Check if running in production environment."""
        return self.ENV == Environment.PROD
    
    def get_database_url(self) -> str:
        """Get the database connection URL."""
        return self.DB_URI


@lru_cache()
def get_settings() -> AppSettings:
    """Get cached application settings.
    
    Loads settings from:
    1. Environment variables
    2. .env file (if exists)
    3. Default values
    
    Returns:
        AppSettings: Cached settings instance
    """
    return AppSettings()


def get_project_root() -> Path:
    """Get the project root directory.
    
    Returns:
        Path: Project root directory
    """
    # From src/nba_scraper/config.py -> project root
    return Path(__file__).parent.parent.parent.parent


def get_data_dir() -> Path:
    """Get the test data directory.
    
    Returns:
        Path: Test data directory
    """
    return get_project_root() / "tests" / "data"


def get_cache_dir() -> Path:
    """Get the cache directory, creating it if needed.
    
    Returns:
        Path: Cache directory
    """
    settings = get_settings()
    cache_dir = get_project_root() / settings.CACHE_DIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_log_dir() -> Path:
    """Get the log directory, creating it if needed.
    
    Returns:
        Path: Log directory
    """
    log_dir = get_project_root() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


# Convenience instance for backward compatibility
settings = get_settings()