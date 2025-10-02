"""Configuration management for NBA scraper."""

import os
import warnings
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from dotenv import load_dotenv
from pydantic import Field, field_validator, model_validator, SecretStr
from pydantic_settings import BaseSettings


class Environment(str, Enum):
    """Application environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


class LogFormat(str, Enum):
    """Log format options."""
    JSON = "json"
    TEXT = "text"
    STRUCTURED = "structured"


class DatabaseConfig(BaseSettings):
    """Database-specific configuration."""
    
    # Main database connection
    url: str = Field(
        default="postgresql+asyncpg://nba_scraper_user@localhost:5432/nba_scraper",
        description="Primary database connection URL"
    )
    
    # Connection pool settings
    pool_size: int = Field(default=10, description="Database connection pool size")
    max_overflow: int = Field(default=20, description="Maximum pool overflow connections")
    pool_timeout: int = Field(default=30, description="Pool connection timeout in seconds")
    pool_recycle: int = Field(default=3600, description="Pool connection recycle time in seconds")
    
    # Query settings
    query_timeout: int = Field(default=30, description="Query timeout in seconds")
    statement_timeout: int = Field(default=300, description="Statement timeout in seconds")
    
    # SSL/TLS settings
    ssl_mode: str = Field(default="prefer", description="PostgreSQL SSL mode")
    ssl_cert_path: Optional[Path] = Field(default=None, description="SSL certificate path")
    ssl_key_path: Optional[Path] = Field(default=None, description="SSL key path")
    ssl_ca_path: Optional[Path] = Field(default=None, description="SSL CA certificate path")
    
    @field_validator('url')
    @classmethod
    def validate_database_url(cls, v):
        """Validate database URL format."""
        try:
            parsed = urlparse(v)
            if not parsed.scheme.startswith('postgresql'):
                raise ValueError("Database URL must use postgresql or postgresql+asyncpg scheme")
            if not parsed.hostname:
                raise ValueError("Database URL must include hostname")
            if not parsed.path or parsed.path == '/':
                raise ValueError("Database URL must include database name")
            return v
        except Exception as e:
            raise ValueError(f"Invalid database URL: {e}")
    
    def get_connection_params(self) -> Dict[str, Union[str, int]]:
        """Get additional connection parameters for asyncpg."""
        params = {
            'server_settings': {
                'statement_timeout': f'{self.statement_timeout}s',
                'idle_in_transaction_session_timeout': '300s'
            }
        }
        
        if self.ssl_cert_path:
            params['ssl'] = {
                'sslmode': self.ssl_mode,
                'sslcert': str(self.ssl_cert_path),
                'sslkey': str(self.ssl_key_path) if self.ssl_key_path else None,
                'sslrootcert': str(self.ssl_ca_path) if self.ssl_ca_path else None
            }
        
        return params


class APIKeysConfig(BaseSettings):
    """API keys and authentication configuration."""
    
    # NBA Stats API (if needed for premium endpoints)
    nba_stats_api_key: Optional[SecretStr] = Field(
        default=None, 
        description="NBA Stats API key for premium endpoints"
    )
    
    # Basketball Reference (if premium subscription)
    bref_api_key: Optional[SecretStr] = Field(
        default=None,
        description="Basketball Reference API key"
    )
    
    # External services
    redis_password: Optional[SecretStr] = Field(
        default=None,
        description="Redis password for caching"
    )
    
    # Monitoring/observability
    sentry_dsn: Optional[SecretStr] = Field(
        default=None,
        description="Sentry DSN for error tracking"
    )
    
    datadog_api_key: Optional[SecretStr] = Field(
        default=None,
        description="Datadog API key for metrics"
    )
    
    # Alerting
    slack_webhook_url: Optional[SecretStr] = Field(
        default=None,
        description="Slack webhook URL for alerts"
    )
    
    pagerduty_integration_key: Optional[SecretStr] = Field(
        default=None,
        description="PagerDuty integration key"
    )

    class Config:
        """Pydantic configuration for API keys."""
        env_prefix = "API_"


class CacheConfig(BaseSettings):
    """Caching configuration."""
    
    # Local cache
    dir: Path = Field(default=Path(".cache"), description="Local cache directory")
    http_ttl: int = Field(default=3600, description="HTTP cache TTL in seconds")
    
    # Redis cache (optional)
    redis_url: Optional[str] = Field(
        default=None,
        description="Redis connection URL for distributed caching"
    )
    redis_ttl: int = Field(default=7200, description="Redis cache TTL in seconds")
    redis_max_connections: int = Field(default=10, description="Maximum Redis connections")
    
    # Cache policies
    enable_http_cache: bool = Field(default=True, description="Enable HTTP response caching")
    enable_query_cache: bool = Field(default=True, description="Enable database query caching")
    cache_compression: bool = Field(default=True, description="Enable cache compression")

    class Config:
        """Pydantic configuration for cache."""
        env_prefix = "CACHE_"


class MonitoringConfig(BaseSettings):
    """Monitoring and observability configuration."""
    
    # Metrics
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    metrics_port: int = Field(default=9090, description="Prometheus metrics port")
    
    # Health checks
    enable_health_checks: bool = Field(default=True, description="Enable health check endpoints")
    health_check_port: int = Field(default=8080, description="Health check port")
    
    # Tracing
    enable_tracing: bool = Field(default=False, description="Enable distributed tracing")
    tracing_sample_rate: float = Field(default=0.1, description="Tracing sample rate (0.0-1.0)")
    
    # Alerting thresholds
    error_rate_threshold: float = Field(default=0.05, description="Error rate alert threshold")
    latency_p99_threshold: float = Field(default=5.0, description="P99 latency threshold in seconds")
    queue_size_threshold: int = Field(default=1000, description="Queue size alert threshold")

    class Config:
        """Pydantic configuration for monitoring."""
        env_prefix = "MONITORING_"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Environment
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Application environment"
    )
    
    # Application info
    app_name: str = Field(default="nba-scraper", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    debug: bool = Field(default=False, description="Debug mode")
    
    # Database configuration
    database: DatabaseConfig = DatabaseConfig()
    
    # API keys configuration  
    api_keys: APIKeysConfig = APIKeysConfig()
    
    # Cache configuration
    cache: CacheConfig = CacheConfig()
    
    # Monitoring configuration
    monitoring: MonitoringConfig = MonitoringConfig()
    
    # HTTP Client
    user_agent: str = Field(
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        description="User agent for HTTP requests"
    )
    
    # Rate Limiting
    requests_per_min: int = Field(default=45, description="Maximum requests per minute")
    retry_max: int = Field(default=5, description="Maximum retry attempts")
    retry_backoff_factor: float = Field(default=2.0, description="Exponential backoff factor")
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: LogFormat = Field(default=LogFormat.JSON, description="Log output format")
    log_file_path: Optional[Path] = Field(default=None, description="Log file path")
    log_max_size: str = Field(default="100MB", description="Maximum log file size")
    log_backup_count: int = Field(default=5, description="Number of backup log files")
    
    # API Base URLs
    nba_stats_base_url: str = Field(
        default="https://stats.nba.com/stats",
        description="NBA Stats API base URL"
    )
    bref_base_url: str = Field(
        default="https://www.basketball-reference.com",
        description="Basketball Reference base URL"
    )
    gamebooks_base_url: str = Field(
        default="https://official.nba.com/referee-assignments",
        description="NBA Game Books base URL"
    )
    
    # Pipeline Configuration
    max_concurrent_requests: int = Field(
        default=5, 
        description="Maximum concurrent requests per pipeline"
    )
    backfill_chunk_size: int = Field(
        default=10,
        description="Number of games to process per chunk"
    )
    checkpoint_enabled: bool = Field(
        default=True,
        description="Enable pipeline checkpointing"
    )
    
    # Security
    allowed_hosts: List[str] = Field(
        default=["localhost", "127.0.0.1"],
        description="Allowed hostnames for the application"
    )
    cors_origins: List[str] = Field(
        default=["http://localhost:3000"],
        description="Allowed CORS origins"
    )
    
    # Data paths
    team_aliases_path: Path = Field(
        default=Path("team_aliases.yaml"),
        description="Path to team aliases YAML file"
    )
    venues_path: Path = Field(
        default=Path("venues.csv"),
        description="Path to venues CSV file"
    )
    
    # Feature flags
    enable_experimental_features: bool = Field(
        default=False,
        description="Enable experimental features"
    )
    enable_async_processing: bool = Field(
        default=True,
        description="Enable asynchronous processing"
    )

    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    @model_validator(mode='after')
    def validate_environment_settings(self):
        """Validate settings based on environment."""
        env = self.environment
        
        # Production environment validations
        if env == Environment.PRODUCTION:
            # Require secure database connection in production
            db_url = self.database.url
            if 'localhost' in db_url:
                warnings.warn(
                    "Using localhost database in production environment", 
                    UserWarning
                )
            
            # Require monitoring in production
            if not self.monitoring.enable_metrics:
                warnings.warn(
                    "Metrics disabled in production environment",
                    UserWarning
                )
        
        return self
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    def get_database_url(self) -> str:
        """Get the complete database URL with all parameters."""
        return self.database.url
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == Environment.DEVELOPMENT
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == Environment.PRODUCTION
    
    def is_test(self) -> bool:
        """Check if running in test environment."""
        return self.environment == Environment.TEST


@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    # Determine environment from ENV variable
    env = os.getenv('ENVIRONMENT', 'development').lower()
    
    # Load environment-specific .env file
    env_files = [
        f".env.{env}",  # Environment-specific file
        f".env.{env}.local",  # Local overrides for environment
        ".env.local",  # Local overrides
        ".env"  # Default file
    ]
    
    for env_file in env_files:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path, override=False)  # Don't override already set values
    
    return Settings()


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent.parent.parent


def get_data_dir() -> Path:
    """Get the data directory for test fixtures."""
    return get_project_root() / "tests" / "data"


def get_cache_dir() -> Path:
    """Get the cache directory, creating it if needed."""
    settings = get_settings()
    cache_dir = get_project_root() / settings.cache.dir
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_log_dir() -> Path:
    """Get the log directory, creating it if needed."""
    log_dir = get_project_root() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def validate_configuration() -> Dict[str, bool]:
    """Validate the current configuration and return status."""
    settings = get_settings()
    validation_results = {}
    
    try:
        # Validate database connection URL
        validation_results['database_url'] = bool(settings.get_database_url())
        
        # Validate required paths exist
        validation_results['team_aliases_exist'] = (
            get_project_root() / settings.team_aliases_path
        ).exists()
        validation_results['venues_exist'] = (
            get_project_root() / settings.venues_path
        ).exists()
        
        # Validate cache directory is writable
        cache_dir = get_cache_dir()
        validation_results['cache_writable'] = os.access(cache_dir, os.W_OK)
        
        # Validate log directory is writable
        log_dir = get_log_dir()
        validation_results['log_writable'] = os.access(log_dir, os.W_OK)
        
        # Validate API endpoints are accessible (basic URL format check)
        validation_results['nba_stats_url'] = bool(
            urlparse(settings.nba_stats_base_url).netloc
        )
        validation_results['bref_url'] = bool(
            urlparse(settings.bref_base_url).netloc
        )
        
    except Exception as e:
        validation_results['validation_error'] = str(e)
    
    return validation_results