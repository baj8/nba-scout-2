"""Database connection and session management with performance optimization."""

import asyncpg
from typing import AsyncGenerator, Optional

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.ext.asyncio.session import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings
from .nba_logging import get_logger

logger = get_logger(__name__)


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""
    metadata = MetaData()


# Global engine instance
_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None
_performance_pool: Optional[asyncpg.Pool] = None


def get_engine() -> AsyncEngine:
    """Get the global database engine."""
    global _engine
    if (_engine is None):
        settings = get_settings()
        _engine = create_async_engine(
            settings.get_database_url(),
            echo=False,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_size=10,
            max_overflow=20,
        )
        logger.info("Database engine created", url=settings.get_database_url().split("@")[0] + "@***")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get the global session factory."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        logger.info("Session factory created")
    return _session_factory


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get an async database session."""
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_performance_pool() -> asyncpg.Pool:
    """Get optimized connection pool for high-performance operations."""
    global _performance_pool
    if _performance_pool is None:
        settings = get_settings()
        # Parse PostgreSQL URL for asyncpg
        import urllib.parse as urlparse
        result = urlparse.urlparse(settings.get_database_url().replace("postgresql+asyncpg://", "postgresql://"))
        
        _performance_pool = await asyncpg.create_pool(
            host=result.hostname,
            port=result.port or 5432,
            user=result.username,
            password=result.password,
            database=result.path.lstrip('/'),
            min_size=5,
            max_size=20,
            command_timeout=60,
            # Performance optimizations
            server_settings={
                'application_name': 'nba_scraper_performance',
                'tcp_keepalives_idle': '600',
                'tcp_keepalives_interval': '30',
                'tcp_keepalives_count': '3',
            }
        )
        logger.info("Performance connection pool created")
    return _performance_pool


async def get_connection():
    """Get a raw database connection (for backwards compatibility)."""
    pool = await get_performance_pool()
    return await pool.acquire()


async def close_engine() -> None:
    """Close the database engine and performance pool."""
    global _engine, _session_factory, _performance_pool
    
    if _performance_pool is not None:
        await _performance_pool.close()
        _performance_pool = None
        logger.info("Performance connection pool closed")
    
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Database engine closed")


async def check_connection() -> bool:
    """Check if database connection is working."""
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connection verified")
        return True
    except Exception as e:
        logger.error("Database connection failed", error=str(e))
        return False


async def create_tables() -> None:
    """Create all database tables."""
    engine = get_engine()
    async with engine.begin() as conn:
        # Read and execute schema.sql
        from .config import get_project_root
        schema_path = get_project_root() / "schema.sql"
        
        if schema_path.exists():
            schema_sql = schema_path.read_text()
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement and not statement.startswith('--'):
                    await conn.execute(text(statement))
                    
            logger.info("Database tables created from schema.sql")
        else:
            logger.warning("schema.sql not found, skipping table creation")


class UpsertStats:
    """Statistics from an upsert operation."""
    
    def __init__(self) -> None:
        self.inserted = 0
        self.updated = 0
        self.unchanged = 0
        self.errors = 0
    
    @property
    def total(self) -> int:
        """Total rows processed."""
        return self.inserted + self.updated + self.unchanged + self.errors
    
    def __str__(self) -> str:
        return f"UpsertStats(inserted={self.inserted}, updated={self.updated}, unchanged={self.unchanged}, errors={self.errors})"