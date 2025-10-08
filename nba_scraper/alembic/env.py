"""Alembic environment configuration for NBA Scraper.

This module configures Alembic to use async SQLAlchemy with PostgreSQL.
It reads database connection info from application settings and supports
both online and offline migration modes.
"""

import asyncio

# Import your application's database configuration
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# Add src to path so we can import nba_scraper
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nba_scraper.config import get_settings
from nba_scraper.db import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url():
    """Get database URL from application settings."""
    settings = get_settings()
    return settings.get_database_url()


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run migrations with the given connection."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations in 'online' mode using async engine.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    # Get database URL from configuration or settings
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        url = get_url()

    # Create async engine configuration
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = url

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    # Get the URL to check if it's async or sync
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        url = get_url()
    
    # Check if URL requires async (contains asyncpg or aiosqlite)
    # If it's a plain sqlite:// or postgresql:// URL, use sync mode
    if "asyncpg" in url or "aiosqlite" in url:
        # Async mode
        asyncio.run(run_async_migrations())
    else:
        # Sync mode for programmatic migrations (e.g., from tests)
        from sqlalchemy import create_engine
        
        connectable = create_engine(
            url,
            poolclass=pool.NullPool,
        )
        
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
                compare_server_default=True,
            )
            
            with context.begin_transaction():
                context.run_migrations()
        
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
