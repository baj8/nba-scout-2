#!/usr/bin/env python3
"""Helper script to run Alembic migrations.

This script provides a convenient wrapper around Alembic commands with
proper environment configuration and error handling.

Usage:
    python migrate.py upgrade head       # Upgrade to latest
    python migrate.py downgrade -1       # Downgrade one version
    python migrate.py current            # Show current version
    python migrate.py history            # Show migration history
"""

import argparse
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_scraper.config import get_settings
from nba_scraper.nba_logging import get_logger

logger = get_logger(__name__)


def get_alembic_config() -> Config:
    """Create Alembic configuration."""
    project_root = Path(__file__).parent
    alembic_ini_path = project_root / "alembic.ini"

    if not alembic_ini_path.exists():
        raise FileNotFoundError(f"alembic.ini not found at {alembic_ini_path}")

    config = Config(str(alembic_ini_path))
    config.set_main_option("script_location", str(project_root / "alembic"))

    # Set database URL from settings
    settings = get_settings()
    config.set_main_option("sqlalchemy.url", settings.get_database_url())

    return config


def run_upgrade(revision: str = "head") -> None:
    """Upgrade database to a specific revision."""
    logger.info("Starting database upgrade", revision=revision)
    config = get_alembic_config()

    try:
        command.upgrade(config, revision)
        logger.info("Database upgrade completed successfully", revision=revision)
    except Exception as e:
        logger.error("Database upgrade failed", error=str(e), revision=revision)
        raise


def run_downgrade(revision: str = "-1") -> None:
    """Downgrade database to a specific revision."""
    logger.warning("Starting database downgrade", revision=revision)
    config = get_alembic_config()

    try:
        command.downgrade(config, revision)
        logger.info("Database downgrade completed successfully", revision=revision)
    except Exception as e:
        logger.error("Database downgrade failed", error=str(e), revision=revision)
        raise


def show_current() -> None:
    """Show current database revision."""
    config = get_alembic_config()
    command.current(config, verbose=True)


def show_history() -> None:
    """Show migration history."""
    config = get_alembic_config()
    command.history(config, verbose=True)


def create_revision(message: str, autogenerate: bool = False) -> None:
    """Create a new migration revision."""
    logger.info("Creating new migration", message=message, autogenerate=autogenerate)
    config = get_alembic_config()

    try:
        command.revision(config, message=message, autogenerate=autogenerate)
        logger.info("Migration created successfully", message=message)
    except Exception as e:
        logger.error("Migration creation failed", error=str(e), message=message)
        raise


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Database migration tool for NBA Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s upgrade head         Upgrade to latest version
  %(prog)s downgrade -1         Downgrade one version
  %(prog)s downgrade base       Downgrade to base (remove all)
  %(prog)s current              Show current version
  %(prog)s history              Show migration history
  %(prog)s revision "add field" Create new migration
  %(prog)s revision --auto "changes" Auto-generate migration
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Migration command")

    # Upgrade command
    upgrade_parser = subparsers.add_parser("upgrade", help="Upgrade database")
    upgrade_parser.add_argument(
        "revision", nargs="?", default="head", help="Target revision (default: head)"
    )

    # Downgrade command
    downgrade_parser = subparsers.add_parser("downgrade", help="Downgrade database")
    downgrade_parser.add_argument(
        "revision", nargs="?", default="-1", help="Target revision (default: -1)"
    )

    # Current command
    subparsers.add_parser("current", help="Show current revision")

    # History command
    subparsers.add_parser("history", help="Show migration history")

    # Revision command
    revision_parser = subparsers.add_parser("revision", help="Create new migration")
    revision_parser.add_argument("message", help="Migration message")
    revision_parser.add_argument(
        "--auto", action="store_true", help="Auto-generate migration from model changes"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    try:
        if args.command == "upgrade":
            run_upgrade(args.revision)
        elif args.command == "downgrade":
            run_downgrade(args.revision)
        elif args.command == "current":
            show_current()
        elif args.command == "history":
            show_history()
        elif args.command == "revision":
            create_revision(args.message, autogenerate=args.auto)
        else:
            parser.print_help()
            return 1

        return 0

    except Exception as e:
        logger.error("Migration command failed", command=args.command, error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
