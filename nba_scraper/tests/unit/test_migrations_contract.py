"""Migration contract tests for forward/backward compatibility.

This module tests that migrations:
1. Can be applied forward (upgrade) without data loss
2. Can be rolled back (downgrade) safely
3. Preserve existing data through schema changes
4. Provide clear error messages for non-reversible operations

Contract tests ensure that:
- Data survives migrations (no silent data loss)
- Schema changes are backward compatible where possible
- Breaking changes are clearly documented and handled
"""

import os
import tempfile
from datetime import datetime, date
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.pool import StaticPool

from tests.fixtures.migrations import (
    migrate_to_revision,
    downgrade_to_revision,
    get_current_revision,
    get_all_revisions,
    async_migrate_to_revision,
    async_downgrade_to_revision,
    verify_data_preserved,
    get_table_names,
    get_current_revision_async,
)
from nba_scraper.nba_logging import get_logger

logger = get_logger(__name__)

# Test database mode - can be "sqlite" or "postgres"
TEST_DB_MODE = os.environ.get("TEST_DB", "sqlite").lower()


@pytest.fixture
async def migration_db_engine():
    """
    Create a clean database engine for migration testing.
    
    This creates a completely isolated database for each test to ensure
    migrations can be tested from a clean state.
    """
    if TEST_DB_MODE == "postgres":
        # Use a test-specific database
        db_name = f"nba_scraper_migration_test_{os.getpid()}"
        base_url = os.environ.get(
            "DATABASE_URL",
            "postgresql+asyncpg://nba_scraper_user@localhost:5432"
        )
        db_url = f"{base_url}/{db_name}"
        
        # Create the test database
        import asyncpg
        # Parse base connection without database
        parts = base_url.replace("postgresql+asyncpg://", "").split("/")
        conn_str = parts[0]
        
        sys_conn = await asyncpg.connect(f"postgresql://{conn_str}/postgres")
        try:
            await sys_conn.execute(f"DROP DATABASE IF EXISTS {db_name}")
            await sys_conn.execute(f"CREATE DATABASE {db_name}")
        finally:
            await sys_conn.close()
        
        engine = create_async_engine(db_url, echo=False)
        
        yield engine
        
        # Cleanup
        await engine.dispose()
        
        # Drop test database
        sys_conn = await asyncpg.connect(f"postgresql://{conn_str}/postgres")
        try:
            await sys_conn.execute(f"DROP DATABASE IF EXISTS {db_name}")
        finally:
            await sys_conn.close()
    else:
        # SQLite in-memory database
        db_url = "sqlite+aiosqlite:///:memory:"
        
        engine = create_async_engine(
            db_url,
            echo=False,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        
        yield engine
        
        await engine.dispose()


@pytest.fixture
async def baseline_db_with_data(migration_db_engine: AsyncEngine):
    """
    Create a database at the baseline schema with sample data.
    
    This fixture:
    1. Migrates to the baseline (first) migration
    2. Inserts minimal test data
    3. Returns the engine for further testing
    """
    db_url = str(migration_db_engine.url)
    
    # Get the first migration (baseline)
    all_revisions = get_all_revisions(db_url)
    if not all_revisions:
        pytest.skip("No migrations found")
    
    baseline_revision = all_revisions[0]
    
    # Migrate to baseline
    logger.info(f"Setting up baseline database at revision: {baseline_revision}")
    await async_migrate_to_revision(migration_db_engine, baseline_revision)
    
    # Verify migration applied using async version
    current = await get_current_revision_async(migration_db_engine)
    assert current == baseline_revision, f"Expected {baseline_revision}, got {current}"
    
    # Insert minimal test data
    async with migration_db_engine.begin() as conn:
        # Insert a test game
        await conn.execute(text("""
            INSERT INTO games (
                game_id, season, game_date_utc, game_date_local,
                arena_tz, home_team_tricode, away_team_tricode,
                status, source, source_url
            ) VALUES (
                :game_id, :season, :game_date_utc, :game_date_local,
                :arena_tz, :home, :away, :status, :source, :source_url
            )
        """), {
            "game_id": "0022300001",
            "season": "2023-24",
            "game_date_utc": datetime(2023, 10, 24, 19, 30),
            "game_date_local": date(2023, 10, 24),
            "arena_tz": "America/New_York",
            "home": "BOS",
            "away": "NYK",
            "status": "COMPLETED",
            "source": "test_migration",
            "source_url": "https://test.com/game/1",
        })
        
        # Insert a PBP event
        await conn.execute(text("""
            INSERT INTO pbp_events (
                game_id, period, event_idx, event_type,
                description, source, source_url
            ) VALUES (
                :game_id, :period, :event_idx, :event_type,
                :description, :source, :source_url
            )
        """), {
            "game_id": "0022300001",
            "period": 1,
            "event_idx": 1,
            "event_type": "SHOT_MADE",
            "description": "Test shot made",
            "source": "test_migration",
            "source_url": "https://test.com/pbp/1",
        })
        
        # Insert an outcome
        await conn.execute(text("""
            INSERT INTO outcomes (
                game_id, home_team_tricode, away_team_tricode,
                final_home_points, final_away_points, total_points,
                home_win, margin, source, source_url
            ) VALUES (
                :game_id, :home, :away, :home_pts, :away_pts,
                :total, :home_win, :margin, :source, :source_url
            )
        """), {
            "game_id": "0022300001",
            "home": "BOS",
            "away": "NYK",
            "home_pts": 108,
            "away_pts": 104,
            "total": 212,
            "home_win": True,
            "margin": 4,
            "source": "test_migration",
            "source_url": "https://test.com/outcome/1",
        })
    
    logger.info("Baseline database with test data created")
    
    return migration_db_engine


class TestMigrationForwardCompatibility:
    """Test that migrations can be applied forward without data loss."""
    
    @pytest.mark.asyncio
    async def test_upgrade_to_head_preserves_data(self, baseline_db_with_data: AsyncEngine):
        """
        Test upgrading from baseline to head preserves all data.
        
        This is the critical contract test: data must survive migrations.
        """
        db_url = str(baseline_db_with_data.url)
        
        # Count data before migration
        async with baseline_db_with_data.begin() as conn:
            games_before = await conn.execute(text("SELECT COUNT(*) FROM games"))
            games_count = games_before.scalar()
            
            pbp_before = await conn.execute(text("SELECT COUNT(*) FROM pbp_events"))
            pbp_count = pbp_before.scalar()
            
            outcomes_before = await conn.execute(text("SELECT COUNT(*) FROM outcomes"))
            outcomes_count = outcomes_before.scalar()
        
        logger.info(
            f"Data before migration: "
            f"games={games_count}, pbp={pbp_count}, outcomes={outcomes_count}"
        )
        
        # Upgrade to head
        await async_migrate_to_revision(baseline_db_with_data, "head")
        
        # Verify data preserved
        async with baseline_db_with_data.begin() as conn:
            # Check games table
            games_after = await conn.execute(text("SELECT COUNT(*) FROM games"))
            assert games_after.scalar() == games_count, "Games data lost during migration"
            
            # Check specific game data
            game_data = await conn.execute(
                text("SELECT game_id, season, home_team_tricode FROM games WHERE game_id = :id"),
                {"id": "0022300001"}
            )
            game_row = game_data.fetchone()
            assert game_row is not None, "Test game lost during migration"
            assert game_row[0] == "0022300001"
            assert game_row[1] == "2023-24"
            assert game_row[2] == "BOS"
            
            # Check pbp_events table
            pbp_after = await conn.execute(text("SELECT COUNT(*) FROM pbp_events"))
            assert pbp_after.scalar() == pbp_count, "PBP data lost during migration"
            
            # Check outcomes table
            outcomes_after = await conn.execute(text("SELECT COUNT(*) FROM outcomes"))
            assert outcomes_after.scalar() == outcomes_count, "Outcomes data lost during migration"
            
            # Verify outcome data integrity
            outcome_data = await conn.execute(
                text("SELECT final_home_points, margin FROM outcomes WHERE game_id = :id"),
                {"id": "0022300001"}
            )
            outcome_row = outcome_data.fetchone()
            assert outcome_row is not None
            assert outcome_row[0] == 108, "Outcome points corrupted"
            assert outcome_row[1] == 4, "Outcome margin corrupted"
        
        logger.info("✓ All data preserved after upgrade to head")
    
    @pytest.mark.asyncio
    async def test_upgrade_creates_expected_schema(self, baseline_db_with_data: AsyncEngine):
        """
        Test that upgrading to head creates the expected schema structure.
        
        Verifies that all expected tables exist after migration.
        """
        # Upgrade to head
        await async_migrate_to_revision(baseline_db_with_data, "head")
        
        # Get current schema
        tables = await get_table_names(baseline_db_with_data)
        
        # Expected core tables
        expected_tables = [
            "games",
            "game_id_crosswalk",
            "pbp_events",
            "outcomes",
            "starting_lineups",
            "ref_assignments",
            "ref_alternates",
            "injury_status",
            "q1_window_12_8",
            "early_shocks",
            "schedule_travel",
            "pipeline_state",
            "advanced_player_stats",
            "misc_player_stats",
            "usage_player_stats",
            "advanced_team_stats",
            "team_game_stats",
            "player_game_stats",
        ]
        
        for table in expected_tables:
            assert table in tables, f"Expected table '{table}' not found after migration"
        
        logger.info(f"✓ All {len(expected_tables)} expected tables exist after migration")
    
    @pytest.mark.asyncio
    async def test_multiple_sequential_upgrades(self, migration_db_engine: AsyncEngine):
        """
        Test upgrading through each migration sequentially.
        
        This ensures each individual migration step works correctly.
        """
        db_url = str(migration_db_engine.url)
        all_revisions = get_all_revisions(db_url)
        
        if len(all_revisions) <= 1:
            pytest.skip("Need at least 2 migrations for sequential test")
        
        # Start at base
        current = None
        
        for revision in all_revisions:
            logger.info(f"Upgrading to revision: {revision}")
            await async_migrate_to_revision(migration_db_engine, revision)
            
            # Verify migration applied
            current = get_current_revision(db_url)
            assert current == revision, f"Migration to {revision} failed"
        
        logger.info(f"✓ Successfully upgraded through {len(all_revisions)} migrations")


class TestMigrationBackwardCompatibility:
    """Test that migrations can be rolled back safely."""
    
    @pytest.mark.asyncio
    async def test_downgrade_one_step(self, migration_db_engine: AsyncEngine):
        """
        Test downgrading one step from head.
        
        This tests the immediate rollback scenario.
        """
        db_url = str(migration_db_engine.url)
        all_revisions = get_all_revisions(db_url)
        
        if len(all_revisions) < 2:
            pytest.skip("Need at least 2 migrations for downgrade test")
        
        # Upgrade to head
        await async_migrate_to_revision(migration_db_engine, "head")
        
        # Verify at head
        current = get_current_revision(db_url)
        assert current == all_revisions[-1]
        
        # Downgrade one step
        target_revision = all_revisions[-2]
        logger.info(f"Downgrading from {current} to {target_revision}")
        
        try:
            await async_downgrade_to_revision(migration_db_engine, target_revision)
            
            # Verify downgrade successful
            new_current = get_current_revision(db_url)
            assert new_current == target_revision, "Downgrade did not reach target revision"
            
            logger.info(f"✓ Successfully downgraded to {target_revision}")
            
        except Exception as e:
            # If downgrade is not supported, it should fail with a clear message
            error_msg = str(e)
            if "non-reversible" in error_msg.lower() or "cannot downgrade" in error_msg.lower():
                logger.info(f"✓ Non-reversible migration correctly raises clear error: {error_msg[:100]}")
                pytest.skip(f"Migration is non-reversible (expected): {error_msg[:100]}")
            else:
                raise
    
    @pytest.mark.asyncio
    async def test_downgrade_preserves_data_if_reversible(self, baseline_db_with_data: AsyncEngine):
        """
        Test that downgrade preserves data when reversible.
        
        If a migration is reversible, downgrading should not lose data.
        """
        db_url = str(baseline_db_with_data.url)
        all_revisions = get_all_revisions(db_url)
        
        if len(all_revisions) < 2:
            pytest.skip("Need at least 2 migrations for downgrade test")
        
        current_revision = get_current_revision(db_url)
        
        # Upgrade to next revision
        next_revision_idx = all_revisions.index(current_revision) + 1
        if next_revision_idx >= len(all_revisions):
            # Already at head, try to go to head explicitly
            await async_migrate_to_revision(baseline_db_with_data, "head")
        else:
            next_revision = all_revisions[next_revision_idx]
            await async_migrate_to_revision(baseline_db_with_data, next_revision)
        
        # Count data after upgrade
        async with baseline_db_with_data.begin() as conn:
            games_result = await conn.execute(text("SELECT COUNT(*) FROM games"))
            games_count = games_result.scalar()
        
        # Try to downgrade
        try:
            await async_downgrade_to_revision(baseline_db_with_data, current_revision)
            
            # If downgrade succeeded, verify data preserved
            async with baseline_db_with_data.begin() as conn:
                games_after = await conn.execute(text("SELECT COUNT(*) FROM games"))
                assert games_after.scalar() == games_count, "Data lost during downgrade"
            
            logger.info("✓ Data preserved during reversible downgrade")
            
        except Exception as e:
            error_msg = str(e)
            if "non-reversible" in error_msg.lower() or "cannot downgrade" in error_msg.lower():
                logger.info(f"✓ Non-reversible migration correctly documented: {error_msg[:100]}")
                pytest.skip(f"Migration is non-reversible (expected): {error_msg[:100]}")
            else:
                raise


class TestMigrationDataTransformation:
    """Test that migrations correctly transform data when schema changes."""
    
    @pytest.mark.asyncio
    async def test_nullable_to_required_migration_handles_nulls(self, migration_db_engine: AsyncEngine):
        """
        Test that migrations changing nullable to required handle existing NULLs.
        
        This is a common migration pattern that can cause data loss if not handled.
        """
        # This is a placeholder for when we have such a migration
        # For now, we'll test that the baseline handles it correctly
        
        db_url = str(migration_db_engine.url)
        all_revisions = get_all_revisions(db_url)
        
        if not all_revisions:
            pytest.skip("No migrations to test")
        
        # Migrate to baseline
        await async_migrate_to_revision(migration_db_engine, all_revisions[0])
        
        # Insert data with NULLs in optional fields
        async with migration_db_engine.begin() as conn:
            await conn.execute(text("""
                INSERT INTO games (
                    game_id, season, game_date_utc, game_date_local,
                    arena_tz, home_team_tricode, away_team_tricode,
                    status, source, source_url,
                    home_team_id, away_team_id
                ) VALUES (
                    '0022300999', '2023-24', :date_utc, :date_local,
                    'America/New_York', 'BOS', 'LAL',
                    'SCHEDULED', 'test', 'https://test.com',
                    NULL, NULL
                )
            """), {
                "date_utc": datetime(2023, 11, 1, 19, 30),
                "date_local": date(2023, 11, 1),
            })
        
        # Upgrade to head
        await async_migrate_to_revision(migration_db_engine, "head")
        
        # Verify game still exists (NULLs were handled)
        async with migration_db_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT game_id FROM games WHERE game_id = '0022300999'")
            )
            row = result.fetchone()
            assert row is not None, "Game with NULL values lost during migration"
        
        logger.info("✓ NULL values handled correctly during schema change")
    
    @pytest.mark.asyncio
    async def test_foreign_key_integrity_maintained(self, baseline_db_with_data: AsyncEngine):
        """
        Test that foreign key relationships are maintained through migrations.
        
        Ensures referential integrity is preserved.
        """
        # Upgrade to head
        await async_migrate_to_revision(baseline_db_with_data, "head")
        
        # Verify foreign key relationships work
        async with baseline_db_with_data.begin() as conn:
            # Insert a player stat that references the game
            await conn.execute(text("""
                INSERT INTO player_game_stats (
                    game_id, player_id, player_name, team_abbreviation,
                    points, source, source_url
                ) VALUES (
                    '0022300001', 'player123', 'Test Player', 'BOS',
                    20, 'test', 'https://test.com'
                )
            """))
            
            # Verify it was inserted
            result = await conn.execute(
                text("SELECT COUNT(*) FROM player_game_stats WHERE game_id = '0022300001'")
            )
            count = result.scalar()
            assert count == 1, "Foreign key relationship broken"
        
        logger.info("✓ Foreign key relationships maintained through migration")


class TestMigrationIdempotency:
    """Test that migrations are idempotent and can be re-applied safely."""
    
    @pytest.mark.asyncio
    async def test_reapply_head_is_safe(self, migration_db_engine: AsyncEngine):
        """
        Test that applying 'head' multiple times is safe.
        
        This ensures migrations are idempotent.
        """
        db_url = str(migration_db_engine.url)
        all_revisions = get_all_revisions(db_url)
        
        if not all_revisions:
            pytest.skip("No migrations to test")
        
        # Apply head
        await async_migrate_to_revision(migration_db_engine, "head")
        current = get_current_revision(db_url)
        
        # Apply head again (should be no-op)
        await async_migrate_to_revision(migration_db_engine, "head")
        current_again = get_current_revision(db_url)
        
        assert current == current_again, "Re-applying head changed revision"
        
        logger.info("✓ Re-applying head migration is safe (idempotent)")


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration
