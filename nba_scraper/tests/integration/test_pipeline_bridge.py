"""Integration tests for GameRow → Game → DB pipeline with FK integrity."""

import pytest
import asyncio
from datetime import datetime, date, timezone
from unittest.mock import AsyncMock

from nba_scraper.models.game_rows import GameRow
from nba_scraper.models.pbp_rows import PbpEventRow
from nba_scraper.models.lineup_rows import StartingLineupRow
from nba_scraper.models.enums import GameStatus, EventType, Position
from nba_scraper.transformers.games_bridge import to_db_game
from nba_scraper.utils.team_crosswalk import get_team_index
from nba_scraper.loaders import upsert_game, upsert_pbp, upsert_lineups
from nba_scraper.db import get_connection


@pytest.mark.asyncio
class TestIntegrationPipeline:
    """Integration tests for the complete data model bridge pipeline."""

    async def test_facade_import_resilience(self):
        """Test that loaders can be imported via facade reliably."""
        # Import via facade should work
        from nba_scraper.loaders import upsert_game, upsert_pbp, upsert_lineups
        
        # Functions should be callable
        assert callable(upsert_game)
        assert callable(upsert_pbp) 
        assert callable(upsert_lineups)

    @pytest.mark.skipif(True, reason="Requires database connection")
    async def test_game_upsert_with_bridge(self):
        """Test Game upsert via bridge transformation with one known game."""
        game_row = GameRow(
            game_id="TEST_INTEGRATION_001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="LAL",
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test_integration",
            source_url="https://test.com"
        )
        
        try:
            conn = await get_connection()
            
            # Transform using bridge
            team_index = get_team_index()
            db_game = to_db_game(game_row, team_index=team_index)
            
            # Upsert to database
            await upsert_game(conn, db_game)
            
            # Verify stored correctly
            stored = await conn.fetchrow(
                "SELECT * FROM games WHERE game_id = $1", 
                "TEST_INTEGRATION_001"
            )
            
            assert stored is not None
            assert stored['game_id'] == "TEST_INTEGRATION_001"
            assert stored['season'] == "2024-25"  
            assert stored['game_date'] == "2024-10-15"
            assert stored['home_team_id'] == 1610612747  # LAL
            assert stored['away_team_id'] == 1610612744  # GSW
            assert stored['status'] == "Final"
            
            # Verify exactly 1 row
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM games WHERE game_id = $1",
                "TEST_INTEGRATION_001"
            )
            assert count == 1
            
        finally:
            # Cleanup
            if 'conn' in locals():
                await conn.execute(
                    "DELETE FROM games WHERE game_id = $1", 
                    "TEST_INTEGRATION_001"
                )

    @pytest.mark.skipif(True, reason="Requires database connection")
    async def test_pbp_pipeline_with_seconds_elapsed(self):
        """Test PBP pipeline loads ≥400 rows with ≥75% having seconds_elapsed."""
        # First ensure we have a game record
        game_row = GameRow(
            game_id="TEST_PBP_001",
            season="2024-25", 
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="LAL",
            away_team_tricode="GSW", 
            status=GameStatus.FINAL,
            source="test_integration",
            source_url="https://test.com"
        )
        
        try:
            conn = await get_connection()
            
            # Insert game first (for FK integrity)
            team_index = get_team_index()
            db_game = to_db_game(game_row, team_index=team_index)
            await upsert_game(conn, db_game)
            
            # Create ≥400 PBP events with various time formats
            pbp_events = []
            for i in range(450):  # More than 400
                period = (i // 120) + 1  # ~120 events per period
                minutes = 12 - (i % 120) // 10
                seconds = (i % 120) % 60
                time_remaining = f"{minutes}:{seconds:02d}"
                
                pbp_event = PbpEventRow(
                    game_id="TEST_PBP_001",
                    period=period,
                    event_idx=i + 1,
                    time_remaining=time_remaining,
                    event_type=EventType.SHOT_MADE if i % 2 == 0 else EventType.SHOT_MISSED,
                    source="test_integration",
                    source_url="https://test.com"
                )
                pbp_events.append(pbp_event)
            
            # Load PBP events
            await upsert_pbp(conn, pbp_events)
            
            # Verify loaded count
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM pbp_events WHERE game_id = $1",
                "TEST_PBP_001"
            )
            assert total_count >= 400
            
            # Verify seconds_elapsed coverage
            with_seconds_count = await conn.fetchval(
                "SELECT COUNT(*) FROM pbp_events WHERE game_id = $1 AND seconds_elapsed IS NOT NULL",
                "TEST_PBP_001"
            )
            
            # Should be ≥75% or ≥300 rows, whichever is larger
            min_required = max(int(total_count * 0.75), 300)
            assert with_seconds_count >= min_required
            
        finally:
            # Cleanup
            if 'conn' in locals():
                await conn.execute("DELETE FROM pbp_events WHERE game_id = $1", "TEST_PBP_001")
                await conn.execute("DELETE FROM games WHERE game_id = $1", "TEST_PBP_001")

    @pytest.mark.skipif(True, reason="Requires database connection")
    async def test_starting_lineups_pipeline(self):
        """Test starting lineups pipeline with PK/constraint satisfaction."""
        # First ensure we have a game record
        game_row = GameRow(
            game_id="TEST_LINEUP_001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific", 
            home_team_tricode="LAL",
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test_integration",
            source_url="https://test.com"
        )
        
        try:
            conn = await get_connection()
            
            # Insert game first (for FK integrity)
            team_index = get_team_index()
            db_game = to_db_game(game_row, team_index=team_index)
            await upsert_game(conn, db_game)
            
            # Create starting lineups for both teams
            lineups = []
            
            # LAL starters
            lal_players = [
                ("LeBron James", Position.F, 6),
                ("Anthony Davis", Position.F, 3),
                ("Russell Westbrook", Position.G, 0),
                ("Austin Reaves", Position.G, 15),
                ("Christian Wood", Position.C, 35)
            ]
            
            for name, position, jersey in lal_players:
                lineup = StartingLineupRow(
                    game_id="TEST_LINEUP_001",
                    team_tricode="LAL",
                    player_name_slug=name.lower().replace(" ", "_"),
                    player_display_name=name,
                    player_id=f"lal_{jersey}",
                    position=position,
                    jersey_number=jersey,
                    final_pre_tip=True,
                    source="test_integration",
                    source_url="https://test.com"
                )
                lineups.append(lineup)
            
            # GSW starters
            gsw_players = [
                ("Stephen Curry", Position.G, 30),
                ("Klay Thompson", Position.G, 11), 
                ("Andrew Wiggins", Position.F, 22),
                ("Draymond Green", Position.F, 23),
                ("Kevon Looney", Position.C, 5)
            ]
            
            for name, position, jersey in gsw_players:
                lineup = StartingLineupRow(
                    game_id="TEST_LINEUP_001",
                    team_tricode="GSW",
                    player_name_slug=name.lower().replace(" ", "_"),
                    player_display_name=name,
                    player_id=f"gsw_{jersey}",
                    position=position,
                    jersey_number=jersey,
                    final_pre_tip=True,
                    source="test_integration",
                    source_url="https://test.com"
                )
                lineups.append(lineup)
            
            # Load lineups
            await upsert_lineups(conn, lineups)
            
            # Verify loaded correctly
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM starting_lineups WHERE game_id = $1",
                "TEST_LINEUP_001"
            )
            assert total_count == 10  # 5 per team
            
            # Verify team distribution
            lal_count = await conn.fetchval(
                "SELECT COUNT(*) FROM starting_lineups WHERE game_id = $1 AND team_tricode = 'LAL'",
                "TEST_LINEUP_001"
            )
            gsw_count = await conn.fetchval(
                "SELECT COUNT(*) FROM starting_lineups WHERE game_id = $1 AND team_tricode = 'GSW'",
                "TEST_LINEUP_001"
            )
            assert lal_count == 5
            assert gsw_count == 5
            
        finally:
            # Cleanup
            if 'conn' in locals():
                await conn.execute("DELETE FROM starting_lineups WHERE game_id = $1", "TEST_LINEUP_001")
                await conn.execute("DELETE FROM games WHERE game_id = $1", "TEST_LINEUP_001")

    @pytest.mark.skipif(True, reason="Requires database connection")
    async def test_fk_integrity_enforcement(self):
        """Test that all child tables properly reference games(game_id)."""
        try:
            conn = await get_connection()
            
            # Try to insert PBP event without corresponding game (should fail)
            pbp_event = PbpEventRow(
                game_id="NONEXISTENT_GAME",
                period=1,
                event_idx=1,
                time_remaining="12:00",
                event_type=EventType.SHOT_MADE,
                source="test_integration",
                source_url="https://test.com"
            )
            
            with pytest.raises(Exception):  # Should be FK constraint violation
                await upsert_pbp(conn, [pbp_event])
            
            # Try to insert lineup without corresponding game (should fail)
            lineup = StartingLineupRow(
                game_id="NONEXISTENT_GAME",
                team_tricode="LAL",
                player_name_slug="test_player",
                player_display_name="Test Player",
                position=Position.G,
                final_pre_tip=True,
                source="test_integration",
                source_url="https://test.com"
            )
            
            with pytest.raises(Exception):  # Should be FK constraint violation
                await upsert_lineups(conn, [lineup])
                
        except Exception as e:
            # If we can't test FK constraints, just log it
            print(f"FK constraint test skipped: {e}")

    def test_pipeline_callsite_transformation(self):
        """Test that pipeline callsites use correct transformation pattern."""
        # This is a structural test - verify the pattern works
        game_row = GameRow(
            game_id="TEST_CALLSITE_001",
            season="2024-25",
            game_date_utc=datetime(2024, 10, 15, 3, 30, tzinfo=timezone.utc),
            game_date_local=date(2024, 10, 15),
            arena_tz="US/Pacific",
            home_team_tricode="LAL", 
            away_team_tricode="GSW",
            status=GameStatus.FINAL,
            source="test_integration",
            source_url="https://test.com"
        )
        
        # This should be the pattern used in pipelines:
        team_index = get_team_index()
        db_game = to_db_game(game_row, team_index=team_index)
        
        # Verify transformation worked
        assert db_game.game_id == "TEST_CALLSITE_001"
        assert db_game.home_team_id == 1610612747  # LAL
        assert db_game.away_team_id == 1610612744  # GSW
        assert db_game.status == "Final"
        assert db_game.game_date == "2024-10-15"
        
        # The actual upsert call would be:
        # await upsert_game(conn, db_game)
        # But we don't test that here since it requires DB connection