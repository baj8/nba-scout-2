"""Integration test for single game ETL pipeline."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, date, timezone

from nba_scraper.models.games import Game
from nba_scraper.models.pbp import PbpEvent
from nba_scraper.pipelines.nba_stats_pipeline import NBAStatsPipeline
from nba_scraper.io_clients import IoFacade


@pytest.fixture
def mock_io_impl():
    """Mock IO implementation for testing."""
    mock_impl = AsyncMock()
    
    # Mock boxscore response
    mock_impl.fetch_boxscore.return_value = {
        'resultSets': [{
            'name': 'GameSummary',
            'headers': ['GAME_ID', 'SEASON', 'GAME_DATE_EST', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID', 'GAME_STATUS_TEXT'],
            'rowSet': [['0022400001', '2024-25', '2024-10-15', 1610612747, 1610612744, 'Final']]
        }]
    }
    
    # Mock PBP response with 450+ events for DQ gate
    pbp_events = []
    for i in range(1, 451):  # 450 events to meet ≥400 requirement
        period = ((i - 1) // 112) + 1  # ~112 events per period
        minutes = 12 - ((i - 1) % 112) // 10
        seconds = ((i - 1) % 112) % 60
        clock = f"{minutes}:{seconds:02d}"
        
        pbp_events.append([
            i,           # EVENTNUM
            1,           # EVENTMSGTYPE (shot made)
            0,           # EVENTMSGACTIONTYPE
            period,      # PERIOD
            clock,       # PCTIMESTRING
            f"{i*2} - {i*2-1}",  # SCORE
            None,        # HOMEDESCRIPTION
            f"Shot {i}",  # VISITORDESCRIPTION
            1610612744,  # PLAYER1_ID
            None,        # PLAYER2_ID
            None,        # PLAYER3_ID
        ])
    
    mock_impl.fetch_pbp.return_value = {
        'resultSets': [{
            'name': 'PlayByPlay',
            'headers': [
                'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 
                'PCTIMESTRING', 'SCORE', 'HOMEDESCRIPTION', 'VISITORDESCRIPTION',
                'PLAYER1_ID', 'PLAYER2_ID', 'PLAYER3_ID'
            ],
            'rowSet': pbp_events
        }]
    }
    
    # Mock lineups response
    mock_impl.fetch_lineups.return_value = {
        'resultSets': [{
            'name': 'LineupStats',
            'headers': ['GAME_ID', 'TEAM_ID', 'PLAYER_ID', 'START_POSITION'],
            'rowSet': [
                ['0022400001', 1610612747, 1001, 'F'],
                ['0022400001', 1610612747, 1002, 'G'],
                ['0022400001', 1610612744, 2001, 'C'],
                ['0022400001', 1610612744, 2002, 'F'],
            ]
        }]
    }
    
    # Mock shots response with good coordinate coverage
    shot_events = []
    for i in range(1, 91):  # 90 shots, 85% will have coordinates
        has_coords = i <= 76  # First 76 shots have coordinates (84.4% > 80%)
        shot_events.append([
            f'0022400001',  # GAME_ID
            i,              # GAME_EVENT_ID
            1610612744,     # PLAYER_ID
            1610612744,     # TEAM_ID
            1,              # PERIOD
            12 - (i % 12),  # MINUTES_REMAINING
            i % 60,         # SECONDS_REMAINING
            1 if i % 3 == 0 else 0,  # SHOT_MADE_FLAG
            100 + (i % 50) if has_coords else None,  # LOC_X
            50 + (i % 30) if has_coords else None,   # LOC_Y
            15 + (i % 10),  # SHOT_DISTANCE
        ])
    
    mock_impl.fetch_shots.return_value = {
        'resultSets': [{
            'name': 'Shot_Chart_Detail',
            'headers': [
                'GAME_ID', 'GAME_EVENT_ID', 'PLAYER_ID', 'TEAM_ID', 'PERIOD',
                'MINUTES_REMAINING', 'SECONDS_REMAINING', 'SHOT_MADE_FLAG',
                'LOC_X', 'LOC_Y', 'SHOT_DISTANCE'
            ],
            'rowSet': shot_events
        }]
    }
    
    return mock_impl


@pytest.fixture
def mock_db_conn():
    """Mock database connection for testing."""
    mock_conn = AsyncMock()
    
    # Mock transaction context manager
    mock_transaction = AsyncMock()
    mock_transaction.__aenter__.return_value = mock_transaction
    mock_transaction.__aexit__.return_value = None
    mock_conn.transaction.return_value = mock_transaction
    
    # Mock query results for DQ checks
    mock_conn.fetchval.side_effect = lambda query, *args: {
        "SELECT COUNT(*) FROM pbp_events WHERE game_id = $1": 450,
        "SELECT COUNT(*) FROM pbp_events WHERE game_id = $1 AND seconds_elapsed IS NOT NULL": 400,
        "SELECT COUNT(*) FROM shot_events WHERE game_id = $1": 90,
        "SELECT COUNT(*) FROM shot_events WHERE game_id = $1 AND loc_x IS NOT NULL AND loc_y IS NOT NULL": 76,
    }.get(query, 0)
    
    return mock_conn


class TestSingleGameETL:
    """Integration tests for single game ETL pipeline."""

    @pytest.mark.asyncio
    @patch('nba_scraper.pipelines.nba_stats_pipeline.get_connection')
    @patch('nba_scraper.loaders.upsert_game')
    @patch('nba_scraper.loaders.upsert_pbp')
    @patch('nba_scraper.loaders.upsert_lineups')
    @patch('nba_scraper.loaders.upsert_shots')
    async def test_single_game_etl_success(self, mock_shots_loader, mock_lineups_loader, 
                                          mock_pbp_loader, mock_game_loader, mock_get_conn,
                                          mock_io_impl, mock_db_conn):
        """Test successful single game ETL with all components."""
        # Setup mocks
        mock_get_conn.return_value = mock_db_conn
        mock_game_loader.return_value = None
        mock_pbp_loader.return_value = None
        mock_lineups_loader.return_value = None
        mock_shots_loader.return_value = None
        
        # Create pipeline
        pipeline = NBAStatsPipeline(mock_io_impl)
        
        # Process game
        result = await pipeline.run_single_game("0022400001")
        
        # Verify success
        assert result['success'] is True
        assert result['game_id'] == "0022400001"
        assert 'duration_seconds' in result
        assert 'records' in result
        
        # Verify all loaders were called
        mock_game_loader.assert_called_once()
        mock_pbp_loader.assert_called_once()
        mock_lineups_loader.assert_called_once()
        mock_shots_loader.assert_called_once()
        
        # Verify transaction was used
        mock_db_conn.transaction.assert_called_once()

    @pytest.mark.asyncio
    @patch('nba_scraper.pipelines.nba_stats_pipeline.get_connection')
    @patch('nba_scraper.loaders.upsert_game')
    @patch('nba_scraper.loaders.upsert_pbp')
    async def test_pbp_seconds_elapsed_coverage(self, mock_pbp_loader, mock_game_loader, 
                                               mock_get_conn, mock_io_impl, mock_db_conn):
        """Test that seconds_elapsed coverage meets threshold."""
        # Setup mocks
        mock_get_conn.return_value = mock_db_conn
        mock_game_loader.return_value = None
        
        # Capture PBP events to verify seconds_elapsed
        captured_pbp_events = []
        async def capture_pbp(conn, events):
            captured_pbp_events.extend(events)
        mock_pbp_loader.side_effect = capture_pbp
        
        # Create pipeline
        pipeline = NBAStatsPipeline(mock_io_impl)
        
        # Process game
        result = await pipeline.run_single_game("0022400001")
        
        # Verify PBP events were captured
        assert len(captured_pbp_events) >= 400
        
        # Verify seconds_elapsed coverage
        events_with_elapsed = [e for e in captured_pbp_events if e.seconds_elapsed is not None]
        coverage = len(events_with_elapsed) / len(captured_pbp_events)
        
        # Should meet 75% threshold
        assert coverage >= 0.75, f"seconds_elapsed coverage {coverage:.2%} < 75%"

    @pytest.mark.asyncio
    @patch('nba_scraper.pipelines.nba_stats_pipeline.get_connection')
    @patch('nba_scraper.loaders.upsert_game')
    async def test_game_uses_game_model_not_gamerow(self, mock_game_loader, mock_get_conn, 
                                                   mock_io_impl, mock_db_conn):
        """Test that pipeline passes Game model, not GameRow to upsert_game."""
        # Setup mocks
        mock_get_conn.return_value = mock_db_conn
        
        # Capture game argument
        captured_game = None
        async def capture_game(conn, game):
            nonlocal captured_game
            captured_game = game
        mock_game_loader.side_effect = capture_game
        
        # Create pipeline
        pipeline = NBAStatsPipeline(mock_io_impl)
        
        # Process game
        await pipeline.run_single_game("0022400001")
        
        # Verify Game model was passed
        assert captured_game is not None
        assert isinstance(captured_game, Game), f"Expected Game, got {type(captured_game)}"
        assert captured_game.game_id == "0022400001"

    @pytest.mark.asyncio
    @patch('nba_scraper.pipelines.nba_stats_pipeline.get_connection')
    @patch('nba_scraper.loaders.upsert_game')
    @patch('nba_scraper.loaders.upsert_pbp')
    async def test_idempotent_upserts(self, mock_pbp_loader, mock_game_loader, 
                                     mock_get_conn, mock_io_impl, mock_db_conn):
        """Test that re-running the same game is idempotent."""
        # Setup mocks
        mock_get_conn.return_value = mock_db_conn
        mock_game_loader.return_value = None
        mock_pbp_loader.return_value = None
        
        # Track call counts
        game_call_count = 0
        pbp_call_count = 0
        
        async def count_game_calls(conn, game):
            nonlocal game_call_count
            game_call_count += 1
        
        async def count_pbp_calls(conn, events):
            nonlocal pbp_call_count
            pbp_call_count += 1
        
        mock_game_loader.side_effect = count_game_calls
        mock_pbp_loader.side_effect = count_pbp_calls
        
        # Create pipeline
        pipeline = NBAStatsPipeline(mock_io_impl)
        
        # Process game twice
        result1 = await pipeline.run_single_game("0022400001")
        result2 = await pipeline.run_single_game("0022400001")
        
        # Both should succeed
        assert result1['success'] is True
        assert result2['success'] is True
        
        # Both should call loaders (idempotency is handled in loaders themselves)
        assert game_call_count == 2
        assert pbp_call_count == 2

    @pytest.mark.asyncio
    @patch('nba_scraper.pipelines.nba_stats_pipeline.get_connection')
    async def test_transaction_rollback_on_error(self, mock_get_conn, mock_io_impl):
        """Test that transaction rolls back on error."""
        # Setup failing connection
        mock_conn = AsyncMock()
        mock_transaction = AsyncMock()
        mock_transaction.__aenter__.return_value = mock_transaction
        mock_transaction.__aexit__.return_value = None
        mock_conn.transaction.return_value = mock_transaction
        
        # Make upsert_game fail
        with patch('nba_scraper.loaders.upsert_game', side_effect=Exception("Database error")):
            mock_get_conn.return_value = mock_conn
            
            # Create pipeline
            pipeline = NBAStatsPipeline(mock_io_impl)
            
            # Process game - should handle error gracefully
            result = await pipeline.run_single_game("0022400001")
            
            # Should return error result
            assert result['success'] is False
            assert 'error' in result
            assert result['game_id'] == "0022400001"
            
            # Transaction should have been entered (and will auto-rollback on exception)
            mock_conn.transaction.assert_called_once()

    @pytest.mark.asyncio
    @patch('nba_scraper.pipelines.nba_stats_pipeline.get_connection')
    @patch('nba_scraper.loaders.upsert_game')
    @patch('nba_scraper.loaders.upsert_pbp')
    @patch('nba_scraper.loaders.upsert_lineups')
    @patch('nba_scraper.loaders.upsert_shots')
    async def test_upsert_order_dependency(self, mock_shots_loader, mock_lineups_loader,
                                          mock_pbp_loader, mock_game_loader, mock_get_conn,
                                          mock_io_impl, mock_db_conn):
        """Test that upserts happen in correct dependency order."""
        # Setup mocks
        mock_get_conn.return_value = mock_db_conn
        
        # Track call order
        call_order = []
        
        async def track_game(conn, game):
            call_order.append('game')
        
        async def track_pbp(conn, events):
            call_order.append('pbp')
            
        async def track_lineups(conn, lineups):
            call_order.append('lineups')
            
        async def track_shots(conn, shots):
            call_order.append('shots')
        
        mock_game_loader.side_effect = track_game
        mock_pbp_loader.side_effect = track_pbp
        mock_lineups_loader.side_effect = track_lineups
        mock_shots_loader.side_effect = track_shots
        
        # Create pipeline
        pipeline = NBAStatsPipeline(mock_io_impl)
        
        # Process game
        result = await pipeline.run_single_game("0022400001")
        
        # Verify success
        assert result['success'] is True
        
        # Verify call order: game first (parent table), then dependent tables
        assert call_order[0] == 'game', "Game should be upserted first"
        assert 'pbp' in call_order, "PBP should be upserted"
        assert 'lineups' in call_order, "Lineups should be upserted"
        assert 'shots' in call_order, "Shots should be upserted"

    @pytest.mark.asyncio
    async def test_io_facade_integration(self, mock_io_impl):
        """Test integration with IoFacade."""
        # Create IoFacade with mock implementation
        io_facade = IoFacade(mock_io_impl)
        
        # Test all required methods exist
        assert hasattr(io_facade, 'fetch_boxscore')
        assert hasattr(io_facade, 'fetch_pbp')
        assert hasattr(io_facade, 'fetch_lineups')
        assert hasattr(io_facade, 'fetch_shots')
        
        # Test methods are callable
        game_id = "0022400001"
        
        boxscore_result = await io_facade.fetch_boxscore(game_id)
        assert isinstance(boxscore_result, dict)
        
        pbp_result = await io_facade.fetch_pbp(game_id)
        assert isinstance(pbp_result, dict)
        
        lineups_result = await io_facade.fetch_lineups(game_id)
        assert isinstance(lineups_result, dict)
        
        shots_result = await io_facade.fetch_shots(game_id)
        assert isinstance(shots_result, dict)