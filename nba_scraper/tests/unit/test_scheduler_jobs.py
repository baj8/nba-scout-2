"""Unit tests for scheduler jobs."""
from datetime import datetime, timezone, date
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine


def test_run_daily_uses_yesterday_et():
    """Test that run_daily correctly computes yesterday in ET timezone."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables, get_watermark
    
    # Create in-memory database
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
    
    # Mock the sync engine getter
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        # Mock yesterday to return a fixed date
        with patch.object(jobs, '_yesterday_et', return_value=date(2025, 10, 7)):
            # Mock discovery to return test game IDs
            with patch.object(jobs, 'discover_game_ids_for_date', return_value=["0022400001", "0022400002"]):
                # Track pipeline calls
                called = {"ran": False, "games": []}
                
                def mock_pipeline(gids):
                    called["ran"] = True
                    called["games"] = gids
                
                # Mock the pipeline runner
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', mock_pipeline):
                    rc = jobs.run_daily()
                    
                    # Verify it ran and processed correct games
                    assert rc == 0
                    assert called["ran"] is True
                    assert called["games"] == ["0022400001", "0022400002"]
                    
                    # Verify watermark was updated
                    with engine.begin() as conn:
                        watermark = get_watermark(conn, stage="schedule", key="daily")
                        assert watermark == "2025-10-07"


def test_run_daily_handles_no_games():
    """Test that run_daily handles case when no games are found."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
    
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        with patch.object(jobs, '_yesterday_et', return_value=date(2025, 10, 7)):
            # Return empty game list
            with patch.object(jobs, 'discover_game_ids_for_date', return_value=[]):
                called = {"ran": False}
                
                def mock_pipeline(gids):
                    called["ran"] = True
                
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', mock_pipeline):
                    rc = jobs.run_daily()
                    
                    # Should return 0 and not call pipeline
                    assert rc == 0
                    assert called["ran"] is False


def test_run_daily_handles_pipeline_error():
    """Test that run_daily handles pipeline errors gracefully."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
    
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        with patch.object(jobs, '_yesterday_et', return_value=date(2025, 10, 7)):
            with patch.object(jobs, 'discover_game_ids_for_date', return_value=["0022400001"]):
                # Make pipeline raise an error
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', side_effect=Exception("API timeout")):
                    rc = jobs.run_daily()
                    
                    # Should return non-zero (1 failure)
                    assert rc == 1


def test_run_backfill_resumes_from_watermark():
    """Test that run_backfill resumes from watermark."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables, set_watermark, get_watermark
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
        # Set existing watermark at game 0022400003
        set_watermark(conn, stage="backfill", key="2024-25", value="0022400003")
    
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        # Mock season_bounds to return single day for test
        with patch('nba_scraper.utils.season_utils.season_bounds', return_value=(date(2024, 10, 1), date(2024, 10, 1))):
            # Mock discovery to return games including ones before and after watermark
            with patch.object(jobs, 'discover_game_ids_for_date_range', return_value=["0022400001", "0022400003", "0022400005"]):
                seen = {"games": []}
                
                def mock_pipeline(gids):
                    seen["games"].extend(gids)
                
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', mock_pipeline):
                    rc = jobs.run_backfill("2024-25", since_game_id=None, chunk_days=1)
                    
                    # Should return 0 and only process games after watermark
                    assert rc == 0
                    # Only game 0022400005 should be processed (after 0022400003)
                    assert seen["games"] == ["0022400005"]
                    
                    # Verify watermark was updated to max processed game
                    with engine.begin() as conn:
                        watermark = get_watermark(conn, stage="backfill", key="2024-25")
                        assert watermark == "0022400005"


def test_run_backfill_explicit_since_overrides_watermark():
    """Test that explicit since_game_id parameter overrides watermark."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables, set_watermark, get_watermark
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
        # Set watermark at game 0022400003
        set_watermark(conn, stage="backfill", key="2024-25", value="0022400003")
    
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        with patch('nba_scraper.utils.season_utils.season_bounds', return_value=(date(2024, 10, 1), date(2024, 10, 1))):
            with patch.object(jobs, 'discover_game_ids_for_date_range', return_value=["0022400001", "0022400003", "0022400005", "0022400007"]):
                seen = {"games": []}
                
                def mock_pipeline(gids):
                    seen["games"].extend(gids)
                
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', mock_pipeline):
                    # Explicitly start from 0022400005, ignoring watermark
                    rc = jobs.run_backfill("2024-25", since_game_id="0022400005", chunk_days=1)
                    
                    assert rc == 0
                    # Should only process game 0022400007 (after explicit since)
                    assert seen["games"] == ["0022400007"]


def test_run_backfill_continues_on_chunk_error():
    """Test that run_backfill continues processing after chunk errors."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
    
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        # Return date range with 3 days (3 chunks with chunk_days=1)
        with patch('nba_scraper.utils.season_utils.season_bounds', return_value=(date(2024, 10, 1), date(2024, 10, 3))):
            call_count = [0]
            
            def mock_discovery(start, end):
                # Return different games for each chunk
                call_count[0] += 1
                if call_count[0] == 1:
                    return ["0022400001"]
                elif call_count[0] == 2:
                    return ["0022400002"]
                else:
                    return ["0022400003"]
            
            with patch.object(jobs, 'discover_game_ids_for_date_range', side_effect=mock_discovery):
                pipeline_calls = [0]
                
                def mock_pipeline(gids):
                    pipeline_calls[0] += 1
                    # Fail on second chunk
                    if pipeline_calls[0] == 2:
                        raise Exception("Chunk 2 failed")
                
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', mock_pipeline):
                    rc = jobs.run_backfill("2024-25", since_game_id=None, chunk_days=1)
                    
                    # Should return 1 (one failure)
                    assert rc == 1
                    # Should have tried all 3 chunks despite error
                    assert pipeline_calls[0] == 3


def test_run_backfill_from_scratch():
    """Test that run_backfill starts from beginning when no watermark exists."""
    from nba_scraper.schedule import jobs
    from nba_scraper.state.watermarks import ensure_tables, get_watermark
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        ensure_tables(conn)
        # No watermark set
    
    with patch.object(jobs, '_get_sync_engine', return_value=engine):
        with patch('nba_scraper.utils.season_utils.season_bounds', return_value=(date(2024, 10, 1), date(2024, 10, 1))):
            with patch.object(jobs, 'discover_game_ids_for_date_range', return_value=["0022400001", "0022400002"]):
                seen = {"games": []}
                
                def mock_pipeline(gids):
                    seen["games"].extend(gids)
                
                with patch('nba_scraper.cli_pipeline.run_pipeline_for_games', mock_pipeline):
                    rc = jobs.run_backfill("2024-25", since_game_id=None, chunk_days=1)
                    
                    assert rc == 0
                    # Should process all games from the beginning
                    assert seen["games"] == ["0022400001", "0022400002"]
                    
                    # Verify watermark was created
                    with engine.begin() as conn:
                        watermark = get_watermark(conn, stage="backfill", key="2024-25")
                        assert watermark == "0022400002"


def test_dates_in_chunks():
    """Test the _dates_in_chunks utility function."""
    from nba_scraper.schedule.jobs import _dates_in_chunks
    
    # Test single day
    chunks = list(_dates_in_chunks(date(2024, 10, 1), date(2024, 10, 1), chunk_days=1))
    assert chunks == [(date(2024, 10, 1), date(2024, 10, 1))]
    
    # Test multiple days with chunk_days=1
    chunks = list(_dates_in_chunks(date(2024, 10, 1), date(2024, 10, 3), chunk_days=1))
    assert chunks == [
        (date(2024, 10, 1), date(2024, 10, 1)),
        (date(2024, 10, 2), date(2024, 10, 2)),
        (date(2024, 10, 3), date(2024, 10, 3))
    ]
    
    # Test multiple days with chunk_days=7
    chunks = list(_dates_in_chunks(date(2024, 10, 1), date(2024, 10, 15), chunk_days=7))
    assert chunks == [
        (date(2024, 10, 1), date(2024, 10, 7)),
        (date(2024, 10, 8), date(2024, 10, 14)),
        (date(2024, 10, 15), date(2024, 10, 15))
    ]


def test_yesterday_et():
    """Test the _yesterday_et timezone calculation."""
    from nba_scraper.schedule.jobs import _yesterday_et
    from datetime import timedelta
    
    # Test with a fixed UTC time: 2025-10-08 03:00 UTC
    # This is 2025-10-07 23:00 ET (Oct 7 in ET)
    # So yesterday in ET is Oct 6
    test_time = datetime(2025, 10, 8, 3, 0, 0, tzinfo=timezone.utc)
    result = _yesterday_et(test_time)
    assert result == date(2025, 10, 6)
    
    # Test with UTC time later in day: 2025-10-08 14:00 UTC
    # This is 2025-10-08 10:00 ET (Oct 8 in ET)
    # So yesterday in ET is Oct 7
    test_time = datetime(2025, 10, 8, 14, 0, 0, tzinfo=timezone.utc)
    result = _yesterday_et(test_time)
    assert result == date(2025, 10, 7)
