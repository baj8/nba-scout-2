"""Unit tests for watermark tracking system."""
from datetime import datetime
from sqlalchemy import create_engine, text


def test_watermark_crud():
    """Test basic watermark CRUD operations."""
    from nba_scraper.state.watermarks import ensure_tables, set_watermark, get_watermark
    
    # Create in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    
    with engine.begin() as conn:
        ensure_tables(conn)
        
        # Test get on empty database
        assert get_watermark(conn, stage="schedule", key="daily") is None
        
        # Test set (insert)
        set_watermark(conn, stage="schedule", key="daily", value="2025-10-01")
        assert get_watermark(conn, stage="schedule", key="daily") == "2025-10-01"
        
        # Test set (update)
        set_watermark(conn, stage="schedule", key="daily", value="2025-10-02")
        assert get_watermark(conn, stage="schedule", key="daily") == "2025-10-02"


def test_watermark_isolation():
    """Test that watermarks are isolated by stage and key."""
    from nba_scraper.state.watermarks import ensure_tables, set_watermark, get_watermark
    
    engine = create_engine("sqlite:///:memory:")
    
    with engine.begin() as conn:
        ensure_tables(conn)
        
        # Set watermarks for different stage/key combinations
        set_watermark(conn, stage="schedule", key="daily", value="2025-10-01")
        set_watermark(conn, stage="backfill", key="2024-25", value="0022400100")
        set_watermark(conn, stage="backfill", key="2023-24", value="0022300500")
        
        # Verify isolation
        assert get_watermark(conn, stage="schedule", key="daily") == "2025-10-01"
        assert get_watermark(conn, stage="backfill", key="2024-25") == "0022400100"
        assert get_watermark(conn, stage="backfill", key="2023-24") == "0022300500"
        
        # Update one watermark shouldn't affect others
        set_watermark(conn, stage="backfill", key="2024-25", value="0022400200")
        assert get_watermark(conn, stage="backfill", key="2024-25") == "0022400200"
        assert get_watermark(conn, stage="backfill", key="2023-24") == "0022300500"


def test_watermark_idempotency():
    """Test that ensure_tables is idempotent."""
    from nba_scraper.state.watermarks import ensure_tables, set_watermark, get_watermark
    
    engine = create_engine("sqlite:///:memory:")
    
    with engine.begin() as conn:
        # Call ensure_tables multiple times
        ensure_tables(conn)
        ensure_tables(conn)
        ensure_tables(conn)
        
        # Should still work correctly
        set_watermark(conn, stage="test", key="key1", value="value1")
        assert get_watermark(conn, stage="test", key="key1") == "value1"
