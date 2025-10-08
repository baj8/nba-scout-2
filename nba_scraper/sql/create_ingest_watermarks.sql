-- Watermark tracking for resumable pipeline operations
-- This table stores checkpoints for each pipeline stage, enabling
-- resumable operations after interruptions or failures

CREATE TABLE IF NOT EXISTS ingest_watermarks (
    stage VARCHAR(50) NOT NULL,
    game_id VARCHAR(20) NOT NULL,
    season VARCHAR(10) NOT NULL DEFAULT 'ALL',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata TEXT,
    
    PRIMARY KEY (stage, season),
    
    -- Index for querying by stage
    CREATE INDEX IF NOT EXISTS idx_watermarks_stage ON ingest_watermarks(stage, updated_at DESC)
);

-- Comments for documentation
COMMENT ON TABLE ingest_watermarks IS 'Tracks last successfully processed game_id for each pipeline stage';
COMMENT ON COLUMN ingest_watermarks.stage IS 'Pipeline stage: fetch, transform, load, derive, backfill';
COMMENT ON COLUMN ingest_watermarks.game_id IS 'Last successfully processed game ID or date marker';
COMMENT ON COLUMN ingest_watermarks.season IS 'Season context (e.g., 2023-24) or ALL for cross-season';
COMMENT ON COLUMN ingest_watermarks.updated_at IS 'Timestamp when watermark was last updated';
COMMENT ON COLUMN ingest_watermarks.metadata IS 'Optional JSON or text metadata (e.g., row counts, errors)';
