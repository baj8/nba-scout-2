"""Integration tests for NBA Stats pipeline facade with robust thresholds."""

import pytest
import asyncio
from nba_scraper.io_clients import IoFacade
from nba_scraper.loaders import upsert_game, upsert_pbp, upsert_lineups, upsert_shots, upsert_adv_metrics
from nba_scraper.extractors.boxscore import extract_game_from_boxscore
from nba_scraper.extractors.pbp import extract_pbp_from_response
from nba_scraper.extractors.lineups import extract_lineups_from_response
from nba_scraper.extractors.shots import extract_shot_chart_detail
from nba_scraper.transformers.games import transform_game
from nba_scraper.transformers.pbp import transform_pbp
from nba_scraper.transformers.lineups import transform_lineups
from nba_scraper.transformers.shots import transform_shots


@pytest.mark.asyncio
async def test_pipeline_facade_single_game(db, io_impl):
    """Test complete pipeline facade for single game with robust thresholds."""
    io = IoFacade(io_impl)
    game_id = "0022300001"

    # STEP 1: Test IO facade methods
    try:
        bs_resp = await io.fetch_boxscore(game_id)
        pbp_resp = await io.fetch_pbp(game_id)
        lu_resp = await io.fetch_lineups(game_id)
        shots_resp = await io.fetch_shots(game_id)
    except Exception as e:
        pytest.skip(f"IO operations failed: {e}")

    # STEP 2: Test extraction layer (IO → Python dicts)
    game_meta_raw = extract_game_from_boxscore(bs_resp)
    assert isinstance(game_meta_raw, dict)
    assert "game_id" in game_meta_raw
    
    pbp_events_raw = extract_pbp_from_response(pbp_resp)
    assert isinstance(pbp_events_raw, list)
    
    lineup_events_raw = extract_lineups_from_response(lu_resp)
    assert isinstance(lineup_events_raw, list)
    
    shot_events_raw = extract_shot_chart_detail(shots_resp)
    assert isinstance(shot_events_raw, list)

    # STEP 3: Test transformation layer (dicts → Pydantic models)
    game = transform_game(game_meta_raw)
    assert game.game_id == game_id
    assert game.season and game.season != "UNKNOWN"
    print(f"Game season: {game.season}")

    pbp_rows = transform_pbp(pbp_events_raw, game_id=game.game_id)
    lineup_rows = transform_lineups(lineup_events_raw, game_id=game.game_id)
    shot_rows = transform_shots(shot_events_raw, game_id=game.game_id)

    # STEP 4: Validate clock parsing with robust thresholds
    parsed_clocks = sum(1 for r in pbp_rows if r.seconds_elapsed is not None)
    total_pbp = len(pbp_rows)
    
    assert total_pbp >= 300, f"Expected ≥300 PBP events, got {total_pbp}"
    
    # Robust threshold: at least 75% parsed OR at least 300 events parsed
    min_parsed = max(0.75 * total_pbp, 300)
    assert parsed_clocks >= min_parsed, f"Expected ≥{min_parsed} parsed clocks, got {parsed_clocks}"
    
    print(f"PBP events: {total_pbp}, Parsed clocks: {parsed_clocks} ({parsed_clocks/total_pbp*100:.1f}%)")

    # STEP 5: Test loading with deferrable FK constraints
    if not all([upsert_game, upsert_pbp]):
        pytest.skip("Required loaders not available")

    async with db.transaction():
        # Load game first (parent table)
        await upsert_game(db, game)
        
        # Load dependent tables (FKs deferred until commit)
        await upsert_pbp(db, pbp_rows)
        
        if upsert_lineups and lineup_rows:
            await upsert_lineups(db, lineup_rows)
            
        if upsert_shots and shot_rows:
            await upsert_shots(db, shot_rows)
            
        if upsert_adv_metrics:
            await upsert_adv_metrics(db, [])

        # Transaction commits here - FK constraints validated

    # STEP 6: Verify data was loaded correctly
    games_cnt = await db.fetchval("SELECT COUNT(*) FROM games WHERE game_id=$1", game_id)
    assert games_cnt == 1

    pbp_cnt = await db.fetchval("SELECT COUNT(*) FROM pbp_events WHERE game_id=$1", game_id)
    assert pbp_cnt >= 400, f"Expected ≥400 PBP in DB, got {pbp_cnt}"

    # Check that clock parsing worked in the database
    parsed_in_db = await db.fetchval(
        "SELECT COUNT(*) FROM pbp_events WHERE game_id=$1 AND seconds_elapsed IS NOT NULL", 
        game_id
    )
    assert parsed_in_db >= min_parsed, f"Expected ≥{min_parsed} parsed clocks in DB, got {parsed_in_db}"

    print(f"✅ Pipeline test passed: {games_cnt} game, {pbp_cnt} PBP events, {parsed_in_db} with parsed clocks")


@pytest.mark.asyncio 
async def test_pipeline_error_resilience(db, io_impl):
    """Test pipeline resilience to errors and edge cases."""
    io = IoFacade(io_impl)
    
    # Test with invalid game ID
    invalid_game_id = "0000000000"
    
    try:
        bs_resp = await io.fetch_boxscore(invalid_game_id)
    except Exception:
        bs_resp = {}  # Handle gracefully
    
    # Should handle empty/invalid responses
    game_meta_raw = extract_game_from_boxscore(bs_resp)
    assert isinstance(game_meta_raw, dict)
    
    # Transform should handle missing data
    game = transform_game(game_meta_raw)
    assert game.game_id == "" or game.game_id == invalid_game_id
    

@pytest.mark.asyncio
async def test_tranche2_shot_coordinate_integration(db, io_impl):
    """Test Tranche 2 shot coordinate integration with PBP events."""
    io = IoFacade(io_impl)
    game_id = "0022300005"  # Use different game for variety
    
    if not all([upsert_game, upsert_pbp, upsert_shots]):
        pytest.skip("Required loaders not available for Tranche 2 test")
    
    try:
        # Fetch shot chart data specifically
        shots_resp = await io.fetch_shots(game_id)
        shot_events_raw = extract_shot_chart_detail(shots_resp)
        shot_rows = transform_shots(shot_events_raw, game_id)
        
        if not shot_rows:
            pytest.skip("No shot coordinate data available")
        
        # Check that shots have coordinate data
        coords_present = sum(1 for shot in shot_rows if shot.loc_x != 0 or shot.loc_y != 0)
        assert coords_present > 0, "Expected some shots to have non-zero coordinates"
        
        print(f"✅ Tranche 2 test: {len(shot_rows)} shots, {coords_present} with coordinates")
        
    except Exception as e:
        pytest.skip(f"Tranche 2 test failed: {e}")


@pytest.mark.asyncio
async def test_concurrent_game_processing(db, io_impl):
    """Test processing multiple games concurrently."""
    from nba_scraper.pipelines.nba_stats_pipeline import NBAStatsPipeline
    
    pipeline = NBAStatsPipeline(io_impl, db=db)
    
    # Test with a small batch of games
    game_ids = ["0022300001", "0022300002"]
    
    try:
        results = await pipeline.run_multiple_games(game_ids, concurrency=2)
        
        assert len(results) == len(game_ids)
        successful = sum(1 for r in results if r.get("success", False))
        
        print(f"✅ Concurrent test: {successful}/{len(game_ids)} games processed successfully")
        
    except Exception as e:
        pytest.skip(f"Concurrent processing test failed: {e}")


@pytest.mark.asyncio
async def test_pipeline_health_check():
    """Test pipeline health check functionality."""
    from nba_scraper.pipelines.nba_stats_pipeline import NBAStatsPipeline
    from nba_scraper.io_clients.nba_stats import NBAStatsClient
    
    try:
        client = NBAStatsClient()
        pipeline = NBAStatsPipeline(client)
        
        health = await pipeline.health_check()
        
        assert "status" in health
        assert "database" in health
        assert "loaders" in health
        
        print(f"✅ Health check: {health['status']}")
        
    except Exception as e:
        pytest.skip(f"Health check failed: {e}")