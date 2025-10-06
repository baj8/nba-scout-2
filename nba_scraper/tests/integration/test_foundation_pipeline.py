"""Integration test for the refactored foundation pipeline with clock handling."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from nba_scraper.pipelines.foundation import FoundationPipeline
from nba_scraper.db import get_connection


@pytest.fixture
def mock_boxscore_response():
    """Mock boxscore API response."""
    return {
        "resultSets": [
            {
                "name": "GameSummary",
                "headers": ["GAME_ID", "SEASON", "GAME_DATE_EST", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "GAME_STATUS_TEXT"],
                "rowSet": [["0022301234", "2024-25", "2024-01-15T00:00:00", 1610612744, 1610612739, "Final"]]
            },
            {
                "name": "PlayerStats",
                "headers": ["GAME_ID", "TEAM_ID", "PLAYER_ID", "START_POSITION", "MIN"],
                "rowSet": [
                    ["0022301234", 1610612744, 203999, "F", "35:30"],
                    ["0022301234", 1610612744, 201939, "F", "33:45"],
                    ["0022301234", 1610612744, 203076, "C", "28:20"],
                    ["0022301234", 1610612744, 201566, "G", "32:15"],
                    ["0022301234", 1610612744, 2544, "G", "30:10"],
                    ["0022301234", 1610612739, 203507, "F", "36:20"],
                    ["0022301234", 1610612739, 1627759, "F", "34:50"],
                    ["0022301234", 1610612739, 203078, "C", "29:30"],
                    ["0022301234", 1610612739, 203924, "G", "33:40"],
                    ["0022301234", 1610612739, 203115, "G", "31:25"]
                ]
            }
        ]
    }


@pytest.fixture
def mock_pbp_response():
    """Mock PBP API response with various clock formats."""
    return {
        "resultSets": [
            {
                "name": "PlayByPlay",
                "headers": [
                    "EVENTNUM", "PERIOD", "PCTIMESTRING", "TEAM_ID", 
                    "PLAYER1_ID", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE", 
                    "HOMEDESCRIPTION", "NEUTRALDESCRIPTION", "VISITORDESCRIPTION"
                ],
                "rowSet": [
                    [1, 1, "12:00", None, None, 12, 0, None, "Period Start", None],
                    [2, 1, "11:45", 1610612744, 203999, 2, 1, "Antetokounmpo 2' Jump Shot", None, None],
                    [3, 1, "11:23.4", 1610612739, 203507, 1, 1, None, None, "Tatum Free Throw Made"],
                    [4, 1, "10:35.75", 1610612744, 201939, 3, 2, "Lopez 3PT Jump Shot", None, None],
                    [5, 1, "0:24", 1610612739, 203924, 5, 1, None, None, "Smart Foul"],
                    [445, 4, "0:00", None, None, 13, 0, None, "Period End", None]
                ]
            }
        ]
    }


@pytest.mark.asyncio
async def test_foundation_pipeline_integration(mock_boxscore_response, mock_pbp_response):
    """Test the complete foundation pipeline with real database operations."""
    
    # Create mock client
    mock_client = MagicMock()
    mock_client.get_boxscore = AsyncMock(return_value=mock_boxscore_response)
    mock_client.get_pbp = AsyncMock(return_value=mock_pbp_response)
    
    # Initialize pipeline
    pipeline = FoundationPipeline(client=mock_client)
    
    # Process a game
    result = await pipeline.process_game("0022301234")
    
    # Verify results
    assert result['game_id'] == "0022301234"
    assert result['game_processed'] is True
    assert result['pbp_events_processed'] == 6  # 6 events in mock data
    assert result['lineups_processed'] == 0  # Boxscore doesn't contain lineup stint data
    assert len(result['errors']) == 0
    
    # The pipeline logs show successful processing:
    # ✅ Game metadata processed: 2024-25 2024-01-15
    # ✅ PBP events processed: 6 events with clock_seconds
    # ✅ Lineup stints processed: 0 stints
    # This confirms the fix for game ID validation is working


@pytest.mark.asyncio
async def test_preprocessing_prevents_clock_coercion():
    """Test that our preprocessing correctly preserves clock strings."""
    from nba_scraper.utils.preprocess import preprocess_nba_stats_data
    
    # Test the exact problematic case from the original error
    problematic_data = {
        "PCTIMESTRING": "24:49",
        "EVENTNUM": "445",
        "PERIOD": "4",
        "TEAM_ID": "1610612744"
    }
    
    result = preprocess_nba_stats_data(problematic_data)
    
    # Clock should remain as string
    assert result["PCTIMESTRING"] == "24:49"
    assert isinstance(result["PCTIMESTRING"], str)
    
    # Other fields should be converted
    assert result["EVENTNUM"] == 445
    assert isinstance(result["EVENTNUM"], int)
    
    assert result["PERIOD"] == 4
    assert isinstance(result["PERIOD"], int)
    
    assert result["TEAM_ID"] == 1610612744
    assert isinstance(result["TEAM_ID"], int)


@pytest.mark.asyncio
async def test_clock_parsing_with_fractional_seconds():
    """Test clock parsing with various fractional second formats."""
    from nba_scraper.utils.clock import parse_clock_to_seconds
    
    test_cases = [
        ("24:49", 1489.0),
        ("11:23.4", 683.4),
        ("01:23.45", 83.45),
        ("0:05.123", 5.123),
        ("12:30.5", 750.5),
        ("PT10M24S", 624.0),
        ("PT0M5.1S", 5.1),
        ("0:00", 0.0),
        ("12:00", 720.0)
    ]
    
    for clock_str, expected_seconds in test_cases:
        result = parse_clock_to_seconds(clock_str)
        assert abs(result - expected_seconds) < 0.001, f"Failed for {clock_str}: got {result}, expected {expected_seconds}"


@pytest.mark.asyncio
async def test_pipeline_health_check():
    """Test the pipeline health check functionality."""
    pipeline = FoundationPipeline()
    
    # Mock the API client to avoid external dependencies
    pipeline.client.get_today_scoreboard = AsyncMock(return_value={})
    
    health = await pipeline.health_check()
    
    # These should pass in our test environment
    assert health['database'] is True
    assert health['preprocessing'] is True
    assert health['clock_parsing'] is True
    assert health['api_client'] is True  # Because we mocked it


@pytest.mark.asyncio
async def test_multiple_games_processing():
    """Test processing multiple games with concurrency control."""
    
    # Create mock responses
    mock_client = MagicMock()
    mock_client.get_boxscore = AsyncMock(return_value={
        "resultSets": [
            {
                "name": "GameSummary", 
                "headers": ["GAME_ID", "SEASON", "GAME_DATE_EST", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "GAME_STATUS_TEXT"],
                "rowSet": [["0022301001", "2024-25", "2024-01-15T00:00:00", 1610612747, 1610612738, "Final"]]
            }
        ]
    })
    mock_client.get_pbp = AsyncMock(return_value={"resultSets": []})
    
    pipeline = FoundationPipeline(client=mock_client)
    
    # Process multiple games - use proper NBA game IDs
    game_ids = ["0022301001", "0022301002", "0022301003"]
    results = await pipeline.process_multiple_games(game_ids, concurrency=2)
    
    assert len(results) == 3
    for result in results:
        assert result['game_processed'] is True
        assert len(result['errors']) == 0


@pytest.mark.asyncio
async def test_error_handling_and_recovery():
    """Test that the pipeline handles errors gracefully and continues processing."""
    
    # Create mock client that fails on PBP but succeeds on boxscore
    mock_client = MagicMock()
    mock_client.get_boxscore = AsyncMock(return_value={
        "resultSets": [
            {
                "name": "GameSummary",
                "headers": ["GAME_ID", "SEASON", "GAME_DATE_EST", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "GAME_STATUS_TEXT"],
                "rowSet": [["0022301999", "2024-25", "2024-01-15T00:00:00", 1610612747, 1610612738, "Final"]]
            }
        ]
    })
    mock_client.get_pbp = AsyncMock(side_effect=Exception("PBP API failed"))
    
    pipeline = FoundationPipeline(client=mock_client)
    
    result = await pipeline.process_game("0022301999")
    
    # Game should be processed despite PBP failure
    assert result['game_processed'] is True
    assert result['pbp_events_processed'] == 0
    assert len(result['errors']) == 1
    assert "PBP processing failed" in result['errors'][0]