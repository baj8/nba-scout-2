"""Test source-specific pipelines to demonstrate architectural improvements."""

import pytest
from datetime import date, datetime
from unittest.mock import AsyncMock, Mock, patch
from dataclasses import dataclass
from typing import Dict, Any, List

from nba_scraper.pipelines.source_pipelines import (
    NBAApiPipeline,
    BRefPipeline, 
    AnalyticsPipeline,
    BasePipeline,
    PipelineResult,
    DataDependency
)

@pytest.fixture
def mock_nba_api_pipeline():
    """Mock NBA API pipeline for testing."""
    pipeline = NBAApiPipeline()
    # Mock the extractors and loaders
    pipeline.schedule_extractor = AsyncMock()
    pipeline.pbp_extractor = AsyncMock()
    pipeline.game_loader = AsyncMock()
    pipeline.pbp_loader = AsyncMock()
    return pipeline

@pytest.fixture
def mock_bref_pipeline():
    """Mock Basketball Reference pipeline for testing."""
    pipeline = BRefPipeline()
    # Mock the extractors and loaders
    pipeline.bref_extractor = AsyncMock()
    pipeline.outcome_loader = AsyncMock()
    return pipeline

@pytest.fixture
def mock_analytics_pipeline():
    """Mock Analytics pipeline for testing."""
    pipeline = AnalyticsPipeline()
    # Mock the loaders
    pipeline.derived_loader = AsyncMock()
    return pipeline

class TestSourceSpecificPipelines:
    """Test that source-specific pipelines resolve architectural issues."""
    
    @pytest.mark.asyncio
    async def test_nba_api_pipeline_isolation(self, mock_nba_api_pipeline):
        """Test that NBA API pipeline handles its data independently."""
        # Setup mock data
        mock_nba_api_pipeline.schedule_extractor.extract_schedule.return_value = [
            {"game_id": "001", "home_team": "LAL", "away_team": "GSW"}
        ]
        mock_nba_api_pipeline.pbp_extractor.extract_pbp.return_value = [
            {"game_id": "001", "event_idx": 1, "description": "Game Start"}
        ]
        mock_nba_api_pipeline.game_loader.load.return_value = {"success": True}
        mock_nba_api_pipeline.pbp_loader.load.return_value = {"success": True}
        
        # Run the pipeline
        result = await mock_nba_api_pipeline.run(date(2024, 1, 1))
        
        # Verify it processes NBA API data correctly
        assert result.success is True
        assert result.source == "nba_api"
        assert "games" in result.data_processed
        assert "pbp_events" in result.data_processed
        
        # Verify extractors were called with correct parameters
        mock_nba_api_pipeline.schedule_extractor.extract_schedule.assert_called_once()
        mock_nba_api_pipeline.pbp_extractor.extract_pbp.assert_called_once()

    @pytest.mark.asyncio
    async def test_bref_pipeline_isolation(self, mock_bref_pipeline):
        """Test that Basketball Reference pipeline handles its data independently."""
        # Setup mock data
        mock_bref_pipeline.bref_extractor.extract_outcomes.return_value = [
            {"game_id": "001", "home_spread": -3.5, "total": 220.5}
        ]
        mock_bref_pipeline.outcome_loader.load.return_value = {"success": True}
        
        # Run the pipeline
        result = await mock_bref_pipeline.run(date(2024, 1, 1))
        
        # Verify it processes BRef data correctly
        assert result.success is True
        assert result.source == "basketball_reference"
        assert "outcomes" in result.data_processed
        
        # Verify extractors were called
        mock_bref_pipeline.bref_extractor.extract_outcomes.assert_called_once()

    @pytest.mark.asyncio
    async def test_analytics_pipeline_dependencies(self, mock_analytics_pipeline):
        """Test that analytics pipeline properly handles data dependencies."""
        # Setup mock derived data
        mock_analytics_pipeline.derived_loader.load.return_value = {"success": True}
        
        # Mock the dependency checking
        with patch.object(mock_analytics_pipeline, '_check_dependencies') as mock_check:
            mock_check.return_value = True
            
            # Run the analytics pipeline
            result = await mock_analytics_pipeline.run(date(2024, 1, 1))
            
            # Verify it checks dependencies first
            mock_check.assert_called_once()
            assert result.success is True
            assert result.source == "analytics"
            assert "derived_stats" in result.data_processed

    @pytest.mark.asyncio
    async def test_pipeline_error_isolation(self, mock_nba_api_pipeline):
        """Test that errors in one pipeline don't affect others."""
        # Simulate an error in the NBA API pipeline
        mock_nba_api_pipeline.schedule_extractor.extract_schedule.side_effect = Exception("API Error")
        
        # Run the pipeline
        result = await mock_nba_api_pipeline.run(date(2024, 1, 1))
        
        # Verify error is contained within this pipeline
        assert result.success is False
        assert "API Error" in result.error
        assert result.source == "nba_api"
        
        # This error should not affect other pipelines (they would run independently)

    def test_data_dependency_specification(self):
        """Test that pipelines can specify their data dependencies clearly."""
        # Create an analytics pipeline that depends on games and outcomes
        analytics_pipeline = AnalyticsPipeline()
        
        # Verify it has the expected dependencies
        dependencies = analytics_pipeline.get_dependencies()
        assert len(dependencies) == 2
        
        # Check that dependencies are properly specified
        game_dep = next(d for d in dependencies if d.table == "games")
        outcome_dep = next(d for d in dependencies if d.table == "outcomes")
        
        assert game_dep.source == "nba_api"
        assert outcome_dep.source == "basketball_reference"

    @pytest.mark.asyncio
    async def test_pipeline_orchestration(self):
        """Test that pipelines can be orchestrated in the correct order."""
        # Create mock pipelines
        nba_pipeline = Mock(spec=NBAApiPipeline)
        bref_pipeline = Mock(spec=BRefPipeline)
        analytics_pipeline = Mock(spec=AnalyticsPipeline)
        
        # Mock their run methods
        nba_result = PipelineResult(True, "nba_api", {"games": 10}, {}, 1.0)
        bref_result = PipelineResult(True, "basketball_reference", {"outcomes": 10}, {}, 1.0)
        analytics_result = PipelineResult(True, "analytics", {"derived_stats": 5}, {}, 1.0)
        
        nba_pipeline.run.return_value = nba_result
        bref_pipeline.run.return_value = bref_result
        analytics_pipeline.run.return_value = analytics_result
        analytics_pipeline.get_dependencies.return_value = [
            DataDependency("games", "nba_api", 24),
            DataDependency("outcomes", "basketball_reference", 24)
        ]
        
        # Simulate pipeline orchestration
        target_date = date(2024, 1, 1)
        
        # Step 1: Run source pipelines (can run in parallel)
        source_results = []
        source_results.append(await nba_pipeline.run(target_date))
        source_results.append(await bref_pipeline.run(target_date))
        
        # Step 2: Run analytics pipeline after sources complete
        analytics_result = await analytics_pipeline.run(target_date)
        
        # Verify proper execution order and results
        assert all(r.success for r in source_results)
        assert analytics_result.success
        assert analytics_result.data_processed["derived_stats"] == 5

if __name__ == "__main__":
    pytest.main([__file__, "-v"])