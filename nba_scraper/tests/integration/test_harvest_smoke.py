"""Integration tests for raw_io harvest system with mocked NBA API client."""

import pytest
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock
import tempfile
from datetime import datetime

from src.nba_scraper.raw_io.backfill import harvest_date, _harvest_single_game
from src.nba_scraper.raw_io.client import RawNbaClient
from src.nba_scraper.raw_io.persist import read_manifest


class MockRawNbaClient:
    """Mock NBA client for testing harvest workflows."""
    
    def __init__(self, rate_limit=5, timeout=30, proxy=None, max_retries=5):
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.closed = False
        
        # Track API calls for verification
        self.call_log = []
    
    async def fetch_scoreboard(self, date_str: str):
        """Mock scoreboard response with test games."""
        self.call_log.append(('scoreboard', date_str))
        
        return {
            "resource": "scoreboardv2",
            "parameters": {"GameDate": date_str, "LeagueID": "00"},
            "resultSets": [
                {
                    "name": "GameHeader",
                    "headers": ["GAME_ID", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "SEASON_TYPE_ID"],
                    "rowSet": [
                        ["0022300001", "Final", 1610612744, 1610612738, "2"],  # Regular season game
                        ["0022300002", "Final", 1610612739, 1610612740, "2"],  # Regular season game  
                        ["0012300001", "Final", 1610612745, 1610612741, "1"]   # Preseason (should be filtered)
                    ]
                }
            ]
        }
    
    async def fetch_boxscoresummary(self, game_id: str):
        """Mock boxscore summary response."""
        self.call_log.append(('boxscoresummary', game_id))
        
        if game_id == "0022300001":
            home_team_id, visitor_team_id = 1610612744, 1610612738
        else:  # 0022300002
            home_team_id, visitor_team_id = 1610612739, 1610612740
        
        return {
            "resource": "boxscoresummaryv2",
            "parameters": {"GameID": game_id},
            "resultSets": [
                {
                    "name": "GameSummary",
                    "headers": ["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "GAME_DATE_EST"],
                    "rowSet": [[game_id, home_team_id, visitor_team_id, "2023-10-27"]]
                },
                {
                    "name": "LineScore",
                    "headers": ["TEAM_ID", "PTS_QTR1", "PTS_QTR2", "PTS_QTR3", "PTS_QTR4", "PTS"],
                    "rowSet": [
                        [home_team_id, 25, 28, 22, 30, 105],
                        [visitor_team_id, 22, 25, 27, 28, 102]
                    ]
                }
            ]
        }
    
    async def fetch_boxscoretraditional(self, game_id: str):
        """Mock traditional boxscore response."""
        self.call_log.append(('boxscoretraditional', game_id))
        
        return {
            "resource": "boxscoretraditionalv2", 
            "parameters": {"GameID": game_id},
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": ["GAME_ID", "PLAYER_ID", "PLAYER_NAME", "MIN", "PTS", "REB", "AST"],
                    "rowSet": [
                        [game_id, 201935, "James Harden", "35:24", 28, 6, 8],
                        [game_id, 201142, "Kevin Durant", "38:12", 32, 8, 5]
                    ]
                },
                {
                    "name": "TeamStats", 
                    "headers": ["GAME_ID", "TEAM_ID", "TEAM_NAME", "PTS", "REB", "AST"],
                    "rowSet": [
                        [game_id, 1610612744, "Golden State Warriors", 105, 45, 28],
                        [game_id, 1610612738, "Boston Celtics", 102, 42, 25]
                    ]
                }
            ]
        }
    
    async def fetch_playbyplay(self, game_id: str):
        """Mock play-by-play response.""" 
        self.call_log.append(('playbyplay', game_id))
        
        return {
            "resource": "playbyplayv2",
            "parameters": {"GameID": game_id},
            "resultSets": [
                {
                    "name": "PlayByPlay",
                    "headers": ["GAME_ID", "EVENTNUM", "PERIOD", "PCTIMESTRING", "HOMEDESCRIPTION", "VISITORDESCRIPTION"],
                    "rowSet": [
                        [game_id, 1, 1, "12:00", "Jump Ball", None],
                        [game_id, 2, 1, "11:45", None, "Durant 3PT Shot: Made"],
                        [game_id, 3, 1, "11:20", "Curry 2PT Shot: Made", None]
                    ]
                }
            ]
        }
    
    async def fetch_shotchart(self, game_id: str, team_ids=None):
        """Mock shot chart response."""
        self.call_log.append(('shotchart', game_id, team_ids))
        
        return {
            "resource": "shotchartdetail",
            "parameters": {"GameID": game_id, "TeamID": "0"},
            "resultSets": [
                {
                    "name": "Shot_Chart_Detail",
                    "headers": ["GAME_ID", "PLAYER_ID", "PERIOD", "MINUTES_REMAINING", "SECONDS_REMAINING", "LOC_X", "LOC_Y", "SHOT_MADE_FLAG"],
                    "rowSet": [
                        [game_id, 201935, 1, 11, 45, -22, 25, 1],  # Harden made shot
                        [game_id, 201142, 1, 11, 20, 15, 30, 1],   # Durant made shot  
                        [game_id, 201935, 2, 8, 15, -18, 22, 0]    # Harden missed shot
                    ]
                }
            ]
        }
    
    async def close(self):
        """Mock close method."""
        self.closed = True
    
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class TestHarvestSmoke:
    """Smoke tests for harvest workflow with mocked client."""
    
    @pytest.mark.asyncio
    async def test_harvest_date_basic_workflow(self):
        """Test basic date harvest workflow with file layout verification."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root_path = Path(tmpdir)
            date_str = "2023-10-27"
            
            # Mock the RawNbaClient
            with patch('src.nba_scraper.raw_io.backfill.RawNbaClient') as mock_client_class:
                mock_client = MockRawNbaClient()
                mock_client_class.return_value.__aenter__.return_value = mock_client
                
                # Run harvest
                summary = await harvest_date(
                    date_str=date_str,
                    root=str(root_path),
                    rate_limit=10,  # Fast for testing
                    max_retries=3
                )
            
            # Verify basic summary stats
            assert summary['date'] == date_str
            assert summary['games_discovered'] == 2  # Only regular season games
            assert summary['games_processed'] == 2
            assert summary['endpoints_succeeded'] > 0
            assert summary['total_bytes'] > 0
            
            # Verify directory structure was created
            date_dir = root_path / date_str
            assert date_dir.exists()
            assert date_dir.is_dir()
            
            # Verify scoreboard file exists
            scoreboard_path = date_dir / "scoreboard.json"
            assert scoreboard_path.exists()
            
            # Verify game directories exist
            game1_dir = date_dir / "0022300001"
            game2_dir = date_dir / "0022300002"
            assert game1_dir.exists() and game1_dir.is_dir()
            assert game2_dir.exists() and game2_dir.is_dir()
            
            # Verify Tier A endpoint files exist for each game
            expected_files = [
                "boxscoresummaryv2.json",
                "boxscoretraditionalv2.json", 
                "playbyplayv2.json",
                "shotchartdetail.json"
            ]
            
            for game_dir in [game1_dir, game2_dir]:
                for filename in expected_files:
                    file_path = game_dir / filename
                    assert file_path.exists(), f"Missing {filename} in {game_dir}"
                    
                    # Verify files contain valid JSON
                    content = json.loads(file_path.read_text())
                    assert isinstance(content, dict)
                    assert 'resource' in content or 'resultSets' in content
            
            # Verify manifest was created and populated
            manifest_path = date_dir / "manifest.json"
            assert manifest_path.exists()
            
            manifest = json.loads(manifest_path.read_text())
            assert manifest['date'] == date_str
            assert len(manifest['games']) == 2
            
            # Verify manifest structure for each game
            for game_record in manifest['games']:
                assert 'game_id' in game_record
                assert 'teams' in game_record
                assert 'endpoints' in game_record
                assert 'errors' in game_record
                
                # Should have all Tier A endpoints
                endpoints = game_record['endpoints']
                assert 'boxscoresummaryv2' in endpoints
                assert 'boxscoretraditionalv2' in endpoints
                assert 'playbyplayv2' in endpoints
                assert 'shotchartdetail' in endpoints
                
                # Each endpoint should have metadata
                for endpoint_data in endpoints.values():
                    assert 'bytes' in endpoint_data
                    assert 'sha1' in endpoint_data
                    assert 'ok' in endpoint_data
                    assert endpoint_data['ok'] is True  # Mock always succeeds
            
            # Verify summary calculations
            summary_data = manifest['summary']
            assert summary_data['games'] == 2
            assert summary_data['ok_games'] == 2
            assert summary_data['failed_games'] == 0
            assert summary_data['total_bytes'] > 0
    
    @pytest.mark.asyncio 
    async def test_harvest_date_no_games(self):
        """Test date harvest when no games are found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root_path = Path(tmpdir)
            date_str = "2023-06-15"  # Off-season date
            
            # Mock client with empty scoreboard
            with patch('src.nba_scraper.raw_io.backfill.RawNbaClient') as mock_client_class:
                mock_client = MockRawNbaClient()
                
                # Override scoreboard to return no games
                async def empty_scoreboard(date_str):
                    return {
                        "resource": "scoreboardv2",
                        "resultSets": [
                            {
                                "name": "GameHeader", 
                                "headers": ["GAME_ID", "GAME_STATUS_TEXT"],
                                "rowSet": []  # No games
                            }
                        ]
                    }
                
                mock_client.fetch_scoreboard = empty_scoreboard
                mock_client_class.return_value.__aenter__.return_value = mock_client
                
                # Run harvest
                summary = await harvest_date(date_str=date_str, root=str(root_path))
            
            # Should handle gracefully
            assert summary['games_discovered'] == 0
            assert summary['games_processed'] == 0
            
            # Directory should still be created
            date_dir = root_path / date_str
            assert date_dir.exists()
            
            # Scoreboard should still be written
            scoreboard_path = date_dir / "scoreboard.json"
            assert scoreboard_path.exists()
    
    @pytest.mark.asyncio
    async def test_harvest_single_game_error_handling(self):
        """Test single game harvest with endpoint failures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            date_dir = Path(tmpdir) / "2023-10-27"
            date_dir.mkdir(parents=True)
            
            # Mock client with one failing endpoint
            mock_client = MockRawNbaClient()
            
            # Override playbyplay to fail
            async def failing_playbyplay(game_id):
                raise Exception("API timeout")
            
            mock_client.fetch_playbyplay = failing_playbyplay
            
            # Track summary for testing
            summary = {
                'endpoints_succeeded': 0,
                'endpoints_failed': 0,
                'total_bytes': 0
            }
            
            # Test single game harvest
            game_success = await _harvest_single_game(
                mock_client, "0022300001", date_dir, summary
            )
            
            # Should be successful despite one failure (3/4 endpoints OK)
            assert game_success is True
            assert summary['endpoints_succeeded'] == 3  # summary, traditional, shotchart
            assert summary['endpoints_failed'] == 1    # playbyplay failed
            
            # Verify game directory and files
            game_dir = date_dir / "0022300001"
            assert game_dir.exists()
            
            # Should have successful endpoint files
            assert (game_dir / "boxscoresummaryv2.json").exists()
            assert (game_dir / "boxscoretraditionalv2.json").exists()
            assert (game_dir / "shotchartdetail.json").exists()
            
            # Should NOT have failed endpoint file
            assert not (game_dir / "playbyplayv2.json").exists()
            
            # Verify manifest was updated with error info
            manifest = read_manifest(date_dir)
            assert manifest is not None
            assert len(manifest['games']) == 1
            
            game_record = manifest['games'][0]
            assert len(game_record['errors']) == 1
            assert game_record['errors'][0]['endpoint'] == 'playbyplayv2'
            assert 'API timeout' in game_record['errors'][0]['error']
    
    @pytest.mark.asyncio
    async def test_client_call_patterns(self):
        """Test that client methods are called in correct order and frequency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root_path = Path(tmpdir)
            date_str = "2023-10-27"
            
            with patch('src.nba_scraper.raw_io.backfill.RawNbaClient') as mock_client_class:
                mock_client = MockRawNbaClient()
                mock_client_class.return_value.__aenter__.return_value = mock_client
                
                await harvest_date(date_str=date_str, root=str(root_path))
            
            # Verify API call patterns
            call_log = mock_client.call_log
            
            # Should start with scoreboard
            assert call_log[0] == ('scoreboard', date_str)
            
            # Should have 4 calls per game (2 games = 8 calls) + 1 scoreboard = 9 total
            assert len(call_log) == 9
            
            # Count calls by type
            call_counts = {}
            for call in call_log:
                call_type = call[0]
                call_counts[call_type] = call_counts.get(call_type, 0) + 1
            
            assert call_counts['scoreboard'] == 1
            assert call_counts['boxscoresummary'] == 2  # One per game
            assert call_counts['boxscoretraditional'] == 2
            assert call_counts['playbyplay'] == 2
            assert call_counts['shotchart'] == 2
    
    @pytest.mark.asyncio
    async def test_gzip_compression_for_large_files(self):
        """Test that large endpoint files get gzipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root_path = Path(tmpdir)
            date_str = "2023-10-27"
            
            with patch('src.nba_scraper.raw_io.backfill.RawNbaClient') as mock_client_class:
                mock_client = MockRawNbaClient()
                
                # Override playbyplay to return large data
                async def large_playbyplay(game_id):
                    return {
                        "resource": "playbyplayv2",
                        "parameters": {"GameID": game_id},
                        "resultSets": [{
                            "name": "PlayByPlay",
                            "headers": ["GAME_ID", "EVENTNUM", "DESCRIPTION"],
                            "rowSet": [
                                [game_id, i, f"Large play description {i} " + "x" * 1000]
                                for i in range(2000)  # Make it > 1MB
                            ]
                        }]
                    }
                
                mock_client.fetch_playbyplay = large_playbyplay
                mock_client_class.return_value.__aenter__.return_value = mock_client
                
                await harvest_date(date_str=date_str, root=str(root_path))
            
            # Check that large playbyplay files got gzipped
            for game_id in ["0022300001", "0022300002"]:
                game_dir = root_path / date_str / game_id
                pbp_path = game_dir / "playbyplayv2.json"
                pbp_gz_path = game_dir / "playbyplayv2.json.gz"
                
                assert pbp_path.exists()
                assert pbp_gz_path.exists()  # Should have .gz version
                
                # Verify file is actually large
                assert pbp_path.stat().st_size > 1024 * 1024  # > 1MB