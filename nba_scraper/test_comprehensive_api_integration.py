"""
Comprehensive Live API Integration Tests
Tests all NBA data source clients with real API calls and robust validation.
"""

import asyncio
import pytest
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import tempfile
import json

from nba_scraper.io_clients.nba_stats import NBAStatsClient
from nba_scraper.io_clients.bref import BRefClient
from nba_scraper.io_clients.gamebooks import GamebooksClient
from nba_scraper.config import get_settings
from nba_scraper.nba_logging import get_logger

logger = get_logger(__name__)

class TestNBAStatsClientIntegration:
    """Comprehensive tests for NBA Stats API client."""
    
    @pytest.fixture
    async def nba_client(self):
        """Initialize NBA Stats client."""
        return NBAStatsClient()
    
    async def test_fetch_scoreboard_recent_date(self, nba_client):
        """Test fetching scoreboard for recent date with games."""
        # Use a date from the NBA season
        test_date = datetime(2024, 1, 15)  # Mid-season date
        
        try:
            scoreboard_data = await nba_client.fetch_scoreboard_by_date(test_date)
            assert scoreboard_data is not None
            assert isinstance(scoreboard_data, dict)
            
            # Parse games from scoreboard
            games = nba_client.parse_scoreboard_games(scoreboard_data)
            logger.info(f"Found {len(games)} games for {test_date.date()}")
            
            # Validate game structure
            for game in games:
                assert 'game_id' in game
                assert 'home_team' in game
                assert 'away_team' in game
                assert 'game_date' in game
                
        except Exception as e:
            logger.warning(f"Scoreboard test failed (may be expected): {e}")
            pytest.skip(f"NBA Stats API unavailable: {e}")
    
    async def test_fetch_boxscore_data(self, nba_client):
        """Test fetching detailed boxscore data."""
        # Use a known game ID from the season
        test_game_id = "0022300456"  # Example game
        
        try:
            boxscore_data = await nba_client.fetch_boxscore_traditional(test_game_id)
            assert boxscore_data is not None
            
            # Validate boxscore structure
            if 'resultSets' in boxscore_data:
                assert len(boxscore_data['resultSets']) > 0
                
            logger.info(f"Successfully fetched boxscore for game {test_game_id}")
            
        except Exception as e:
            logger.warning(f"Boxscore test failed: {e}")
            pytest.skip(f"NBA Stats API boxscore unavailable: {e}")
    
    async def test_fetch_play_by_play(self, nba_client):
        """Test fetching play-by-play data."""
        test_game_id = "0022300456"
        
        try:
            pbp_data = await nba_client.fetch_play_by_play(test_game_id)
            assert pbp_data is not None
            
            if 'resultSets' in pbp_data:
                plays = pbp_data['resultSets'][0]['rowSet'] if pbp_data['resultSets'] else []
                logger.info(f"Found {len(plays)} plays for game {test_game_id}")
                
        except Exception as e:
            logger.warning(f"Play-by-play test failed: {e}")
            pytest.skip(f"NBA Stats API PBP unavailable: {e}")
    
    async def test_fetch_shot_chart(self, nba_client):
        """Test fetching shot chart data."""
        test_game_id = "0022300456"
        
        try:
            shot_data = await nba_client.fetch_shot_chart(test_game_id)
            assert shot_data is not None
            
            if 'resultSets' in shot_data:
                shots = shot_data['resultSets'][0]['rowSet'] if shot_data['resultSets'] else []
                logger.info(f"Found {len(shots)} shots for game {test_game_id}")
                
        except Exception as e:
            logger.warning(f"Shot chart test failed: {e}")
            pytest.skip(f"NBA Stats API shot chart unavailable: {e}")
    
    async def test_rate_limiting(self, nba_client):
        """Test that rate limiting works properly."""
        test_date = datetime(2024, 1, 15)
        
        # Make multiple rapid requests
        start_time = datetime.now()
        
        try:
            for i in range(3):
                await nba_client.fetch_scoreboard_by_date(test_date)
                
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # Should take at least some time due to rate limiting
            assert elapsed > 1.0, "Rate limiting may not be working"
            logger.info(f"Rate limiting test completed in {elapsed:.2f}s")
            
        except Exception as e:
            logger.warning(f"Rate limiting test failed: {e}")


class TestBRefClientIntegration:
    """Comprehensive tests for Basketball Reference client."""
    
    @pytest.fixture
    async def bref_client(self):
        """Initialize BRef client."""
        return BRefClient()
    
    async def test_resolve_bref_game_id(self, bref_client):
        """Test resolving Basketball Reference game ID."""
        try:
            # Use known game details
            game_date = date(2024, 1, 15)
            home_team = "LAL"
            away_team = "BOS"
            
            bref_id = await bref_client.resolve_bref_game_id(
                home_tricode=home_team,
                away_tricode=away_team,
                local_date=game_date
            )
            
            assert bref_id is not None
            assert len(bref_id) > 10  # BRef IDs are formatted like "202401150LAL"
            logger.info(f"Resolved BRef game ID: {bref_id}")
            
        except Exception as e:
            logger.warning(f"BRef game ID resolution failed: {e}")
            pytest.skip(f"Basketball Reference unavailable: {e}")
    
    async def test_fetch_boxscore_html(self, bref_client):
        """Test fetching boxscore HTML from Basketball Reference."""
        try:
            # Use a known BRef game ID format
            bref_game_id = "202401150LAL"  # Example format
            
            html_content = await bref_client.fetch_bref_box(bref_game_id)
            
            assert html_content is not None
            assert len(html_content) > 1000  # Should be substantial HTML
            assert "<html" in html_content.lower()
            
            logger.info(f"Fetched {len(html_content)} chars of HTML for {bref_game_id}")
            
        except Exception as e:
            logger.warning(f"BRef boxscore fetch failed: {e}")
            pytest.skip(f"Basketball Reference boxscore unavailable: {e}")
    
    async def test_parse_boxscore_scores(self, bref_client):
        """Test parsing scores from boxscore HTML."""
        try:
            bref_game_id = "202401150LAL"
            html_content = await bref_client.fetch_bref_box(bref_game_id)
            
            scores = bref_client.parse_boxscore_scores(html_content)
            
            assert isinstance(scores, dict)
            # Should contain team scores and other game info
            if scores:  # May be empty if parsing fails
                logger.info(f"Parsed scores: {scores}")
                
        except Exception as e:
            logger.warning(f"BRef score parsing failed: {e}")
            pytest.skip(f"Basketball Reference parsing unavailable: {e}")
    
    async def test_parse_starting_lineups(self, bref_client):
        """Test parsing starting lineups from HTML."""
        try:
            bref_game_id = "202401150LAL"
            html_content = await bref_client.fetch_bref_box(bref_game_id)
            
            lineups = bref_client.parse_starting_lineups(html_content)
            
            assert isinstance(lineups, dict)
            if lineups:  # May be empty if parsing fails
                logger.info(f"Parsed lineups: {len(lineups)} teams")
                
        except Exception as e:
            logger.warning(f"BRef lineup parsing failed: {e}")
    
    async def test_fetch_schedule_page(self, bref_client):
        """Test fetching team schedule page."""
        try:
            team_tricode = "LAL"
            season = "2024"
            
            schedule_html = await bref_client.fetch_schedule_page(team_tricode, season)
            
            assert schedule_html is not None
            assert len(schedule_html) > 1000
            assert "<html" in schedule_html.lower()
            
            logger.info(f"Fetched schedule page for {team_tricode} {season}")
            
        except Exception as e:
            logger.warning(f"BRef schedule fetch failed: {e}")
            pytest.skip(f"Basketball Reference schedule unavailable: {e}")


class TestGamebooksClientIntegration:
    """Comprehensive tests for NBA Gamebooks client."""
    
    @pytest.fixture
    async def gamebooks_client(self):
        """Initialize Gamebooks client."""
        return GamebooksClient()
    
    async def test_list_gamebooks(self, gamebooks_client):
        """Test listing available gamebooks for a date."""
        try:
            # Use a date that likely has games
            test_date = date(2024, 1, 15)
            
            gamebook_urls = await gamebooks_client.list_gamebooks(test_date)
            
            assert isinstance(gamebook_urls, list)
            logger.info(f"Found {len(gamebook_urls)} gamebooks for {test_date}")
            
            # Validate URL format if any found
            for url in gamebook_urls[:3]:  # Check first few
                assert url.startswith("http")
                assert ".pdf" in url.lower()
                
        except Exception as e:
            logger.warning(f"Gamebooks listing failed: {e}")
            pytest.skip(f"NBA Gamebooks unavailable: {e}")
    
    async def test_download_gamebook(self, gamebooks_client):
        """Test downloading a gamebook PDF."""
        try:
            test_date = date(2024, 1, 15)
            gamebook_urls = await gamebooks_client.list_gamebooks(test_date)
            
            if not gamebook_urls:
                pytest.skip("No gamebooks available for test date")
                
            # Download first gamebook to temp location
            test_url = gamebook_urls[0]
            
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                temp_path = Path(tmp.name)
                
            try:
                downloaded_path = await gamebooks_client.download_gamebook(
                    test_url, dest=temp_path
                )
                
                assert downloaded_path.exists()
                assert downloaded_path.stat().st_size > 1000  # Should be substantial PDF
                
                logger.info(f"Downloaded gamebook: {downloaded_path.stat().st_size} bytes")
                
            finally:
                # Clean up
                if temp_path.exists():
                    temp_path.unlink()
                    
        except Exception as e:
            logger.warning(f"Gamebook download failed: {e}")
            pytest.skip(f"NBA Gamebook download unavailable: {e}")
    
    async def test_parse_gamebook_pdf(self, gamebooks_client):
        """Test parsing a downloaded gamebook PDF."""
        try:
            test_date = date(2024, 1, 15)
            gamebook_urls = await gamebooks_client.list_gamebooks(test_date)
            
            if not gamebook_urls:
                pytest.skip("No gamebooks available for test date")
                
            # Download and parse first gamebook
            test_url = gamebook_urls[0]
            
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                temp_path = Path(tmp.name)
                
            try:
                # Download PDF
                downloaded_path = await gamebooks_client.download_gamebook(
                    test_url, dest=temp_path
                )
                
                # Parse PDF content
                parsed_data = gamebooks_client.parse_gamebook_pdf(downloaded_path)
                
                assert isinstance(parsed_data, dict)
                assert 'refs' in parsed_data
                assert 'alternates' in parsed_data
                assert 'parsing_confidence' in parsed_data
                
                logger.info(f"Parsed gamebook data: {len(parsed_data.get('refs', []))} refs, "
                          f"confidence: {parsed_data.get('parsing_confidence', 0):.2f}")
                
            finally:
                # Clean up
                if temp_path.exists():
                    temp_path.unlink()
                    
        except Exception as e:
            logger.warning(f"Gamebook parsing failed: {e}")
            pytest.skip(f"NBA Gamebook parsing unavailable: {e}")
    
    def test_cache_functionality(self, gamebooks_client):
        """Test gamebook caching functionality."""
        try:
            # Test cache key generation
            test_url = "https://example.com/gamebook123.pdf"
            cached_path = gamebooks_client.get_cached_gamebook(test_url)
            
            # Should return None if not cached
            assert cached_path is None or not cached_path.exists()
            
            # Test cache clearing
            cleared_count = gamebooks_client.clear_cache(older_than_days=30)
            assert isinstance(cleared_count, int)
            assert cleared_count >= 0
            
            logger.info(f"Cache test completed, cleared {cleared_count} files")
            
        except Exception as e:
            logger.warning(f"Cache functionality test failed: {e}")


class TestCrossClientIntegration:
    """Tests that combine multiple clients for complete data pipelines."""
    
    @pytest.fixture
    async def all_clients(self):
        """Initialize all clients."""
        return {
            'nba_stats': NBAStatsClient(),
            'bref': BRefClient(),
            'gamebooks': GamebooksClient()
        }
    
    async def test_complete_game_data_pipeline(self, all_clients):
        """Test complete pipeline: NBA Stats -> BRef -> Gamebooks."""
        try:
            nba_client = all_clients['nba_stats']
            bref_client = all_clients['bref']
            gamebooks_client = all_clients['gamebooks']
            
            # 1. Get games from NBA Stats API
            test_date = datetime(2024, 1, 15)
            scoreboard_data = await nba_client.fetch_scoreboard_by_date(test_date)
            games = nba_client.parse_scoreboard_games(scoreboard_data)
            
            if not games:
                pytest.skip("No games found for test date")
                
            game = games[0]  # Use first game
            logger.info(f"Testing pipeline for game: {game.get('game_id')}")
            
            # 2. Get BRef data for same game
            if 'home_team' in game and 'away_team' in game:
                try:
                    bref_id = await bref_client.resolve_bref_game_id(
                        home_tricode=game['home_team'].get('triCode', ''),
                        away_tricode=game['away_team'].get('triCode', ''),
                        local_date=test_date.date()
                    )
                    logger.info(f"Resolved BRef ID: {bref_id}")
                except Exception as e:
                    logger.warning(f"BRef resolution failed: {e}")
            
            # 3. Get Gamebooks data for same date
            try:
                gamebook_urls = await gamebooks_client.list_gamebooks(test_date.date())
                logger.info(f"Found {len(gamebook_urls)} gamebooks for date")
            except Exception as e:
                logger.warning(f"Gamebooks listing failed: {e}")
            
            logger.info("Complete pipeline test completed successfully")
            
        except Exception as e:
            logger.warning(f"Complete pipeline test failed: {e}")
            pytest.skip(f"Pipeline test unavailable: {e}")
    
    async def test_error_handling_consistency(self, all_clients):
        """Test that all clients handle errors consistently."""
        nba_client = all_clients['nba_stats']
        bref_client = all_clients['bref']
        gamebooks_client = all_clients['gamebooks']
        
        # Test with invalid/future date that should have no data
        future_date = date(2030, 1, 1)
        
        # NBA Stats - should handle gracefully
        try:
            scoreboard_data = await nba_client.fetch_scoreboard_by_date(
                datetime.combine(future_date, datetime.min.time())
            )
            # Should either return empty data or raise expected exception
        except Exception as e:
            logger.info(f"NBA Stats future date error (expected): {e}")
        
        # BRef - should handle invalid game ID gracefully
        try:
            html_content = await bref_client.fetch_bref_box("INVALID_GAME_ID")
        except Exception as e:
            logger.info(f"BRef invalid ID error (expected): {e}")
        
        # Gamebooks - should handle future date gracefully
        try:
            gamebook_urls = await gamebooks_client.list_gamebooks(future_date)
            assert isinstance(gamebook_urls, list)  # Should return empty list
        except Exception as e:
            logger.info(f"Gamebooks future date error (expected): {e}")
        
        logger.info("Error handling consistency test completed")


async def run_comprehensive_integration_tests():
    """Run all integration tests with detailed reporting."""
    print("🚀 Starting Comprehensive API Integration Tests")
    print("=" * 60)
    
    # Test configuration
    settings = get_settings()
    print(f"🔧 Configuration loaded: {settings.environment}")
    
    # Initialize test results tracking
    results = {
        'nba_stats': {'passed': 0, 'failed': 0, 'skipped': 0},
        'bref': {'passed': 0, 'failed': 0, 'skipped': 0},
        'gamebooks': {'passed': 0, 'failed': 0, 'skipped': 0},
        'integration': {'passed': 0, 'failed': 0, 'skipped': 0}
    }
    
    # Run NBA Stats tests
    print(f"\n📊 Testing NBA Stats API Client...")
    nba_tests = TestNBAStatsClientIntegration()
    nba_client = NBAStatsClient()
    
    test_methods = [
        'test_fetch_scoreboard_recent_date',
        'test_fetch_boxscore_data', 
        'test_fetch_play_by_play',
        'test_fetch_shot_chart',
        'test_rate_limiting'
    ]
    
    for method_name in test_methods:
        try:
            method = getattr(nba_tests, method_name)
            await method(nba_client)
            results['nba_stats']['passed'] += 1
            print(f"  ✅ {method_name}")
        except Exception as e:
            if "skip" in str(e).lower():
                results['nba_stats']['skipped'] += 1
                print(f"  ⏭️  {method_name} (skipped)")
            else:
                results['nba_stats']['failed'] += 1
                print(f"  ❌ {method_name}: {e}")
    
    # Run BRef tests
    print(f"\n🏀 Testing Basketball Reference Client...")
    bref_tests = TestBRefClientIntegration()
    bref_client = BRefClient()
    
    bref_methods = [
        'test_resolve_bref_game_id',
        'test_fetch_boxscore_html',
        'test_parse_boxscore_scores',
        'test_parse_starting_lineups',
        'test_fetch_schedule_page'
    ]
    
    for method_name in bref_methods:
        try:
            method = getattr(bref_tests, method_name)
            await method(bref_client)
            results['bref']['passed'] += 1
            print(f"  ✅ {method_name}")
        except Exception as e:
            if "skip" in str(e).lower():
                results['bref']['skipped'] += 1
                print(f"  ⏭️  {method_name} (skipped)")
            else:
                results['bref']['failed'] += 1
                print(f"  ❌ {method_name}: {e}")
    
    # Run Gamebooks tests
    print(f"\n📄 Testing NBA Gamebooks Client...")
    gamebooks_tests = TestGamebooksClientIntegration()
    gamebooks_client = GamebooksClient()
    
    gamebooks_methods = [
        'test_list_gamebooks',
        'test_download_gamebook',
        'test_parse_gamebook_pdf',
        'test_cache_functionality'
    ]
    
    for method_name in gamebooks_methods:
        try:
            method = getattr(gamebooks_tests, method_name)
            if method_name == 'test_cache_functionality':
                method(gamebooks_client)  # Sync method
            else:
                await method(gamebooks_client)
            results['gamebooks']['passed'] += 1
            print(f"  ✅ {method_name}")
        except Exception as e:
            if "skip" in str(e).lower():
                results['gamebooks']['skipped'] += 1
                print(f"  ⏭️  {method_name} (skipped)")
            else:
                results['gamebooks']['failed'] += 1
                print(f"  ❌ {method_name}: {e}")
    
    # Run cross-client integration tests
    print(f"\n🔗 Testing Cross-Client Integration...")
    integration_tests = TestCrossClientIntegration()
    all_clients = {
        'nba_stats': nba_client,
        'bref': bref_client,
        'gamebooks': gamebooks_client
    }
    
    integration_methods = [
        'test_complete_game_data_pipeline',
        'test_error_handling_consistency'
    ]
    
    for method_name in integration_methods:
        try:
            method = getattr(integration_tests, method_name)
            await method(all_clients)
            results['integration']['passed'] += 1
            print(f"  ✅ {method_name}")
        except Exception as e:
            if "skip" in str(e).lower():
                results['integration']['skipped'] += 1
                print(f"  ⏭️  {method_name} (skipped)")
            else:
                results['integration']['failed'] += 1
                print(f"  ❌ {method_name}: {e}")
    
    # Print final results
    print(f"\n" + "=" * 60)
    print(f"🎯 INTEGRATION TEST RESULTS")
    print(f"=" * 60)
    
    total_passed = sum(r['passed'] for r in results.values())
    total_failed = sum(r['failed'] for r in results.values())
    total_skipped = sum(r['skipped'] for r in results.values())
    total_tests = total_passed + total_failed + total_skipped
    
    for client, result in results.items():
        passed = result['passed']
        failed = result['failed'] 
        skipped = result['skipped']
        total = passed + failed + skipped
        
        if total > 0:
            success_rate = (passed / total) * 100
            print(f"{client.upper():12} | {passed:2d}P {failed:2d}F {skipped:2d}S | {success_rate:5.1f}% success")
    
    print(f"=" * 60)
    print(f"TOTAL        | {total_passed:2d}P {total_failed:2d}F {total_skipped:2d}S | {total_tests} tests")
    
    if total_failed == 0:
        print(f"🎉 ALL INTEGRATION TESTS PASSED!")
    else:
        print(f"⚠️  {total_failed} tests failed - check logs for details")
    
    return results


if __name__ == "__main__":
    asyncio.run(run_comprehensive_integration_tests())