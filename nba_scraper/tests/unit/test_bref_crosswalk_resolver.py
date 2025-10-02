"""Unit tests for Basketball Reference crosswalk resolver edge cases."""

import pytest
from datetime import date, timedelta
from unittest.mock import Mock, patch

from nba_scraper.transformers.games import BRefCrosswalkResolver, GameCrosswalkTransformer
from nba_scraper.models.enums import GameStatus
from nba_scraper.models.crosswalk_rows import GameIdCrosswalkRow


class TestBRefCrosswalkResolver:
    """Test cases for Basketball Reference crosswalk resolver."""
    
    @pytest.fixture
    def resolver(self):
        """Create BRefCrosswalkResolver instance."""
        return BRefCrosswalkResolver()
    
    @pytest.fixture
    def transformer(self):
        """Create GameCrosswalkTransformer instance."""
        return GameCrosswalkTransformer("bref_resolver")
    
    def test_primary_resolution_success(self, resolver):
        """Test successful primary B-Ref ID resolution."""
        game_date = date(2024, 1, 15)
        bref_id = resolver.resolve_bref_game_id(
            game_id="0022400567",
            home_team="LAL",
            away_team="BOS",
            game_date=game_date,
            game_status=GameStatus.FINAL
        )
        
        expected = "202401150LAL"
        assert bref_id == expected
    
    def test_alternative_tricode_mapping(self, resolver):
        """Test resolution with alternative tricode mappings."""
        game_date = date(2024, 1, 15)
        
        # Test Charlotte mapping
        bref_id = resolver.resolve_bref_game_id(
            game_id="0022400567",
            home_team="CHA",
            away_team="BOS", 
            game_date=game_date
        )
        
        expected = "202401150CHO"  # CHA maps to CHO on B-Ref
        assert bref_id == expected
        
        # Test Phoenix mapping
        bref_id = resolver.resolve_bref_game_id(
            game_id="0022400568",
            home_team="PHX",
            away_team="LAL",
            game_date=game_date
        )
        
        expected = "202401150PHO"  # PHX maps to PHO on B-Ref
        assert bref_id == expected
    
    def test_historical_variations(self, resolver):
        """Test resolution with historical team variations."""
        game_date = date(2024, 1, 15)
        
        # Brooklyn Nets historical variations
        historical_vars = resolver._get_historical_variations("BRK")
        assert "NJN" in historical_vars  # New Jersey Nets
        assert "BKN" in historical_vars  # Brooklyn alternative
        
        # Oklahoma City historical variations
        historical_vars = resolver._get_historical_variations("OKC")
        assert "SEA" in historical_vars  # Seattle SuperSonics
    
    def test_postponed_game_with_actual_date(self, resolver):
        """Test postponed game resolution with known actual date."""
        original_date = date(2024, 1, 15)
        actual_date = date(2024, 1, 18)  # Game played 3 days later
        
        bref_id = resolver.resolve_bref_game_id(
            game_id="0022400567",
            home_team="LAL",
            away_team="BOS",
            game_date=original_date,
            game_status=GameStatus.POSTPONED,
            actual_date=actual_date
        )
        
        expected = "202401180LAL"  # Uses actual date, not original
        assert bref_id == expected
    
    def test_postponed_game_without_actual_date(self, resolver):
        """Test postponed game falls back to fuzzy matching when no actual date."""
        original_date = date(2024, 1, 15)
        
        bref_id = resolver.resolve_bref_game_id(
            game_id="0022400567",
            home_team="LAL",
            away_team="BOS",
            game_date=original_date,
            game_status=GameStatus.POSTPONED
        )
        
        # Should fall back to fuzzy date matching (±7 days)
        # Since we can't know the exact makeup date, it should try various dates
        assert bref_id is not None
        assert "LAL" in bref_id
    
    def test_suspended_game_resume_logic(self, resolver):
        """Test suspended game logic tries subsequent dates."""
        original_date = date(2024, 1, 15)
        
        # Mock the primary resolution to fail for original date but succeed for +2 days
        with patch.object(resolver, '_try_primary_resolution') as mock_primary:
            # Return None for original date, success for +2 days
            def side_effect(team, test_date):
                if test_date == original_date:
                    return None
                elif test_date == original_date + timedelta(days=2):
                    return f"{test_date.strftime('%Y%m%d')}0{team}"
                return None
            
            mock_primary.side_effect = side_effect
            
            bref_id = resolver.resolve_bref_game_id(
                game_id="0022400567",
                home_team="LAL",
                away_team="BOS",
                game_date=original_date,
                game_status=GameStatus.SUSPENDED,
                status=GameStatus.SUSPENDED
            )
            
            expected = "202401170LAL"  # +2 days from original
            assert bref_id == expected
    
    def test_makeup_game_fuzzy_matching(self, resolver):
        """Test makeup game fuzzy date matching within window."""
        scheduled_date = date(2024, 1, 15)
        
        # Mock primary resolution to succeed only for a date 5 days later
        with patch.object(resolver, '_try_primary_resolution') as mock_primary:
            def side_effect(team, test_date):
                if test_date == scheduled_date + timedelta(days=5):
                    return f"{test_date.strftime('%Y%m%d')}0{team}"
                return None
            
            mock_primary.side_effect = side_effect
            
            bref_id = resolver.resolve_bref_game_id(
                game_id="0022400567",
                home_team="LAL",
                away_team="BOS",
                game_date=scheduled_date,
                is_makeup=True,
                makeup_window_days=10
            )
            
            expected = "202401200LAL"  # +5 days from scheduled
            assert bref_id == expected
    
    def test_date_extraction_from_game_id(self, resolver):
        """Test date extraction from various game ID formats."""
        # Test YYYYMMDD pattern
        game_id = "20240115567"
        extracted = resolver._extract_date_from_game_id(game_id)
        assert extracted == date(2024, 1, 15)
        
        # Test ISO format
        game_id = "game_2024-01-15_567"
        extracted = resolver._extract_date_from_game_id(game_id)
        assert extracted == date(2024, 1, 15)
        
        # Test no date found
        game_id = "0022400567"
        extracted = resolver._extract_date_from_game_id(game_id)
        assert extracted is None
    
    def test_fuzzy_date_fallback_exhaustive(self, resolver):
        """Test fuzzy date fallback tries all combinations."""
        target_date = date(2024, 1, 15)
        
        # Mock to succeed only on last attempt (CHO, +7 days)
        with patch.object(resolver, '_try_primary_resolution') as mock_primary:
            def side_effect(team, test_date):
                if team == "CHO" and test_date == target_date + timedelta(days=7):
                    return f"{test_date.strftime('%Y%m%d')}0{team}"
                return None
            
            mock_primary.side_effect = side_effect
            
            bref_id = resolver._fuzzy_date_fallback("CHA", "BOS", target_date)
            
            expected = "202401220CHO"  # +7 days, CHO tricode
            assert bref_id == expected
    
    def test_unresolvable_game_returns_none(self, resolver):
        """Test that completely unresolvable games return None."""
        target_date = date(2024, 1, 15)
        
        # Mock all resolution attempts to fail
        with patch.object(resolver, '_try_primary_resolution', return_value=None):
            bref_id = resolver.resolve_bref_game_id(
                game_id="0022400567",
                home_team="UNKNOWN",
                away_team="TEAM",
                game_date=target_date
            )
            
            assert bref_id is None
    
    def test_create_crosswalk_with_confidence(self, resolver):
        """Test crosswalk creation with confidence metadata."""
        crosswalk = resolver.create_crosswalk_with_confidence(
            game_id="0022400567",
            bref_game_id="202401150LAL",
            resolution_method="primary",
            confidence_score=1.0,
            source_url="https://test.com"
        )
        
        assert crosswalk.game_id == "0022400567"
        assert crosswalk.bref_game_id == "202401150LAL"
        assert crosswalk.nba_stats_game_id == "0022400567"
        assert crosswalk.source == "bref_resolver_primary"
        assert crosswalk.source_url == "https://test.com"
    
    def test_create_crosswalk_failed_resolution(self, resolver):
        """Test crosswalk creation when resolution fails."""
        crosswalk = resolver.create_crosswalk_with_confidence(
            game_id="0022400567",
            bref_game_id=None,
            resolution_method="failed",
            confidence_score=0.0,
            source_url="https://test.com"
        )
        
        assert crosswalk.game_id == "0022400567"
        assert crosswalk.bref_game_id == "UNRESOLVED_0022400567"
        assert crosswalk.source == "bref_resolver_failed"
    
    def test_transformer_resolve_bref_crosswalk(self, transformer):
        """Test transformer's resolve_bref_crosswalk method."""
        game_date = date(2024, 1, 15)
        
        crosswalk = transformer.resolve_bref_crosswalk(
            game_id="0022400567",
            home_team="LAL",
            away_team="BOS",
            game_date=game_date,
            game_status=GameStatus.FINAL,
            source_url="https://test.com"
        )
        
        assert isinstance(crosswalk, GameIdCrosswalkRow)
        assert crosswalk.game_id == "0022400567"
        assert crosswalk.bref_game_id == "202401150LAL"
        assert "bref_resolver" in crosswalk.source
    
    def test_transformer_fuzzy_matched_confidence(self, transformer):
        """Test transformer assigns correct confidence for fuzzy matches."""
        game_date = date(2024, 1, 15)
        
        # Mock resolver to return a different ID than primary would generate
        with patch.object(transformer.bref_resolver, 'resolve_bref_game_id') as mock_resolve:
            mock_resolve.return_value = "202401160LAL"  # Different date
            
            with patch.object(transformer.bref_resolver, '_try_primary_resolution') as mock_primary:
                mock_primary.return_value = "202401150LAL"  # Original date
                
                crosswalk = transformer.resolve_bref_crosswalk(
                    game_id="0022400567",
                    home_team="LAL",
                    away_team="BOS",
                    game_date=game_date,
                    game_status=GameStatus.POSTPONED
                )
                
                assert "fuzzy_matched" in crosswalk.source
    
    def test_edge_case_empty_inputs(self, resolver):
        """Test handling of empty or invalid inputs."""
        # Empty tricode
        bref_id = resolver.resolve_bref_game_id("", "", "", date(2024, 1, 15))
        assert bref_id is not None  # Should still generate something
        
        # None tricode handling in normalize
        normalized = resolver._normalize_bref_tricode(None)
        assert normalized is None
        
        # Empty game_id for date extraction
        extracted = resolver._extract_date_from_game_id("")
        assert extracted is None
    
    def test_comprehensive_edge_case_flow(self, resolver):
        """Test comprehensive flow through multiple edge case handlers."""
        original_date = date(2024, 1, 15)
        
        # Mock sequence: primary fails, postponed logic fails, makeup succeeds
        with patch.object(resolver, '_try_primary_resolution') as mock_primary:
            with patch.object(resolver, '_resolve_postponed_game') as mock_postponed:
                with patch.object(resolver, '_resolve_makeup_game') as mock_makeup:
                    
                    mock_primary.return_value = None  # Primary fails
                    mock_postponed.return_value = None  # Postponed logic fails
                    mock_makeup.return_value = "202401200LAL"  # Makeup succeeds
                    
                    bref_id = resolver.resolve_bref_game_id(
                        game_id="0022400567",
                        home_team="LAL",
                        away_team="BOS",
                        game_date=original_date,
                        game_status=GameStatus.POSTPONED,
                        is_makeup=True  # Triggers makeup logic
                    )
                    
                    assert bref_id == "202401200LAL"
                    mock_postponed.assert_called_once()
                    mock_makeup.assert_called_once()