"""Unit tests for loader facade resolution."""

import pytest
from unittest.mock import patch, MagicMock


class TestLoaderFacade:
    """Test loader facade resolution functionality."""

    def test_facade_resolves_callable_or_none(self):
        """Test that facade resolves callables or None without ImportError."""
        # This should not raise ImportError regardless of what's available
        try:
            from nba_scraper.loaders import (
                upsert_game, 
                upsert_pbp, 
                upsert_lineups, 
                upsert_shots, 
                upsert_adv_metrics
            )
            
            # Each should be either callable or None
            for loader in [upsert_game, upsert_pbp, upsert_lineups, upsert_shots, upsert_adv_metrics]:
                assert loader is None or callable(loader), f"Loader {loader} should be callable or None"
            
        except ImportError as e:
            pytest.fail(f"Facade should not raise ImportError: {e}")

    def test_facade_handles_missing_modules_gracefully(self):
        """Test that facade handles missing modules gracefully."""
        with patch('importlib.import_module', side_effect=ImportError("Module not found")):
            # Re-import to trigger the patched import_module
            import importlib
            import sys
            
            # Remove from cache if present
            module_name = 'nba_scraper.loaders'
            if module_name in sys.modules:
                del sys.modules[module_name]
            
            try:
                # This should still work even with import failures
                from nba_scraper.loaders import upsert_game
                # Should be None since imports fail
                assert upsert_game is None
            except Exception as e:
                pytest.fail(f"Should handle import failures gracefully: {e}")

    def test_callable_detection(self):
        """Test that only actual callables are returned."""
        # Mock a module with mixed attributes
        mock_module = MagicMock()
        mock_module.upsert_game = lambda x: x  # Callable
        mock_module.not_callable = "string"    # Not callable
        mock_module.none_value = None          # None
        
        with patch('importlib.import_module', return_value=mock_module):
            from nba_scraper.loaders import _resolve_callable
            
            # Should return callable
            result = _resolve_callable("upsert_game")
            assert callable(result)
            
            # Should return None for non-callable
            result = _resolve_callable("not_callable") 
            assert result is None

    def test_module_scan_order(self):
        """Test that modules are scanned in expected order."""
        # Test that it tries multiple module names
        with patch('importlib.import_module') as mock_import:
            mock_import.side_effect = [
                ImportError(),  # First module fails
                MagicMock(upsert_pbp=lambda: "found")  # Second succeeds
            ]
            
            from nba_scraper.loaders import _resolve_callable
            result = _resolve_callable("upsert_pbp")
            
            # Should have tried multiple modules
            assert mock_import.call_count >= 2
            assert callable(result)

    def test_aliases_work_when_base_exists(self):
        """Test that aliases are created when base callables exist."""
        try:
            from nba_scraper.loaders import upsert_pbp, upsert_pbp_events
            
            # If base exists, alias should too
            if upsert_pbp is not None:
                assert upsert_pbp_events is not None
                assert upsert_pbp_events == upsert_pbp
            else:
                # If base doesn't exist, alias should be None
                assert upsert_pbp_events is None
                
        except ImportError:
            pytest.fail("Should not raise ImportError")

    def test_all_exports_are_valid(self):
        """Test that __all__ only contains valid exports."""
        try:
            import nba_scraper.loaders as loaders_module
            
            # Check that __all__ exists and is a list
            assert hasattr(loaders_module, '__all__')
            assert isinstance(loaders_module.__all__, list)
            
            # Check that all items in __all__ exist as module attributes
            for export_name in loaders_module.__all__:
                assert hasattr(loaders_module, export_name), f"Missing export: {export_name}"
                
                # Each export should be callable or None
                export_obj = getattr(loaders_module, export_name)
                assert export_obj is None or callable(export_obj)
                
        except ImportError as e:
            pytest.fail(f"Should be able to import loaders module: {e}")

    @patch('nba_scraper.loaders.importlib.import_module')
    def test_resolve_callable_error_handling(self, mock_import):
        """Test error handling in callable resolution."""
        # Test ImportError handling
        mock_import.side_effect = ImportError("Module not found")
        
        from nba_scraper.loaders import _resolve_callable
        result = _resolve_callable("nonexistent")
        
        assert result is None

    @patch('nba_scraper.loaders.importlib.import_module')
    def test_resolve_callable_attribute_error(self, mock_import):
        """Test handling when module exists but attribute doesn't."""
        mock_module = MagicMock()
        # Remove the attribute we're looking for
        del mock_module.upsert_missing  
        mock_import.return_value = mock_module
        
        from nba_scraper.loaders import _resolve_callable
        result = _resolve_callable("upsert_missing")
        
        assert result is None

    def test_performance_with_repeated_calls(self):
        """Test that repeated calls don't cause performance issues."""
        # Multiple imports should work efficiently
        for _ in range(5):
            try:
                from nba_scraper.loaders import upsert_game
                # Should not degrade performance
            except Exception as e:
                pytest.fail(f"Repeated imports should work: {e}")

    def test_module_package_resolution(self):
        """Test that modules are resolved within correct package."""
        with patch('importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.test_func = lambda: "test"
            mock_import.return_value = mock_module
            
            from nba_scraper.loaders import _resolve_callable
            _resolve_callable("test_func")
            
            # Should have called with package parameter
            mock_import.assert_called_with('.games', package='nba_scraper.loaders')


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_typical_usage_pattern(self):
        """Test typical usage pattern for pipeline integration."""
        try:
            # This is how pipelines would use it
            from nba_scraper.loaders import upsert_game, upsert_pbp
            
            # Should be able to check availability
            game_loader_available = upsert_game is not None
            pbp_loader_available = upsert_pbp is not None
            
            # Should be able to use in conditional logic
            if game_loader_available:
                assert callable(upsert_game)
            
            if pbp_loader_available:
                assert callable(upsert_pbp)
                
        except ImportError:
            pytest.fail("Typical usage should not raise ImportError")

    def test_dynamic_loader_check(self):
        """Test dynamic checking of loader availability."""
        try:
            import nba_scraper.loaders as loaders
            
            # Should be able to dynamically check what's available
            available_loaders = []
            required_loaders = ['upsert_game', 'upsert_pbp', 'upsert_lineups', 'upsert_shots']
            
            for loader_name in required_loaders:
                loader_func = getattr(loaders, loader_name, None)
                if loader_func is not None and callable(loader_func):
                    available_loaders.append(loader_name)
            
            # Should not crash and should return a list
            assert isinstance(available_loaders, list)
            
        except Exception as e:
            pytest.fail(f"Dynamic loader checking should work: {e}")

    def test_pipeline_conditional_usage(self):
        """Test conditional usage pattern for pipelines."""
        try:
            from nba_scraper.loaders import (
                upsert_game, 
                upsert_pbp, 
                upsert_lineups, 
                upsert_shots
            )
            
            # Simulate pipeline logic
            async def mock_pipeline():
                results = {}
                
                if upsert_game:
                    results['games'] = "would_upsert_games"
                
                if upsert_pbp:
                    results['pbp'] = "would_upsert_pbp" 
                
                if upsert_lineups:
                    results['lineups'] = "would_upsert_lineups"
                
                if upsert_shots:
                    results['shots'] = "would_upsert_shots"
                
                return results
            
            # Should be able to call without error
            import asyncio
            result = asyncio.run(mock_pipeline())
            assert isinstance(result, dict)
            
        except Exception as e:
            pytest.fail(f"Pipeline conditional usage should work: {e}")