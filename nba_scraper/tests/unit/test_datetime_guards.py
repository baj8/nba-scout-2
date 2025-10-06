"""Unit tests to prevent datetime regressions."""

import ast
import importlib
import warnings
from pathlib import Path


def test_no_datetime_utcnow_usage():
    """Assert that datetime.utcnow is not used in core modules."""
    # Core modules to check for datetime.utcnow usage
    core_modules = [
        "nba_scraper.nba_logging",
        "nba_scraper.transformers.games", 
        "nba_scraper.pipelines.foundation",
        "nba_scraper.utils.preprocess",
    ]
    
    src_path = Path(__file__).parent.parent.parent / "src"
    
    for module_name in core_modules:
        module_file = src_path / module_name.replace(".", "/") / "__init__.py"
        if not module_file.exists():
            # Try direct module file
            module_file = src_path / (module_name.replace(".", "/") + ".py")
        
        if module_file.exists():
            content = module_file.read_text()
            
            # Parse the AST to find datetime.utcnow calls
            try:
                tree = ast.parse(content)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Attribute):
                        if (isinstance(node.value, ast.Attribute) and 
                            isinstance(node.value.value, ast.Name) and
                            node.value.value.id == "datetime" and
                            node.value.attr == "datetime" and
                            node.attr == "utcnow"):
                            assert False, f"Found datetime.datetime.utcnow in {module_name}"
                        elif (isinstance(node.value, ast.Name) and
                              node.value.id == "datetime" and
                              node.attr == "utcnow"):
                            assert False, f"Found datetime.utcnow in {module_name}"
            except SyntaxError:
                # If we can't parse the file, at least check string content
                assert "datetime.utcnow" not in content, f"Found datetime.utcnow string in {module_name}"
                assert ".utcnow(" not in content, f"Found .utcnow( call in {module_name}"


def test_nba_logging_no_deprecation_warnings():
    """Test that importing nba_logging doesn't emit deprecation warnings."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")  # Catch all warnings
        
        # Import the logging module
        import nba_scraper.nba_logging
        
        # Check for deprecation warnings from our code
        our_deprecations = [
            warning for warning in w 
            if issubclass(warning.category, DeprecationWarning) and
            "nba_scraper" in str(warning.filename)
        ]
        
        assert len(our_deprecations) == 0, f"Found deprecation warnings in nba_logging: {our_deprecations}"


def test_core_modules_importable():
    """Test that core modules can be imported without errors."""
    core_modules = [
        "nba_scraper.nba_logging",
        "nba_scraper.transformers.games",
        "nba_scraper.pipelines.foundation", 
        "nba_scraper.utils.preprocess",
        "nba_scraper.models.games",
    ]
    
    for module_name in core_modules:
        try:
            importlib.import_module(module_name)
        except ImportError as e:
            assert False, f"Failed to import {module_name}: {e}"