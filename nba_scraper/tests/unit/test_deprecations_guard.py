"""Regression tests to prevent reintroduction of deprecated patterns in our codebase."""

import os
import re
from pathlib import Path
import pytest


def get_source_files() -> list[Path]:
    """Get all Python files in our source package."""
    src_dir = Path(__file__).parent.parent.parent / "src" / "nba_scraper"
    if not src_dir.exists():
        pytest.fail(f"Source directory not found: {src_dir}")
    
    python_files = []
    for root, dirs, files in os.walk(src_dir):
        # Skip __pycache__ directories
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        
        for file in files:
            if file.endswith(".py"):
                python_files.append(Path(root) / file)
    
    return python_files


def test_no_deprecated_datetime_utcnow():
    """Ensure our code doesn't use the deprecated datetime.utcnow() method."""
    source_files = get_source_files()
    violations = []
    
    # Pattern to match datetime.utcnow() calls
    utcnow_pattern = re.compile(r'datetime\.utcnow\(')
    
    for file_path in source_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check for deprecated datetime.utcnow() usage
            matches = utcnow_pattern.findall(content)
            if matches:
                violations.append(f"{file_path}: Found {len(matches)} datetime.utcnow() call(s)")
        
        except (UnicodeDecodeError, PermissionError) as e:
            # Skip files that can't be read
            continue
    
    if violations:
        violation_msg = "\n".join(violations)
        pytest.fail(
            f"Found deprecated datetime.utcnow() usage in our code:\n{violation_msg}\n\n"
            "Replace with: datetime.now(datetime.UTC) or datetime.now(UTC) if UTC is imported"
        )


def test_no_deprecated_pydantic_field_constraints():
    """Ensure our code doesn't use deprecated Pydantic min_items/max_items Field constraints."""
    source_files = get_source_files()
    violations = []
    
    # Patterns to match deprecated Pydantic Field constraints
    min_items_pattern = re.compile(r'min_items\s*=')
    max_items_pattern = re.compile(r'max_items\s*=')
    
    for file_path in source_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check for deprecated min_items/max_items usage
            min_items_matches = min_items_pattern.findall(content)
            max_items_matches = max_items_pattern.findall(content)
            
            if min_items_matches:
                violations.append(f"{file_path}: Found {len(min_items_matches)} min_items usage(s)")
            if max_items_matches:
                violations.append(f"{file_path}: Found {len(max_items_matches)} max_items usage(s)")
        
        except (UnicodeDecodeError, PermissionError) as e:
            # Skip files that can't be read
            continue
    
    if violations:
        violation_msg = "\n".join(violations)
        pytest.fail(
            f"Found deprecated Pydantic Field constraints in our code:\n{violation_msg}\n\n"
            "Replace min_items= with min_length= and max_items= with max_length= for sequence/dict fields"
        )


def test_source_directory_exists_and_has_files():
    """Sanity check that we're actually scanning files."""
    source_files = get_source_files()
    
    assert len(source_files) > 0, "No Python files found in source directory - test setup may be wrong"
    
    # Verify we're scanning the right directory structure
    src_dir = Path(__file__).parent.parent.parent / "src" / "nba_scraper"
    assert src_dir.exists(), f"Source directory does not exist: {src_dir}"
    
    # Should find at least some core files
    file_names = [f.name for f in source_files]
    assert any("__init__.py" in name for name in file_names), "Should find at least one __init__.py file"