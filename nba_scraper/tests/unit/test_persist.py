"""Unit tests for raw_io.persist module - JSON writing, compression, and manifest management."""

import pytest
import json
import gzip
import hashlib
from pathlib import Path
from unittest.mock import patch, mock_open
import tempfile
import shutil

from src.nba_scraper.raw_io.persist import (
    write_json, 
    update_manifest, 
    read_manifest,
    append_quarantine,
    ensure_dir
)


class TestWriteJson:
    """Test JSON writing functionality with compression and hashing."""
    
    def test_write_small_json_no_gzip(self):
        """Test writing small JSON file without gzip compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = Path(tmpdir) / "test.json"
            test_data = {"test": "data", "number": 42}
            
            result = write_json(test_path, test_data, gzip_if_big=True)
            
            # Verify file was written
            assert test_path.exists()
            
            # Verify content is pretty JSON
            content = test_path.read_text(encoding='utf-8')
            loaded_data = json.loads(content)
            assert loaded_data == test_data
            assert '\n' in content  # Pretty printed
            assert '  ' in content  # Indented
            
            # Verify return metadata
            assert result["bytes"] > 0
            assert result["gz"] is False  # Small file, no gzip
            assert len(result["sha1"]) == 40  # SHA1 hash length
            
            # Verify SHA1 is correct
            expected_sha1 = hashlib.sha1(content.encode('utf-8')).hexdigest()
            assert result["sha1"] == expected_sha1
            
            # Verify no .gz file was created
            gz_path = test_path.with_suffix('.json.gz')
            assert not gz_path.exists()
    
    def test_write_large_json_with_gzip(self):
        """Test writing large JSON file with gzip compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = Path(tmpdir) / "large.json"
            
            # Create data that will be > 1MB when serialized
            large_data = {
                "items": [{"id": i, "data": "x" * 1000} for i in range(1500)]
            }
            
            result = write_json(test_path, large_data, gzip_if_big=True)
            
            # Verify file was written
            assert test_path.exists()
            
            # Verify content
            content = test_path.read_text(encoding='utf-8')
            loaded_data = json.loads(content)
            assert loaded_data == large_data
            
            # Verify return metadata
            assert result["bytes"] > 1024 * 1024  # > 1MB
            assert result["gz"] is True  # Large file, gzip enabled
            assert len(result["sha1"]) == 40
            
            # Verify .gz file was created
            gz_path = test_path.with_suffix('.json.gz')
            assert gz_path.exists()
            
            # Verify .gz content is identical
            with gzip.open(gz_path, 'rt', encoding='utf-8') as gz_file:
                gz_content = gz_file.read()
                gz_data = json.loads(gz_content)
                assert gz_data == large_data
    
    def test_write_json_gzip_disabled(self):
        """Test writing large JSON with gzip disabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = Path(tmpdir) / "large_no_gz.json"
            large_data = {"items": [{"id": i, "data": "x" * 1000} for i in range(1500)]}
            
            result = write_json(test_path, large_data, gzip_if_big=False)
            
            # Should not create .gz file even for large files
            assert result["gz"] is False
            gz_path = test_path.with_suffix('.json.gz')
            assert not gz_path.exists()
    
    def test_write_json_creates_parent_dirs(self):
        """Test that write_json creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = Path(tmpdir) / "deep" / "nested" / "path" / "test.json"
            test_data = {"nested": True}
            
            result = write_json(nested_path, test_data)
            
            assert nested_path.exists()
            assert nested_path.parent.exists()
            loaded_data = json.loads(nested_path.read_text())
            assert loaded_data == test_data


class TestManifestOperations:
    """Test manifest reading, writing, and updating."""
    
    def test_update_manifest_new_file(self):
        """Test creating new manifest file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            date_dir = Path(tmpdir) / "2023-10-27"
            date_dir.mkdir()
            
            game_record = {
                "game_id": "0022300001",
                "teams": {"home_team_id": 1610612744, "visitor_team_id": 1610612738},
                "endpoints": {
                    "boxscoresummaryv2": {"bytes": 1500, "sha1": "abc123", "ok": True},
                    "playbyplayv2": {"bytes": 2500, "sha1": "def456", "ok": True}
                },
                "errors": []
            }
            
            update_manifest(date_dir, game_record)
            
            # Verify manifest was created
            manifest_path = date_dir / "manifest.json"
            assert manifest_path.exists()
            
            # Verify content
            manifest = json.loads(manifest_path.read_text())
            assert manifest["date"] == "2023-10-27"
            assert len(manifest["games"]) == 1
            assert manifest["games"][0]["game_id"] == "0022300001"
            
            # Verify summary
            summary = manifest["summary"]
            assert summary["games"] == 1
            assert summary["ok_games"] == 1
            assert summary["failed_games"] == 0
            assert summary["total_bytes"] == 4000  # 1500 + 2500
    
    def test_update_manifest_existing_file(self):
        """Test updating existing manifest with new game."""
        with tempfile.TemporaryDirectory() as tmpdir:
            date_dir = Path(tmpdir) / "2023-10-27"
            date_dir.mkdir()
            
            # Create initial manifest
            initial_record = {
                "game_id": "0022300001",
                "teams": {"home_team_id": 1610612744, "visitor_team_id": 1610612738},
                "endpoints": {"boxscoresummaryv2": {"bytes": 1000, "sha1": "abc", "ok": True}},
                "errors": []
            }
            update_manifest(date_dir, initial_record)
            
            # Add second game
            second_record = {
                "game_id": "0022300002", 
                "teams": {"home_team_id": 1610612739, "visitor_team_id": 1610612740},
                "endpoints": {"playbyplayv2": {"bytes": 2000, "sha1": "def", "ok": True}},
                "errors": []
            }
            update_manifest(date_dir, second_record)
            
            # Verify updated manifest
            manifest_path = date_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            
            assert len(manifest["games"]) == 2
            assert manifest["summary"]["games"] == 2
            assert manifest["summary"]["total_bytes"] == 3000
    
    def test_update_manifest_merge_game_record(self):
        """Test updating existing game record with new endpoints."""
        with tempfile.TemporaryDirectory() as tmpdir:
            date_dir = Path(tmpdir) / "2023-10-27"
            date_dir.mkdir()
            
            # Initial record with one endpoint
            initial_record = {
                "game_id": "0022300001",
                "teams": {"home_team_id": 1610612744, "visitor_team_id": 1610612738},
                "endpoints": {"boxscoresummaryv2": {"bytes": 1000, "sha1": "abc", "ok": True}},
                "errors": []
            }
            update_manifest(date_dir, initial_record)
            
            # Update same game with additional endpoint
            update_record = {
                "game_id": "0022300001",
                "endpoints": {"playbyplayv2": {"bytes": 2000, "sha1": "def", "ok": True}},
                "errors": [{"endpoint": "shotchartdetail", "error": "API timeout"}]
            }
            update_manifest(date_dir, update_record)
            
            # Verify merged record
            manifest_path = date_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            
            assert len(manifest["games"]) == 1
            game = manifest["games"][0]
            
            # Should have both endpoints
            assert "boxscoresummaryv2" in game["endpoints"]
            assert "playbyplayv2" in game["endpoints"]
            
            # Should have merged errors
            assert len(game["errors"]) == 1
            assert game["errors"][0]["endpoint"] == "shotchartdetail"
    
    def test_read_manifest_nonexistent(self):
        """Test reading nonexistent manifest returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            date_dir = Path(tmpdir) / "nonexistent"
            
            result = read_manifest(date_dir)
            assert result is None
    
    def test_read_manifest_invalid_json(self):
        """Test reading corrupted manifest returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            date_dir = Path(tmpdir) / "2023-10-27"
            date_dir.mkdir()
            
            # Write invalid JSON
            manifest_path = date_dir / "manifest.json"
            manifest_path.write_text("invalid json content")
            
            result = read_manifest(date_dir)
            assert result is None


class TestQuarantineOperations:
    """Test quarantine file operations."""
    
    def test_append_quarantine_new_file(self):
        """Test creating new quarantine file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ops_dir = Path(tmpdir) / "ops"
            quarantine_file = ops_dir / "quarantine_game_ids.txt"
            
            append_quarantine("0022300001", "playbyplayv2", "API timeout", quarantine_file)
            
            assert quarantine_file.exists()
            content = quarantine_file.read_text()
            
            assert "0022300001" in content
            assert "playbyplayv2" in content
            assert "API timeout" in content
            # Should have ISO timestamp
            assert "T" in content and ":" in content
    
    def test_append_quarantine_existing_file(self):
        """Test appending to existing quarantine file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ops_dir = Path(tmpdir) / "ops"
            ops_dir.mkdir()
            quarantine_file = ops_dir / "quarantine_game_ids.txt"
            
            # Add first entry
            append_quarantine("0022300001", "playbyplayv2", "Timeout", quarantine_file)
            
            # Add second entry
            append_quarantine("0022300002", "shotchartdetail", "Rate limited", quarantine_file)
            
            content = quarantine_file.read_text()
            lines = content.strip().split('\n')
            
            assert len(lines) == 2
            assert "0022300001" in lines[0]
            assert "0022300002" in lines[1]


class TestEnsureDir:
    """Test directory creation utility."""
    
    def test_ensure_dir_creates_nested_dirs(self):
        """Test creating nested directory structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = Path(tmpdir) / "deep" / "nested" / "structure"
            
            ensure_dir(nested_path)
            
            assert nested_path.exists()
            assert nested_path.is_dir()
    
    def test_ensure_dir_existing_dir(self):
        """Test ensure_dir with existing directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            existing_path = Path(tmpdir) / "existing"
            existing_path.mkdir()
            
            # Should not raise error
            ensure_dir(existing_path)
            assert existing_path.exists()