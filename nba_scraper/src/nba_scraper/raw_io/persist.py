"""Persistence utilities for raw NBA data with JSON writing, compression, and manifest management."""

import json
import gzip
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, UTC

from ..nba_logging import get_logger

logger = get_logger(__name__)


def ensure_dir(path: Path) -> None:
    """Ensure directory exists, creating parent directories as needed.
    
    Args:
        path: Directory path to create
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
        logger.debug("Directory ensured", path=str(path))
    except Exception as e:
        logger.error("Failed to create directory", path=str(path), error=str(e))
        raise


def write_json(path: Path, payload: Dict[str, Any], gzip_if_big: bool = True) -> Dict[str, Any]:
    """Write pretty JSON to file with optional gzip compression for large files.
    
    Args:
        path: File path to write JSON to
        payload: Dictionary data to write
        gzip_if_big: If True, also write .json.gz for files > 1MB
        
    Returns:
        Dictionary with metadata: {"bytes": int, "gz": bool, "sha1": str}
    """
    try:
        # Ensure parent directory exists
        ensure_dir(path.parent)
        
        # Write pretty JSON
        json_content = json.dumps(payload, indent=2, ensure_ascii=False)
        json_bytes = json_content.encode('utf-8')
        
        path.write_bytes(json_bytes)
        
        # Calculate SHA1 hash
        sha1_hash = hashlib.sha1(json_bytes).hexdigest()
        
        # Determine if we should gzip
        should_gzip = gzip_if_big and len(json_bytes) > 1024 * 1024  # 1MB threshold
        gz_written = False
        
        if should_gzip:
            # Write compressed version
            gz_path = path.with_suffix('.json.gz')
            with gzip.open(gz_path, 'wb') as gz_file:
                gz_file.write(json_bytes)
            gz_written = True
            logger.debug("Wrote compressed JSON", 
                        path=str(path), 
                        gz_path=str(gz_path),
                        original_size=len(json_bytes),
                        compressed_size=gz_path.stat().st_size)
        
        logger.debug("Wrote JSON file", 
                    path=str(path), 
                    size=len(json_bytes),
                    gzipped=gz_written)
        
        return {
            "bytes": len(json_bytes),
            "gz": gz_written,
            "sha1": sha1_hash
        }
        
    except Exception as e:
        logger.error("Failed to write JSON file", path=str(path), error=str(e))
        raise


def update_manifest(date_dir: Path, record: Dict[str, Any]) -> None:
    """Update or create manifest.json file with game record.
    
    The manifest tracks all games processed for a date with endpoint results and errors.
    
    Args:
        date_dir: Directory path for the date (e.g., raw/2023-10-27/)
        record: Game record to add/update in manifest
        
    Manifest structure:
    {
      "date": "YYYY-MM-DD",
      "games": [
        {
          "game_id": "...",
          "teams": {"home_team_id": int, "visitor_team_id": int},
          "endpoints": {
            "scoreboard": {"bytes": int, "sha1": "...", "ok": bool},
            "boxscoresummaryv2": {...},
            "boxscoretraditionalv2": {...},
            "playbyplayv2": {...},
            "shotchartdetail": {...}
          },
          "errors": [ {"endpoint": "playbyplayv2", "error": "..."}, ... ]
        }, ...
      ],
      "summary": {"games": int, "ok_games": int, "failed_games": int, "total_bytes": int}
    }
    """
    try:
        manifest_path = date_dir / "manifest.json"
        
        # Load existing manifest or create new one
        if manifest_path.exists():
            try:
                with open(manifest_path, 'r', encoding='utf-8') as f:
                    manifest = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning("Failed to load existing manifest, creating new one", 
                              path=str(manifest_path), error=str(e))
                manifest = {}
        else:
            manifest = {}
        
        # Initialize manifest structure if needed
        if "date" not in manifest:
            manifest["date"] = date_dir.name
        if "games" not in manifest:
            manifest["games"] = []
        
        # Find existing game record or add new one
        game_id = record.get("game_id")
        if not game_id:
            logger.error("No game_id in record for manifest update")
            return
        
        # Update or append game record
        existing_game_idx = None
        for i, game in enumerate(manifest["games"]):
            if game.get("game_id") == game_id:
                existing_game_idx = i
                break
        
        if existing_game_idx is not None:
            # Update existing record (merge endpoints and errors)
            existing_game = manifest["games"][existing_game_idx]
            
            # Merge endpoints
            if "endpoints" not in existing_game:
                existing_game["endpoints"] = {}
            if "endpoints" in record:
                existing_game["endpoints"].update(record["endpoints"])
            
            # Merge errors
            if "errors" not in existing_game:
                existing_game["errors"] = []
            if "errors" in record:
                existing_game["errors"].extend(record["errors"])
            
            # Update other fields
            for key in ["teams"]:
                if key in record:
                    existing_game[key] = record[key]
                    
        else:
            # Add new game record
            manifest["games"].append(record)
        
        # Recalculate summary
        total_games = len(manifest["games"])
        ok_games = 0
        failed_games = 0
        total_bytes = 0
        
        for game in manifest["games"]:
            endpoints = game.get("endpoints", {})
            errors = game.get("errors", [])
            
            # Count bytes from all endpoints
            for endpoint_data in endpoints.values():
                if isinstance(endpoint_data, dict) and "bytes" in endpoint_data:
                    total_bytes += endpoint_data["bytes"]
            
            # Game is OK if it has no errors and at least one successful endpoint
            has_successful_endpoint = any(
                ep.get("ok", False) for ep in endpoints.values() 
                if isinstance(ep, dict)
            )
            
            if errors or not has_successful_endpoint:
                failed_games += 1
            else:
                ok_games += 1
        
        manifest["summary"] = {
            "games": total_games,
            "ok_games": ok_games,
            "failed_games": failed_games,
            "total_bytes": total_bytes
        }
        
        # Write updated manifest
        manifest_content = json.dumps(manifest, indent=2, ensure_ascii=False)
        manifest_path.write_text(manifest_content, encoding='utf-8')
        
        logger.debug("Updated manifest", 
                    path=str(manifest_path),
                    game_id=game_id,
                    total_games=total_games,
                    total_bytes=total_bytes)
        
    except Exception as e:
        logger.error("Failed to update manifest", 
                    date_dir=str(date_dir), 
                    game_id=record.get("game_id"),
                    error=str(e))
        raise


def read_manifest(date_dir: Path) -> Optional[Dict[str, Any]]:
    """Read manifest.json file for a date directory.
    
    Args:
        date_dir: Directory path for the date
        
    Returns:
        Manifest dictionary or None if not found/invalid
    """
    try:
        manifest_path = date_dir / "manifest.json"
        
        if not manifest_path.exists():
            return None
        
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        logger.debug("Read manifest", path=str(manifest_path))
        return manifest
        
    except Exception as e:
        logger.warning("Failed to read manifest", 
                      path=str(date_dir / "manifest.json"),
                      error=str(e))
        return None


def append_quarantine(game_id: str, endpoint: str, error: str, quarantine_file: Path = None) -> None:
    """Append failed game_id to quarantine file.
    
    Args:
        game_id: NBA game ID that failed
        endpoint: Endpoint that failed
        error: Error message
        quarantine_file: Path to quarantine file (defaults to ./ops/quarantine_game_ids.txt)
    """
    try:
        if quarantine_file is None:
            quarantine_file = Path("./ops/quarantine_game_ids.txt")
        
        # Ensure ops directory exists
        ensure_dir(quarantine_file.parent)
        
        # Format quarantine entry with timestamp
        timestamp = datetime.now(UTC).isoformat()
        quarantine_entry = f"{timestamp} {game_id} {endpoint} {error}\n"
        
        # Append to quarantine file
        with open(quarantine_file, 'a', encoding='utf-8') as f:
            f.write(quarantine_entry)
        
        logger.warning("Added to quarantine", 
                      game_id=game_id,
                      endpoint=endpoint,
                      error=error,
                      quarantine_file=str(quarantine_file))
        
    except Exception as e:
        logger.error("Failed to write to quarantine file", 
                    game_id=game_id,
                    endpoint=endpoint,
                    quarantine_file=str(quarantine_file),
                    error=str(e))