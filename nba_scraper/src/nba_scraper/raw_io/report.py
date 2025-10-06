"""Reporting utilities for raw NBA data harvest summaries and analysis."""

from pathlib import Path
from typing import Dict, Any, List, Optional
import json

from .persist import read_manifest
from ..nba_logging import get_logger

logger = get_logger(__name__)


def summarize_date(date_dir: Path) -> Dict[str, Any]:
    """Read manifest.json and compute comprehensive harvest summary.
    
    Args:
        date_dir: Directory path for the date (e.g., raw/2023-10-27/)
        
    Returns:
        Dictionary with harvest summary:
        {
            "date": "YYYY-MM-DD",
            "games": int,
            "endpoints_per_game": float,
            "total_bytes": int,
            "total_bytes_mb": float,
            "successful_games": int,
            "failed_games": int,
            "quarantined_ids": List[str],
            "endpoint_stats": {
                "boxscoresummaryv2": {"ok": int, "failed": int},
                "boxscoretraditionalv2": {"ok": int, "failed": int},
                "playbyplayv2": {"ok": int, "failed": int},
                "shotchartdetail": {"ok": int, "failed": int}
            },
            "common_errors": List[str],
            "success_rate": float
        }
    """
    try:
        manifest = read_manifest(date_dir)
        
        if not manifest:
            logger.warning("No manifest found for date", date_dir=str(date_dir))
            return {
                "date": date_dir.name,
                "games": 0,
                "endpoints_per_game": 0.0,
                "total_bytes": 0,
                "total_bytes_mb": 0.0,
                "successful_games": 0,
                "failed_games": 0,
                "quarantined_ids": [],
                "endpoint_stats": {},
                "common_errors": [],
                "success_rate": 0.0
            }
        
        games = manifest.get("games", [])
        date_str = manifest.get("date", date_dir.name)
        
        # Basic stats
        total_games = len(games)
        total_bytes = 0
        successful_games = 0
        failed_games = 0
        quarantined_ids = []
        
        # Endpoint statistics
        endpoint_stats = {
            "boxscoresummaryv2": {"ok": 0, "failed": 0},
            "boxscoretraditionalv2": {"ok": 0, "failed": 0}, 
            "playbyplayv2": {"ok": 0, "failed": 0},
            "shotchartdetail": {"ok": 0, "failed": 0}
        }
        
        # Error tracking
        all_errors = []
        
        # Process each game
        for game in games:
            game_id = game.get("game_id", "")
            endpoints = game.get("endpoints", {})
            errors = game.get("errors", [])
            
            # Count bytes
            for endpoint_data in endpoints.values():
                if isinstance(endpoint_data, dict) and "bytes" in endpoint_data:
                    total_bytes += endpoint_data["bytes"]
            
            # Track endpoint success/failure
            for endpoint_name, endpoint_data in endpoints.items():
                if endpoint_name in endpoint_stats:
                    if isinstance(endpoint_data, dict) and endpoint_data.get("ok", False):
                        endpoint_stats[endpoint_name]["ok"] += 1
                    else:
                        endpoint_stats[endpoint_name]["failed"] += 1
            
            # Collect errors
            for error in errors:
                if isinstance(error, dict) and "error" in error:
                    all_errors.append(error["error"])
            
            # Determine if game was successful
            successful_endpoints = sum(1 for ep in endpoints.values() 
                                     if isinstance(ep, dict) and ep.get("ok", False))
            
            if successful_endpoints >= 2 and not errors:
                successful_games += 1
            else:
                failed_games += 1
                quarantined_ids.append(game_id)
        
        # Calculate derived metrics
        endpoints_per_game = 0.0
        if total_games > 0:
            total_successful_endpoints = sum(stats["ok"] for stats in endpoint_stats.values())
            endpoints_per_game = total_successful_endpoints / total_games
        
        total_bytes_mb = total_bytes / (1024 * 1024)
        success_rate = (successful_games / total_games) if total_games > 0 else 0.0
        
        # Find most common errors
        error_counts = {}
        for error in all_errors:
            # Normalize error messages for grouping
            normalized_error = _normalize_error_message(error)
            error_counts[normalized_error] = error_counts.get(normalized_error, 0) + 1
        
        # Sort by frequency and take top 5
        common_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        common_errors = [f"{error} ({count}x)" for error, count in common_errors]
        
        summary = {
            "date": date_str,
            "games": total_games,
            "endpoints_per_game": round(endpoints_per_game, 2),
            "total_bytes": total_bytes,
            "total_bytes_mb": round(total_bytes_mb, 2),
            "successful_games": successful_games,
            "failed_games": failed_games,
            "quarantined_ids": quarantined_ids,
            "endpoint_stats": endpoint_stats,
            "common_errors": common_errors,
            "success_rate": round(success_rate, 3)
        }
        
        logger.debug("Generated date summary", 
                    date=date_str,
                    games=total_games,
                    success_rate=success_rate,
                    total_mb=round(total_bytes_mb, 1))
        
        return summary
        
    except Exception as e:
        logger.error("Failed to summarize date", date_dir=str(date_dir), error=str(e))
        raise


def _normalize_error_message(error: str) -> str:
    """Normalize error message for grouping similar errors.
    
    Args:
        error: Raw error message
        
    Returns:
        Normalized error message for grouping
    """
    # Convert to lowercase and remove specific identifiers
    normalized = error.lower()
    
    # Remove specific game IDs, timestamps, URLs
    import re
    normalized = re.sub(r'\b00\d{8}\b', '[GAME_ID]', normalized)  # NBA game ID pattern
    normalized = re.sub(r'\d{4}-\d{2}-\d{2}', '[DATE]', normalized)  # Date pattern
    normalized = re.sub(r'https?://[^\s]+', '[URL]', normalized)  # URL pattern
    normalized = re.sub(r'\b\d+\.\d+\.\d+\.\d+\b', '[IP]', normalized)  # IP pattern
    
    # Truncate very long messages
    if len(normalized) > 100:
        normalized = normalized[:100] + "..."
    
    return normalized.strip()


def summarize_season(season_root: Path, season: str) -> Dict[str, Any]:
    """Summarize harvest results for an entire season.
    
    Args:
        season_root: Root directory containing date subdirectories
        season: Season string (e.g., "2023-24")
        
    Returns:
        Dictionary with season-wide summary
    """
    try:
        season_summary = {
            "season": season,
            "dates_processed": 0,
            "total_games": 0,
            "total_bytes": 0,
            "total_bytes_gb": 0.0,
            "successful_games": 0,
            "failed_games": 0,
            "overall_success_rate": 0.0,
            "endpoint_totals": {
                "boxscoresummaryv2": {"ok": 0, "failed": 0},
                "boxscoretraditionalv2": {"ok": 0, "failed": 0},
                "playbyplayv2": {"ok": 0, "failed": 0},
                "shotchartdetail": {"ok": 0, "failed": 0}
            },
            "date_summaries": []
        }
        
        # Find all date directories
        date_dirs = []
        if season_root.exists():
            for item in season_root.iterdir():
                if item.is_dir() and _is_valid_date_dir(item.name):
                    date_dirs.append(item)
        
        date_dirs.sort()  # Process chronologically
        
        for date_dir in date_dirs:
            try:
                date_summary = summarize_date(date_dir)
                season_summary["date_summaries"].append(date_summary)
                
                # Aggregate stats
                season_summary["dates_processed"] += 1
                season_summary["total_games"] += date_summary["games"]
                season_summary["total_bytes"] += date_summary["total_bytes"]
                season_summary["successful_games"] += date_summary["successful_games"]
                season_summary["failed_games"] += date_summary["failed_games"]
                
                # Aggregate endpoint stats
                for endpoint, stats in date_summary["endpoint_stats"].items():
                    if endpoint in season_summary["endpoint_totals"]:
                        season_summary["endpoint_totals"][endpoint]["ok"] += stats["ok"]
                        season_summary["endpoint_totals"][endpoint]["failed"] += stats["failed"]
                
            except Exception as e:
                logger.warning("Failed to summarize date for season", 
                              date_dir=str(date_dir), error=str(e))
        
        # Calculate derived metrics
        season_summary["total_bytes_gb"] = round(season_summary["total_bytes"] / (1024**3), 2)
        
        if season_summary["total_games"] > 0:
            season_summary["overall_success_rate"] = round(
                season_summary["successful_games"] / season_summary["total_games"], 3
            )
        
        logger.info("Generated season summary", 
                   season=season,
                   dates=season_summary["dates_processed"],
                   games=season_summary["total_games"],
                   success_rate=season_summary["overall_success_rate"])
        
        return season_summary
        
    except Exception as e:
        logger.error("Failed to summarize season", season=season, error=str(e))
        raise


def _is_valid_date_dir(dir_name: str) -> bool:
    """Check if directory name is a valid date format (YYYY-MM-DD).
    
    Args:
        dir_name: Directory name to validate
        
    Returns:
        True if valid date format
    """
    try:
        from datetime import datetime
        datetime.strptime(dir_name, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def format_summary_for_display(summary: Dict[str, Any]) -> str:
    """Format summary dictionary for pretty console display.
    
    Args:
        summary: Summary dictionary from summarize_date or summarize_season
        
    Returns:
        Formatted string for console output
    """
    if "season" in summary:
        # Season summary
        lines = [
            f"🏀 Season {summary['season']} Harvest Summary",
            "=" * 50,
            f"📅 Dates processed: {summary['dates_processed']}",
            f"🎮 Total games: {summary['total_games']}",
            f"✅ Successful: {summary['successful_games']} ({summary['overall_success_rate']:.1%})",
            f"❌ Failed: {summary['failed_games']}",
            f"💾 Total data: {summary['total_bytes_gb']:.2f} GB",
            "",
            "📊 Endpoint Success Rates:",
        ]
        
        for endpoint, stats in summary['endpoint_totals'].items():
            total = stats['ok'] + stats['failed']
            if total > 0:
                success_rate = stats['ok'] / total
                lines.append(f"  • {endpoint}: {stats['ok']}/{total} ({success_rate:.1%})")
        
    else:
        # Date summary
        lines = [
            f"📅 {summary['date']} Harvest Summary",
            "=" * 40,
            f"🎮 Games: {summary['games']}",
            f"✅ Successful: {summary['successful_games']} ({summary['success_rate']:.1%})",
            f"❌ Failed: {summary['failed_games']}",
            f"💾 Data: {summary['total_bytes_mb']:.1f} MB",
            f"📡 Avg endpoints/game: {summary['endpoints_per_game']:.1f}",
        ]
        
        if summary['quarantined_ids']:
            lines.extend([
                "",
                f"⚠️  Quarantined games: {len(summary['quarantined_ids'])}",
                f"   {', '.join(summary['quarantined_ids'][:5])}{'...' if len(summary['quarantined_ids']) > 5 else ''}"
            ])
        
        if summary['common_errors']:
            lines.extend([
                "",
                "🚨 Common errors:",
                *[f"   • {error}" for error in summary['common_errors'][:3]]
            ])
    
    return "\n".join(lines)