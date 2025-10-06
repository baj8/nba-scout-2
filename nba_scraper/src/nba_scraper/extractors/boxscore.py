"""Extractors for NBA boxscore data."""

from typing import Dict, Any, Optional, List
from datetime import datetime
import re

from ..nba_logging import get_logger
from ..utils.season import derive_season_smart

logger = get_logger(__name__)


def _find_resultset(response: dict, name: str) -> Optional[dict]:
    """Find a resultSet by name in the response."""
    if not response or "resultSets" not in response:
        return None
    
    for result_set in response["resultSets"]:
        if result_set.get("name") == name:
            return result_set
    return None


def _rs_value(result_set: Optional[dict], column_name: str, row_index: int = 0) -> Any:
    """Read a value from a resultSet by column name and row index."""
    if not result_set:
        return None
    
    headers = result_set.get("headers", [])
    rows = result_set.get("rowSet", [])
    
    if not headers or not rows or row_index >= len(rows):
        return None
    
    try:
        col_index = headers.index(column_name)
        return rows[row_index][col_index]
    except (ValueError, IndexError):
        return None


def extract_game_meta(summary_resp: dict, boxscore_resp: dict) -> dict:
    """
    Build a dict with:
      - game_id (str)
      - season (str)
      - game_date (YYYY-MM-DD string)
      - home_team_id (int)
      - away_team_id (int)
      - status (str) e.g. 'Final' or from summary
    
    Priority:
      1) Prefer summary_resp.parameters.GameID
      2) Else boxscore_resp.parameters.GameID
      3) Else fallback to GameSummary resultSet and read "GAME_ID"
      4) Optionally also read SEASON, GAME_DATE_EST, HOME_TEAM_ID, VISITOR_TEAM_ID, GAME_STATUS_TEXT into game_meta if available
      5) Raise ValueError only if none of the above yields a game_id
    """
    game_meta = {}
    
    # Extract game_id following priority order
    game_id = None
    
    # 1. Prefer summary_resp.parameters.GameID
    if summary_resp and "parameters" in summary_resp:
        game_id = summary_resp["parameters"].get("GameID")
    
    # 2. Else boxscore_resp.parameters.GameID
    if not game_id and boxscore_resp and "parameters" in boxscore_resp:
        game_id = boxscore_resp["parameters"].get("GameID")
    
    # 3. Else fallback to GameSummary resultSet and read "GAME_ID"
    if not game_id:
        # Try summary_resp first
        game_summary = _find_resultset(summary_resp, "GameSummary")
        if not game_summary:
            # Fallback to boxscore_resp
            game_summary = _find_resultset(boxscore_resp, "GameSummary")
        
        if game_summary:
            game_id = _rs_value(game_summary, "GAME_ID")
    
    # 4. Raise ValueError only if none of the above yields a game_id
    if not game_id:
        raise ValueError("Missing game_id in both summary and boxscore parameters. Switch to extract_game_meta(summary_resp, boxscore_resp) for better reliability.")
    
    game_meta["game_id"] = str(game_id)
    
    # Extract additional fields from GameSummary if available
    game_summary = _find_resultset(summary_resp, "GameSummary")
    if not game_summary:
        game_summary = _find_resultset(boxscore_resp, "GameSummary")
    
    # Initialize with defaults
    season = None
    game_date = None
    home_team_id = None
    away_team_id = None
    status = "Final"
    
    if game_summary:
        # Read available fields from GameSummary
        season = _rs_value(game_summary, "SEASON")
        raw_date = _rs_value(game_summary, "GAME_DATE_EST")
        home_team_id = _rs_value(game_summary, "HOME_TEAM_ID")
        away_team_id = _rs_value(game_summary, "VISITOR_TEAM_ID")
        status = _rs_value(game_summary, "GAME_STATUS_TEXT") or "Final"
        
        # Format game date
        if raw_date:
            try:
                if isinstance(raw_date, str):
                    # Handle formats like "2024-01-15T00:00:00" or "2024-01-15"
                    game_date = raw_date.split('T')[0]  # Strip time portion
                    # Validate format
                    datetime.strptime(game_date, '%Y-%m-%d')
            except (ValueError, AttributeError):
                game_date = None
    
    # Fallback: Get team IDs from BoxScore TeamStats if not available from GameSummary
    if not home_team_id or not away_team_id:
        team_stats = _find_resultset(boxscore_resp, "TeamStats")
        if team_stats:
            rows = team_stats.get("rowSet", [])
            if len(rows) == 2:  # Should have exactly 2 teams
                # NBA convention: away team first, home team second
                if not away_team_id:
                    away_team_id = _rs_value(team_stats, "TEAM_ID", 0)
                if not home_team_id:
                    home_team_id = _rs_value(team_stats, "TEAM_ID", 1)
    
    # Derive season if not available
    if not season:
        season = derive_season_smart(game_id, game_date, None)
    
    # Validate required fields
    missing_fields = []
    if not game_date:
        missing_fields.append("game_date")
    if not home_team_id:
        missing_fields.append("home_team_id")
    if not away_team_id:
        missing_fields.append("away_team_id")
    
    if missing_fields:
        raise ValueError(
            f"Missing critical fields {missing_fields} for game_id={game_id!r}. "
            f"Switch to extract_game_meta(summary_resp, boxscore_resp) for better reliability."
        )
    
    # Normalize and validate types
    try:
        game_meta.update({
            "season": str(season),
            "game_date": str(game_date),
            "home_team_id": int(home_team_id),
            "away_team_id": int(away_team_id),
            "status": str(status)
        })
    except (ValueError, TypeError) as e:
        raise ValueError(f"Type conversion failed for game_id={game_id!r}: {e}")
    
    return game_meta


def extract_game_from_boxscore(boxscore_resp: dict) -> dict:
    """
    DEPRECATED: Use extract_game_meta(summary_resp, boxscore_resp) instead.
    
    This function is kept for backward compatibility but will delegate to 
    extract_game_meta with an empty summary to maintain the same interface.
    """
    logger.warning(
        "extract_game_from_boxscore is deprecated. "
        "Use extract_game_meta(summary_resp, boxscore_resp) for better reliability."
    )
    
    # Simply call extract_game_meta with empty summary - do not wrap/re-raise
    return extract_game_meta({}, boxscore_resp)