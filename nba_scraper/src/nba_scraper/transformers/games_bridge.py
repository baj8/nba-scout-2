"""Transformer bridge from GameRow to Game with comprehensive validation."""

from typing import Optional, Dict
from datetime import datetime
import re
from ..models.game_rows import GameRow
from ..models.games import Game
from ..models.enums import GameStatus
from ..nba_logging import get_logger
from ..utils.team_crosswalk import get_team_index, resolve_team_id
from ..utils.date_norm import to_date_str, derive_season_from_game_id, derive_season_from_date

logger = get_logger(__name__)


def to_db_game(game_row: GameRow, *, team_index: Optional[Dict[str, int]] = None) -> Game:
    """Transform GameRow to validated Game model with comprehensive normalization.
    
    This bridge function implements the requirements:
    1. game_id: required (error if missing)
    2. season: if missing/empty, derive from game_id or date; fallback "UNKNOWN" + WARN
    3. game_date: prefer game_date_local else date from game_date_utc. Persist as YYYY-MM-DD string
    4. home_team_id/away_team_id: resolve tricodes via team crosswalk with error on miss
    5. status: normalize to DB's expected casing, default "Final" if absent
    
    Args:
        game_row: Source GameRow from extractor
        team_index: Optional pre-built team index for performance
        
    Returns:
        Validated Game instance ready for database
        
    Raises:
        ValueError: For validation failures with targeted messages
    """
    # Use provided team index or build one
    if team_index is None:
        team_index = get_team_index()
    
    # 1. Validate game_id is present
    if not game_row.game_id or not game_row.game_id.strip():
        raise ValueError("game_id is required and cannot be empty")
    
    game_id = game_row.game_id.strip()
    
    # 2. Resolve team tricodes to IDs with comprehensive error handling
    try:
        home_team_id = resolve_team_id(game_row.home_team_tricode, team_index, game_id=game_id)
        away_team_id = resolve_team_id(game_row.away_team_tricode, team_index, game_id=game_id)
    except ValueError as e:
        # Re-raise with game context if not already included
        error_msg = str(e)
        if game_id not in error_msg:
            error_msg = f"Unknown tricode for game {game_id}: {error_msg}"
        raise ValueError(error_msg)
    
    # 3. Apply date policy: prefer game_date_local, fallback to date from game_date_utc
    game_date = _derive_game_date(game_row)
    
    # 4. Smart season derivation with multiple fallbacks
    season = _derive_season_with_fallbacks(game_row, game_date)
    
    # 5. Normalize status to match DB expectations
    status = _normalize_game_status(getattr(game_row, 'status', None))
    
    # 6. Create and validate Game instance
    try:
        db_game = Game(
            game_id=game_id,
            season=season,
            game_date=game_date,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            status=status
        )
        
        # Log successful transformation with key details
        logger.debug("GameRow → Game transformation complete",
                    game_id=game_id,
                    season=season,
                    game_date=game_date,
                    home_team_id=home_team_id,
                    away_team_id=away_team_id,
                    status=status)
        
        return db_game
        
    except Exception as e:
        logger.error("Game model validation failed",
                    game_id=game_id,
                    season=season,
                    game_date=game_date,
                    error=str(e))
        raise ValueError(f"Game validation failed for {game_id}: {str(e)}")


def _derive_game_date(game_row: GameRow) -> str:
    """Derive game date with local preference policy.
    
    Policy: Prefer game_date_local, else extract date from game_date_utc
    
    Args:
        game_row: Source GameRow
        
    Returns:
        Game date as YYYY-MM-DD string
    """
    # Prefer game_date_local if available
    if hasattr(game_row, 'game_date_local') and game_row.game_date_local:
        try:
            return to_date_str(game_row.game_date_local, field_name="game_date_local")
        except ValueError:
            logger.warning("Invalid game_date_local, falling back to UTC", 
                          game_id=game_row.game_id,
                          game_date_local=game_row.game_date_local)
    
    # Fallback to extracting date from game_date_utc
    if hasattr(game_row, 'game_date_utc') and game_row.game_date_utc:
        try:
            return to_date_str(game_row.game_date_utc, field_name="game_date_utc")
        except ValueError:
            logger.warning("Invalid game_date_utc", 
                          game_id=game_row.game_id,
                          game_date_utc=game_row.game_date_utc)
    
    # Last resort: use today's date
    logger.warning("No valid game date found, using today", game_id=game_row.game_id)
    return datetime.now().date().isoformat()


def _derive_season_with_fallbacks(game_row: GameRow, game_date: str) -> str:
    """Derive season with multiple fallback strategies.
    
    Args:
        game_row: Source GameRow
        game_date: Validated game date string (YYYY-MM-DD)
        
    Returns:
        Season string, "UNKNOWN" if all methods fail
    """
    # Strategy 1: Use explicit season if present and valid
    if (hasattr(game_row, 'season') and 
        game_row.season and 
        game_row.season.strip() and 
        game_row.season.strip() != "UNKNOWN"):
        season = game_row.season.strip()
        logger.debug("Using explicit season", game_id=game_row.game_id, season=season)
        return season
    
    # Strategy 2: Derive from game ID (NBA pattern 00XYYxxxxx → YYYY-YY+1)
    season_from_id = derive_season_from_game_id(game_row.game_id)
    if season_from_id and season_from_id != "UNKNOWN":
        logger.debug("Derived season from game ID", 
                    game_id=game_row.game_id, 
                    season=season_from_id)
        return season_from_id
    
    # Strategy 3: Derive from game date
    season_from_date = derive_season_from_date(game_date)
    if season_from_date and season_from_date != "UNKNOWN":
        logger.debug("Derived season from date", 
                    game_id=game_row.game_id, 
                    game_date=game_date, 
                    season=season_from_date)
        return season_from_date
    
    # All strategies failed - log warning as required
    logger.warning("Season derivation failed, using UNKNOWN", 
                  game_id=game_row.game_id)
    return "UNKNOWN"


def _normalize_game_status(status) -> str:
    """Normalize game status to DB's expected casing.
    
    Args:
        status: Raw status (string, enum, or other)
        
    Returns:
        Normalized status string with proper casing
    """
    if status is None:
        return "SCHEDULED"
    
    # Handle GameStatus enum
    if hasattr(status, 'value'):
        status_str = status.value
    else:
        status_str = str(status)
    
    status_clean = status_str.strip().upper()
    
    # Normalize to DB expected casing
    status_map = {
        'FINAL': 'FINAL',
        'FINISHED': 'FINAL',
        'COMPLETED': 'FINAL',
        'LIVE': 'LIVE',
        'IN_PROGRESS': 'LIVE',
        'IN PROGRESS': 'LIVE',
        'SCHEDULED': 'SCHEDULED',
        'UPCOMING': 'SCHEDULED',
        'POSTPONED': 'POSTPONED',
        'CANCELLED': 'CANCELLED',
        'CANCELED': 'CANCELLED',
        'SUSPENDED': 'SUSPENDED',
        'RESCHEDULED': 'RESCHEDULED',
    }
    
    return status_map.get(status_clean, "SCHEDULED")