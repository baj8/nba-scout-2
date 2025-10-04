"""Game transformation functions - pure, synchronous."""

from typing import Dict, Any
from ..models.games import Game
from ..utils.preprocess import preprocess_nba_stats_data
from ..utils.season import derive_season_smart


def transform_game(game_meta_raw: Dict[str, Any]) -> Game:
    """Transform extracted game metadata to validated Game model.
    
    Args:
        game_meta_raw: Dictionary from extract_game_from_boxscore
        
    Returns:
        Validated Game instance with smart season derivation
    """
    # Apply preprocessing to handle mixed data types
    game_data = preprocess_nba_stats_data(game_meta_raw)
    
    # Extract required fields with safe defaults
    game_id = str(game_data.get("game_id", ""))
    
    # Use smart season derivation as fallback
    season = game_data.get("season") or derive_season_smart(
        game_id=game_id,
        game_date=game_data.get("game_date"),
        fallback_season=None
    ) or "UNKNOWN"
    
    # Extract other required fields
    game_date = str(game_data.get("game_date", ""))
    home_team_id = int(game_data.get("home_team_id", 0))
    away_team_id = int(game_data.get("away_team_id", 0))
    status = str(game_data.get("status", "Final"))
    
    # Create and return validated Game model
    return Game(
        game_id=game_id,
        season=season,
        game_date=game_date,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        status=status
    )