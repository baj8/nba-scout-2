"""Game transformation functions - pure, synchronous."""

import re
import warnings
from typing import Dict, Any, Optional, List
from ..models.games import Game
from ..utils.preprocess import preprocess_nba_stats_data
from ..utils.season import derive_season_smart
from ..nba_logging import get_logger

logger = get_logger(__name__)


def transform_game(game_meta_raw: Dict[str, Any]) -> Game:
    """Transform extracted game metadata to validated Game model.
    
    Args:
        game_meta_raw: Dictionary from extract_game_from_boxscore
        
    Returns:
        Validated Game instance with strict validation and smart season derivation
        
    Raises:
        ValueError: If game_id doesn't match required format or critical fields are missing
    """
    # Apply preprocessing to handle mixed data types
    game_data = preprocess_nba_stats_data(game_meta_raw)
    
    # Extract and validate game_id with strict format checking
    game_id = str(game_data.get("game_id", ""))
    
    # NBA regular season game ID format: 0022YYGGGGG (exactly 10 digits)
    # where 0022 = regular season prefix, YY = season year, GGGGG = game number
    if not re.match(r"^0022\d{6}$", game_id):
        raise ValueError(f"invalid game_id: {game_id!r} - must be 10-char string matching ^002\\d{{7}}$")
    
    # Extract and validate season with format checking and smart derivation
    season = game_data.get("season")
    if season is not None:
        season_str = str(season)  # Convert to string if present
        
        # Check if season matches YYYY-YY format
        if re.fullmatch(r"\d{4}-\d{2}", season_str):
            season = season_str  # Valid format, use as-is
        else:
            # Invalid format - log warning and derive smart season
            # Use logger that can be captured by pytest caplog
            import logging
            logging.getLogger(__name__).warning(f"season format invalid: {season!r} - expected YYYY-YY format, deriving from game data")
            season = derive_season_smart(
                game_id=game_id,
                game_date=game_data.get("game_date"),
                fallback_season=None
            ) or "UNKNOWN"
    else:
        # Missing season - derive without warning (existing behavior)
        season = derive_season_smart(
            game_id=game_id,
            game_date=game_data.get("game_date"),
            fallback_season=None
        ) or "UNKNOWN"
    
    # Ensure final season value is always a string
    season = str(season)
    
    # Extract and validate other required fields
    game_date = str(game_data.get("game_date", ""))
    if not game_date:
        raise ValueError("Missing required field: game_date")
    
    try:
        home_team_id = int(game_data.get("home_team_id", 0))
        away_team_id = int(game_data.get("away_team_id", 0))
    except (ValueError, TypeError):
        raise ValueError("Invalid team_id format: must be numeric")
    
    if home_team_id == 0 or away_team_id == 0:
        raise ValueError("Missing required team IDs")
    
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


class BRefCrosswalkResolver:
    """Resolver for mapping Basketball Reference IDs to NBA Stats IDs."""
    
    def __init__(self):
        """Initialize the crosswalk resolver with default mappings."""
        # Basketball Reference to NBA Stats ID mappings
        # These would typically be loaded from a database or config file
        self._bref_to_nba_stats = {}
        
    def resolve_bref_game_id(self, bref_game_id: str) -> Optional[str]:
        """Resolve Basketball Reference game ID to NBA Stats game ID.
        
        Args:
            bref_game_id: Basketball Reference game identifier
            
        Returns:
            NBA Stats game ID if found, None otherwise
        """
        return self._bref_to_nba_stats.get(bref_game_id)
        
    def add_mapping(self, bref_id: str, nba_stats_id: str) -> None:
        """Add a new crosswalk mapping.
        
        Args:
            bref_id: Basketball Reference game ID
            nba_stats_id: NBA Stats game ID
        """
        self._bref_to_nba_stats[bref_id] = nba_stats_id
        
    def get_all_mappings(self) -> Dict[str, str]:
        """Get all current crosswalk mappings."""
        return self._bref_to_nba_stats.copy()


class GameCrosswalkTransformer:
    """Transformer for standardizing and resolving game crosswalk IDs between data sources."""
    
    def __init__(self, resolver: Optional[BRefCrosswalkResolver] = None):
        """Initialize with an optional crosswalk resolver.
        
        Args:
            resolver: BRefCrosswalkResolver instance for ID mapping
        """
        self.resolver = resolver or BRefCrosswalkResolver()
        
    def transform_crosswalk_ids(self, game_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform and standardize game IDs across data sources.
        
        Args:
            game_data: Raw game data with potentially mixed ID formats
            
        Returns:
            Standardized game data with resolved IDs
        """
        transformed = game_data.copy()
        
        # If we have a Basketball Reference ID, try to resolve it
        bref_id = game_data.get('bref_game_id')
        if bref_id:
            nba_stats_id = self.resolver.resolve_bref_game_id(bref_id)
            if nba_stats_id:
                transformed['game_id'] = nba_stats_id
                transformed['nba_stats_game_id'] = nba_stats_id
                
        # Ensure we have a standardized game_id
        if not transformed.get('game_id'):
            # Try other potential ID fields
            for id_field in ['nba_stats_game_id', 'official_game_id', 'game_key']:
                if game_data.get(id_field):
                    transformed['game_id'] = str(game_data[id_field])
                    break
                    
        return transformed
        
    def validate_crosswalk_integrity(self, games: List[Dict[str, Any]]) -> List[str]:
        """Validate crosswalk integrity across a list of games.
        
        Args:
            games: List of game data dictionaries
            
        Returns:
            List of validation errors found
        """
        errors = []
        seen_ids = set()
        
        for i, game in enumerate(games):
            game_id = game.get('game_id')
            if not game_id:
                errors.append(f"Game {i}: Missing game_id")
                continue
                
            if game_id in seen_ids:
                errors.append(f"Game {i}: Duplicate game_id {game_id}")
            else:
                seen_ids.add(game_id)
                
        return errors