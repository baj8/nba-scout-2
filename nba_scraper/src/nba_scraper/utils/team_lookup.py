"""Centralized team lookup utilities using canonical team aliases."""

import yaml
from pathlib import Path
from typing import Dict, Optional, Tuple
from functools import lru_cache

from ..config import get_project_root
from ..nba_logging import get_logger

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def _load_team_aliases() -> Dict[str, any]:
    """Load team aliases from YAML file with caching."""
    try:
        aliases_path = get_project_root() / "team_aliases.yaml"
        with open(aliases_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error("Failed to load team aliases", error=str(e))
        return {"teams": {}}


def get_team_index() -> Dict[str, int]:
    """Get canonical tricode → team_id mapping.
    
    Returns:
        Dictionary mapping canonical tricodes to integer team IDs
    """
    aliases = _load_team_aliases()
    team_index = {}
    
    for canonical_tricode, team_data in aliases.get("teams", {}).items():
        team_id = team_data.get("id")
        if team_id is not None:
            team_index[canonical_tricode] = int(team_id)
    
    logger.debug("Built team index", team_count=len(team_index))
    return team_index


def resolve_tricode_to_id(tricode: str, *, team_index: Optional[Dict[str, int]] = None) -> int:
    """Resolve a tricode (including aliases) to canonical team ID.
    
    Args:
        tricode: Team tricode to resolve
        team_index: Optional pre-built team index for performance
        
    Returns:
        Integer team ID
        
    Raises:
        ValueError: If tricode cannot be resolved
    """
    if not tricode:
        raise ValueError("Empty tricode provided")
    
    # Normalize input
    tricode_clean = tricode.strip().upper()
    
    # Use provided index or build one
    if team_index is None:
        team_index = get_team_index()
    
    # Check direct canonical match first
    if tricode_clean in team_index:
        logger.debug("Resolved tricode directly", tricode=tricode_clean, team_id=team_index[tricode_clean])
        return team_index[tricode_clean]
    
    # Check aliases
    aliases = _load_team_aliases()
    for canonical, team_data in aliases.get("teams", {}).items():
        # Check NBA Stats variations
        nba_stats_aliases = [t.upper() for t in team_data.get("nba_stats", [])]
        if tricode_clean in nba_stats_aliases:
            team_id = team_index.get(canonical)
            if team_id is not None:
                logger.debug("Resolved tricode via NBA Stats alias", 
                           tricode=tricode_clean, canonical=canonical, team_id=team_id)
                return team_id
        
        # Check Basketball Reference variations
        bref_aliases = [t.upper() for t in team_data.get("bref", [])]
        if tricode_clean in bref_aliases:
            team_id = team_index.get(canonical)
            if team_id is not None:
                logger.debug("Resolved tricode via B-Ref alias", 
                           tricode=tricode_clean, canonical=canonical, team_id=team_id)
                return team_id
        
        # Check general aliases
        general_aliases = [t.upper() for t in team_data.get("aliases", [])]
        if tricode_clean in general_aliases:
            team_id = team_index.get(canonical)
            if team_id is not None:
                logger.debug("Resolved tricode via general alias", 
                           tricode=tricode_clean, canonical=canonical, team_id=team_id)
                return team_id
    
    # Log available keys at DEBUG level (not in exception message)
    available_keys = sorted(team_index.keys())
    logger.debug("Available tricodes", available_keys=available_keys)
    
    # Targeted error message
    raise ValueError(f"Unknown tricode '{tricode_clean}'")


def get_canonical_tricode(tricode: str) -> str:
    """Get the canonical tricode for any valid tricode/alias.
    
    Args:
        tricode: Team tricode or alias
        
    Returns:
        Canonical tricode
        
    Raises:
        ValueError: If tricode cannot be resolved
    """
    if not tricode:
        raise ValueError("Empty tricode provided")
    
    tricode_clean = tricode.strip().upper()
    
    # Check if already canonical
    team_index = get_team_index()
    if tricode_clean in team_index:
        return tricode_clean
    
    # Search through aliases to find canonical
    aliases = _load_team_aliases()
    for canonical, team_data in aliases.get("teams", {}).items():
        # Check all alias types
        all_aliases = []
        all_aliases.extend([t.upper() for t in team_data.get("nba_stats", [])])
        all_aliases.extend([t.upper() for t in team_data.get("bref", [])])
        all_aliases.extend([t.upper() for t in team_data.get("aliases", [])])
        
        if tricode_clean in all_aliases:
            return canonical
    
    raise ValueError(f"Unknown tricode '{tricode_clean}'")


def validate_tricode_pair(home_tricode: str, away_tricode: str, game_id: str) -> Tuple[int, int]:
    """Validate and resolve both team tricodes for a game.
    
    Args:
        home_tricode: Home team tricode
        away_tricode: Away team tricode  
        game_id: Game ID for error context
        
    Returns:
        Tuple of (home_team_id, away_team_id)
        
    Raises:
        ValueError: If either tricode cannot be resolved
    """
    team_index = get_team_index()  # Build once for both lookups
    
    try:
        home_id = resolve_tricode_to_id(home_tricode, team_index=team_index)
        away_id = resolve_tricode_to_id(away_tricode, team_index=team_index)
        return home_id, away_id
    except ValueError as e:
        # Add game context to error
        raise ValueError(f"{str(e)} for game {game_id}")