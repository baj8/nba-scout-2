"""Team crosswalk utility - single source of truth for tricode to ID mapping."""

from typing import Dict
from ..config import get_project_root
from ..nba_logging import get_logger

logger = get_logger(__name__)

# Canonical team mapping - single source of truth
_CANONICAL_TEAMS = {
    "ATL": 1610612737, "BOS": 1610612738, "BRK": 1610612751, "CHA": 1610612766,
    "CHI": 1610612741, "CLE": 1610612739, "DAL": 1610612742, "DEN": 1610612743,
    "DET": 1610612765, "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763, "MIA": 1610612748,
    "MIL": 1610612749, "MIN": 1610612750, "NOP": 1610612740, "NYK": 1610612752,
    "OKC": 1610612760, "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759, "TOR": 1610612761,
    "UTA": 1610612762, "WAS": 1610612764,
}

# Team aliases - map variations to canonical tricodes
_TEAM_ALIASES = {
    "BKN": "BRK",  # Brooklyn alias
    "PHO": "PHX",  # Phoenix alias  
    "NOH": "NOP",  # New Orleans Hornets legacy
    "CHO": "CHA",  # Charlotte variation
}

_team_index_cache: Dict[str, int] = None


def get_team_index() -> Dict[str, int]:
    """Get complete team tricode to ID mapping with aliases resolved."""
    global _team_index_cache
    
    if _team_index_cache is None:
        # Start with canonical teams
        index = _CANONICAL_TEAMS.copy()
        
        # Add aliases
        for alias, canonical in _TEAM_ALIASES.items():
            if canonical in _CANONICAL_TEAMS:
                index[alias] = _CANONICAL_TEAMS[canonical]
        
        _team_index_cache = index
    
    return _team_index_cache


def resolve_team_id(tricode: str, index: Dict[str, int], *, game_id: str = None) -> int:
    """Resolve a team tricode to its team ID.
    
    Args:
        tricode: Team tricode (will be normalized)
        index: Team index from get_team_index()
        game_id: Optional game_id for error context
        
    Returns:
        Team ID integer
        
    Raises:
        ValueError: If tricode cannot be resolved
    """
    if not tricode:
        raise ValueError(f"Unknown tricode '' for game {game_id or 'unknown'}")
    
    # Normalize tricode
    normalized = tricode.strip().upper()
    
    if normalized in index:
        return index[normalized]
    
    # Generate error with game context
    game_context = f" for game {game_id}" if game_id else ""
    raise ValueError(f"Unknown tricode '{tricode}'{game_context}")