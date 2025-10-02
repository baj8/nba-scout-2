"""Transformers for converting raw data to standardized models."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypeVar, Generic

T = TypeVar('T')


class BaseTransformer(ABC, Generic[T]):
    """Base transformer for converting raw data to model instances."""
    
    def __init__(self, source: str):
        """Initialize transformer.
        
        Args:
            source: Data source identifier (e.g., 'nba_stats', 'bref')
        """
        self.source = source
    
    @abstractmethod
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List[T]:
        """Transform raw data to model instances.
        
        Args:
            raw_data: Raw data from source
            **kwargs: Additional context (game_id, season, etc.)
            
        Returns:
            List of transformed model instances
        """
        pass
    
    def _safe_get(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Safely get value from dictionary with fallback."""
        return data.get(key, default)
    
    def _parse_int(self, value: Any, default: Optional[int] = None) -> Optional[int]:
        """Safely parse integer value."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def _parse_float(self, value: Any, default: Optional[float] = None) -> Optional[float]:
        """Safely parse float value."""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _parse_bool(self, value: Any, default: Optional[bool] = None) -> Optional[bool]:
        """Safely parse boolean value."""
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'y')
        try:
            return bool(int(value))
        except (ValueError, TypeError):
            return default


# Import transformers after BaseTransformer is defined
from .games import GameTransformer, GameCrosswalkTransformer
from .refs import RefTransformer
from .lineups import LineupTransformer

# Create a simple PbpTransformer since the full implementation isn't ready yet
class PbpTransformer(BaseTransformer):
    """Transformer for play-by-play data."""
    
    def __init__(self, source: str):
        super().__init__(source)
    
    def transform(self, data: dict) -> dict:
        """Transform play-by-play data."""
        # TODO: Implement play-by-play transformation logic
        return data


__all__ = ['BaseTransformer', 'GameTransformer', 'GameCrosswalkTransformer', 'RefTransformer', 'LineupTransformer', 'PbpTransformer']