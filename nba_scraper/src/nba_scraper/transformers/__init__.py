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
    
    def _preprocess_nba_stats_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply NBA Stats preprocessing to prevent int/str enum comparison errors.
        
        This method ensures all enum-related fields are converted to strings
        before they reach Pydantic model validation, preventing int/str comparison errors.
        """
        if not isinstance(data, dict):
            return data
        
        # Import here to avoid circular imports
        try:
            from ..models.utils import preprocess_nba_stats_data
            return preprocess_nba_stats_data(data)
        except ImportError:
            # Fallback preprocessing if import fails
            return self._basic_enum_preprocessing(data)
    
    def _basic_enum_preprocessing(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Basic enum preprocessing fallback."""
        processed_data = data.copy()
        
        # Critical enum fields that need string conversion
        enum_fields = {
            'GAME_STATUS_TEXT', 'status', 'STATUS', 'GAME_STATUS',
            'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'EVENT_MSG_TYPE', 'EVENT_MSG_ACTION_TYPE',
            'REFEREE_ROLE', 'referee_role', 'ROLE', 'role',
            'POSITION', 'position', 'START_POSITION'
        }
        
        for key, value in processed_data.items():
            if value is not None and key in enum_fields:
                processed_data[key] = str(value)
        
        return processed_data
    
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

# Create a proper PbpTransformer since the full implementation isn't ready yet
class PbpTransformer(BaseTransformer):
    """Transformer for play-by-play data."""
    
    def __init__(self, source: str):
        super().__init__(source)
    
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List:
        """Transform play-by-play data."""
        if not raw_data:
            return []
        
        # Import here to avoid circular imports
        from ..extractors.nba_stats import extract_pbp_from_response
        from ..extractors.bref import extract_game_outcomes
        from ..models import PbpEventRow
        
        game_id = kwargs.get('game_id', 'unknown')
        source_url = kwargs.get('source_url', '')
        
        # Apply preprocessing to prevent int/str comparison errors
        if self.source == 'nba_stats':
            raw_data = self._preprocess_nba_stats_data(raw_data)
            # Use the NBA Stats extractor which already handles PBP data properly
            return extract_pbp_from_response(raw_data, game_id, source_url)
        elif self.source == 'bref':
            # Basketball Reference doesn't have detailed PBP, just outcomes
            return []
        elif self.source == 'gamebooks':
            # Gamebooks don't have PBP data
            return []
        else:
            return []


__all__ = ['BaseTransformer', 'GameTransformer', 'GameCrosswalkTransformer', 'RefTransformer', 'LineupTransformer', 'PbpTransformer']