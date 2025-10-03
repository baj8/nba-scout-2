"""Referee assignment row Pydantic models."""

import re
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from rapidfuzz import fuzz

from .enums import RefRole


def normalize_name_slug(name: str) -> str:
    """Normalize name to PascalCase slug."""
    if not name:
        return ""
    
    # Remove punctuation and normalize spacing
    clean_name = re.sub(r'[^\w\s]', '', name)
    
    # Convert to PascalCase
    words = clean_name.split()
    return ''.join(word.capitalize() for word in words)


class RefAssignmentRow(BaseModel):
    """Referee assignment row model."""
    
    game_id: str = Field(..., description="Game identifier")
    referee_name_slug: str = Field(..., description="Referee name in PascalCase")
    referee_display_name: str = Field(..., description="Referee display name")
    role: RefRole = Field(..., description="Referee role")
    crew_position: Optional[int] = Field(None, description="Position in crew (1-3)")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @model_validator(mode='before')
    @classmethod
    def preprocess_data(cls, data: Any) -> Any:
        """Apply comprehensive preprocessing before field validation to prevent int/str comparison errors."""
        if isinstance(data, dict):
            # Import here to avoid circular imports
            from .utils import preprocess_nba_stats_data
            
            # Apply NBA Stats preprocessing to handle mixed data types
            processed_data = preprocess_nba_stats_data(data)
            
            return processed_data
        return data

    @field_validator('referee_name_slug', mode='before')
    @classmethod
    def normalize_name_slug(cls, v: str) -> str:
        """Normalize referee name to slug format."""
        return normalize_name_slug(v)

    @field_validator('role', mode='before')
    @classmethod
    def normalize_role(cls, v) -> RefRole:
        """Normalize referee role from various sources."""
        if isinstance(v, RefRole):
            return v
        
        # CRITICAL: Convert any input to string first to prevent int/str comparison errors
        if v is None:
            role_str = 'REFEREE'
        else:
            # Ensure we always work with a string, regardless of input type
            role_str = str(v).strip().upper()
        
        # Map various role formats to our enum
        role_map = {
            'CREW_CHIEF': RefRole.CREW_CHIEF,
            'CREW CHIEF': RefRole.CREW_CHIEF,
            'CHIEF': RefRole.CREW_CHIEF,
            'REFEREE': RefRole.REFEREE,
            'REF': RefRole.REFEREE,
            'UMPIRE': RefRole.UMPIRE,
            'UMP': RefRole.UMPIRE,
            'OFFICIAL': RefRole.OFFICIAL,
            '1': RefRole.CREW_CHIEF,  # Handle numeric role codes
            '2': RefRole.REFEREE,
            '3': RefRole.UMPIRE,
        }
        
        return role_map.get(role_str, RefRole.REFEREE)

    @classmethod
    def from_gamebook(
        cls, 
        game_id: str, 
        referee_data: Dict[str, Any], 
        source_url: str
    ) -> 'RefAssignmentRow':
        """Create from parsed game book data."""
        name = referee_data.get('name', '')
        role_text = referee_data.get('role', '').upper()
        
        # Map role text to enum
        role_mapping = {
            'CREW CHIEF': RefRole.CREW_CHIEF,
            'REFEREE': RefRole.REFEREE,
            'UMPIRE': RefRole.UMPIRE,
            'OFFICIAL': RefRole.OFFICIAL,
        }
        
        role = role_mapping.get(role_text, RefRole.OFFICIAL)
        
        return cls(
            game_id=game_id,
            referee_name_slug=normalize_name_slug(name),
            referee_display_name=name,
            role=role,
            crew_position=referee_data.get('position'),
            source='gamebook',
            source_url=source_url,
        )


class RefAlternateRow(BaseModel):
    """Referee alternate row model."""
    
    game_id: str = Field(..., description="Game identifier")
    referee_name_slug: str = Field(..., description="Referee name in PascalCase")
    referee_display_name: str = Field(..., description="Referee display name")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @model_validator(mode='before')
    @classmethod
    def preprocess_data(cls, data: Any) -> Any:
        """Apply comprehensive preprocessing before field validation to prevent int/str comparison errors."""
        if isinstance(data, dict):
            # Import here to avoid circular imports
            from .utils import preprocess_nba_stats_data
            
            # Apply NBA Stats preprocessing to handle mixed data types
            processed_data = preprocess_nba_stats_data(data)
            
            return processed_data
        return data
    
    @field_validator('referee_name_slug', mode='before')
    @classmethod
    def normalize_name_slug(cls, v: str) -> str:
        """Normalize referee name to slug format."""
        return normalize_name_slug(v)
    
    @classmethod
    def from_gamebook(
        cls, 
        game_id: str, 
        referee_name: str, 
        source_url: str
    ) -> 'RefAlternateRow':
        """Create from parsed game book data."""
        return cls(
            game_id=game_id,
            referee_name_slug=normalize_name_slug(referee_name),
            referee_display_name=referee_name,
            source='gamebook',
            source_url=source_url,
        )


def fuzzy_match_referee_name(name: str, known_refs: list[str], threshold: int = 85) -> Optional[str]:
    """Fuzzy match referee name against known referee list."""
    if not name or not known_refs:
        return None
    
    best_match = None
    best_score = 0
    
    for ref in known_refs:
        score = fuzz.ratio(name.lower(), ref.lower())
        if score > best_score and score >= threshold:
            best_score = score
            best_match = ref
    
    return best_match