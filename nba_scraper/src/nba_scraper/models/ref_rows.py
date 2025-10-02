"""Referee assignment row Pydantic models."""

import re
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator
from rapidfuzz import fuzz

from .enums import RefRole


def normalize_name_slug(name: str) -> str:
    """Normalize name to PascalCase slug."""
    if not name:
        return ""
    
    # If already in PascalCase format (contains uppercase letters in the middle), preserve it
    if re.match(r'^[A-Z][a-z]*([A-Z][a-z]*)+$', name):
        return name
    
    # Remove common prefixes/suffixes and punctuation
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\b(Jr|Sr|III|II)\b', '', name, flags=re.IGNORECASE)
    
    # Split into words and capitalize each
    words = name.strip().split()
    return ''.join(word.capitalize() for word in words if word)


class RefAssignmentRow(BaseModel):
    """Referee assignment row model."""
    
    game_id: str = Field(..., description="Game identifier")
    referee_name_slug: str = Field(..., description="Referee name in PascalCase")
    referee_display_name: str = Field(..., description="Referee display name")
    role: RefRole = Field(..., description="Referee role")
    crew_position: Optional[int] = Field(None, description="Position in crew (1-3)")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @field_validator('referee_name_slug', mode='before')
    @classmethod
    def normalize_name_slug(cls, v: str) -> str:
        """Normalize referee name to slug format."""
        return normalize_name_slug(v)
    
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
    
    for known_ref in known_refs:
        score = fuzz.ratio(name.lower(), known_ref.lower())
        if score > best_score and score >= threshold:
            best_score = score
            best_match = known_ref
    
    return best_match