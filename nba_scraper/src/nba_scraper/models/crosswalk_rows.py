"""Game ID crosswalk row Pydantic model."""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class GameIdCrosswalkRow(BaseModel):
    """Game ID crosswalk row model for mapping between data sources."""
    
    game_id: str = Field(..., description="Primary game identifier")
    bref_game_id: str = Field(..., description="Basketball Reference game ID")
    nba_stats_game_id: Optional[str] = Field(None, description="NBA Stats game ID")
    espn_game_id: Optional[str] = Field(None, description="ESPN game ID")
    yahoo_game_id: Optional[str] = Field(None, description="Yahoo game ID")
    source: str = Field(..., description="Data source")
    source_url: str = Field(..., description="Source URL")
    
    @classmethod
    def from_mapping(
        cls, 
        game_id: str, 
        bref_game_id: str, 
        source: str, 
        source_url: str,
        **other_ids: Optional[str]
    ) -> 'GameIdCrosswalkRow':
        """Create crosswalk from ID mapping."""
        return cls(
            game_id=game_id,
            bref_game_id=bref_game_id,
            nba_stats_game_id=other_ids.get('nba_stats_game_id'),
            espn_game_id=other_ids.get('espn_game_id'),
            yahoo_game_id=other_ids.get('yahoo_game_id'),
            source=source,
            source_url=source_url,
        )
    
    @classmethod
    def from_bref_url(cls, game_id: str, bref_url: str, source_url: str) -> 'GameIdCrosswalkRow':
        """Extract B-Ref game ID from URL pattern."""
        # Extract from URL like: /boxscores/202310180LAL.html
        import re
        match = re.search(r'/boxscores/(\w+)\.html', bref_url)
        if match:
            bref_game_id = match.group(1)
        else:
            # Fallback: use the URL as-is
            bref_game_id = bref_url
            
        return cls(
            game_id=game_id,
            bref_game_id=bref_game_id,
            source='bref_url_extract',
            source_url=source_url,
        )