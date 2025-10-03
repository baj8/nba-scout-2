"""Lineup data transformers."""

from typing import Any, Dict, List, Optional
from datetime import datetime

from . import BaseTransformer
from ..models.lineup_rows import StartingLineupRow


class LineupTransformer(BaseTransformer[StartingLineupRow]):
    """Transformer for lineup data."""
    
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List[StartingLineupRow]:
        """Transform lineup data based on source."""
        source = kwargs.get('source', self.source)
        
        if source == 'bref':
            return self._transform_bref(raw_data, **kwargs)
        elif source == 'nba_stats':
            return self._transform_nba_stats(raw_data, **kwargs)
        elif source == 'gamebooks':
            return self._transform_gamebooks(raw_data, **kwargs)
        else:
            raise ValueError(f"Unsupported source: {source}")
    
    def _transform_bref(self, raw_data: Dict[str, Any], **kwargs) -> List[StartingLineupRow]:
        """Transform Basketball Reference lineup data."""
        game_id = kwargs.get('game_id')
        if not game_id:
            raise ValueError("game_id is required for lineup data transformation")
            
        lineups = []
        
        # Basketball Reference may have lineup data in different formats
        if 'lineups' in raw_data:
            for lineup_data in raw_data['lineups']:
                lineup = self._create_lineup_from_bref(game_id, lineup_data, **kwargs)
                if lineup:
                    lineups.append(lineup)
        elif 'starters' in raw_data:
            # Handle starting lineup data
            for team, starters in raw_data['starters'].items():
                lineup = self._create_starting_lineup(game_id, team, starters, **kwargs)
                if lineup:
                    lineups.append(lineup)
                    
        return lineups
    
    def _create_lineup_from_bref(self, game_id: str, lineup_data: Dict[str, Any], **kwargs) -> Optional[StartingLineupRow]:
        """Create StartingLineupRow from Basketball Reference data."""
        players = self._safe_get(lineup_data, 'players', [])
        if len(players) < 1:  # Need at least one player
            return None
            
        # For now, just create one lineup row per player
        # In a full implementation, you'd handle all 5 players in a lineup
        return StartingLineupRow.from_bref(
            game_id=game_id,
            team_tricode=self._safe_get(lineup_data, 'team_tricode', 'UNK'),
            player_data={'player': players[0] if players else 'Unknown'},
            source_url=kwargs.get('source_url', '')
        )
    
    def _create_starting_lineup(self, game_id: str, team_id: str, starters: List[str], **kwargs) -> Optional[StartingLineupRow]:
        """Create starting lineup from starter list."""
        if not starters:
            return None
            
        # For now, just create one lineup row for the first starter
        # In a full implementation, you'd create rows for all 5 starters
        return StartingLineupRow(
            game_id=game_id,
            team_tricode=team_id,
            player_name_slug=starters[0],
            player_display_name=starters[0],
            final_pre_tip=True,
            source=self.source,
            source_url=kwargs.get('source_url', '')
        )
    
    def _transform_nba_stats(self, raw_data: Dict[str, Any], **kwargs) -> List[StartingLineupRow]:
        """Transform NBA Stats lineup data."""
        game_id = kwargs.get('game_id')
        if not game_id:
            raise ValueError("game_id is required for lineup data transformation")
            
        lineups = []
        
        # NBA Stats lineup format
        if 'lineups' in raw_data:
            for lineup_data in raw_data['lineups']:
                # CRITICAL FIX: Apply preprocessing to prevent int/str enum comparison errors
                lineup_data = self._preprocess_nba_stats_data(lineup_data)
                
                lineup = StartingLineupRow.from_nba_stats(
                    game_id=game_id,
                    team_tricode=self._safe_get(lineup_data, 'TEAM_ABBREVIATION', 'UNK'),
                    player_data=lineup_data,
                    source_url=kwargs.get('source_url', '')
                )
                lineups.append(lineup)
                
        return lineups
    
    def _transform_gamebooks(self, raw_data: Dict[str, Any], **kwargs) -> List[StartingLineupRow]:
        """Transform Gamebooks lineup data."""
        game_id = kwargs.get('game_id')
        if not game_id:
            raise ValueError("game_id is required for lineup data transformation")
            
        lineups = []
        
        # Gamebooks format may vary
        if 'lineups' in raw_data:
            for lineup_data in raw_data['lineups']:
                # Map gamebooks format to standard format
                players = self._safe_get(lineup_data, 'players', [])
                if players:
                    lineup = StartingLineupRow(
                        game_id=game_id,
                        team_tricode=self._safe_get(lineup_data, 'team', 'UNK'),
                        player_name_slug=players[0],
                        player_display_name=players[0],
                        final_pre_tip=True,
                        source=self.source,
                        source_url=kwargs.get('source_url', '')
                    )
                    lineups.append(lineup)
                    
        return lineups