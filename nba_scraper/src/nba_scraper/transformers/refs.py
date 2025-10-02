"""Referee data transformers."""

from typing import Any, Dict, List, Optional
from datetime import datetime

from . import BaseTransformer
from ..models.ref_rows import RefAssignmentRow


class RefTransformer(BaseTransformer[RefAssignmentRow]):
    """Transformer for referee assignment data."""
    
    def transform(self, raw_data: Dict[str, Any], **kwargs) -> List[RefAssignmentRow]:
        """Transform referee data based on source."""
        source = kwargs.get('source', self.source)
        
        if source == 'bref':
            return self._transform_bref(raw_data, **kwargs)
        elif source == 'nba_stats':
            return self._transform_nba_stats(raw_data, **kwargs)
        elif source == 'gamebooks':
            return self._transform_gamebooks(raw_data, **kwargs)
        else:
            raise ValueError(f"Unsupported source: {source}")
    
    def transform_bref_refs(self, game_id: str, referees_data: List[Dict[str, Any]]) -> List[RefAssignmentRow]:
        """Transform Basketball Reference referee data."""
        assignments = []
        
        for i, ref_data in enumerate(referees_data):
            assignment = RefAssignmentRow(
                game_id=game_id,
                ref_name=self._safe_get(ref_data, 'name'),
                ref_position=self._safe_get(ref_data, 'position') or f"Official {i+1}",
                ref_years_experience=self._parse_int(self._safe_get(ref_data, 'years_experience')),
                source=self.source,
                source_url=self._safe_get(ref_data, 'source_url')
            )
            assignments.append(assignment)
            
        return assignments
    
    def _transform_bref(self, raw_data: Dict[str, Any], **kwargs) -> List[RefAssignmentRow]:
        """Transform Basketball Reference referee data."""
        game_id = kwargs.get('game_id')
        if not game_id:
            raise ValueError("game_id is required for referee data transformation")
            
        if isinstance(raw_data, list):
            return self.transform_bref_refs(game_id, raw_data)
        elif 'referees' in raw_data:
            return self.transform_bref_refs(game_id, raw_data['referees'])
        else:
            return []
    
    def _transform_nba_stats(self, raw_data: Dict[str, Any], **kwargs) -> List[RefAssignmentRow]:
        """Transform NBA Stats referee data."""
        game_id = kwargs.get('game_id')
        if not game_id:
            raise ValueError("game_id is required for referee data transformation")
            
        assignments = []
        
        # NBA Stats typically has officials in game info
        if 'officials' in raw_data:
            for i, official in enumerate(raw_data['officials']):
                assignment = RefAssignmentRow(
                    game_id=game_id,
                    ref_name=self._safe_get(official, 'FIRST_NAME', '') + ' ' + self._safe_get(official, 'LAST_NAME', ''),
                    ref_position=self._safe_get(official, 'JERSEY_NUM') or f"Official {i+1}",
                    ref_years_experience=None,
                    source=self.source,
                    source_url=kwargs.get('source_url')
                )
                assignments.append(assignment)
                
        return assignments
    
    def _transform_gamebooks(self, raw_data: Dict[str, Any], **kwargs) -> List[RefAssignmentRow]:
        """Transform Gamebooks referee data."""
        game_id = kwargs.get('game_id')
        if not game_id:
            raise ValueError("game_id is required for referee data transformation")
            
        assignments = []
        
        # Gamebooks format may vary
        if 'officials' in raw_data:
            officials_data = raw_data['officials']
            if isinstance(officials_data, list):
                for i, official in enumerate(officials_data):
                    assignment = RefAssignmentRow(
                        game_id=game_id,
                        ref_name=self._safe_get(official, 'name'),
                        ref_position=self._safe_get(official, 'position') or f"Official {i+1}",
                        ref_years_experience=self._parse_int(self._safe_get(official, 'experience')),
                        source=self.source,
                        source_url=kwargs.get('source_url')
                    )
                    assignments.append(assignment)
                    
        return assignments