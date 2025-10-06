"""Raw data reader for Bronze layer files."""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator


class RawReader:
    """Reader for Bronze layer raw NBA data files."""
    
    def __init__(self, root: str):
        """Initialize with root directory containing raw data.
        
        Args:
            root: Path to root directory (e.g., "raw")
        """
        self.root = Path(root)
        
    def get_date_directory(self, date_str: str) -> Path:
        """Get the directory path for a specific date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            Path to the date directory
        """
        return self.root / date_str
        
    def iter_game_directories(self, date_str: str) -> Iterator[Path]:
        """Iterate over game directories for a specific date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
            
        Yields:
            Path objects for each game directory
        """
        date_dir = self.get_date_directory(date_str)
        
        if not date_dir.exists():
            return
            
        # Yield all directories that look like game IDs (not __pycache__ etc.)
        for item in date_dir.iterdir():
            if item.is_dir() and not item.name.startswith('__') and not item.name.startswith('.'):
                yield item
    
    def read_json(self, path: Path) -> Optional[Dict[str, Any]]:
        """Read JSON file with error handling.
        
        Args:
            path: Path to JSON file
            
        Returns:
            Parsed JSON data or None if file doesn't exist or is invalid
        """
        try:
            if not path.exists():
                return None
                
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError, UnicodeDecodeError):
            return None
    
    def get_scoreboard(self, date_str: str) -> Optional[Dict[str, Any]]:
        """Get scoreboard data for a date.
        
        Args:
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            Scoreboard JSON data or None
        """
        scoreboard_path = self.get_date_directory(date_str) / "scoreboard.json"
        return self.read_json(scoreboard_path)
    
    def get_boxscore_summary(self, game_dir: Path) -> Optional[Dict[str, Any]]:
        """Get boxscore summary data for a game.
        
        Args:
            game_dir: Path to game directory
            
        Returns:
            Boxscore summary JSON data or None
        """
        return self.read_json(game_dir / "boxscoresummary.json")
    
    def get_boxscore_traditional(self, game_dir: Path) -> Optional[Dict[str, Any]]:
        """Get traditional boxscore data for a game.
        
        Args:
            game_dir: Path to game directory
            
        Returns:
            Traditional boxscore JSON data or None
        """
        return self.read_json(game_dir / "boxscoretraditional.json")
    
    def get_playbyplay(self, game_dir: Path) -> Optional[Dict[str, Any]]:
        """Get play-by-play data for a game.
        
        Args:
            game_dir: Path to game directory
            
        Returns:
            Play-by-play JSON data or None
        """
        return self.read_json(game_dir / "playbyplay.json")
    
    def get_shotchart(self, game_dir: Path) -> Optional[Dict[str, Any]]:
        """Get shot chart data for a game.
        
        Args:
            game_dir: Path to game directory
            
        Returns:
            Shot chart JSON data or None
        """
        return self.read_json(game_dir / "shotchart.json")
    
    def get_game_id(self, game_dir: Path) -> str:
        """Extract game ID from directory name.
        
        Args:
            game_dir: Path to game directory
            
        Returns:
            Game ID string
        """
        return game_dir.name