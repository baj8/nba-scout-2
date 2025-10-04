"""Data preprocessing utilities with null-safe parsing and normalization."""

import re
from typing import Optional, Union, Any
from decimal import Decimal, InvalidOperation


def safe_str_strip(value: Any, default: str = "") -> str:
    """Safely strip whitespace from any value, handling None and non-strings.
    
    Args:
        value: Input value of any type
        default: Default value if input is None or empty after stripping
        
    Returns:
        Stripped string or default value
    """
    if value is None:
        return default
    
    try:
        stripped = str(value).strip()
        return stripped if stripped else default
    except (AttributeError, ValueError):
        return default


def safe_int_parse(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Safely parse integer from any value, handling None and invalid inputs.
    
    Args:
        value: Input value of any type
        default: Default value if parsing fails
        
    Returns:
        Parsed integer or default value
    """
    if value is None:
        return default
    
    # Handle already parsed integers
    if isinstance(value, int):
        return value
    
    # Handle floats (convert to int if no fractional part)
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return default
    
    # Handle string parsing
    try:
        # Clean the string first
        cleaned = safe_str_strip(value)
        if not cleaned:
            return default
            
        # Remove common formatting characters
        cleaned = re.sub(r'[,$%]', '', cleaned)
        
        # Try direct int conversion
        return int(cleaned)
    except (ValueError, TypeError):
        return default


def safe_float_parse(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Safely parse float from any value, handling None and invalid inputs.
    
    Args:
        value: Input value of any type
        default: Default value if parsing fails
        
    Returns:
        Parsed float or default value
    """
    if value is None:
        return default
    
    # Handle already parsed numbers
    if isinstance(value, (int, float)):
        return float(value)
    
    # Handle string parsing
    try:
        # Clean the string first
        cleaned = safe_str_strip(value)
        if not cleaned:
            return default
            
        # Remove common formatting characters
        cleaned = re.sub(r'[,$%]', '', cleaned)
        
        # Try direct float conversion
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def safe_bool_parse(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    """Safely parse boolean from any value, handling None and various formats.
    
    Args:
        value: Input value of any type
        default: Default value if parsing fails
        
    Returns:
        Parsed boolean or default value
    """
    if value is None:
        return default
    
    # Handle already parsed booleans
    if isinstance(value, bool):
        return value
    
    # Handle numeric values
    if isinstance(value, (int, float)):
        return bool(value)
    
    # Handle string parsing
    try:
        cleaned = safe_str_strip(value).lower()
        if not cleaned:
            return default
            
        # True values
        if cleaned in ('true', '1', 'yes', 'y', 't', 'on'):
            return True
        # False values
        elif cleaned in ('false', '0', 'no', 'n', 'f', 'off'):
            return False
        else:
            return default
    except (AttributeError, TypeError):
        return default


def normalize_team_tricode(tricode: Any) -> Optional[str]:
    """Normalize team tricode to standard 3-letter format.
    
    Args:
        tricode: Team tricode of any type
        
    Returns:
        Normalized tricode or None if invalid
    """
    if tricode is None:
        return None
    
    try:
        cleaned = safe_str_strip(tricode).upper()
        if not cleaned:
            return None
            
        # Handle common variations
        tricode_map = {
            'NOR': 'NOP',  # New Orleans Pelicans
            'NOH': 'NOP',  # Legacy New Orleans
            'CHA': 'CHA',  # Charlotte Hornets
            'CHO': 'CHA',  # Legacy Charlotte
            'BKN': 'BKN',  # Brooklyn Nets
            'NJN': 'BKN',  # Legacy New Jersey
        }
        
        # Apply mapping if exists
        normalized = tricode_map.get(cleaned, cleaned)
        
        # Validate length
        if len(normalized) == 3 and normalized.isalpha():
            return normalized
        
        return None
    except (AttributeError, TypeError):
        return None


def clean_player_name(name: Any) -> Optional[str]:
    """Clean and normalize player name.
    
    Args:
        name: Player name of any type
        
    Returns:
        Cleaned player name or None if invalid
    """
    if name is None:
        return None
    
    try:
        cleaned = safe_str_strip(name)
        if not cleaned:
            return None
            
        # Remove common suffixes and prefixes
        cleaned = re.sub(r'\b(Jr\.?|Sr\.?|III?|IV)\b', '', cleaned, flags=re.IGNORECASE)
        
        # Clean multiple spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Validate has at least one letter
        if re.search(r'[a-zA-Z]', cleaned):
            return cleaned
        
        return None
    except (AttributeError, TypeError):
        return None


def normalize_description(description: Any) -> Optional[str]:
    """Normalize event description text.
    
    Args:
        description: Description text of any type
        
    Returns:
        Normalized description or None if invalid
    """
    if description is None:
        return None
    
    try:
        cleaned = safe_str_strip(description)
        if not cleaned:
            return None
            
        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove excessive punctuation
        cleaned = re.sub(r'([.!?]){2,}', r'\1', cleaned)
        
        return cleaned
    except (AttributeError, TypeError):
        return None


def extract_numeric_from_text(text: Any) -> Optional[float]:
    """Extract first numeric value from text.
    
    Args:
        text: Text containing numeric values
        
    Returns:
        First numeric value found or None
    """
    if text is None:
        return None
    
    try:
        cleaned = safe_str_strip(text)
        if not cleaned:
            return None
            
        # Find first number (including decimals)
        match = re.search(r'-?\d+(?:\.\d+)?', cleaned)
        if match:
            return float(match.group())
        
        return None
    except (AttributeError, TypeError, ValueError):
        return None


def validate_game_id_format(game_id: Any) -> Optional[str]:
    """Validate and normalize NBA game ID format.
    
    Args:
        game_id: Game ID of any type
        
    Returns:
        Validated game ID or None if invalid
    """
    if game_id is None:
        return None
    
    try:
        cleaned = safe_str_strip(game_id)
        if not cleaned:
            return None
            
        # NBA game IDs are typically 10 digits: 0022300001
        # Format: 00 (league) + 2 (season type) + 23 (season year) + 00001 (game number)
        if re.match(r'^\d{10}$', cleaned):
            return cleaned
        
        return None
    except (AttributeError, TypeError):
        return None