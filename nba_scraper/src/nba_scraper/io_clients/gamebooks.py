"""NBA Game Books PDF client with caching and parsing."""

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import hashlib

import pypdf
from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams

from ..config import get_cache_dir, get_settings
from ..http import get
from ..logging import get_logger

logger = get_logger(__name__)


class GamebooksClient:
    """Client for NBA Game Books PDF downloading and parsing."""
    
    def __init__(self) -> None:
        """Initialize GameBooks client."""
        self.settings = get_settings()
        self.base_url = self.settings.gamebooks_base_url
        self.cache_dir = get_cache_dir() / "gamebooks"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize cache for extraction results
        self._cache: Dict[str, Dict[str, Any]] = {}
        
        # Referee name patterns for better matching
        self._referee_name_patterns = [
            # Standard "First Last" format
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)',
            # "Last, First" format
            r'([A-Z][a-z]+,\s+[A-Z][a-z]+)',
            # Names with Jr/Sr/III etc
            r'([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+(?:Jr|Sr|III|II)\.?)?)',
        ]
    
    async def list_gamebooks(self, date_local: date) -> List[str]:
        """List available game book URLs for a date.
        
        Args:
            date_local: Date to fetch game books for
            
        Returns:
            List of game book PDF URLs
        """
        date_str = date_local.strftime('%Y-%m-%d')
        
        try:
            # Enhanced URL patterns for different types of NBA game books
            url_patterns = [
                # Last 2 Minute Reports
                f"https://official.nba.com/wp-content/uploads/sites/4/{date_local.year}/{date_local.month:02d}/L2M-{date_str}*.pdf",
                # Official Game Books
                f"https://official.nba.com/wp-content/uploads/sites/4/{date_local.year}/{date_local.month:02d}/OfficialGameBook-{date_str}*.pdf",
                # Referee Reports
                f"https://official.nba.com/wp-content/uploads/sites/4/{date_local.year}/{date_local.month:02d}/RefReport-{date_str}*.pdf",
            ]
            
            # TODO: In production, this would scrape the NBA official site
            # For now, return mock URLs based on known patterns
            mock_urls = []
            for i in range(1, 6):  # Assume up to 5 games per day
                mock_urls.append(
                    f"https://official.nba.com/wp-content/uploads/sites/4/{date_local.year}/{date_local.month:02d}/L2M-{date_str}-Game{i}.pdf"
                )
            
            logger.debug("Listed potential game books", date=date_str, count=len(mock_urls))
            return mock_urls
            
        except Exception as e:
            logger.error("Failed to list game books", date=date_str, error=str(e))
            return []
    
    async def download_gamebook(self, url: str, dest: Optional[Path] = None) -> Path:
        """Download game book PDF with caching.
        
        Args:
            url: PDF URL to download
            dest: Destination path (optional, will use cache if not provided)
            
        Returns:
            Path to downloaded PDF file
        """
        if dest is None:
            # Generate cache filename from URL
            filename = url.split('/')[-1]
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            dest = self.cache_dir / filename
        
        # Check if file already exists and is recent
        if dest.exists():
            logger.debug("Using cached game book", url=url, path=str(dest))
            return dest
        
        try:
            logger.info("Downloading game book", url=url, dest=str(dest))
            
            response = await get(url)
            
            # Write PDF content to file
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(response.content)
            
            logger.info("Downloaded game book", 
                       url=url, dest=str(dest), size=len(response.content))
            
            return dest
            
        except Exception as e:
            logger.error("Failed to download game book", url=url, error=str(e))
            raise
    
    def parse_gamebook_pdf(self, path: Path) -> Dict[str, Any]:
        """Parse game book PDF for referee assignments with enhanced extraction.
        
        Args:
            path: Path to PDF file
            
        Returns:
            Dictionary with game_id, refs, alternates, metadata, and source_url
        """
        try:
            logger.info("Parsing game book PDF", path=str(path))
            
            # Try multiple extraction methods for maximum compatibility
            text_content = self._extract_text_with_fallbacks(path)
            
            if not text_content or len(text_content.strip()) < 50:
                logger.warning("Insufficient text extracted from PDF", 
                             path=str(path), 
                             text_length=len(text_content.strip()) if text_content else 0)
                return self._create_empty_result(path)
            
            # Parse referee information from text with enhanced patterns
            result = self._parse_referee_text_enhanced(text_content)
            result['source_url'] = f"file://{path.absolute()}"
            result['pdf_metadata'] = self._extract_pdf_metadata(path)
            
            # Validate and clean the results
            result = self._validate_and_clean_result(result)
            
            logger.info("Parsed game book PDF", 
                       path=str(path), 
                       game_id=result.get('game_id'),
                       refs_count=len(result.get('refs', [])),
                       alternates_count=len(result.get('alternates', [])),
                       text_length=len(text_content))
            
            return result
            
        except Exception as e:
            logger.error("Failed to parse game book PDF", path=str(path), error=str(e))
            return self._create_empty_result(path)
    
    def _extract_text_with_fallbacks(self, path: Path) -> str:
        """Extract text using multiple methods for maximum compatibility."""
        extraction_methods = [
            ("pypdf_standard", self._extract_text_pypdf),
            ("pdfminer_high_level", self._extract_text_pdfminer_enhanced),
            ("pdfminer_basic", self._extract_text_pdfminer),
        ]
        
        best_text = ""
        best_length = 0
        
        for method_name, method_func in extraction_methods:
            try:
                text = method_func(path)
                if text and len(text.strip()) > best_length:
                    best_text = text
                    best_length = len(text.strip())
                    logger.debug("Text extraction successful", 
                               method=method_name, 
                               text_length=best_length)
                    
                    # If we got substantial text, use it
                    if best_length > 200:
                        break
                        
            except Exception as e:
                logger.debug("Text extraction method failed", 
                           method=method_name, 
                           error=str(e))
                continue
        
        return best_text
    
    def _extract_text_pypdf(self, path: Path) -> str:
        """Extract text using pypdf."""
        try:
            with open(path, 'rb') as file:
                reader = pypdf.PdfReader(file)
                text_parts = []
                
                for page in reader.pages:
                    text_parts.append(page.extract_text())
                
                return '\n'.join(text_parts)
                
        except Exception as e:
            logger.debug("pypdf extraction failed", error=str(e))
            return ""
    
    def _extract_text_pdfminer(self, path: Path) -> str:
        """Extract text using pdfminer.six."""
        try:
            return extract_text(str(path))
        except Exception as e:
            logger.debug("pdfminer extraction failed", error=str(e))
            return ""
    
    def _extract_text_pdfminer_enhanced(self, path: Path) -> str:
        """Enhanced pdfminer extraction with layout parameters."""
        try:
            # Use LAParams for better text layout detection
            laparams = LAParams(
                boxes_flow=0.5,
                word_margin=0.1,
                char_margin=2.0,
                line_margin=0.5
            )
            
            return extract_text_high_level(str(path), laparams=laparams)
            
        except Exception as e:
            logger.debug("Enhanced pdfminer extraction failed", error=str(e))
            return ""
    
    def _parse_referee_text_enhanced(self, text_content: str) -> Dict[str, Any]:
        """Enhanced referee parsing with multiple patterns and validation.
        
        Args:
            text_content: Extracted PDF text
            
        Returns:
            Dictionary with parsed referee data
        """
        result = {
            'game_id': None,
            'refs': [],
            'alternates': [],
            'parsing_confidence': 0.0,
            'text_sections': {},
            'technical_fouls': [],  # NEW: Track technical fouls
            'arena': None,  # NEW: Track game venue
            'teams': {}  # NEW: Track team information
        }
        
        try:
            # Clean and normalize text
            cleaned_text = self._clean_text_content(text_content)
            result['text_sections'] = self._identify_text_sections(cleaned_text)
            
            # Extract game ID with enhanced patterns
            result['game_id'] = self._extract_game_id_enhanced(cleaned_text)
            
            # NEW: Extract arena/venue information
            result['arena'] = self._extract_arena_info(cleaned_text)
            
            # NEW: Extract technical fouls information
            result['technical_fouls'] = self._extract_technical_fouls(cleaned_text)
            
            # NEW: Extract team information
            result['teams'] = self._extract_team_info(cleaned_text)
            
            # Parse referee assignments with role detection
            refs_data = self._parse_referee_assignments_enhanced(cleaned_text)
            result['refs'] = refs_data['refs']
            result['parsing_confidence'] += refs_data['confidence']
            
            # Parse alternates with better detection
            alternates_data = self._parse_alternates_enhanced(cleaned_text, refs_data['found_names'])
            result['alternates'] = alternates_data['alternates']
            result['parsing_confidence'] += alternates_data['confidence']
            
            # Normalize confidence score (0.0 to 1.0)
            result['parsing_confidence'] = min(result['parsing_confidence'] / 2.0, 1.0)
            
            logger.debug("Enhanced referee text parsing completed", 
                        game_id=result['game_id'],
                        refs_count=len(result['refs']),
                        alternates_count=len(result['alternates']),
                        arena=result.get('arena'),
                        technical_fouls_count=len(result['technical_fouls']),
                        confidence=result['parsing_confidence'])
            
        except Exception as e:
            logger.warning("Failed to parse referee text enhanced", error=str(e))
        
        return result
    
    def _clean_text_content(self, text: str) -> str:
        """Clean and normalize PDF text content."""
        if not text:
            return ""
        
        # Remove excessive whitespace and normalize line breaks
        cleaned = re.sub(r'\n+', '\n', text)
        cleaned = re.sub(r'[ \t]+', ' ', cleaned)
        
        # Remove common PDF artifacts
        cleaned = re.sub(r'[^\x20-\x7E\n]', '', cleaned)  # Keep only printable ASCII + newlines
        
        return cleaned.strip()
    
    def _identify_text_sections(self, text: str) -> Dict[str, str]:
        """Identify different sections of the game book text."""
        sections = {}
        
        # Common section headers in NBA game books
        section_patterns = {
            'game_info': r'(game\s+information|game\s+details).*?(?=\n[A-Z]|\n\n|$)',
            'officials': r'(officials?|referees?|crew).*?(?=\n[A-Z]|\n\n|$)',
            'summary': r'(game\s+summary|final\s+score).*?(?=\n[A-Z]|\n\n|$)',
            'notes': r'(notes?|comments?).*?(?=\n[A-Z]|\n\n|$)',
        }
        
        for section_name, pattern in section_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                sections[section_name] = match.group(0)
        
        return sections
    
    def _extract_game_id_enhanced(self, text: str) -> Optional[str]:
        """Enhanced game ID extraction with multiple patterns."""
        # Enhanced patterns for game ID detection
        game_id_patterns = [
            # NBA standard game ID format (10 digits)
            r'(?:game\s+id|game\s+number)[:\s]*(\d{10})',
            r'(\d{10})',  # Standalone 10-digit number
            
            # Alternative formats
            r'game[:\s]*([A-Z0-9]{8,12})',
            r'id[:\s]*([A-Z0-9]{8,12})',
            
            # Date-based patterns (YYYYMMDD + game number)
            r'(\d{8}[A-Z0-9]{2,4})',
        ]
        
        for pattern in game_id_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Validate game ID format
                if self._is_valid_game_id(match):
                    return match
        
        return None
    
    def _is_valid_game_id(self, game_id: str) -> bool:
        """Validate if a string looks like a valid NBA game ID."""
        if not game_id:
            return False
        
        # NBA game IDs are typically 10 digits starting with 00223XXXXX
        if len(game_id) == 10 and game_id.isdigit():
            return game_id.startswith('0022')  # Regular season format
        
        # Alternative formats (8-12 alphanumeric characters)
        if 8 <= len(game_id) <= 12 and game_id.isalnum():
            return True
        
        return False
    
    def _parse_referee_assignments_enhanced(self, text: str) -> Dict[str, Any]:
        """Enhanced referee assignment parsing with role detection."""
        result = {'refs': [], 'found_names': set(), 'confidence': 0.0}
        
        # Enhanced patterns for different referee roles
        role_patterns = [
            # Crew Chief patterns
            (r'crew\s+chief[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)', 'CREW_CHIEF'),
            
            # Referee patterns  
            (r'referee[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)', 'REFEREE'),
            
            # Umpire patterns (older NBA terminology)
            (r'umpire[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)', 'UMPIRE'),
            
            # General official patterns
            (r'official[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)', 'OFFICIAL'),
            
            # Numbered positions (Official #1, #2, #3)
            (r'official\s*#?([123])[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)', None),
        ]
        
        position_counter = 1
        
        for pattern, default_role in role_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            
            for match in matches:
                if isinstance(match, tuple) and len(match) == 2:
                    # Numbered official pattern
                    position_num, name = match
                    role = f'OFFICIAL_{position_num}'
                    name = name.strip()
                else:
                    # Regular pattern
                    name = match.strip()
                    role = default_role or 'OFFICIAL'
                
                # Clean and validate name
                cleaned_name = self._clean_referee_name(name)
                if cleaned_name and cleaned_name not in result['found_names']:
                    result['refs'].append({
                        'name': cleaned_name,
                        'role': role,
                        'position': position_counter
                    })
                    result['found_names'].add(cleaned_name)
                    position_counter += 1
                    result['confidence'] += 0.3  # Each found ref increases confidence
        
        # If no specific roles found, try general name extraction
        if not result['refs']:
            result.update(self._extract_names_general_patterns(text))
        
        return result
    
    def _clean_referee_name(self, name: str) -> Optional[str]:
        """Clean and validate referee name."""
        if not name:
            return None
        
        # Remove extra whitespace and punctuation
        cleaned = re.sub(r'[^\w\s\.]', '', name).strip()
        
        # Validate name format (at least first and last name)
        name_parts = cleaned.split()
        if len(name_parts) < 2:
            return None
        
        # Check for reasonable name length
        if len(cleaned) < 3 or len(cleaned) > 50:
            return None
        
        # Filter out common false positives from PDF headers/titles
        invalid_patterns = [
            r'game\s+book',
            r'official\s+game',
            r'nba\s+official',
            r'report\s+game',
            r'book\s+game',
        ]
        
        cleaned_lower = cleaned.lower()
        for pattern in invalid_patterns:
            if re.search(pattern, cleaned_lower):
                return None
        
        # Ensure it looks like a real person's name (no repeated words)
        if len(set(name_parts)) != len(name_parts):
            return None
        
        # Capitalize properly
        return ' '.join(part.capitalize() for part in name_parts)
    
    def _extract_names_general_patterns(self, text: str) -> Dict[str, Any]:
        """Extract names using general patterns when specific roles aren't found."""
        result = {'refs': [], 'found_names': set(), 'confidence': 0.0}
        
        # Look for lists of names or name-like patterns
        general_patterns = [
            # Names in lists (comma or line separated)
            r'([A-Z][a-z]+\s+[A-Z][a-z]+)(?:\s*,\s*|\s*\n\s*)',
            
            # Names after colons
            r':\s*([A-Z][a-z]+\s+[A-Z][a-z]+)',
            
            # Names in parentheses
            r'\(\s*([A-Z][a-z]+\s+[A-Z][a-z]+)\s*\)',
        ]
        
        position = 1
        for pattern in general_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                cleaned_name = self._clean_referee_name(match)
                if cleaned_name and cleaned_name not in result['found_names']:
                    result['refs'].append({
                        'name': cleaned_name,
                        'role': 'OFFICIAL',
                        'position': position
                    })
                    result['found_names'].add(cleaned_name)
                    position += 1
                    result['confidence'] += 0.2  # Lower confidence for general patterns
        
        return result
    
    def _parse_alternates_enhanced(self, text: str, exclude_names: set) -> Dict[str, Any]:
        """Enhanced alternate referee parsing."""
        result = {'alternates': [], 'confidence': 0.0}
        
        # First try to find the alternates section with a more precise pattern
        alternates_section_pattern = r'Alternates?:\s*(.*?)(?=\n\s*(?:VENUE|TECHNICAL|MATCHUP|HOME|AWAY|VISITING|Final\s+Score)|\n\n|$)'
        section_match = re.search(alternates_section_pattern, text, re.IGNORECASE | re.DOTALL)
        
        if section_match:
            alternates_text = section_match.group(1).strip()
            # Split by newlines to get individual alternates
            potential_alternates = [line.strip() for line in alternates_text.split('\n') if line.strip()]
            
            for alternate in potential_alternates:
                # Clean and validate the alternate name
                cleaned_name = self._clean_referee_name(alternate)
                if (cleaned_name and 
                    cleaned_name not in exclude_names and 
                    cleaned_name not in result['alternates']):
                    result['alternates'].append(cleaned_name)
                    result['confidence'] += 0.2
        
        # Fallback: try individual alternate patterns if section parsing didn't work
        if not result['alternates']:
            alternate_patterns = [
                r'alternate[s]?[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)',
                r'standby[s]?[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)',
                r'backup[s]?[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)',
                r'substitute[s]?[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+)',
            ]
            
            found_alternates = set()
            
            for pattern in alternate_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    cleaned_name = self._clean_referee_name(match)
                    if (cleaned_name and 
                        cleaned_name not in exclude_names and 
                        cleaned_name not in found_alternates):
                        
                        result['alternates'].append(cleaned_name)
                        found_alternates.add(cleaned_name)
                        result['confidence'] += 0.2
        
        return result
    
    def _extract_pdf_metadata(self, path: Path) -> Dict[str, Any]:
        """Extract PDF metadata for additional context."""
        metadata = {}
        
        try:
            with open(path, 'rb') as file:
                reader = pypdf.PdfReader(file)
                if reader.metadata:
                    metadata['title'] = reader.metadata.get('/Title', '')
                    metadata['author'] = reader.metadata.get('/Author', '')
                    metadata['creator'] = reader.metadata.get('/Creator', '')
                    metadata['creation_date'] = str(reader.metadata.get('/CreationDate', ''))
                
                metadata['page_count'] = len(reader.pages)
                metadata['file_size'] = path.stat().st_size
                
        except Exception as e:
            logger.debug("Failed to extract PDF metadata", error=str(e))
        
        return metadata
    
    def _validate_and_clean_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean the parsed result."""
        # Remove duplicates from refs
        seen_names = set()
        unique_refs = []
        
        for ref in result.get('refs', []):
            name = ref.get('name')
            if name and name not in seen_names:
                unique_refs.append(ref)
                seen_names.add(name)
        
        result['refs'] = unique_refs
        
        # Remove alternates that are already in refs
        result['alternates'] = [
            alt for alt in result.get('alternates', [])
            if alt not in seen_names
        ]
        
        # Ensure game_id is present (generate fallback if needed)
        if not result.get('game_id'):
            logger.warning("No game_id found, generating fallback")
            # Could generate from filename or timestamp
            result['game_id'] = f"UNKNOWN_{hash(str(result)) % 10000000000:010d}"
        
        return result
    
    def _create_empty_result(self, path: Path) -> Dict[str, Any]:
        """Create empty result structure for failed parsing."""
        return {
            'game_id': None,
            'refs': [],
            'alternates': [],
            'parsing_confidence': 0.0,
            'source_url': f"file://{path.absolute()}",
            'error': 'Failed to parse PDF content'
        }
    
    def get_cached_gamebook(self, url: str) -> Optional[Path]:
        """Get cached game book path if it exists.
        
        Args:
            url: Original PDF URL
            
        Returns:
            Path to cached file if it exists, None otherwise
        """
        filename = url.split('/')[-1]
        if not filename.endswith('.pdf'):
            filename += '.pdf'
        
        cached_path = self.cache_dir / filename
        return cached_path if cached_path.exists() else None
    
    def clear_cache(self, older_than_days: int = 30) -> int:
        """Clear old cached game books.
        
        Args:
            older_than_days: Remove files older than this many days
            
        Returns:
            Number of files removed
        """
        import time
        from datetime import timedelta
        
        cutoff_time = time.time() - (older_than_days * 24 * 60 * 60)
        removed_count = 0
        
        try:
            for pdf_file in self.cache_dir.glob('*.pdf'):
                if pdf_file.stat().st_mtime < cutoff_time:
                    pdf_file.unlink()
                    removed_count += 1
                    logger.debug("Removed cached game book", path=str(pdf_file))
            
            logger.info("Cleared game book cache", 
                       removed_count=removed_count, 
                       older_than_days=older_than_days)
            
        except Exception as e:
            logger.warning("Failed to clear game book cache", error=str(e))
        
        return removed_count

    def _extract_arena_info(self, text: str) -> Optional[str]:
        """Extract arena/venue information from gamebook text.
        
        Args:
            text: Cleaned PDF text content
            
        Returns:
            Arena name if found, None otherwise
        """
        arena_patterns = [
            # Simpler, more precise patterns that stop at the next section
            r'VENUE:\s*([^:\n]+?)(?=\s*\n|$)',
            r'Game\s+Location:\s*([^:\n]+?)(?=\s*\n|$)',
            r'Arena:\s*([^:\n]+?)(?=\s*\n|$)',
            # Pattern that stops before the next all-caps word
            r'(?:VENUE|Arena|Game\s+Location):\s*([A-Z][A-Za-z\s&\.,]+?)(?=\s*[A-Z]{2,}|\s*\n|$)',
        ]
        
        for pattern in arena_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                arena = match.group(1).strip()
                # Clean up common artifacts and trailing commas
                arena = re.sub(r'\s+', ' ', arena)  # Normalize whitespace
                arena = arena.rstrip(',').strip()   # Remove trailing commas
                if len(arena) > 5 and len(arena) < 100:  # Reasonable length validation
                    return arena
        
        return None

    def _extract_technical_fouls(self, text: str) -> List[Dict[str, Any]]:
        """Extract technical foul information from gamebook text.
        
        Args:
            text: Cleaned PDF text content
            
        Returns:
            List of technical foul records
        """
        tech_fouls = []
        all_matches = []  # Collect all matches with their positions
        
        # More precise patterns for technical fouls with team extraction
        tech_patterns = [
            # Pattern: "T1: Jayson Tatum (BOS) - 10:30 Q1 - Arguing call" (with reason)
            r'T\d*[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*\(([A-Z]{3})\)\s*-\s*(\d{1,2}:\d{2}\s*Q\d)\s*-\s*([A-Za-z\s]+?)(?=\n|\s*$)',
            # Pattern: "T2: Bench Technical (NYK) - 5:15 Q2" (bench technical)
            r'T\d*[:\s]*(Bench\s+Technical)\s*\(([A-Z]{3})\)\s*-\s*(\d{1,2}:\d{2}\s*Q\d)(?=\n|\s*$)',
            # Pattern: "T1: LeBron James (LAL) - 8:45 Q2" (basic format)
            r'T\d*[:\s]*([A-Z][a-z]+\s+[A-Z][a-z]+)\s*\(([A-Z]{3})\)\s*-\s*(\d{1,2}:\d{2}\s*Q\d)(?=\n|\s*$)',
        ]
        
        # Collect all matches with their positions in the text
        for pattern in tech_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE):
                all_matches.append((match.start(), match))
        
        # Sort matches by their position in the text (chronological order)
        all_matches.sort(key=lambda x: x[0])
        
        processed_matches = set()  # Track processed matches to avoid duplicates
        
        for position, match in all_matches:
            # Use the full match text as a key to avoid duplicates
            match_key = match.group(0).strip()
            if match_key in processed_matches:
                continue
            
            processed_matches.add(match_key)
            groups = match.groups()
            
            if len(groups) >= 3:
                player_name = groups[0].strip()
                team = groups[1].strip()
                time = groups[2].strip()  # Now includes Q1, Q2, etc.
                reason = groups[3].strip() if len(groups) > 3 else None
                
                # Clean up reason field - remove extra whitespace and newlines
                if reason:
                    reason = re.sub(r'\s+', ' ', reason).strip()
                    # Remove common artifacts
                    if len(reason) > 50:  # If reason is too long, likely captured extra text
                        reason = reason.split('\n')[0].strip()  # Take only first line
                
                tech_foul = {
                    'player': player_name,
                    'team': team,
                    'time': time,
                    'reason': reason,
                    'raw_text': match_key
                }
                tech_fouls.append(tech_foul)
        
        return tech_fouls

    def _extract_team_info(self, text: str) -> Dict[str, Any]:
        """Extract team information from gamebook text.
        
        Args:
            text: Cleaned PDF text content
            
        Returns:
            Dictionary with team information
        """
        teams = {'home': None, 'away': None}
        
        # Try explicit HOME/AWAY patterns first (most reliable)
        home_away_patterns = [
            # Pattern: "HOME TEAM: Los Angeles Lakers"
            r'HOME\s+TEAM:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?=\s*\n|\s*$)',
            r'HOME:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?=\s*\n|\s*$)',
            # Pattern: "AWAY TEAM: Golden State Warriors" 
            r'AWAY\s+TEAM:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?=\s*\n|\s*$)',
            r'VISITING:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?=\s*\n|\s*$)',
        ]
        
        for i, pattern in enumerate(home_away_patterns):
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                team_name = match.group(1).strip()
                # Clean up team name
                team_name = re.sub(r'\s+', ' ', team_name).strip()
                
                # Validate team name
                if len(team_name) > 2 and len(team_name) < 30:
                    if i < 2:  # HOME patterns
                        teams['home'] = team_name
                    else:  # AWAY/VISITING patterns
                        teams['away'] = team_name
        
        # If we have explicit HOME/AWAY teams, return them
        if teams['home'] and teams['away']:
            return teams
        
        # Only try matchup patterns if we don't have explicit HOME/AWAY data
        matchup_patterns = [
            # Pattern: "MATCHUP: Boston Celtics at New York Knicks" - more specific
            r'MATCHUP:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+at\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?=\s*\n|\s*$)',
            # Pattern: "Boston Celtics at New York Knicks" - general matchup (avoid "vs" which can be ambiguous)
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+at\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?=\s*\n|\s*$)',
            # Pattern: "BOS @ NYK" (tricode format)
            r'([A-Z]{3})\s+(?:at|@)\s+([A-Z]{3})(?=\s*\n|\s*$)',
        ]
        
        # Try matchup patterns (capture both teams at once)
        for i, pattern in enumerate(matchup_patterns):
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                away_team = match.group(1).strip()
                home_team = match.group(2).strip()
                
                # Clean up team names - remove extra whitespace and artifacts
                away_team = re.sub(r'\s+', ' ', away_team).strip()
                home_team = re.sub(r'\s+', ' ', home_team).strip()
                
                # Validate team names (reasonable length, no weird characters)
                if (len(away_team) > 2 and len(away_team) < 30 and 
                    len(home_team) > 2 and len(home_team) < 30):
                    # Only update teams that weren't already found by explicit patterns
                    if not teams['away']:
                        teams['away'] = away_team
                    if not teams['home']:
                        teams['home'] = home_team
                    break  # Use first valid match
        
        # Return None if no teams found instead of empty dict
        if not teams['home'] and not teams['away']:
            return None
        
        return teams

    def extract_refs(self, pdf_path: str, game_id: Optional[str] = None) -> Dict[str, Any]:
        """Extract referee information from NBA gamebook PDF.
        
        Args:
            pdf_path: Path to the PDF file
            game_id: Optional game ID for validation
            
        Returns:
            Dictionary containing extracted referee and game information
        """
        cache_key = self._get_cache_key(pdf_path, game_id)
        
        # Check cache first
        if cache_key in self._cache:
            logger.debug(f"Cache hit for {pdf_path}")
            return self._cache[cache_key]
        
        try:
            # Extract text with confidence scoring
            extraction_results = self._extract_text_with_confidence(pdf_path)
            text = extraction_results['text']
            
            if not text or len(text.strip()) < 100:
                raise ValueError(f"Insufficient text extracted from PDF: {pdf_path}")
            
            # Clean the text
            cleaned_text = self._clean_text_content(text)
            
            # Extract game ID and validate
            extracted_game_id = self._extract_game_id_enhanced(cleaned_text)
            if game_id and extracted_game_id and extracted_game_id != game_id:
                logger.warning(f"Game ID mismatch: expected {game_id}, found {extracted_game_id}")
            
            # Extract referee information
            refs = self._extract_refs_from_text(cleaned_text)
            
            # Extract additional game information
            arena_info = self._extract_arena_info(cleaned_text)
            technical_fouls = self._extract_technical_fouls(cleaned_text)
            team_info = self._extract_team_info(cleaned_text)
            
            # Process raw data through extractor functions for compatibility
            from ..extractors.gamebooks import extract_referee_assignments, extract_referee_alternates
            
            # Create parsed gamebook data structure for the extractors
            parsed_gamebook = {
                'game_id': extracted_game_id or game_id,
                'refs': refs,
                'alternates': self._parse_alternates_enhanced(cleaned_text, set())['alternates'],
                'parsing_confidence': extraction_results['confidence'],
                'pdf_metadata': self._extract_pdf_metadata(Path(pdf_path)),
                'text_sections': self._identify_text_sections(cleaned_text)
            }
            
            # Extract referee assignments and alternates using the extractors
            referee_assignments = extract_referee_assignments(parsed_gamebook, str(pdf_path))
            referee_alternates = extract_referee_alternates(parsed_gamebook, str(pdf_path))
            
            # Prepare result with both formats for compatibility
            result = {
                'refs': refs,  # Raw format
                'referee_assignments': referee_assignments,  # Processed format
                'referee_alternates': referee_alternates,    # Processed format
                'game_id': extracted_game_id or game_id,
                'extraction_method': extraction_results['method'],
                'confidence': extraction_results['confidence'],
                'text_length': len(text),
                'cleaned_text_length': len(cleaned_text),
                'arena': arena_info,
                'technical_fouls': technical_fouls,
                'teams': team_info,
                'timestamp': datetime.now().isoformat()
            }
            
            # Cache the result
            self._cache[cache_key] = result
            
            logger.info(f"Successfully extracted {len(refs)} referees from {pdf_path}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to extract refs from {pdf_path}: {str(e)}"
            logger.error(error_msg)
            
            # Return error result
            error_result = {
                'refs': [],
                'referee_assignments': [],
                'referee_alternates': [],
                'game_id': game_id,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
            
            # Don't cache errors
            return error_result

    def _get_cache_key(self, pdf_path: str, game_id: Optional[str] = None) -> str:
        """Generate cache key for extraction results."""
        key_data = f"{pdf_path}_{game_id or 'unknown'}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _extract_text_with_confidence(self, pdf_path: str) -> Dict[str, Any]:
        """Extract text with confidence scoring."""
        path = Path(pdf_path)
        text = self._extract_text_with_fallbacks(path)
        
        # Calculate confidence based on text quality
        confidence = 0.0
        if text:
            # Base confidence from text length
            if len(text.strip()) > 1000:
                confidence += 0.4
            elif len(text.strip()) > 500:
                confidence += 0.3
            elif len(text.strip()) > 200:
                confidence += 0.2
            elif len(text.strip()) > 50:
                confidence += 0.1
            
            # Bonus for referee-related keywords
            ref_keywords = ['referee', 'official', 'crew', 'umpire']
            keyword_count = sum(1 for keyword in ref_keywords if keyword.lower() in text.lower())
            confidence += min(keyword_count * 0.1, 0.3)
            
            # Bonus for proper names pattern
            name_matches = len(re.findall(r'[A-Z][a-z]+\s+[A-Z][a-z]+', text))
            confidence += min(name_matches * 0.02, 0.3)
        
        # Determine extraction method used (simplified)
        method = "enhanced_fallback"
        
        return {
            'text': text,
            'confidence': min(confidence, 1.0),
            'method': method
        }
    
    def _extract_refs_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Extract referee information from cleaned text."""
        refs_data = self._parse_referee_assignments_enhanced(text)
        return refs_data['refs']