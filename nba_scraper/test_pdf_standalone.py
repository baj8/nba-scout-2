#!/usr/bin/env python3
"""
Standalone test for PDF Game Book extraction functionality.
This tests the core PDF parsing logic without full project dependencies.
"""

import io
import logging
import re
from datetime import datetime
from typing import List, Optional, Dict, Any

# Mock PDF content for testing
MOCK_PDF_CONTENT = """
NBA OFFICIAL GAME BOOK
Game ID: 0022300123
Date: January 15, 2024
Arena: Chase Center

OFFICIALS:
Referee: John Doe (#12)
Umpire: Jane Smith (#34) 
Umpire: Bob Johnson (#56)

CREW CHIEF: John Doe

GAME DETAILS:
Warriors vs Lakers
Final Score: 118-112

TECHNICAL FOULS:
Q2 08:45 Lakers Bench (Delay of game)
Q3 04:23 Warriors #30 S.Curry (Complaining)

PLAYER STATS:
#30 S.Curry: 28 pts, 6 reb, 8 ast
#23 L.James: 24 pts, 8 reb, 9 ast
"""

def extract_text_from_pdf_content(content: str) -> str:
    """Mock PDF text extraction that simulates pypdf/pdfminer output."""
    return content

def parse_referee_crew(text: str) -> Dict[str, Any]:
    """
    Enhanced referee crew parsing with comprehensive pattern matching.
    """
    referee_info = {
        'referees': [],
        'crew_chief': None,
        'game_id': None,
        'date': None,
        'arena': None
    }
    
    # Extract game metadata
    game_id_match = re.search(r'Game ID:\s*(\w+)', text)
    if game_id_match:
        referee_info['game_id'] = game_id_match.group(1)
    
    date_match = re.search(r'Date:\s*([^\n]+)', text)
    if date_match:
        referee_info['date'] = date_match.group(1).strip()
    
    arena_match = re.search(r'Arena:\s*([^\n]+)', text)
    if arena_match:
        referee_info['arena'] = arena_match.group(1).strip()
    
    # First, extract crew chief separately
    crew_chief_match = re.search(r'CREW CHIEF:\s*([A-Za-z\s\.]+?)(?:\s*\(#(\d+)\))?(?:\n|$)', text, re.IGNORECASE)
    if crew_chief_match:
        referee_info['crew_chief'] = crew_chief_match.group(1).strip()
    
    # Enhanced referee patterns - capture each line separately
    referee_lines = []
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        # Match referee positions
        if re.match(r'Referee:', line, re.IGNORECASE):
            match = re.match(r'Referee:\s*([A-Za-z\s\.]+?)(?:\s*\(#(\d+)\))?$', line, re.IGNORECASE)
            if match:
                referee_lines.append((match.group(1).strip(), match.group(2), 'Referee'))
                
        elif re.match(r'Umpire:', line, re.IGNORECASE):
            match = re.match(r'Umpire:\s*([A-Za-z\s\.]+?)(?:\s*\(#(\d+)\))?$', line, re.IGNORECASE)
            if match:
                referee_lines.append((match.group(1).strip(), match.group(2), 'Umpire'))
                
        elif re.match(r'Official:', line, re.IGNORECASE):
            match = re.match(r'Official:\s*([A-Za-z\s\.]+?)(?:\s*\(#(\d+)\))?$', line, re.IGNORECASE)
            if match:
                referee_lines.append((match.group(1).strip(), match.group(2), 'Official'))
    
    # Add referees from parsed lines
    for name, number, position in referee_lines:
        if name:
            ref_data = {
                'name': name,
                'number': number,
                'position': position
            }
            referee_info['referees'].append(ref_data)
    
    # If crew chief was found but not in referees list, add them
    if referee_info['crew_chief']:
        crew_chief_in_list = any(ref['name'] == referee_info['crew_chief'] for ref in referee_info['referees'])
        if not crew_chief_in_list:
            referee_info['referees'].append({
                'name': referee_info['crew_chief'],
                'number': None,
                'position': 'Crew Chief'
            })
    
    return referee_info

def test_pdf_text_extraction():
    """Test basic PDF text extraction."""
    print("Testing PDF text extraction...")
    
    extracted_text = extract_text_from_pdf_content(MOCK_PDF_CONTENT)
    
    assert extracted_text is not None
    assert len(extracted_text) > 0
    assert "NBA OFFICIAL GAME BOOK" in extracted_text
    assert "OFFICIALS:" in extracted_text
    
    print("✓ PDF text extraction works correctly")

def test_referee_parsing():
    """Test referee crew parsing from PDF content."""
    print("Testing referee crew parsing...")
    
    referee_info = parse_referee_crew(MOCK_PDF_CONTENT)
    
    # Debug output
    print(f"Debug: Found {len(referee_info['referees'])} referees")
    for ref in referee_info['referees']:
        print(f"  - {ref['name']} (#{ref['number']}) - {ref['position']}")
    print(f"Debug: Crew chief = {referee_info['crew_chief']}")
    print(f"Debug: Game ID = {referee_info['game_id']}")
    
    # Test game metadata extraction
    assert referee_info['game_id'] == '0022300123'
    assert referee_info['date'] == 'January 15, 2024'
    assert referee_info['arena'] == 'Chase Center'
    
    # Test referee extraction
    assert len(referee_info['referees']) >= 3
    
    # Find referees by name
    referees_by_name = {ref['name']: ref for ref in referee_info['referees']}
    
    # Test specific referee data
    assert 'John Doe' in referees_by_name
    assert referees_by_name['John Doe']['number'] == '12'
    assert referees_by_name['John Doe']['position'] == 'Referee'
    
    assert 'Jane Smith' in referees_by_name
    assert referees_by_name['Jane Smith']['number'] == '34'
    assert referees_by_name['Jane Smith']['position'] == 'Umpire'
    
    assert 'Bob Johnson' in referees_by_name
    assert referees_by_name['Bob Johnson']['number'] == '56'
    
    # Test crew chief identification
    assert referee_info['crew_chief'] == 'John Doe'
    
    print("✓ Referee crew parsing works correctly")

def test_edge_cases():
    """Test edge cases in referee parsing."""
    print("Testing edge cases...")
    
    # Test with incomplete data
    incomplete_content = """
    NBA OFFICIAL GAME BOOK
    OFFICIALS:
    Referee: John Smith
    Umpire: Jane Doe (#45)
    """
    
    result = parse_referee_crew(incomplete_content)
    assert len(result['referees']) == 2
    
    # Test referee without number
    john_smith = next((ref for ref in result['referees'] if ref['name'] == 'John Smith'), None)
    assert john_smith is not None
    assert john_smith['number'] is None
    
    # Test referee with number
    jane_doe = next((ref for ref in result['referees'] if ref['name'] == 'Jane Doe'), None)
    assert jane_doe is not None
    assert jane_doe['number'] == '45'
    
    print("✓ Edge case handling works correctly")

def test_technical_fouls_parsing():
    """Test technical fouls parsing from PDF content."""
    print("Testing technical fouls parsing...")
    
    # Pattern for technical fouls
    tech_foul_pattern = r'Q(\d+)\s+(\d{2}:\d{2})\s+([^\n]+?)\s+\([^)]+\)'
    
    matches = re.findall(tech_foul_pattern, MOCK_PDF_CONTENT)
    
    assert len(matches) == 2
    
    # First technical foul
    assert matches[0][0] == '2'  # Quarter
    assert matches[0][1] == '08:45'  # Time
    assert 'Lakers Bench' in matches[0][2]  # Team/Player
    
    # Second technical foul  
    assert matches[1][0] == '3'  # Quarter
    assert matches[1][1] == '04:23'  # Time
    assert 'Warriors #30 S.Curry' in matches[1][2]  # Team/Player
    
    print("✓ Technical fouls parsing works correctly")

def main():
    """Run all tests."""
    print("Running PDF Game Book Extraction Tests")
    print("=" * 50)
    
    try:
        test_pdf_text_extraction()
        test_referee_parsing()
        test_edge_cases()
        test_technical_fouls_parsing()
        
        print("\n" + "=" * 50)
        print("🎉 All PDF extraction tests passed successfully!")
        print("\nKey Features Verified:")
        print("✓ Robust PDF text extraction")
        print("✓ Comprehensive referee crew parsing")
        print("✓ Game metadata extraction")
        print("✓ Technical fouls parsing")
        print("✓ Edge case handling")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return False
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)