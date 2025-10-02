import re

# Sample text from the test
sample_text = """
NBA Official Game Book
Game ID: 0022400123

OFFICIALS:
Crew Chief: John Smith (#12)
Referee: Jane Doe (#34)
Official: Bob Johnson (#56)

Alternates:
Mike Wilson
Sarah Davis

Game Summary: Lakers vs Warriors
Final Score: 112-108

VENUE: Crypto.com Arena, Los Angeles, CA

TECHNICAL FOULS:
T1: LeBron James (LAL) - 8:45 Q2
T2: Stephen Curry (GSW) - 3:22 Q3

HOME TEAM: Los Angeles Lakers
AWAY TEAM: Golden State Warriors
"""

# Test our regex patterns
arena_patterns = [
    r'VENUE:\s*([A-Z][A-Za-z\s&\.,]+(?:Center|Arena|Garden|Stadium|Coliseum|Court|Fieldhouse)(?:\s*,\s*[A-Z][A-Za-z\s,]+)?)',
    r'(?:at\s+)?([A-Z][A-Za-z\s&\.]+(?:Center|Arena|Garden|Stadium|Coliseum|Court|Fieldhouse)(?:\s*,\s*[A-Z][A-Za-z\s]+,?\s*[A-Z]{2})?)(?=\s*\n|\s*$|\s+[A-Z]{2,})',
    r'Game\s+Location:\s*([A-Z][A-Za-z\s&\.,]+?)(?=\s*\n|\s*$)',
    r'Arena:\s*([A-Z][A-Za-z\s&\.,]+?)(?=\s*\n|\s*$)',
]

print("Testing arena extraction patterns:")
for i, pattern in enumerate(arena_patterns):
    print(f"\nPattern {i+1}: {pattern}")
    matches = re.finditer(pattern, sample_text, re.IGNORECASE | re.MULTILINE)
    for match in matches:
        arena = match.group(1).strip()
        print(f"  Match: '{arena}'")
        print(f"  Full match: '{match.group(0)}'")
