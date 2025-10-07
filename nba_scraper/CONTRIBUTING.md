# Contributing to NBA Scraper

Welcome! 🎉 We're excited you're interested in contributing to `nba_scraper`.  
This project is designed to maintain **strict data validation** and **production-grade quality**.  
Please review these guidelines carefully before opening a pull request (PR).

---

## 📜 Code of Conduct
All contributors are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).  
Be respectful, constructive, and collaborative.

---

## 🛠 Development Setup
1. Clone the repo:  
   ```bash
   git clone https://github.com/your-org/nba_scraper.git
   cd nba_scraper
   ```

2. Set up your environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -U pip
   pip install -e .[dev]
   ```

3. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

4. Set up database (optional for most contributions):
   ```bash
   cp .env.example .env
   # Edit .env with your PostgreSQL credentials
   createdb nba_scraper
   psql nba_scraper < schema.sql
   ```

5. Verify installation:
   ```bash
   pytest -q
   ruff check .
   mypy src
   ```

---

## 🔒 Strict Validation Rules

⚠️ **CRITICAL**: These rules are **mandatory** and enforced by CI/CD:

### Game ID Format
```python
# ✅ CORRECT: Must be exactly 10 chars, numeric, starts with "0022"
game_id = "0022301234"

# ❌ WRONG: Will raise ValueError
"0012301234"  # Wrong prefix (preseason)  
"22301234"    # Too short (missing leading zeros)
"0022abc123"  # Non-numeric characters
```

### Season Format  
```python
# ✅ CORRECT: YYYY-YY format only
season = "2024-25"

# ⚠️ WARNING: Logs warning but continues processing
"2024"        # Missing second year
"2024-2025"   # Wrong separator  
"24-25"       # Two-digit years
```

### Pipeline Rules
- **Critical failures** (bad Game IDs) → `raise ValueError`
- **Non-critical issues** (bad seasons) → log warning, continue processing  
- **Error aggregation** → collect errors in `result["errors"]` list
- **Pipeline continuation** → PBP/lineups continue even if game metadata fails

---

## 🧪 Testing Requirements

### Coverage Standards
- **90% minimum coverage** for new code
- **100% coverage** for validation functions  
- **All error paths** must be tested

### Running Tests
```bash
# Quick validation
pytest tests/unit/test_transformers_games.py -v

# Full test suite
pytest

# With coverage
pytest --cov=src --cov-report=term-missing
```

### Test Pattern Examples
```python
# ✅ GOOD: Test both success AND failure
def test_game_id_validation():
    # Valid case
    result = transform_game({"game_id": "0022301234", ...})
    assert result.game_id == "0022301234"
    
    # Invalid case - must raise ValueError
    with pytest.raises(ValueError, match="invalid game_id"):
        transform_game({"game_id": "invalid", ...})
```

---

## 🚀 Development Workflow

### 1. Create Feature Branch
```bash
git checkout main
git pull origin main
git checkout -b feature/your-feature-name
```

### 2. Make Changes
- Write tests **first** (TDD approach)
- Follow validation rules **strictly**  
- Update docs if needed

### 3. Quality Checks
```bash
# Before committing
ruff check .        # Linting
ruff format .       # Formatting  
mypy src           # Type checking
pytest             # All tests
```

### 4. Commit & Push
```bash
# Use conventional commit format
git commit -m "feat(validation): add strict game ID validation"
git push origin feature/your-feature-name
```

---

## 📋 Pull Request Process

### Use Our PR Template
We provide `.github/PULL_REQUEST_TEMPLATE/strict-validation.md` with a comprehensive checklist.

### PR Requirements
- [ ] **Tests pass**: `pytest` completes successfully
- [ ] **Linting passes**: `ruff check .` has no errors  
- [ ] **Types check**: `mypy src` has no errors
- [ ] **Coverage maintained**: New code is tested
- [ ] **Validation rules followed**: Strict rules enforced
- [ ] **Small, focused changes**: One feature/fix per PR

### CI/CD Pipeline
All PRs **must pass**:
- ✅ pytest (all tests)
- ✅ ruff check (linting)  
- ✅ mypy (type checking)
- ✅ coverage thresholds

---

## ⚠️ Common Pitfalls

### 1. Game ID String Coercion
```python
# ❌ WRONG: Leading zeros lost!
game_id = int("0022301234")  # Becomes 22301234
game_id = str(game_id)       # Now "22301234" (invalid)

# ✅ CORRECT: Keep as string
game_id = "0022301234"  # Preserve leading zeros
```

### 2. Season Format Confusion
```python
# ❌ WRONG: Inconsistent formats
season = "2024"          # Too short
season = "2024-2025"     # Too long  
season = "24-25"         # Two-digit years

# ✅ CORRECT: Standard format
season = "2024-25"       # Always YYYY-YY
```

### 3. Missing Async/Await
```python
# ❌ WRONG: Sync code in async context
def process_game(game_id):
    data = client.get_game_data(game_id)  # Missing await!

# ✅ CORRECT: Proper async handling
async def process_game(game_id: str) -> dict:
    data = await client.get_game_data(game_id)
    return data
```

### 4. Error Handling Anti-patterns
```python
# ❌ WRONG: Failing fast stops entire pipeline
if not valid_game_id(game_id):
    return {"error": "Invalid"}  # Pipeline stops!

# ✅ CORRECT: Aggregate errors, continue processing  
results = {"errors": []}
try:
    game_data = process_game_metadata(game_id)
except ValueError as e:
    results["errors"].append(f"Game validation: {e}")
    # Continue with PBP processing anyway
```

---

## 🏀 Understanding NBA Data

New to NBA data? Key concepts:

### Game Structure
- **Regular Season**: ~82 games per team (Oct-Apr)
- **Game ID**: `"0022301234"` where:
  - `0022` = regular season identifier
  - `23` = season year (2023-24) 
  - `01234` = sequential game number

### Season Format  
- **Correct**: `"2024-25"` (spans calendar years)
- **Why**: NBA seasons cross calendar years (Oct 2024 → Jun 2025)

### Data Sources
- **NBA Stats API**: Primary source (games, PBP, box scores)
- **Basketball Reference**: Backup source + additional stats
- **NBA Game Books**: Official PDFs (referee assignments)

---

## 📚 Resources

- **Documentation**: `README.md`, `DEV_NOTES.md`
- **Examples**: Check `tests/` directory for patterns
- **Architecture**: Review `src/nba_scraper/` structure  
- **Database Schema**: See `schema.sql`

---

## 🆘 Getting Help

- **GitHub Issues**: Bugs, features, questions
- **GitHub Discussions**: Technical discussions
- **Code Reviews**: Implementation feedback

### Debugging Tips
1. Enable verbose logging: `LOG_LEVEL=DEBUG` in `.env`
2. Test with single games first: `nba-scraper daily --date-range 2024-01-15`
3. Check database state: Use SQL queries to inspect data
4. Run tests frequently: Catch issues early

---

## 🎉 Thank You!

We appreciate your contributions! By following these guidelines, you help maintain a high-quality, reliable codebase for the NBA analytics community.

**Remember**: Quality over quantity. We prefer small, well-tested, properly validated contributions.

Happy coding! 🏀🚀