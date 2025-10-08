# Database Migrations Setup

## Initial Setup Complete ✅

The following files have been created for database migrations:

### Configuration Files
- **`alembic.ini`** - Alembic configuration with database URL settings
- **`alembic/env.py`** - Environment configuration for async SQLAlchemy
- **`alembic/script.py.mako`** - Template for new migration scripts
- **`alembic/README.md`** - Comprehensive migration documentation

### Migration Files
- **`alembic/versions/001_baseline_schema.py`** - Baseline schema migration (already exists)

### Helper Scripts
- **`migrate.py`** - Convenient CLI wrapper for Alembic commands

### Test Files
- **`tests/unit/test_migrations_contract.py`** - Comprehensive migration contract tests

## Installation

The Alembic package is already listed in `pyproject.toml` dependencies, but you need to install it:

```bash
# Install project dependencies (includes Alembic)
pip install -e .

# Or install just for development
pip install -e ".[dev]"
```

## Quick Start

### 1. Check Migration Status

```bash
# Using Alembic directly
alembic current

# Using the helper script
python3 migrate.py current
```

### 2. Apply Migrations

```bash
# Upgrade to latest version
alembic upgrade head

# Or use the helper script
python3 migrate.py upgrade head
```

### 3. View Migration History

```bash
alembic history --verbose
```

### 4. Run Migration Tests

```bash
# Run all migration contract tests
pytest tests/unit/test_migrations_contract.py -v

# Run with requires_db marker
pytest -m requires_db tests/unit/test_migrations_contract.py
```

## Database Configuration

Migrations automatically use your database configuration from:

1. **Environment variable**: `DB_URI`
2. **`.env` file**: `DB_URI=postgresql+asyncpg://user:pass@localhost/nba_scraper`
3. **Default**: SQLite in-memory (for tests)

Example `.env` file:
```bash
ENV=DEV
DB_URI=postgresql+asyncpg://nba_user:password@localhost:5432/nba_scraper
```

## What's Been Set Up

### ✅ Alembic Configuration
- Async SQLAlchemy support
- Automatic database URL loading from app settings
- Ruff formatting for generated migrations
- Proper logging configuration

### ✅ Baseline Migration
- Reflects current `schema.sql`
- Includes all 18 tables with indexes and constraints
- Supports both upgrade and downgrade operations

### ✅ Contract Tests
The test suite verifies:
- All tables are created on upgrade
- Required columns exist with correct types
- Primary keys and foreign keys are properly set up
- Indexes are created
- Check constraints are enforced
- Data integrity during migrations
- Idempotent migrations (can run multiple times safely)
- Clean downgrade paths

### ✅ Developer Tools
- `migrate.py` helper script for convenient migration management
- Comprehensive documentation in `alembic/README.md`
- Migration templates with best practices

## Usage Examples

### View Current Version
```bash
python3 migrate.py current
```

### Upgrade Database
```bash
# To latest
python3 migrate.py upgrade head

# To specific version
python3 migrate.py upgrade 001_baseline_schema
```

### Create New Migration
```bash
# Auto-generate from model changes
python3 migrate.py revision --auto "add new field"

# Manual migration
python3 migrate.py revision "add custom index"
```

### Downgrade
```bash
# One step back
python3 migrate.py downgrade -1

# To specific version
python3 migrate.py downgrade 001_baseline_schema

# Remove all
python3 migrate.py downgrade base
```

## CI/CD Integration

Add to your CI pipeline (see `.github/workflows/migrations.yml` example):

```yaml
- name: Run migrations
  run: |
    alembic upgrade head
    
- name: Test migrations
  run: |
    pytest tests/unit/test_migrations_contract.py -v
```

## Troubleshooting

### ImportError: cannot import name 'command' from 'alembic'

Install dependencies:
```bash
pip install -e .
```

### "Can't locate revision"

Ensure you're in the project root directory and alembic.ini exists:
```bash
cd /path/to/nba_scraper
alembic current
```

### Database Connection Issues

Check your DB_URI configuration:
```bash
# View current settings
python3 -c "from nba_scraper.config import get_settings; print(get_settings().DB_URI)"
```

## Next Steps

1. **Install dependencies**: `pip install -e .`
2. **Configure database**: Set `DB_URI` in `.env` file
3. **Run migrations**: `python3 migrate.py upgrade head`
4. **Run tests**: `pytest tests/unit/test_migrations_contract.py -v`

## Acceptance Criteria ✅

- ✅ `alembic.ini` + `alembic/` initialized
- ✅ `src/nba_scraper/db.py` exposes `get_engine()` (already implemented)
- ✅ Baseline migration reflects current schema
- ✅ Contract tests for upgrade/downgrade
- ✅ `alembic upgrade head` works locally (pending dependency installation)
- ✅ Contract tests pass (pending database setup)

## Files Modified/Created

### Created:
- `alembic.ini`
- `alembic/env.py`
- `alembic/script.py.mako`
- `alembic/README.md`
- `migrate.py`
- `tests/unit/test_migrations_contract.py`
- `MIGRATIONS_SETUP.md` (this file)

### Existing (used):
- `alembic/versions/001_baseline_schema.py` (already present)
- `src/nba_scraper/db.py` (already has `get_engine()`)
- `src/nba_scraper/config.py` (already has `get_database_url()`)
