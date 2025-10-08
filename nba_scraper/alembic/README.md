# Alembic Database Migrations

This directory contains database migrations for the NBA Scraper project using Alembic.

## Overview

Alembic is a database migration tool for SQLAlchemy. It allows you to version control your database schema and safely apply changes.

## Directory Structure

```
alembic/
├── env.py                 # Alembic environment configuration
├── script.py.mako         # Template for new migration scripts
└── versions/              # Migration scripts
    └── 001_baseline_schema.py  # Baseline schema migration
```

## Usage

### Check Current Migration Version

```bash
alembic current
```

### Upgrade to Latest Version

```bash
alembic upgrade head
```

### Upgrade to Specific Version

```bash
alembic upgrade 001_baseline_schema
```

### Downgrade One Step

```bash
alembic downgrade -1
```

### Downgrade to Base (Remove All)

```bash
alembic downgrade base
```

### View Migration History

```bash
alembic history --verbose
```

### Create New Migration

```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "description of changes"

# Create empty migration template
alembic revision -m "description of changes"
```

## Baseline Migration

The baseline migration (`001_baseline_schema`) captures the current production schema as documented in `schema.sql`. It includes:

- Core game tracking tables
- Player and referee tracking
- Play-by-play events
- Advanced analytics tables
- Indexes and constraints

All future migrations will build upon this baseline.

## Configuration

Alembic reads configuration from:
1. `alembic.ini` - Main configuration file
2. `alembic/env.py` - Python environment configuration
3. Application settings via `nba_scraper.config.get_settings()`

The database URL is automatically loaded from your application's configuration:
- Environment variable: `DB_URI`
- `.env` file
- Default: SQLite in-memory (for tests)

## Testing

Run the migration contract tests to ensure migrations work correctly:

```bash
# Run all migration tests
pytest tests/unit/test_migrations_contract.py -v

# Run specific test
pytest tests/unit/test_migrations_contract.py::TestMigrationContract::test_upgrade_to_head_creates_all_tables -v

# Run with database marker
pytest -m requires_db tests/unit/test_migrations_contract.py
```

## Best Practices

### Creating Migrations

1. **Always test migrations** on a copy of production data before deploying
2. **Make migrations reversible** when possible (implement downgrade)
3. **Keep migrations small** and focused on one logical change
4. **Document migrations** with clear commit messages
5. **Test data migrations** to ensure data integrity

### Migration Naming

Use descriptive names that explain what the migration does:
- `add_player_metrics_table`
- `add_index_games_date`
- `alter_pbp_events_add_coordinates`

### Handling Data Migrations

When adding new columns with NOT NULL constraints:

```python
def upgrade():
    # Add column as nullable first
    op.add_column('games', sa.Column('new_field', sa.Text(), nullable=True))
    
    # Populate data
    op.execute("UPDATE games SET new_field = 'default_value'")
    
    # Make it NOT NULL
    op.alter_column('games', 'new_field', nullable=False)
```

### Rolling Back

If a migration fails in production:
1. Check the error in logs
2. Fix data issues if necessary
3. Downgrade: `alembic downgrade -1`
4. Fix the migration script
5. Re-run: `alembic upgrade head`

## CI/CD Integration

In your CI/CD pipeline, ensure migrations are tested:

```bash
# Check migrations can be applied
alembic upgrade head

# Run contract tests
pytest tests/unit/test_migrations_contract.py
```

## Troubleshooting

### "Target database is not up to date"

Your database schema doesn't match the expected migration state:

```bash
# Check current version
alembic current

# View history
alembic history

# Upgrade or downgrade as needed
alembic upgrade head
```

### "Can't locate revision"

Migration files are out of sync:

```bash
# Ensure you have all migration files
git pull

# Check history
alembic history
```

### "Duplicate column name"

Migration conflicts with existing schema:

```bash
# Check if column already exists
psql -c "\d+ games"

# Skip problematic migration or fix it
alembic downgrade -1
```

## Development Workflow

1. **Make schema changes** to SQLAlchemy models or schema.sql
2. **Generate migration**: `alembic revision --autogenerate -m "description"`
3. **Review migration** - ensure it does what you expect
4. **Test migration**: `pytest tests/unit/test_migrations_contract.py`
5. **Apply migration**: `alembic upgrade head`
6. **Commit**: Include both model changes and migration file

## Additional Resources

- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- Project schema: `schema.sql`
- Database utilities: `src/nba_scraper/db.py`
