<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# NBA Historical Scraping & Ingestion Engine - Copilot Instructions

This is a production-grade data pipeline for NBA historical data with the following key characteristics:

## Architecture & Patterns
- **Async-first**: All IO operations use asyncio and httpx
- **Rate limiting**: Token bucket with 45 req/min limit and exponential backoff
- **Idempotent upserts**: Use `ON CONFLICT DO UPDATE SET ... WHERE excluded.col IS DISTINCT FROM target.col`
- **Streaming**: No season-wide in-memory loads, process in chunks
- **Provenance**: Every table has `source`, `source_url`, and `ingested_at_utc` columns
- **UTC normalization**: All timestamps stored in UTC, arena timezone preserved

## Data Flow
1. **Extract**: NBA Stats API, Basketball Reference HTML, NBA Game Books PDFs
2. **Transform**: Pydantic models with validation, tricode normalization, name slugs
3. **Load**: PostgreSQL with diff-aware upserts, foreign key constraints
4. **Derive**: Q1 analytics, early shocks, schedule travel, outcomes

## Key Conventions
- **Team tricodes**: Normalized via `team_aliases.yaml`, always uppercase
- **Name slugs**: PascalCase with punctuation removed (e.g., "LeBron James" → "LebronJames")
- **Enums**: Strict validation for event types, injury status, referee roles
- **Error handling**: Structured logging with trace IDs, graceful degradation
- **Testing**: Golden files for deterministic parsing, no live API calls in CI

## Database Schema
- Primary tables: `games`, `game_id_crosswalk`, `ref_assignments`, `starting_lineups`, `pbp_events`
- Derived tables: `q1_window_12_8`, `early_shocks`, `schedule_travel`, `outcomes`
- All tables have composite natural primary keys where appropriate
- Indexes on commonly queried columns (game_id, team_tricode, date ranges)

## Code Style
- Type hints everywhere, Pydantic for data validation
- Pure functions for extractors and transformers
- Async context managers for resources
- Rich CLI with progress bars and structured output
- Defensive parsing with fallbacks and logging