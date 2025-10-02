# Tasks (Kanban)

## IN PROGRESS

## NEXT
- [ ] **Analytics pipeline execution** - Run derived analytics on ingested game data
- [ ] **PBP transformer implementation** - Complete play-by-play data transformation (currently stub)

## BACKLOG

## DONE
- [x] **Core project structure** - Initial scaffolding, models, enums complete
- [x] **PBP event normalization** - EventType taxonomy, player name slugs established  
- [x] **Rate limiting + HTTP client** - Token bucket (45 req/min), exponential backoff with live API functionality tested and working
- [x] **Database schema** - PostgreSQL DDL with proper indexes and constraints
- [x] **early_shocks transformer** - Q1 disruption detection (TWO_PF_EARLY, TECH, FLAGRANT, INJURY_LEAVE) with comprehensive tests
- [x] **derived.py loader upserts** - Idempotent upserts for q1_window_12_8, early_shocks, schedule_travel with diff-aware updates and enum mapping
- [x] **Q1 window transformer** - Enhanced bonus timing detection, pace calculations, rebound percentages with 17 comprehensive test cases
- [x] **Integration test for early shocks** - End-to-end pipeline validation with realistic fixture data covering all shock types and database integration
- [x] **Schedule travel transformer** - Circadian/altitude analytics with haversine distance, B2B/3in4/5in7 detection, timezone impact modeling, and Denver altitude effects
- [x] **Ref/alternate extraction from Game Books (PDF)** - Complete PDF → text fallback with pdfminer; structured referee crew parsing with enhanced role detection, arena extraction, technical fouls parsing, and confidence scoring system. Tested and validated for production use.
- [x] **Data quality: stricter FK/uniqueness checks** - Comprehensive validation system with pre-insert FK validation, orphaned record detection, uniqueness constraint checking, cross-table consistency validation, and integrated validation in DerivedLoader. Prevents FK violations and provides detailed data quality reporting with confidence scoring.
- [x] **PBP monotonicity validation** - Complete play-by-play sequence validation with event index gap detection, overlap detection, clock progression validation, period boundary checks, and completeness statistics. Integrated into main validation workflow with comprehensive test coverage.
- [x] **BRef crosswalk resolver edge cases** - Complete Basketball Reference game ID resolution with fuzzy date matching for postponed/suspended/makeup games. Handles alternative tricode mappings (CHA→CHO, PHX→PHO), historical team variations (BRK→NJN, OKC→SEA), date extraction from game IDs, and comprehensive fallback logic with confidence scoring. Includes 15 comprehensive test cases covering all edge scenarios.
- [x] **Dependency management & testing infrastructure** - Fix version conflicts, enable full test suite execution, add dev/prod dependency separation.
- [x] **Live API integration testing** - Comprehensive integration tests for NBA Stats API, Basketball Reference scraping, and gamebook downloads with proper mocking, golden fixtures, error handling scenarios, and end-to-end pipeline validation. All 19 tests passing with production-ready reliability testing.
- [x] **CLI interface completion** - Complete command-line interface with BackfillPipeline, DailyPipeline, DerivePipeline, and ValidationPipeline. Rich progress bars, comprehensive status command with database statistics, error handling, and dry-run support for all major operations.
- [x] **Configuration management enhancement** - Comprehensive environment-specific configuration system with nested config structures (DatabaseConfig, APIKeysConfig, CacheConfig, MonitoringConfig), SecretStr for secure API key handling, environment validation, and CLI config command. Supports development/staging/production environments with proper validation and extensive documentation.
- [x] **Error handling & monitoring** - Production-ready error tracking and monitoring system with enhanced structured logging, Sentry integration, Slack/PagerDuty alerting, circuit breaker pattern, enhanced retry logic with multiple backoff strategies, comprehensive health checks, HTTP monitoring endpoints, Prometheus metrics export, and CLI commands for monitoring management. Includes thread-safe metrics collection, alert cooldown logic, and comprehensive test coverage.
- [x] **Performance optimization** - Database queries, transformer logic optimization with query performance improvements, database indexes, parallel processing for large datasets.
- [x] **Dependency injection & transformer system** - Fixed all BaseTransformer dependency injection issues, resolved LineupTransformer/PbpTransformer import conflicts, corrected source parameter requirements, and established proper transformer inheritance patterns. System now successfully initializes all pipeline components without errors.
- [x] **Production data ingestion** - NBA game discovery and population system working end-to-end. Successfully integrated NBA Stats API with proper season format conversion ('2023' → '2023-24'), validation, and database storage. Created populate_games.py script for backfilling game data. Tested with February 14, 2024 (13 games) - all games successfully discovered, validated, and stored in database.

---

# Copilot Working Agreement

## Before Starting Any Task
- Always read **DEV_NOTES.md** first for project context
- Review **CURRENT TASK** section for active focus area
- Check **Authoritative Context Pins** for relevant files to open

## Code Standards
- Adhere to **Non-Negotiables** (rate limits, idempotence, provenance, UTC time)
- Follow **Output Contract** (full files, tests, commit summary, next steps)
- Prefer **pure, small functions** with explicit type hints
- Use existing logging infrastructure (`logging.py`/structlog) with trace IDs

## Testing Requirements  
- No live HTTP calls in tests; use **golden fixtures** from `tests/data/`
- Unit tests for transformers must be deterministic and fast
- Integration tests should use sample dates with known outcomes
- All new code requires meaningful test coverage

## Database Patterns
- Use idempotent upserts: `ON CONFLICT DO UPDATE ... WHERE excluded.col IS DISTINCT FROM target.col`
- Include provenance: `source`, `source_url`, `ingested_at_utc` in all rows
- Respect foreign key constraints and natural primary keys

## When Task Complete
- Move task from current section to **DONE** with brief completion note
- Update **CURRENT TASK** in DEV_NOTES.md if shifting focus
- Run full test suite and ensure CI passes before marking done