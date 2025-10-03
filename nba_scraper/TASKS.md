# Tasks (Kanban)

## TRANCHE APPROACH: ADVANCED METRICS IMPLEMENTATION

### TRANCHE 1: NBA API ADVANCED METRICS (PRIORITY)
**Goal**: Maximize NBA Stats API coverage for advanced metrics since it provides rich, real-time data
**Target**: Get 80%+ of advanced metrics from NBA API to minimize external dependencies

**NBA API Advanced Metrics Available**:
- **Player Efficiency**: Usage Rate, Player Efficiency Rating (PER), True Shooting %, Effective FG%
- **Team Efficiency**: Offensive/Defensive Rating, Pace, Assist/TO Ratio, Rebound %
- **Advanced Box Score**: Plus/Minus, Win Shares components, Box Plus/Minus foundations
- **Shot Quality**: Shot Chart data, Shooting efficiency by zone/distance
- **Lineup Analytics**: Net Rating, Efficiency differentials when players on/off court

**Implementation**:
- [ ] Enhance NBA Stats boxscore extractor to capture advanced player stats
- [ ] Add team efficiency metrics extraction from NBA API responses
- [ ] Implement player impact metrics (usage rate, efficiency rating) from API data
- [ ] Create advanced team analytics from NBA API team stats endpoints

### TRANCHE 2: DERIVED ANALYTICS FROM NBA API DATA
**Goal**: Compute sophisticated metrics from NBA API's rich play-by-play and box score data
**Target**: Fill gaps in NBA API coverage with computed metrics from their detailed data

**Computed Advanced Metrics**:
- **Player Impact**: Net Rating, On/Off Court differentials, Clutch performance
- **Team Chemistry**: Ball movement metrics, assist networks, pace impact
- **Game Flow**: Momentum shifts, run analysis, lead changes impact
- **Situational Analytics**: Performance in close games, back-to-backs, altitude effects

**Implementation**:
- [ ] Enhance analytics pipeline to compute player impact from PBP data
- [ ] Add team chemistry metrics from NBA API assist/turnover data
- [ ] Implement game flow analysis from NBA API play-by-play events
- [ ] Create situational performance metrics from NBA API game context

### TRANCHE 3: BASKETBALL REFERENCE SUPPLEMENTAL METRICS
**Goal**: Fill specific gaps that NBA API doesn't provide well
**Target**: Only scrape B-Ref for metrics not available or poorly covered by NBA API

**B-Ref Unique Value**:
- **Historical Context**: All-time rankings, career trajectories, season comparisons
- **Advanced Ratios**: Some proprietary B-Ref calculations (BPM, VORP nuances)
- **Injury Context**: More detailed injury reporting and impact analysis
- **Matchup History**: Head-to-head performance trends

**Implementation**:
- [ ] Identify specific metrics not well-covered by NBA API
- [ ] Implement targeted B-Ref scraping for unique metrics only
- [ ] Focus on historical context and proprietary B-Ref calculations
- [ ] Add injury impact analysis from B-Ref detailed reports

### TRANCHE 4: EXTERNAL ANALYTICS INTEGRATION
**Goal**: Integrate specialized external sources for niche advanced metrics
**Target**: Add unique insights not available from primary sources

**External Sources**:
- **Cleaning the Glass**: Opponent-adjusted metrics, garbage time filtering
- **PBP Stats**: Detailed possession-level analytics
- **Synergy Sports**: Play type efficiency (if accessible)
- **Custom Calculations**: Proprietary metrics specific to scouting needs

**Implementation**:
- [ ] Research API availability for external advanced metrics sources
- [ ] Implement selective integration for unique metric categories
- [ ] Add custom scouting-specific metrics calculations
- [ ] Create composite metrics combining multiple sources

### TRANCHE 5: PREDICTIVE & ML-BASED ADVANCED METRICS
**Goal**: Implement predictive analytics and machine learning-derived metrics for scouting insights
**Target**: Add forward-looking metrics and player/team trend analysis for competitive advantage

**Predictive Analytics**:
- **Performance Forecasting**: Next-game performance predictions based on historical patterns
- **Injury Risk Assessment**: Predictive models for injury likelihood based on workload/fatigue metrics
- **Breakout Player Detection**: ML models to identify players likely to have career years
- **Team Chemistry Prediction**: Models to predict lineup effectiveness and player compatibility
- **Clutch Performance Modeling**: Situational performance predictions in high-pressure moments

**Machine Learning Metrics**:
- **Player Similarity Scores**: Vector-based player comparison using advanced stats
- **Shot Quality Models**: xFG% models predicting shot success probability
- **Lineup Optimization**: ML-driven recommendations for optimal player combinations
- **Game Flow Prediction**: Models predicting momentum shifts and run probabilities
- **Value Over Replacement**: Dynamic VORP calculations based on current market conditions

**Scouting-Specific Analytics**:
- **Development Trajectory**: Young player improvement rate predictions
- **Trade Value Models**: Player value assessment for trade scenarios
- **Contract Efficiency**: Performance per dollar analysis with future projections
- **Matchup Exploitation**: Models identifying favorable player/team matchups
- **Load Management Optimization**: Fatigue-aware performance and health predictions

**Implementation**:
- [ ] Build ML pipeline infrastructure for model training and inference
- [ ] Implement feature engineering from existing NBA API and derived metrics
- [ ] Create predictive models for player performance and team outcomes
- [ ] Add real-time model scoring and prediction storage
- [ ] Develop scouting dashboard with ML-driven insights and recommendations

## IN PROGRESS
- [ ] **End-to-end pipeline validation** - Run comprehensive backfill test across multiple data sources (NBA Stats, BRef, Gamebooks) to verify robust data ingestion now that enum issues are resolved
- [ ] **Production data backfill** - Execute actual historical data ingestion for 2024-25 season with validated enum preprocessing

## NEXT
- [ ] **Analytics pipeline execution** - Run derived analytics (Q1 window, early shocks, schedule travel) on ingested game data
- [ ] **Performance monitoring** - Monitor enum preprocessing impact on large-scale data ingestion
- [ ] **Data quality validation** - Comprehensive validation of ingested data quality and completeness
- [ ] **PBP transformer implementation** - Complete play-by-play data transformation (currently stub)
- [ ] **Error monitoring dashboard** - Set up production monitoring for ongoing data quality issues

## IMMEDIATE PRIORITIES (Next 24-48 Hours)
1. **NBA API Advanced Metrics** - Enhance NBA Stats extractors to capture all available advanced metrics (Tranche 1)
2. **Analytics Pipeline Enhancement** - Extend current analytics pipeline to compute derived advanced metrics from NBA API data (Tranche 2)
3. **Production backfill execution** - Run full 2024-25 season backfill with enum fixes validated
4. **Analytics pipeline testing** - Verify derived transformers work with real ingested data
5. **Performance benchmarking** - Measure preprocessing overhead on large datasets
6. **Monitoring setup** - Configure alerts for data ingestion health
7. **Documentation update** - Document the enum preprocessing solution

## TECHNICAL DEBT & IMPROVEMENTS
- [ ] **Comprehensive enum field validation audit** - Review all model field validators for potential int/str comparison issues in other models (RefAssignmentRow, StartingLineupRow, InjuryStatusRow)
- [ ] **Preprocessing consistency** - Ensure all model instantiation points in extractors and transformers apply preprocessing consistently
- [ ] **Performance benchmarking** - Measure impact of model-level preprocessing on ingestion throughput
- [ ] **Type safety improvements** - Add more robust type hints for NBA Stats API response handling
- [ ] **Logging enhancement** - Add detailed logging for enum preprocessing steps to aid debugging

**Advanced Metrics Technical Debt**:
- [ ] **NBA API Response Mapping** - Create comprehensive mapping of all NBA Stats API response fields to advanced metrics
- [ ] **Metrics Calculation Library** - Build reusable library for advanced basketball analytics calculations
- [ ] **Performance Optimization** - Optimize advanced metrics calculations for large dataset processing
- [ ] **Metrics Validation** - Add validation comparing computed metrics against known external sources

## VALIDATION & TESTING
- [ ] **Unit tests for preprocessing** - Add comprehensive tests for model-level preprocessing functions
- [ ] **Integration tests for enum handling** - Test NBA Stats API data with known problematic integer enum codes
- [ ] **Edge case testing** - Test with malformed data, missing fields, and unexpected enum values
- [ ] **Performance tests** - Benchmark preprocessing overhead on large datasets
- [ ] **Data quality tests** - Validate that preprocessing maintains data integrity and accuracy

**Advanced Metrics Testing**:
- [ ] **NBA API Coverage Testing** - Validate that NBA API extractors capture all available advanced metrics fields
- [ ] **Metrics Calculation Testing** - Unit tests for all advanced basketball analytics formulas
- [ ] **Cross-Source Validation** - Compare computed metrics against Basketball Reference for accuracy
- [ ] **Performance Benchmarking** - Test advanced metrics computation performance on large datasets

## MONITORING & OBSERVABILITY
- [ ] **Enum validation metrics** - Add metrics for preprocessing success/failure rates
- [ ] **Data quality dashboards** - Create monitoring for enum validation errors and preprocessing statistics
- [ ] **Alert configuration** - Set up alerts for enum validation failures and data ingestion issues
- [ ] **Performance monitoring** - Track preprocessing impact on pipeline performance

**Advanced Metrics Monitoring**:
- [ ] **Metrics Completeness Tracking** - Monitor percentage of games with complete advanced metrics
- [ ] **Calculation Accuracy Monitoring** - Track advanced metrics calculation success rates
- [ ] **NBA API Coverage Metrics** - Monitor which advanced metrics are successfully extracted from NBA API

## DOCUMENTATION
- [ ] **Enum preprocessing documentation** - Document the int/str comparison issue and our preprocessing solution
- [ ] **NBA Stats API integration guide** - Create comprehensive guide for handling NBA Stats data types
- [ ] **Troubleshooting guide** - Document common enum validation issues and solutions
- [ ] **Architecture decision record** - Document the model-level preprocessing approach and rationale

**Advanced Metrics Documentation**:
- [ ] **Tranche Implementation Guide** - Document the prioritized approach to advanced metrics implementation
- [ ] **NBA API Advanced Metrics Mapping** - Comprehensive documentation of NBA Stats API advanced metrics coverage
- [ ] **Metrics Calculation Reference** - Document all basketball analytics formulas and data sources used
- [ ] **Source Priority Decision Matrix** - Guide for choosing between NBA API, B-Ref, and external sources for specific metrics

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
- [x] **Enum validation architecture** - Identified root cause of int/str comparison errors in Pydantic enum validation. NBA Stats API sends integer enum codes (1, 2, 3) but Pydantic tries to compare with string enum values. Implemented comprehensive model-level preprocessing with `@model_validator(mode='before')` decorators on critical models (PbpEventRow, GameRow) to convert all integer enum codes to strings before field validation runs.
- [x] **Critical int/str enum validation fixes** - Implemented model-level preprocessing to prevent int/str comparison errors in Pydantic enum validation. Added `@model_validator(mode='before')` to PbpEventRow and GameRow with comprehensive NBA Stats preprocessing pipeline. Tested and validated successfully.

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