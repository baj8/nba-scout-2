# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2025-10-05

### Added
- Strict `transform_game` validation with game_id format `^0022\d{6}$` (10-char numeric, starts with "0022")
- Season validation with warning + fallback mechanism for malformed formats
- Comprehensive error aggregation in foundation pipeline with `result["errors"]` list
- Smart season derivation using `derive_season_smart()` for invalid/missing season data
- Robust pipeline error handling that continues PBP/lineup processing on game validation failure

### Changed
- Foundation pipeline now continues processing other components when game validation fails
- Error handling strategy: aggregate errors instead of failing fast
- Season validation: log warning "season format invalid" then derive instead of strict rejection
- Enhanced logging with structured context throughout validation pipeline

### Fixed
- Preserved leading zeros in game_id during preprocessing (e.g., "0022301234" stays string)
- Removed deprecated `datetime.utcnow()` usage throughout codebase
- Improved error message format consistency for validation failures
- Pipeline API response preservation and reuse across processing steps

### Tests
- Game transformer validation tests: 22/22 passing
- Foundation pipeline validation tests: 8/8 passing  
- Integration test coverage for error handling scenarios
- Comprehensive edge case validation coverage

## [1.0.0] - 2025-10-05

### Added
- Initial release of NBA Historical Scraping & Ingestion Engine
- Complete NBA Stats API integration with game data extraction
- Foundation pipeline with Extract-Transform-Load architecture
- Database models and schema for games, play-by-play, and lineup data
- Robust preprocessing and clock parsing utilities
- CLI interface with Typer integration
- Comprehensive test suite with pytest and async support