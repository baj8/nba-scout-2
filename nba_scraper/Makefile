# NBA Scraper Development Makefile
.PHONY: help setup install test test-unit test-integration lint format clean docs run-dev

# Default target
help:
	@echo "🏀 NBA Scraper Development Commands"
	@echo "===================================="
	@echo ""
	@echo "Setup & Installation:"
	@echo "  setup          - Complete development environment setup"
	@echo "  install        - Install dependencies (production + dev)"
	@echo "  install-prod   - Install production dependencies only"
	@echo ""
	@echo "Testing:"
	@echo "  test           - Run all tests"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint           - Run linting (ruff + mypy)"
	@echo "  format         - Format code (black + ruff)"
	@echo "  check-format   - Check if code is formatted"
	@echo ""
	@echo "Development:"
	@echo "  run-dev        - Run development server/CLI"
	@echo "  docs           - Generate documentation"
	@echo "  clean          - Clean build artifacts and cache"
	@echo ""
	@echo "Database:"
	@echo "  db-setup       - Initialize database schema"
	@echo "  db-test        - Run database tests"

# Environment setup
setup:
	@echo "🔧 Setting up development environment..."
	@chmod +x setup-dev.sh
	@./setup-dev.sh

# Dependency installation
install:
	@echo "📦 Installing all dependencies..."
	@pip install -e ".[dev]"

install-prod:
	@echo "📦 Installing production dependencies..."
	@pip install -e .

# Testing targets
test:
	@echo "🧪 Running all tests..."
	@python -m pytest tests/ -v --tb=short

test-unit:
	@echo "🧪 Running unit tests..."
	@python -m pytest tests/unit/ -v --tb=short

test-integration:
	@echo "🧪 Running integration tests..."
	@python -m pytest tests/integration/ -v --tb=short

test-coverage:
	@echo "📊 Running tests with coverage..."
	@python -m pytest tests/ --cov=src/nba_scraper --cov-report=html --cov-report=term-missing

# Code quality
lint:
	@echo "🔍 Running linters..."
	@echo "  → ruff check..."
	@python -m ruff check src/ tests/
	@echo "  → mypy check..."
	@python -m mypy src/nba_scraper/

format:
	@echo "🎨 Formatting code..."
	@echo "  → black formatting..."
	@python -m black src/ tests/
	@echo "  → ruff auto-fix..."
	@python -m ruff check src/ tests/ --fix

check-format:
	@echo "🔍 Checking code formatting..."
	@python -m black --check src/ tests/
	@python -m ruff check src/ tests/

# Development utilities
run-dev:
	@echo "🚀 Running in development mode..."
	@python -m nba_scraper --help

docs:
	@echo "📖 Generating documentation..."
	@echo "Documentation generation not yet implemented"

clean:
	@echo "🧹 Cleaning build artifacts..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf dist/ build/ htmlcov/ 2>/dev/null || true

# Database utilities
db-setup:
	@echo "🗄️  Setting up database..."
	@echo "Database setup not yet implemented - see schema.sql"

db-test:
	@echo "🗄️  Testing database connection..."
	@echo "Database tests not yet implemented"

# Development workflow shortcuts
dev-setup: setup install lint test-unit
	@echo "✅ Development environment ready!"

ci-test: lint test-coverage
	@echo "✅ CI pipeline complete!"

# Quick validation
validate:
	@echo "✅ Running quick validation..."
	@python -c "import nba_scraper; print('✅ Package import successful')"
	@python -m pytest tests/unit/test_bref_crosswalk_resolver.py -q
	@echo "✅ All validations passed!"