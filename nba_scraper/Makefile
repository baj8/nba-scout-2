# NBA Scraper Development Makefile
.PHONY: help setup install install-prod lint typecheck test test-unit test-int cov precommit-install format clean dev-setup ci-test validate

# Default target
help:
	@echo "NBA Scraper Development Tools"
	@echo "============================="
	@echo ""
	@echo "Setup:"
	@echo "  setup          - Create venv and install dev dependencies"
	@echo "  install        - Install all dependencies"
	@echo "  install-prod   - Install production dependencies only"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint           - Run linting (ruff check)"
	@echo "  typecheck      - Run type checking (mypy)"
	@echo "  format         - Format code (ruff --fix)"
	@echo "  precommit-install - Install pre-commit hooks"
	@echo ""
	@echo "Testing:"
	@echo "  test           - Run all tests"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-int       - Run integration tests only"
	@echo "  cov            - Run tests with coverage report"
	@echo ""
	@echo "Development:"
	@echo "  clean          - Clean build artifacts and cache"
	@echo "  dev-setup      - Complete development setup"
	@echo "  ci-test        - Run CI pipeline locally"
	@echo "  validate       - Quick validation"

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

# Code quality targets
lint:
	@echo "🔍 Running linting..."
	@python -m ruff check src/ tests/

typecheck:
	@echo "🔍 Running type checking..."
	@python -m mypy src/

format:
	@echo "🎨 Formatting code..."
	@python -m ruff check --fix src/ tests/
	@python -m ruff format src/ tests/

precommit-install:
	@echo "🔗 Installing pre-commit hooks..."
	@pre-commit install

# Testing targets
test:
	@echo "🧪 Running all tests..."
	@python -m pytest tests/ -v --tb=short

test-unit:
	@echo "🧪 Running unit tests..."
	@python -m pytest tests/unit/ -v --tb=short

test-int:
	@echo "🧪 Running integration tests..."
	@python -m pytest tests/integration/ -v --tb=short -m integration

cov:
	@echo "📊 Running tests with coverage..."
	@python -m pytest tests/ --cov=src/nba_scraper --cov-report=term-missing --cov-report=html

# Clean up
clean:
	@echo "🧹 Cleaning up..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
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

ci-test: lint typecheck cov
	@echo "✅ CI pipeline complete!"

# Quick validation
validate:
	@echo "✅ Running quick validation..."
	@python -c "import nba_scraper; print('✅ Package import successful')"
	@python -m pytest tests/unit/test_bref_crosswalk_resolver.py -q
	@echo "✅ All validations passed!"