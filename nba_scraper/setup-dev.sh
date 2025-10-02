#!/bin/bash
# NBA Scraper Development Setup Script
set -e

echo "🏀 NBA Scraper Development Setup"
echo "================================="

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
echo "✅ Python version: $python_version"

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
else
    echo "✅ Virtual environment exists"
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1

# Install dependencies
echo "📚 Installing dependencies..."
pip install -e ".[dev]" > /dev/null 2>&1

# Verify key imports
echo "🧪 Verifying key imports..."
python -c "
import nba_scraper
from nba_scraper.transformers.games import BRefCrosswalkResolver
from nba_scraper.io_clients.gamebooks import GamebooksClient
print('✅ All key imports successful')
"

# Run a quick test
echo "🎯 Running quick test..."
python -m pytest tests/unit/test_bref_crosswalk_resolver.py -q > /dev/null 2>&1 && echo "✅ BRef crosswalk tests pass"

echo ""
echo "🎉 Development environment setup complete!"
echo ""
echo "📋 Next steps:"
echo "   1. source .venv/bin/activate  # Activate environment"
echo "   2. nba-scraper --help         # Check CLI commands"
echo "   3. pytest tests/unit/         # Run unit tests"
echo "   4. make lint                  # Run code quality checks"
echo ""
echo "📖 See README.md and DEV_NOTES.md for more information"