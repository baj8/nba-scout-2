#!/bin/bash
# Daily NBA data ingestion cron job
# Runs at 6 AM ET daily to process yesterday's games
# Add to crontab: 0 6 * * * /path/to/nba_scraper/ops/cron/run_daily.sh

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs/cron"
LOG_FILE="${LOG_DIR}/daily_$(date +%Y%m%d_%H%M%S).log"

# Create log directory
mkdir -p "$LOG_DIR"

# Activate virtual environment
source "${PROJECT_ROOT}/.venv/bin/activate"

# Run the daily job and capture output
echo "Starting daily job at $(date)" | tee -a "$LOG_FILE"
cd "$PROJECT_ROOT"

if python -m nba_scraper.cli schedule daily >> "$LOG_FILE" 2>&1; then
    echo "Daily job completed successfully at $(date)" | tee -a "$LOG_FILE"
    exit_code=0
else
    echo "Daily job failed at $(date)" | tee -a "$LOG_FILE"
    exit_code=1
fi

# Optional: Clean up old logs (keep last 30 days)
find "$LOG_DIR" -name "daily_*.log" -mtime +30 -delete 2>/dev/null || true

exit $exit_code
