"""Raw NBA data harvesting module for bronze layer data capture.

This module provides comprehensive tools for harvesting raw NBA Stats API data
with rate limiting, retry logic, and structured persistence.

Components:
- client: RawNbaClient for API interactions with browser-like headers
- persist: JSON writing, compression, and manifest management utilities  
- backfill: Core orchestration for date-by-date and game-by-game harvesting
- report: Summary and analysis utilities for harvest results

Command-line tools:
- raw_harvest_date: Harvest data for a single date
- raw_harvest_season: Harvest data for an entire season (2021-22 to 2024-25)
"""

from .client import RawNbaClient
from .persist import write_json, update_manifest, read_manifest, append_quarantine, ensure_dir
from .backfill import harvest_date
from .report import summarize_date, summarize_season, format_summary_for_display

__all__ = [
    'RawNbaClient',
    'write_json', 
    'update_manifest',
    'read_manifest', 
    'append_quarantine',
    'ensure_dir',
    'harvest_date',
    'summarize_date',
    'summarize_season', 
    'format_summary_for_display'
]