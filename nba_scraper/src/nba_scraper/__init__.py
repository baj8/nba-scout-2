"""NBA Historical Scraping & Ingestion Engine.

Production-grade data pipeline for fetching, normalizing, validating,
and persisting NBA historical datasets with async IO and rate limiting.
"""

__version__ = "1.0.0"
__author__ = "NBA Scout"
__email__ = "contact@nbascout.com"

from .config import Settings, get_settings

__all__ = ["Settings", "get_settings"]