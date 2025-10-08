"""NBA Historical Scraping & Ingestion Engine.

Production-grade data pipeline for fetching, normalizing, validating,
and persisting NBA historical datasets with async IO and rate limiting.
"""

from .version import __version__, __author__, __email__
from .config import AppSettings, get_settings

# Backward compatibility alias
Settings = AppSettings

__all__ = [
    "__version__",
    "__author__", 
    "__email__",
    "Settings",
    "AppSettings",
    "get_settings",
    # Key subpackages
    "models",
    "extractors",
    "transformers",
    "loaders",
    "pipelines",
    "io_clients",
    "utils",
    "validation",
]