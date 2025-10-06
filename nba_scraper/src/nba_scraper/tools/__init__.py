"""NBA Scraper tools module - lazy loading to avoid import side effects."""

__all__ = []

# No imports at import-time to avoid pulling in heavy dependencies
# Tools should be imported directly when needed