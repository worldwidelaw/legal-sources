"""
World Wide Law - Common Libraries
Shared infrastructure for building EU legal data scrapers.
"""

from .base_scraper import BaseScraper
from .storage import StorageManager
from .rate_limiter import RateLimiter
from .validators import SchemaValidator

# BrowserScraper is imported lazily to avoid requiring playwright
# for sources that don't need it. Import explicitly:
#   from common.browser_scraper import BrowserScraper

__all__ = ["BaseScraper", "StorageManager", "RateLimiter", "SchemaValidator"]
