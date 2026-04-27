#!/usr/bin/env python3
"""
INTL/OpenLegalData -- Open Legal Data Platform (DE/EU Case Law)

Fetches German and EU court decisions from the Open Legal Data Platform.

Strategy:
  - Search API at /api/cases/search/?text=X with pagination
  - Use broad search terms to enumerate the corpus
  - Full text in 'text' field of results
  - Paginate with limit/offset

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OpenLegalData")

SEARCH_URL = "https://de.openlegaldata.io/api/cases/search/"
PAGE_SIZE = 50

# Broad search terms to cover different document types
SEARCH_TERMS = [
    "Urteil",       # Judgment
    "Beschluss",    # Order/Resolution
    "Entscheidung", # Decision
    "Verordnung",   # Regulation
    "judgment",     # English judgments (EU courts)
    "decision",     # English decisions
    "Berufung",     # Appeal
]


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class OpenLegalDataScraper(BaseScraper):
    """
    Scraper for INTL/OpenLegalData -- Open Legal Data Platform.
    Country: INTL
    URL: https://de.openlegaldata.io/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })

    def _search(self, term: str, offset: int, limit: int) -> dict:
        """Search cases with pagination."""
        params = {
            "text": term,
            "limit": limit,
            "offset": offset,
            "format": "json",
        }
        self.rate_limiter.wait()
        r = self.session.get(SEARCH_URL, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _make_title(self, case: dict) -> str:
        """Generate a title from case metadata."""
        court = case.get("court", "")
        date = case.get("date", "")
        slug = case.get("slug", "")
        decision_type = case.get("decision_type", "")

        parts = []
        if court:
            parts.append(court)
        if decision_type:
            parts.append(decision_type)
        if date:
            parts.append(date)
        if not parts:
            parts.append(slug)

        return " - ".join(parts)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw case record into standard schema."""
        text = strip_html(raw.get("text", "")).strip()
        if not text or len(text) < 200:
            return None

        slug = raw.get("slug", "")
        court = raw.get("court", "")
        date = raw.get("date", "")
        decision_type = raw.get("decision_type", "")

        title = self._make_title(raw)

        return {
            "_id": f"OLD-{slug}" if slug else f"OLD-{court}-{date}",
            "_source": "INTL/OpenLegalData",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date if date else None,
            "url": f"https://de.openlegaldata.io/case/{slug}" if slug else "",
            "court": court,
            "decision_type": decision_type,
            "court_jurisdiction": raw.get("court_jurisdiction"),
            "court_level_of_appeal": raw.get("court_level_of_appeal"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all cases via search API with multiple terms."""
        seen_slugs = set()
        total = 0

        for term in SEARCH_TERMS:
            logger.info(f"Searching for '{term}'...")

            try:
                data = self._search(term, 0, 1)
                count = data.get("count", 0)
                logger.info(f"  '{term}': {count} results")
            except Exception as e:
                logger.warning(f"  Search '{term}' failed: {e}")
                continue

            offset = 0
            while offset < min(count, 10000):  # ES max window
                try:
                    data = self._search(term, offset, PAGE_SIZE)
                except Exception as e:
                    logger.warning(f"  Failed at offset={offset}: {e}")
                    offset += PAGE_SIZE
                    time.sleep(5)
                    continue

                results = data.get("results", [])
                if not results:
                    break

                for case in results:
                    slug = case.get("slug", "")
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)

                    text = case.get("text", "")
                    if len(text) < 200:
                        continue

                    yield case
                    total += 1

                offset += PAGE_SIZE
                time.sleep(1)

        logger.info(f"Total unique cases fetched: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases by searching for recent terms."""
        # No date filter in search API; re-fetch all
        yield from self.fetch_all()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/OpenLegalData data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = OpenLegalDataScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            data = scraper._search("Urteil", 0, 3)
            count = data.get("count", 0)
            logger.info(f"OK: {count} results for 'Urteil'")

            results = data.get("results", [])
            for i, case in enumerate(results):
                text = case.get("text", "")
                logger.info(f"  [{i}] {case.get('court','')} {case.get('date','')} | text_len={len(text)}")

            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
