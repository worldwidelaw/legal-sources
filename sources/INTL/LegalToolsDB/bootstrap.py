#!/usr/bin/env python3
"""
INTL/LegalToolsDB -- ICC Legal Tools Database (CLD)

Fetches case law findings from the ICC Case Law Database (CLD) via the
Legal Tools Database API. Each finding contains full text of the legal
reasoning extracted from ICC decisions.

Strategy:
  - CLD search API at /api/clddocs/search (Elasticsearch-backed)
  - Paginate with from/limit (up to 100 per page)
  - Full text in legalFindingText field (plain text)
  - ~18,800 legal findings from ICC proceedings
  - robots.txt crawl-delay: 10 — we use 2s between requests

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
from urllib.parse import quote

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.LegalToolsDB")

CLD_SEARCH_URL = "https://legal-tools.org/api/clddocs/search"
CLD_DETAIL_URL = "https://legal-tools.org/api/clddocs"
PAGE_SIZE = 100


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


class LegalToolsDBScraper(BaseScraper):
    """
    Scraper for INTL/LegalToolsDB -- ICC Legal Tools Database (CLD).
    Country: INTL
    URL: https://www.legal-tools.org/

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

    def _search_cld(self, offset: int, limit: int) -> dict:
        """Search CLD with pagination."""
        filter_obj = {"from": offset, "limit": limit}
        params = {"filter": json.dumps(filter_obj)}
        self.rate_limiter.wait()
        r = self.session.get(CLD_SEARCH_URL, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw CLD record into standard schema."""
        # Get text from plain text field, fall back to HTML field
        text = (raw.get("legalFindingText") or "").strip()
        if not text:
            text = strip_html(raw.get("legalFinding", ""))
        if not text or len(text) < 50:
            return None

        title = (raw.get("Title") or "").strip()
        if not title:
            return None

        doc_id = raw.get("ID") or raw.get("id", "")
        situation_case = raw.get("situationCase", "")
        case_name = raw.get("caseName", situation_case)

        # Parse date
        date = None
        date_created = raw.get("dateCreated", "")
        if date_created:
            try:
                dt = datetime.fromisoformat(date_created.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                date = None

        doc_number = raw.get("Document Number", "")
        importance = raw.get("Importance01", "")
        keywords = raw.get("Keyword", [])
        confidentiality = raw.get("Confidentiality", "")

        return {
            "_id": f"CLD-{doc_id}" if doc_id else f"CLD-{raw.get('id', '')}",
            "_source": "INTL/LegalToolsDB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://www.legal-tools.org/case-matrix/",
            "document_number": doc_number,
            "case_name": case_name,
            "situation_case": situation_case,
            "importance": importance,
            "confidentiality": confidentiality,
            "keywords": keywords if isinstance(keywords, list) else [],
            "decision_type": raw.get("Decision Type", ""),
            "pages": raw.get("Page(s)_", ""),
            "paragraphs": raw.get("Para(s)_", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CLD findings with full text."""
        # Get total count
        data = self._search_cld(0, 1)
        total = data.get("total", 0)
        logger.info(f"Total CLD findings: {total}")

        offset = 0
        fetched = 0
        while offset < total:
            logger.info(f"Fetching CLD offset={offset} ({fetched} records so far)")
            try:
                data = self._search_cld(offset, PAGE_SIZE)
            except Exception as e:
                logger.warning(f"Failed at offset={offset}: {e}")
                offset += PAGE_SIZE
                time.sleep(5)
                continue

            results = data.get("results", [])
            if not results:
                break

            for r in results:
                hit = r.get("hit", {})
                text = (hit.get("legalFindingText") or "").strip()
                if not text:
                    text = strip_html(hit.get("legalFinding", ""))
                if len(text) < 50:
                    continue

                yield hit
                fetched += 1

            offset += PAGE_SIZE
            time.sleep(2)

        logger.info(f"Total CLD records fetched: {fetched}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch findings updated since a given date."""
        since_str = since.isoformat() if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching updates since {since_str}")
        # CLD search doesn't support date filtering directly,
        # so we paginate through all and filter client-side
        for hit in self.fetch_all():
            updated = hit.get("updated", "")
            if updated and updated >= since_str:
                yield hit


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/LegalToolsDB data fetcher")
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

    scraper = LegalToolsDBScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            data = scraper._search_cld(0, 3)
            total = data.get("total", 0)
            logger.info(f"OK: {total} CLD findings available")

            results = data.get("results", [])
            for i, r in enumerate(results):
                hit = r["hit"]
                text = (hit.get("legalFindingText") or "").strip()
                title = (hit.get("Title") or "")[:80]
                logger.info(f"  [{i}] {title} | text_len={len(text)}")

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
