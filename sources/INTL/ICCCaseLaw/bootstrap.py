#!/usr/bin/env python3
"""
INTL/ICCCaseLaw -- ICC Case Law Database (Legal Tools)

Fetches legal findings from the ICC Case Law Database via REST API.

Strategy:
  - Paginate through LoopBack REST API at /api/clddocs
  - Each record contains legalFindingText (curated legal reasoning)
  - No authentication required
  - 18,760+ records

Data:
  - ICC decisions since 2004
  - Curated legal findings with extracted reasoning
  - Keywords, case numbers, importance levels
  - Public access, no auth

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
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
logger = logging.getLogger("legal-data-hunter.INTL.ICCCaseLaw")

API_BASE = "https://www.legal-tools.org/api"
CLD_URL = f"{API_BASE}/clddocs"
PAGE_SIZE = 2000


class ICCCaseLawScraper(BaseScraper):
    """
    Scraper for INTL/ICCCaseLaw -- ICC Case Law Database.
    Country: INTL
    URL: https://www.legal-tools.org/cld

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "application/json",
        })

    def _get_count(self) -> int:
        """Get total number of CLD records."""
        r = self.session.get(f"{CLD_URL}/count", timeout=30)
        r.raise_for_status()
        return r.json().get("count", 0)

    def _fetch_page(self, skip: int, limit: int) -> list:
        """Fetch a page of CLD records."""
        params = {
            "filter[limit]": str(limit),
            "filter[skip]": str(skip),
        }
        r = self.session.get(CLD_URL, params=params, timeout=120)
        r.raise_for_status()
        return r.json()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw CLD record into standard schema."""
        text = raw.get("legalFindingText", "").strip()
        if not text:
            # Try HTML version and strip tags
            html_text = raw.get("legalFinding", "")
            if html_text:
                from bs4 import BeautifulSoup
                text = BeautifulSoup(html_text, "html.parser").get_text(separator="\n", strip=True)

        if not text:
            return None

        title = raw.get("Title", "").strip()
        if not title:
            title = f"ICC {raw.get('Document Number', raw.get('id', 'Unknown'))}"

        # Parse date
        date = None
        date_created = raw.get("dateCreated", "")
        if date_created:
            try:
                dt = datetime.fromisoformat(date_created.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        doc_number = raw.get("Document Number", "")
        case_number = raw.get("situationCase", "")
        record_id = raw.get("ID", "") or raw.get("id", "")

        # Build URL to the document viewer
        ltd_doc_id = raw.get("ltdDocId", "")
        url = f"https://www.legal-tools.org/cld"
        if ltd_doc_id:
            url = f"https://www.legal-tools.org/doc/{ltd_doc_id}"

        # Keywords
        keywords_raw = raw.get("Keyword", [])
        keywords = []
        if isinstance(keywords_raw, list):
            for kw in keywords_raw:
                if isinstance(kw, str):
                    # Strip ID suffix like " -ID008584"
                    clean = kw.split(" -ID")[0].strip()
                    if clean:
                        keywords.append(clean)

        return {
            "_id": f"ICC-CLD-{record_id}" if record_id else f"ICC-CLD-{raw.get('id', '')}",
            "_source": "INTL/ICCCaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "document_number": doc_number,
            "case_number": case_number,
            "importance": raw.get("Importance01", ""),
            "decision_type": raw.get("Decision Type", ""),
            "confidentiality": raw.get("Confidentiality", ""),
            "court": "International Criminal Court",
            "keywords": keywords,
            "pages": raw.get("Page(s)_", ""),
            "paragraphs": raw.get("Para(s)_", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CLD records via paginated API."""
        total = self._get_count()
        logger.info(f"Total CLD records: {total}")

        skip = 0
        fetched = 0
        while skip < total:
            logger.info(f"Fetching records {skip+1}-{min(skip+PAGE_SIZE, total)} of {total}")
            page = self._fetch_page(skip, PAGE_SIZE)

            if not page:
                break

            for record in page:
                yield record
                fetched += 1

            skip += PAGE_SIZE

        logger.info(f"Fetched {fetched} total records")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch records modified since a date."""
        since_iso = since.isoformat() if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching records modified since {since_iso}")

        skip = 0
        while True:
            params = {
                "filter[limit]": str(PAGE_SIZE),
                "filter[skip]": str(skip),
                "filter[where][dateModified][gte]": since_iso,
            }
            r = self.session.get(CLD_URL, params=params, timeout=120)
            r.raise_for_status()
            page = r.json()

            if not page:
                break

            for record in page:
                yield record

            if len(page) < PAGE_SIZE:
                break
            skip += PAGE_SIZE


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ICCCaseLaw data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = ICCCaseLawScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            count = scraper._get_count()
            logger.info(f"OK: {count} CLD records available")
            page = scraper._fetch_page(0, 2)
            logger.info(f"First record: {page[0].get('Title', '')[:80]}")
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
