#!/usr/bin/env python3
"""
NG/CBN -- Central Bank of Nigeria Circulars

Fetches regulatory circulars from CBN via JSON API, extracts full text from PDFs.

Strategy:
  - GET /api/GetAllCirculars returns JSON array of all circulars (~500+)
  - Each record has a PDF link at /Out/{year}/{dept}/{filename}.pdf
  - Download PDFs and extract text via common/pdf_extract

API:
  - Base: https://www.cbn.gov.ng
  - All circulars: /api/GetAllCirculars
  - PDFs: {base_url}{link} where link is e.g. /Out/2026/CCD/file.pdf
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.CBN")

BASE_URL = "https://www.cbn.gov.ng"
API_URL = "/api/GetAllCirculars"


def parse_cbn_date(date_str: str) -> Optional[str]:
    """Parse CBN date format 'DD/MM/YYYY' to ISO 8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


class CBNScraper(BaseScraper):
    """Scraper for NG/CBN -- Central Bank of Nigeria circulars."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _fetch_circulars_list(self) -> List[Dict]:
        """Fetch all circulars from the CBN API."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(API_URL)
            if not resp or resp.status_code != 200:
                logger.error(f"Failed to fetch circulars list: {resp.status_code if resp else 'no response'}")
                return []
            data = resp.json()
            if isinstance(data, list):
                logger.info(f"Fetched {len(data)} circulars from API")
                return data
            logger.warning(f"Unexpected API response type: {type(data)}")
            return []
        except Exception as e:
            logger.error(f"Error fetching circulars list: {e}")
            return []

    def _extract_pdf_text(self, pdf_link: str, doc_id: str) -> Optional[str]:
        """Download PDF and extract text."""
        if not pdf_link:
            return None
        pdf_url = f"{BASE_URL}{pdf_link}" if pdf_link.startswith("/") else pdf_link
        text = extract_pdf_markdown(
            source="NG/CBN",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
        )
        return text

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all circulars from the CBN API."""
        circulars = self._fetch_circulars_list()
        for circ in circulars:
            yield circ

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch circulars updated since a given date."""
        circulars = self._fetch_circulars_list()
        for circ in circulars:
            date_str = parse_cbn_date(circ.get("documentDate", ""))
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if doc_date >= since:
                        yield circ
                except ValueError:
                    yield circ
            else:
                yield circ

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw API record into standard schema, fetching PDF text."""
        title = (raw.get("title") or "").strip()
        ref_no = (raw.get("refNo") or "").strip()
        pdf_link = (raw.get("link") or "").strip()
        date_str = parse_cbn_date(raw.get("documentDate", ""))
        description = (raw.get("description") or "").strip()
        keywords = (raw.get("keywords") or "").strip()
        cbn_id = raw.get("id", "")

        if not title or not pdf_link:
            return None

        doc_id = f"CBN-{cbn_id}" if cbn_id else f"CBN-{re.sub(r'[^a-zA-Z0-9._-]', '_', ref_no or title[:50])}"

        text = self._extract_pdf_text(pdf_link, doc_id)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
            return None

        pdf_url = f"{BASE_URL}{pdf_link}" if pdf_link.startswith("/") else pdf_link

        record = {
            "_id": doc_id,
            "_source": "NG/CBN",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "reference_number": ref_no,
            "jurisdiction": "NG",
            "language": "en",
        }
        if description:
            record["description"] = description
        if keywords:
            record["keywords"] = keywords

        return record

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing CBN API...")

        circulars = self._fetch_circulars_list()
        if not circulars:
            print("FAILED: No circulars returned from API")
            return

        print(f"Total circulars: {len(circulars)}")

        # Show first 3
        for circ in circulars[:3]:
            title = circ.get("title", "")[:80]
            ref = circ.get("refNo", "")
            date = circ.get("documentDate", "")
            link = circ.get("link", "")
            print(f"\n  Ref: {ref}")
            print(f"  Title: {title}")
            print(f"  Date: {date}")
            print(f"  Link: {link}")

        # Test PDF extraction on first circular
        circ = circulars[0]
        link = circ.get("link", "")
        if link:
            doc_id = f"CBN-{circ.get('id', 'test')}"
            print(f"\nTesting PDF extraction: {link}")
            text = self._extract_pdf_text(link, doc_id)
            if text:
                print(f"  Extracted {len(text)} chars")
                print(f"  Sample: {text[:200]}...")
            else:
                print("  FAILED: No text extracted from PDF")

        print("\nTest complete!")


def main():
    scraper = CBNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
