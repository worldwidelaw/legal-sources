#!/usr/bin/env python3
"""
SC/SRC-TaxGuidance -- Seychelles Revenue Commission Tax Guidance

Fetches tax rulings, guidance brochures, BTI rulings, and legislation PDFs
from the Seychelles Revenue Commission via the WordPress REST API.

Strategy:
  1. Use WP REST API media endpoint to enumerate all PDFs (748 items)
  2. Also scrape /publications/ and /legislation/ pages for additional metadata
  3. Download each PDF and extract full text via common.pdf_extract

Coverage:
  - Public tax rulings (~12)
  - Binding Tariff Information rulings (~163)
  - Tax guidance brochures/guidelines (~15)
  - Legislation (Business Tax Act, VAT Act, etc.) (~300+)
  - Double Tax Agreements (~39)
  - E-invoicing and forms guidance

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SC.SRC-TaxGuidance")

BASE_URL = "https://src.gov.sc"
API_BASE = f"{BASE_URL}/wp-json/wp/v2"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
}

# Categories for classifying PDFs by filename patterns
CATEGORY_PATTERNS = [
    (r"(?i)^(public[_-]?)?ruling", "ruling", "ruling"),
    (r"(?i)^BTI[\._]", "bti", "ruling"),
    (r"(?i)brochure", "brochure", "guidance"),
    (r"(?i)guideline", "guideline", "guidance"),
    (r"(?i)advice", "advice", "guidance"),
    (r"(?i)(?:act|amendment|statutory|regulation|ordinance|SI[_-]?\d)", "legislation", "legislation"),
    (r"(?i)(?:DTA|agreement|convention|treaty)", "dta", "agreement"),
    (r"(?i)(?:circular|notice|directive)", "circular", "guidance"),
]


def classify_pdf(filename: str) -> tuple:
    """Classify a PDF by its filename. Returns (category, doc_type)."""
    for pattern, category, doc_type in CATEGORY_PATTERNS:
        if re.search(pattern, filename):
            return category, doc_type
    return "other", "guidance"


class SRCTaxGuidanceScraper(BaseScraper):
    """Scraper for Seychelles Revenue Commission tax guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_all_media_pdfs(self) -> Generator[dict, None, None]:
        """Fetch all PDF media items via WP REST API."""
        page = 1
        total_pages = None

        while True:
            self.rate_limiter.wait()
            url = f"{API_BASE}/media"
            params = {
                "per_page": 100,
                "page": page,
                "mime_type": "application/pdf",
            }

            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 400:
                    # Past last page
                    break
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch media page {page}: {e}")
                break

            if total_pages is None:
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                total = resp.headers.get("X-WP-Total", "?")
                logger.info(f"WP API: {total} PDF media items, {total_pages} pages")

            items = resp.json()
            if not items:
                break

            for item in items:
                source_url = item.get("source_url", "")
                if not source_url:
                    continue

                title = item.get("title", {}).get("rendered", "").strip()
                if not title:
                    # Use filename as title
                    title = unquote(source_url.split("/")[-1]).replace(".pdf", "").replace("-", " ").replace("_", " ")

                date_str = item.get("date", "")
                date_iso = None
                if date_str:
                    try:
                        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        date_iso = dt.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass

                filename = unquote(source_url.split("/")[-1])
                category, doc_type = classify_pdf(filename)

                yield {
                    "wp_id": item.get("id"),
                    "title": title,
                    "pdf_url": source_url,
                    "date": date_iso,
                    "filename": filename,
                    "category": category,
                    "doc_type": doc_type,
                }

            if page >= (total_pages or 1):
                break
            page += 1
            time.sleep(1)

    def _make_id(self, raw: dict) -> str:
        """Generate a unique ID from the WP media ID or filename."""
        wp_id = raw.get("wp_id", "")
        filename = raw.get("filename", "")
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "_", filename.replace(".pdf", ""))
        clean_name = re.sub(r"_+", "_", clean_name).strip("_")
        if wp_id:
            return f"SC_SRC_{wp_id}_{clean_name}"[:200]
        return f"SC_SRC_{clean_name}"[:200]

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all SRC tax guidance documents with full text."""
        logger.info("Fetching all PDF media from WP REST API...")

        for raw in self._fetch_all_media_pdfs():
            self.rate_limiter.wait()
            doc_id = self._make_id(raw)

            text = extract_pdf_markdown(
                source="SC/SRC-TaxGuidance",
                source_id=doc_id,
                pdf_url=raw["pdf_url"],
                table="doctrine",
            )

            if text and len(text) >= 50:
                raw["text"] = text
                yield raw
            else:
                logger.debug(f"Skipping {raw['title'][:60]} — no text extracted")

            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since a date."""
        for raw in self.fetch_all():
            date_str = raw.get("date", "")
            if date_str:
                try:
                    doc_date = datetime.fromisoformat(date_str)
                    if doc_date.replace(tzinfo=None) >= since.replace(tzinfo=None):
                        yield raw
                except (ValueError, TypeError):
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw SRC record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        _id = self._make_id(raw)

        return {
            "_id": _id,
            "_source": "SC/SRC-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "category": raw.get("category"),
            "doc_type": raw.get("doc_type"),
            "language": "en",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SC/SRC-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = SRCTaxGuidanceScraper()

    if args.command == "test-api":
        logger.info("Testing SRC WP API connectivity...")
        try:
            resp = scraper.session.get(
                f"{API_BASE}/media",
                params={"per_page": 1, "mime_type": "application/pdf"},
                timeout=20,
            )
            resp.raise_for_status()
            total = resp.headers.get("X-WP-Total", "?")
            items = resp.json()
            logger.info(f"OK: {total} total PDF media items")
            if items:
                logger.info(f"Sample: {items[0].get('title', {}).get('rendered', '')[:80]}")
        except Exception as e:
            logger.error(f"FAIL: {e}")
            sys.exit(1)
    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=15)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
