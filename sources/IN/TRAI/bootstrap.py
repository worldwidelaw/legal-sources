#!/usr/bin/env python3
"""
IN/TRAI -- Telecom Regulatory Authority of India

Fetches regulations, directions, recommendations, and tariff orders from TRAI
with full text extracted from PDFs.

Strategy:
  - Iterate paginated HTML listing pages (?page=N) for each category
  - Parse PDF download links and titles from aria-label attributes
  - Download PDFs and extract full text using pdfplumber
  - Extract dates from PDF filenames (DDMMYYYY pattern)

Data:
  - ~350 regulations, ~130 directions, ~340 recommendations, ~260 tariff orders
  - All documents are PDFs with selectable text
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent documents
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.TRAI")

BASE_URL = "https://www.trai.gov.in"

# Categories: (url_path, category_name, max_pages)
CATEGORIES = [
    ("/release-publication/regulations", "regulations", 15),
    ("/release-publication/directions", "directions", 15),
    ("/release-publication/recommendation", "recommendations", 15),
    ("/broadcasting/tariff-orders", "tariff_orders", 5),
]


class TRAIScraper(BaseScraper):
    """Scraper for IN/TRAI -- Telecom Regulatory Authority of India."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def _fetch_listing_page(self, url_path: str, page: int) -> List[Dict]:
        """Fetch one page of listings and parse records."""
        self.rate_limiter.wait()
        url = f"{BASE_URL}{url_path}?page={page}"
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        records = []

        for row in soup.select("div.views-row"):
            # Find PDF download link with aria-label containing the title
            pdf_link = row.select_one('a[href*="/sites/default/files/"]')
            if not pdf_link:
                continue

            href = pdf_link.get("href", "")
            if not href:
                continue

            # Make URL absolute
            if not href.startswith("http"):
                pdf_url = urljoin(BASE_URL, href)
            else:
                pdf_url = href

            # Extract title from aria-label
            aria = pdf_link.get("aria-label", "")
            title = self._parse_title_from_aria(aria)
            if not title:
                # Fallback: use the span text + label text
                span = row.select_one("span")
                title = span.get_text(strip=True) if span else ""
                if not title:
                    title = pdf_link.get_text(strip=True)

            # Extract date from filename
            date_iso = self._parse_date_from_filename(href)

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "date": date_iso,
            })

        return records

    def _parse_title_from_aria(self, aria: str) -> str:
        """Extract document title from aria-label attribute."""
        if not aria:
            return ""
        # Pattern: "Download PDF for TITLE - (X.XX MB), opens in new tab"
        match = re.match(
            r"Download PDF for (.+?)\s*-\s*\([\d.]+\s*[KMG]B\)",
            aria, re.IGNORECASE
        )
        if match:
            return match.group(1).strip()
        # Fallback: just strip prefix
        if aria.startswith("Download PDF for "):
            return aria[len("Download PDF for "):].strip()
        return ""

    def _parse_date_from_filename(self, filename: str) -> Optional[str]:
        """Extract date from PDF filename patterns like _DDMMYYYY.pdf."""
        # Pattern 1: _DDMMYYYY.pdf
        match = re.search(r"_(\d{2})(\d{2})(\d{4})\.pdf", filename)
        if match:
            d, m, y = match.groups()
            try:
                return f"{y}-{m}-{d}"
            except ValueError:
                pass

        # Pattern 2: YYYY-MM in the path
        match = re.search(r"/(\d{4})-(\d{2})/", filename)
        if match:
            y, m = match.groups()
            return f"{y}-{m}-01"

        return None

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IN/TRAI",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _make_id(self, pdf_url: str) -> str:
        """Create unique ID from PDF URL."""
        # Extract filename from URL
        match = re.search(r"/([^/]+\.pdf)", pdf_url, re.IGNORECASE)
        if match:
            return match.group(1).replace(".pdf", "").replace(".PDF", "")
        return hashlib.md5(pdf_url.encode()).hexdigest()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw TRAI record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "IN/TRAI",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all TRAI documents."""
        for url_path, category, max_pages in CATEGORIES:
            logger.info("Fetching category: %s", category)
            yield from self._fetch_category(url_path, category, max_pages)

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (first 2 pages of each category)."""
        for url_path, category, _ in CATEGORIES:
            logger.info("Fetching updates for %s", category)
            yield from self._fetch_category(url_path, category, max_pages=2)

    def _fetch_category(self, url_path: str, category: str,
                        max_pages: int) -> Generator[Dict[str, Any], None, None]:
        """Fetch all records from a single category."""
        for page in range(max_pages):
            records = self._fetch_listing_page(url_path, page)
            if not records:
                logger.info("Category %s: no more records at page %d", category, page)
                break

            logger.info("Category %s page %d: %d records", category, page, len(records))

            for rec in records:
                doc = self._process_record(rec, category)
                if doc:
                    yield doc

    def _process_record(self, rec: dict, category: str) -> Optional[dict]:
        """Process a single record: download PDF and extract text."""
        pdf_url = rec["pdf_url"]
        title = rec["title"]
        doc_id = self._make_id(pdf_url)

        text = self._download_pdf_text(pdf_url)
        if not text:
            logger.warning("No text for: %s", title[:80])
            return None

        raw = {
            "_id": doc_id,
            "title": title,
            "text": text,
            "date": rec.get("date"),
            "pdf_url": pdf_url,
            "category": category,
        }

        return self.normalize(raw)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            records = self._fetch_listing_page("/release-publication/regulations", 0)
            logger.info("Connection OK: %d regulations on page 0", len(records))
            return len(records) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode (15 records)")
            count = 0
            target = 15
            # Take a few from each category
            per_cat = 4
            for url_path, category, _ in CATEGORIES:
                if count >= target:
                    break
                records = self._fetch_listing_page(url_path, 0)
                logger.info("Category %s: %d records on page 0", category, len(records))

                for rec in records[:per_cat + 1]:
                    if count >= target:
                        break
                    doc = self._process_record(rec, category)
                    if doc:
                        fname = re.sub(r'[^\w\-.]', '_', f"{doc['_id'][:80]}.json")
                        with open(sample_dir / fname, "w", encoding="utf-8") as f:
                            json.dump(doc, f, ensure_ascii=False, indent=2)
                        count += 1
                        logger.info("[%d/%d] %s: %s (%d chars)",
                                    count, target, category, doc["title"][:50], len(doc["text"]))

            logger.info("Sample bootstrap complete: %d records saved", count)
            return count
        else:
            count = 0
            for doc in self.fetch_all():
                self.storage.save(doc)
                count += 1
                if count % 50 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IN/TRAI Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = TRAIScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for doc in scraper.fetch_updates():
            scraper.storage.save(doc)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
