#!/usr/bin/env python3
"""
INTL/ASEAN-Legal -- ASEAN Legal Instruments Database

Fetches ASEAN agreements, protocols, conventions, and other legal instruments
with full text extracted from PDFs.

Strategy:
  - Fetch paginated AJAX listing pages at ajax_list_data/{page}.html
  - Parse HTML table to extract title, PDF URL, detail ID, signature info
  - Download PDFs and extract full text using pdfplumber
  - Optionally fetch detail pages for richer metadata

Data:
  - ~260 legal instruments (agreements, protocols, conventions, treaties, etc.)
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
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

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
logger = logging.getLogger("legal-data-hunter.INTL.ASEAN-Legal")

BASE_URL = "https://agreement.asean.org"
LISTING_URL = BASE_URL + "/search/ajax_list_data/{page}.html"
DETAIL_URL = BASE_URL + "/agreement/detail/{id}.html"
MAX_PAGES = 20


class ASEANLegalScraper(BaseScraper):
    """Scraper for INTL/ASEAN-Legal -- ASEAN Legal Instruments Database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "X-Requested-With": "XMLHttpRequest",
        })

    def _fetch_listing_page(self, page: int) -> List[Dict]:
        """Fetch one page of the AJAX listing and parse records."""
        self.rate_limiter.wait()
        url = LISTING_URL.format(page=page)
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()

        html = resp.text.strip()
        if not html or len(html) < 50:
            return []

        soup = BeautifulSoup(html, "html.parser")
        records = []

        # Parse table rows -- each row has: number, title+pdf, place/date, ratification, status, detail link
        for row in soup.select("tr"):
            cells = row.select("td")
            if len(cells) < 4:
                continue

            # Find PDF link in the row
            pdf_link = row.select_one('a[href*="media/download"]')
            if not pdf_link:
                pdf_link = row.select_one('a[href$=".pdf"]')
            if not pdf_link:
                continue

            href = pdf_link.get("href", "")
            if not href:
                continue

            # Make URL absolute
            if href.startswith("/"):
                pdf_url = BASE_URL + href
            elif not href.startswith("http"):
                pdf_url = BASE_URL + "/" + href
            else:
                pdf_url = href

            # Extract title from the link text or cell text
            title = pdf_link.get_text(strip=True)
            if not title or len(title) < 5:
                # Try the cell containing the link
                title_cell = pdf_link.find_parent("td")
                if title_cell:
                    title = title_cell.get_text(strip=True)

            # Extract detail ID from detail link
            detail_link = row.select_one('a[href*="agreement/detail"]')
            detail_id = None
            if detail_link:
                match = re.search(r"detail/(\d+)", detail_link.get("href", ""))
                if match:
                    detail_id = match.group(1)

            # Extract signature date from place/date cell
            date_iso = None
            # Look for date patterns in row text
            row_text = row.get_text()
            date_match = re.search(
                r"(\d{1,2})\s+(January|February|March|April|May|June|July|"
                r"August|September|October|November|December)\s+(\d{4})",
                row_text, re.IGNORECASE
            )
            if date_match:
                try:
                    d, m, y = date_match.groups()
                    dt = datetime.strptime(f"{d} {m} {y}", "%d %B %Y")
                    date_iso = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            # Extract document type from status/type indicators
            doc_type = ""
            status_text = ""
            for cell in cells:
                ct = cell.get_text(strip=True).lower()
                if ct in ("agreement", "protocol", "convention", "treaty",
                          "charter", "memorandum", "instrument of extension"):
                    doc_type = ct.title()
                if "in force" in ct or "not in force" in ct:
                    status_text = cell.get_text(strip=True)

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "date": date_iso,
                "detail_id": detail_id,
                "document_type": doc_type,
                "status": status_text,
            })

        return records

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/ASEAN-Legal",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    def _make_id(self, record: dict) -> str:
        """Create unique ID from detail_id or PDF URL."""
        if record.get("detail_id"):
            return f"ASEAN-{record['detail_id']}"
        # Fallback: hash the PDF URL
        return "ASEAN-" + hashlib.md5(record["pdf_url"].encode()).hexdigest()[:12]

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw ASEAN record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "INTL/ASEAN-Legal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "document_type": raw.get("document_type", ""),
            "status": raw.get("status", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all ASEAN legal instruments."""
        seen_urls = set()
        for page in range(1, MAX_PAGES + 1):
            records = self._fetch_listing_page(page)
            if not records:
                logger.info("No more records at page %d", page)
                break

            logger.info("Page %d: %d records", page, len(records))

            for rec in records:
                if rec["pdf_url"] in seen_urls:
                    continue
                seen_urls.add(rec["pdf_url"])

                doc = self._process_record(rec)
                if doc:
                    yield doc

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (first 3 pages)."""
        seen_urls = set()
        for page in range(1, 4):
            records = self._fetch_listing_page(page)
            if not records:
                break
            for rec in records:
                if rec["pdf_url"] in seen_urls:
                    continue
                seen_urls.add(rec["pdf_url"])
                doc = self._process_record(rec)
                if doc:
                    yield doc

    def _process_record(self, rec: dict) -> Optional[dict]:
        """Process a single record: download PDF and extract text."""
        pdf_url = rec["pdf_url"]
        title = rec["title"]
        doc_id = self._make_id(rec)

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
            "document_type": rec.get("document_type", ""),
            "status": rec.get("status", ""),
        }

        return self.normalize(raw)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            records = self._fetch_listing_page(1)
            logger.info("Connection OK: %d records on page 1", len(records))
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
            seen_urls = set()

            for page in range(1, MAX_PAGES + 1):
                if count >= target:
                    break
                records = self._fetch_listing_page(page)
                if not records:
                    break

                logger.info("Page %d: %d records", page, len(records))

                for rec in records:
                    if count >= target:
                        break
                    if rec["pdf_url"] in seen_urls:
                        continue
                    seen_urls.add(rec["pdf_url"])

                    doc = self._process_record(rec)
                    if doc:
                        fname = re.sub(r'[^\w\-.]', '_', f"{doc['_id'][:80]}.json")
                        with open(sample_dir / fname, "w", encoding="utf-8") as f:
                            json.dump(doc, f, ensure_ascii=False, indent=2)
                        count += 1
                        logger.info("[%d/%d] %s (%d chars)",
                                    count, target, doc["title"][:60], len(doc["text"]))

            logger.info("Sample bootstrap complete: %d records saved", count)
            return count
        else:
            count = 0
            for doc in self.fetch_all():
                self.storage.save(doc)
                count += 1
                if count % 25 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/ASEAN-Legal Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = ASEANLegalScraper()

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
