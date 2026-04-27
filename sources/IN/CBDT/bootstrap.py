#!/usr/bin/env python3
"""
IN/CBDT -- Central Board of Direct Taxes (India) Circulars

Fetches CBDT circulars from incometaxindia.gov.in via Liferay DXP Search API.

Strategy:
  - POST to /o/search/v1.0/search with CIRCULAR_BP_ERC blueprint
  - Paginate at 500 items per page (3 pages for ~1,385 circulars)
  - For "Content" type: extract full text from inline HTML (documentContent field)
  - For "PDF" type: download PDF from /documents/d/guest/... and extract text

Data:
  - 1,385 CBDT circulars (1960s - 2026)
  - ~70% inline HTML, ~30% PDF
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 required. pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.CBDT")

BASE_URL = "https://www.incometaxindia.gov.in"
SEARCH_URL = f"{BASE_URL}/o/search/v1.0/search"
BLUEPRINT_ERC = "CIRCULAR_BP_ERC"
PAGE_SIZE = 500


class CBDTScraper(BaseScraper):
    """Scraper for IN/CBDT -- Central Board of Direct Taxes circulars."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
        })

    def _search_page(self, page: int) -> dict:
        """Fetch one page of circulars from the Liferay Search API."""
        self.rate_limiter.wait()
        params = {
            "page": page,
            "pageSize": PAGE_SIZE,
            "nestedFields": "embedded",
            "fields": "embedded.contentFields,itemURL,title",
        }
        body = {
            "attributes": {
                "search.empty.search": True,
                "search.experiences.blueprint.external.reference.code": BLUEPRINT_ERC,
            }
        }
        resp = self.session.post(SEARCH_URL, json=body, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _extract_fields(self, item: dict) -> dict:
        """Extract structured fields from a Liferay search result item."""
        fields = {}
        for cf in item.get("embedded", {}).get("contentFields", []):
            name = cf.get("name")
            val = cf.get("contentFieldValue", {})
            if name == "reportFile":
                fields[name] = val.get("document", {})
            else:
                fields[name] = val.get("data", "")
        fields["title"] = item.get("title", "")
        fields["itemURL"] = item.get("itemURL", "")
        return fields

    def _html_to_text(self, html: str) -> str:
        """Convert HTML content to clean text."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _download_pdf_text(self, content_url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        if not content_url:
            return ""
        pdf_url = content_url if content_url.startswith("http") else BASE_URL + content_url
        return extract_pdf_markdown(
            source="IN/CBDT",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
            force=True,
        ) or ""

    def _make_id(self, fields: dict) -> str:
        """Generate unique ID from circular number or title."""
        num = fields.get("circularNotificationNumber", "")
        if num:
            slug = re.sub(r"[^a-zA-Z0-9]+", "-", num.strip()).strip("-").lower()
            return f"cbdt-{slug}"
        title = fields.get("title", "unknown")
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title[:80]).strip("-").lower()
        return f"cbdt-{slug}"

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "IN/CBDT",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "circular_number": raw.get("circular_number", ""),
        }

    def _process_item(self, item: dict) -> Optional[dict]:
        """Process a single search result into a normalized record."""
        fields = self._extract_fields(item)
        doc_id = self._make_id(fields)
        title = fields.get("title", "")
        select_type = fields.get("selectType", "")
        date_raw = fields.get("circularNotificationDate", "")
        date_iso = date_raw[:10] if date_raw and len(date_raw) >= 10 else None

        # Extract text based on type
        if select_type == "Content":
            text = self._html_to_text(fields.get("documentContent", ""))
        elif select_type == "PDF":
            report = fields.get("reportFile", {})
            content_url = report.get("contentUrl", "")
            text = self._download_pdf_text(content_url, doc_id)
        else:
            logger.warning("Unknown type '%s' for %s", select_type, title[:60])
            text = self._html_to_text(fields.get("documentContent", ""))

        if not text or len(text) < 50:
            logger.warning("Insufficient text (%d chars) for: %s", len(text) if text else 0, title[:60])
            return None

        url = fields.get("itemURL", "")
        if url and not url.startswith("http"):
            url = BASE_URL + url

        raw = {
            "_id": doc_id,
            "title": title,
            "text": text,
            "date": date_iso,
            "url": url,
            "circular_number": fields.get("circularNotificationNumber", ""),
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CBDT circulars."""
        page = 0
        total_yielded = 0
        while True:
            logger.info("Fetching page %d (pageSize=%d)...", page, PAGE_SIZE)
            data = self._search_page(page)
            items = data.get("items", [])
            if not items:
                break
            for item in items:
                record = self._process_item(item)
                if record:
                    yield record
                    total_yielded += 1
            total_count = data.get("totalCount") or data.get("total", 0)
            logger.info("Page %d: %d items, %d yielded so far (total: %s)",
                        page, len(items), total_yielded, total_count)
            if len(items) < PAGE_SIZE:
                break
            page += 1

    def fetch_updates(self, since=None):
        """Fetch all (API has no date filter)."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            data = self._search_page(0)
            total = data.get("totalCount", 0)
            items = data.get("items", [])
            logger.info("Connection OK: %d total circulars, %d on first page", total, len(items))
            return total > 0 and len(items) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IN/CBDT Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBDTScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
