#!/usr/bin/env python3
"""
SA/ZATCA-Guidance -- Saudi ZATCA Tax Guidelines & Circulars

Fetches tax guidelines and circulars from zatca.gov.sa via SharePoint
PortalHandler.ashx JSON API. Downloads PDFs and extracts full text.

Strategy:
  - GET PortalHandler.ashx?op=LoadItems for guidelines (74) and publications (19)
  - Both return JSON arrays with PDF URLs
  - Download each PDF and extract text

Data:
  - 74 tax guidelines (VAT, Zakat, CIT, Excise, TP, E-Invoicing, Customs)
  - 19 circulars/publications
  - All PDFs, no authentication required

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
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SA.ZATCA-Guidance")

BASE_URL = "https://zatca.gov.sa"
HANDLER_URL = f"{BASE_URL}/en/_LAYOUTS/15/GAZTInternet/PortalHandler.ashx"

# Data categories: (list_url, view_name, category, pdf_url_builder)
CATEGORIES = [
    {
        "list_url": "/en/HelpCenter/guidelines/Lists/GuideLines/Home.aspx",
        "view_name": "Home",
        "category": "guidelines",
        "pdf_base": "",  # URL field has full path
    },
    {
        "list_url": "/en/MediaCenter/Publications/Documents",
        "view_name": "Home",
        "category": "publications",
        "pdf_base": "/en/MediaCenter/Publications/Documents/",
    },
]


class ZATCAScraper(BaseScraper):
    """Scraper for SA/ZATCA-Guidance -- ZATCA Tax Guidelines & Circulars."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _fetch_list(self, list_url: str, view_name: str) -> list:
        """Fetch items from a SharePoint list via PortalHandler."""
        self.rate_limiter.wait()
        params = {
            "op": "LoadItems",
            "listUrl": list_url,
            "viewName": view_name,
        }
        resp = self.session.get(HANDLER_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _build_pdf_url(self, item: dict, cat: dict) -> Optional[str]:
        """Build the full PDF download URL for an item."""
        if cat["category"] == "guidelines":
            url_field = item.get("URL", "")
            if not url_field:
                return None
            if url_field.startswith("http"):
                return url_field
            return BASE_URL + url_field
        elif cat["category"] == "publications":
            filename = item.get("FileLeafRef", "")
            if not filename:
                return None
            return BASE_URL + cat["pdf_base"] + quote(filename)
        return None

    def _make_id(self, item: dict, category: str) -> str:
        """Generate unique ID from item."""
        sp_id = item.get("ID", "")
        title = item.get("Title", item.get("LinkTitle", "unknown"))
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title[:60]).strip("-").lower()
        return f"zatca-{category}-{sp_id}-{slug}"

    def _parse_date(self, item: dict) -> Optional[str]:
        """Extract date from item if available."""
        for field in ("ReleaseDate", "DateToShow"):
            val = item.get(field)
            if val:
                # Try DD/MM/YYYY
                try:
                    return datetime.strptime(val, "%d/%m/%Y").strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    pass
                # Try ISO format
                try:
                    return val[:10]
                except (ValueError, TypeError):
                    pass
        return None

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        return extract_pdf_markdown(
            source="SA/ZATCA-Guidance",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
            force=True,
        ) or ""

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "SA/ZATCA-Guidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "activity_type": raw.get("activity_type", ""),
            "tax_type": raw.get("tax_type", ""),
            "category": raw.get("category", ""),
        }

    def _process_item(self, item: dict, cat: dict) -> Optional[dict]:
        """Process a single item: download PDF and extract text."""
        title = item.get("Title", item.get("LinkTitle", ""))
        if not title:
            return None

        # Skip hidden items
        visibility = item.get("Visibility", "1")
        if str(visibility) == "0":
            logger.debug("Skipping hidden item: %s", title[:60])
            return None

        doc_id = self._make_id(item, cat["category"])
        pdf_url = self._build_pdf_url(item, cat)
        if not pdf_url:
            logger.warning("No PDF URL for: %s", title[:60])
            return None

        text = self._download_pdf_text(pdf_url, doc_id)
        if not text or len(text) < 50:
            logger.warning("Insufficient text (%d chars) for: %s",
                           len(text) if text else 0, title[:60])
            return None

        raw = {
            "_id": doc_id,
            "title": title,
            "text": text,
            "date": self._parse_date(item),
            "url": pdf_url,
            "activity_type": item.get("ActivityType", ""),
            "tax_type": item.get("TaxType", ""),
            "category": cat["category"],
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all ZATCA guidelines and publications."""
        for cat in CATEGORIES:
            logger.info("Fetching %s from %s...", cat["category"], cat["list_url"])
            items = self._fetch_list(cat["list_url"], cat["view_name"])
            logger.info("Got %d items for %s", len(items), cat["category"])
            for item in items:
                record = self._process_item(item, cat)
                if record:
                    yield record

    def fetch_updates(self, since=None):
        """Fetch all (no date filter available)."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            items = self._fetch_list(CATEGORIES[0]["list_url"], CATEGORIES[0]["view_name"])
            logger.info("Connection OK: %d guidelines found", len(items))
            return len(items) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SA/ZATCA-Guidance Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ZATCAScraper()

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
