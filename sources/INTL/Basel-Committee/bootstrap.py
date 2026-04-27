#!/usr/bin/env python3
"""
INTL/Basel-Committee -- Basel Committee on Banking Supervision (BCBS) Standards

Fetches BCBS publications (standards, guidelines, consultative documents, etc.)
with full text extracted from PDFs.

Strategy:
  - GET /api/document_lists/bcbspubls.json for full catalog (795 items)
  - For each document, download PDF at https://www.bis.org{path}.pdf
  - Extract full text via pdfplumber

Data:
  - ~795 publications (1975-present)
  - Types: Standards, Guidelines, Consultative, Working papers, etc.
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
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.Basel-Committee")

BASE_URL = "https://www.bis.org"
CATALOG_URL = BASE_URL + "/api/document_lists/bcbspubls.json"


class BaselCommitteeScraper(BaseScraper):
    """Scraper for INTL/Basel-Committee -- BCBS Standards."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json,text/html,*/*;q=0.8",
        })

    def _fetch_catalog(self) -> List[Dict]:
        """Fetch the complete BCBS publications catalog."""
        self.rate_limiter.wait()
        resp = self.session.get(CATALOG_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # API returns {"list": {"/path": {item}, ...}, "document_date_type": ...}
        catalog = data.get("list", {})
        items = list(catalog.values())

        logger.info("Catalog returned %d items", len(items))
        return items

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/Basel-Committee",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _make_id(self, item: dict) -> str:
        """Create unique ID from the document path."""
        path = item.get("path", "")
        # Extract the document identifier from the path
        # e.g., /bcbs/publ/d610 -> bcbs-d610
        # e.g., /publ/bcbs161 -> bcbs161
        match = re.search(r"/(d\d+|bcbs\d+|bcbs_nl\d+|wp\d+)$", path)
        if match:
            return f"BCBS-{match.group(1)}"
        # Fallback: use the path
        slug = path.strip("/").replace("/", "-")
        return f"BCBS-{slug}"

    def _get_pdf_url(self, item: dict) -> str:
        """Build PDF URL from the document path."""
        path = item.get("path", "")
        return f"{BASE_URL}{path}.pdf"

    def _get_detail_url(self, item: dict) -> str:
        """Build detail page URL from the document path."""
        path = item.get("path", "")
        return f"{BASE_URL}{path}.htm"

    def _get_date(self, item: dict) -> Optional[str]:
        """Extract date from item metadata."""
        for field in ("publication_start_date", "publication_timestamp"):
            val = item.get(field)
            if val:
                # Format: "2026-03-24" or "2026-03-24T..."
                match = re.match(r"(\d{4}-\d{2}-\d{2})", val)
                if match:
                    return match.group(1)
        return None

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw BCBS record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "INTL/Basel-Committee",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["detail_url"],
            "publication_type": raw.get("publication_type", ""),
            "publication_status": raw.get("publication_status", ""),
            "topics": raw.get("topics", []),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all BCBS publications."""
        catalog = self._fetch_catalog()
        for i, item in enumerate(catalog):
            logger.info("[%d/%d] %s", i + 1, len(catalog),
                        item.get("short_title", "")[:60])
            doc = self._process_item(item)
            if doc:
                yield doc

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent publications (last 180 days)."""
        catalog = self._fetch_catalog()
        cutoff = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        recent = [item for item in catalog
                  if (self._get_date(item) or "") >= cutoff]
        logger.info("Found %d recent items (since %s)", len(recent), cutoff)
        for item in recent:
            doc = self._process_item(item)
            if doc:
                yield doc

    def _process_item(self, item: dict) -> Optional[dict]:
        """Process a single catalog item: download PDF, extract text."""
        pdf_url = self._get_pdf_url(item)
        title = item.get("short_title", "")
        doc_id = self._make_id(item)

        text = self._download_pdf_text(pdf_url)
        if not text:
            return None

        pub_type = item.get("publication_type", "")
        pub_status = item.get("publication_status", "")
        topics = item.get("topics", [])
        if isinstance(topics, str):
            topics = [topics]

        raw = {
            "_id": doc_id,
            "title": title,
            "text": text,
            "date": self._get_date(item),
            "detail_url": self._get_detail_url(item),
            "publication_type": pub_type,
            "publication_status": pub_status,
            "topics": topics,
        }

        return self.normalize(raw)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            catalog = self._fetch_catalog()
            logger.info("Connection OK: %d publications in catalog", len(catalog))
            return len(catalog) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode (15 records)")
            catalog = self._fetch_catalog()
            count = 0
            target = 15

            # Sample from different publication types
            for item in catalog:
                if count >= target:
                    break

                doc = self._process_item(item)
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
                if count % 50 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/Basel-Committee Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BaselCommitteeScraper()

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
