#!/usr/bin/env python3
"""
US/PresidentialDocuments -- Presidential Documents Fetcher

Fetches executive orders, proclamations, and other presidential documents
from the Federal Register API. No auth required. Full text via raw_text_url.

Strategy:
  - Paginate through Federal Register API with conditions[type][]=PRESDOCU
  - For each document, fetch full text from raw_text_url
  - Supports incremental updates via publication_date filtering

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.PresidentialDocuments")

API_BASE = "https://www.federalregister.gov/api/v1"


class PresidentialDocsScraper(BaseScraper):
    """
    Scraper for US/PresidentialDocuments.
    Country: US
    URL: https://www.federalregister.gov

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- Helpers ------------------------------------------------------------

    def _paginate_docs(self, extra_params=None, max_pages=None):
        """Paginate through presidential documents. Yields raw doc dicts."""
        page = 1
        while True:
            if max_pages and page > max_pages:
                return

            params = {
                "conditions[type][]": "PRESDOCU",
                "per_page": 20,
                "page": page,
                "order": "newest",
            }
            if extra_params:
                params.update(extra_params)

            self.rate_limiter.wait()
            try:
                resp = self.client.get("/documents.json", params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"API error on page {page}: {e}")
                return

            results = data.get("results", [])
            if not results:
                return

            total = data.get("count", 0)
            if page == 1:
                logger.info(f"Total presidential documents: {total}")

            for doc in results:
                yield doc

            # Check if more pages
            next_url = data.get("next_page_url")
            if not next_url:
                return

            page += 1
            if page % 10 == 0:
                logger.info(f"  Page {page} (fetched ~{page * 20}/{total})")

    def _fetch_full_text(self, doc):
        """Fetch full text for a document. Returns cleaned text string."""
        # Prefer raw text, then HTML, then XML
        text_url = doc.get("raw_text_url")
        if not text_url:
            # Try to get it from the individual document endpoint
            doc_num = doc.get("document_number", "")
            if doc_num:
                self.rate_limiter.wait()
                try:
                    resp = self.client.get(f"/documents/{doc_num}.json")
                    resp.raise_for_status()
                    detail = resp.json()
                    text_url = detail.get("raw_text_url") or detail.get("body_html_url")
                except Exception:
                    pass

        if not text_url:
            return ""

        self.rate_limiter.wait()
        try:
            resp = self.client.get(text_url)
            resp.raise_for_status()
            text = resp.text

            # If HTML, strip tags
            if "<html" in text.lower() or "<body" in text.lower():
                text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL)
                text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
                text = re.sub(r'<[^>]+>', ' ', text)

            text = re.sub(r'\s+', ' ', text).strip()
            return text
        except Exception as e:
            logger.warning(f"Failed to fetch text from {text_url}: {e}")
            return ""

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all presidential documents with full text."""
        for doc in self._paginate_docs():
            text = self._fetch_full_text(doc)
            if text and len(text) >= 100:
                doc["_full_text"] = text
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents published since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        params = {"conditions[publication_date][gte]": since_str}
        for doc in self._paginate_docs(extra_params=params):
            text = self._fetch_full_text(doc)
            if text and len(text) >= 100:
                doc["_full_text"] = text
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw Federal Register doc into standard schema."""
        date = raw.get("signing_date") or raw.get("publication_date", "")

        # Determine subtype
        subtype = raw.get("subtype", "")
        eo_number = raw.get("executive_order_number", "")

        doc_number = raw.get("document_number", "")

        return {
            "_id": doc_number,
            "_source": "US/PresidentialDocuments",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_full_text", ""),
            "date": date,
            "url": raw.get("html_url", ""),
            "document_number": doc_number,
            "executive_order_number": eo_number,
            "document_subtype": subtype,
            "signing_date": raw.get("signing_date", ""),
            "publication_date": raw.get("publication_date", ""),
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample records for validation."""
        samples = []

        for doc in self._paginate_docs(max_pages=2):
            # Get full document details for text URL
            doc_num = doc.get("document_number", "")
            if not doc_num:
                continue

            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/documents/{doc_num}.json")
                resp.raise_for_status()
                detail = resp.json()
            except Exception as e:
                logger.warning(f"Failed to get detail for {doc_num}: {e}")
                continue

            text = self._fetch_full_text(detail)
            if not text or len(text) < 100:
                continue

            detail["_full_text"] = text
            normalized = self.normalize(detail)
            samples.append(normalized)
            logger.info(f"  {doc_num}: {normalized['title'][:60]} ({len(text)} chars)")

            if len(samples) >= 12:
                break

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/PresidentialDocuments fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PresidentialDocsScraper()

    if args.command == "test-api":
        print("Testing Federal Register API for presidential documents...")
        count = 0
        for doc in scraper._paginate_docs(max_pages=1):
            count += 1
        print(f"OK: Got {count} results from first page")
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                    assert s.get("date"), f"Missing date: {s['_id']}"
                print("All validation checks passed!")
            return

        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
