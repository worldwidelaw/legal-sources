#!/usr/bin/env python3
"""
INTL/ICCLegalTools -- ICC Legal Tools Database

Fetches ICC judicial documents from the Legal Tools Database via REST API,
downloads PDFs from S3, and extracts full text.

Strategy:
  - Paginate through LoopBack REST API at /api/ltddocs
  - Filter by organisation=ICC, contentType=judicial_document
  - Download PDFs from S3 (orignalPdfURL field)
  - Extract full text via common/pdf_extract.extract_pdf_markdown
  - 52,000+ ICC judicial documents

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
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
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ICCLegalTools")

API_BASE = "https://www.legal-tools.org/api"
LTD_URL = f"{API_BASE}/ltddocs"
PAGE_SIZE = 1000

# LoopBack filter for ICC judicial documents
ICC_FILTER = {
    "organisation": "International Criminal Court (ICC)",
    "contentType": "judicial_document",
}

# Fields to fetch in listing (minimize payload)
LIST_FIELDS = {
    "id": True,
    "slug": True,
    "title": True,
    "externalId": True,
    "dateCreated": True,
    "orignalPdfURL": True,
    "pdfURL": True,
    "organisation": True,
    "contentType": True,
    "judicialDocumentType": True,
    "numberOfPages": True,
    "confidentiality": True,
}


class ICCLegalToolsScraper(BaseScraper):
    """
    Scraper for INTL/ICCLegalTools -- ICC Legal Tools Database.
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
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "application/json",
        })
        self._existing_ids: Optional[set] = None

    def _get_existing_ids(self) -> set:
        """Load IDs already in Neon with text."""
        if self._existing_ids is None:
            self._existing_ids = preload_existing_ids("INTL/ICCLegalTools", table="case_law")
            logger.info(f"Preloaded {len(self._existing_ids)} existing IDs from Neon")
        return self._existing_ids

    def _get_count(self, where: Optional[dict] = None) -> int:
        """Get total number of matching documents."""
        w = where or ICC_FILTER
        params = {"where": json.dumps(w)}
        r = self.session.get(f"{LTD_URL}/count", params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("count", 0)

    def _fetch_page(self, skip: int, limit: int, where: Optional[dict] = None) -> list:
        """Fetch a page of LTD documents."""
        w = where or ICC_FILTER
        filt = {
            "limit": limit,
            "skip": skip,
            "fields": LIST_FIELDS,
            "where": w,
            "order": "dateCreated DESC",
        }
        params = {"filter": json.dumps(filt)}
        r = self.session.get(LTD_URL, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _extract_text(self, doc: dict) -> Optional[str]:
        """Download PDF and extract text for a document."""
        doc_id = doc.get("externalId") or doc.get("slug") or doc.get("id", "")
        pdf_url = doc.get("orignalPdfURL") or doc.get("pdfURL")

        if not pdf_url:
            logger.debug(f"  No PDF URL for {doc_id}")
            return None

        # Use canonical pdfURL if orignalPdfURL is missing
        if not pdf_url.startswith("http"):
            slug = doc.get("slug", "")
            if slug:
                pdf_url = f"https://www.legal-tools.org/doc/{slug}/pdf/"
            else:
                return None

        text = extract_pdf_markdown(
            source="INTL/ICCLegalTools",
            source_id=str(doc_id),
            pdf_url=pdf_url,
            table="case_law",
        )
        return text

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw LTD document into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        ext_id = raw.get("externalId", "")
        slug = raw.get("slug", "")
        title = raw.get("title", "").strip()

        # Parse date
        date = None
        date_raw = raw.get("dateCreated")
        if date_raw:
            try:
                date_str = str(date_raw)
                if "T" in date_str:
                    date = date_str[:10]
                elif len(date_str) >= 10:
                    date = date_str[:10]
            except (ValueError, TypeError):
                pass

        url = f"https://www.legal-tools.org/doc/{slug}/" if slug else "https://www.legal-tools.org/"

        return {
            "_id": f"ICC-LTD-{ext_id}" if ext_id else f"ICC-LTD-{slug}",
            "_source": "INTL/ICCLegalTools",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "external_id": ext_id,
            "slug": slug,
            "document_type": raw.get("judicialDocumentType", ""),
            "num_pages": raw.get("numberOfPages"),
            "court": "International Criminal Court",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all ICC judicial documents with full text from PDFs."""
        total = self._get_count()
        logger.info(f"Total ICC judicial documents: {total}")

        existing = self._get_existing_ids()
        skip = 0
        yielded = 0
        skipped_existing = 0

        while skip < total:
            logger.info(f"Fetching page at offset {skip}/{total}")
            try:
                docs = self._fetch_page(skip, PAGE_SIZE)
            except requests.RequestException as e:
                logger.error(f"Failed to fetch page at skip={skip}: {e}")
                skip += PAGE_SIZE
                continue

            if not docs:
                break

            for doc in docs:
                doc_id = doc.get("externalId") or doc.get("slug") or str(doc.get("id", ""))

                # Skip if already in Neon
                if doc_id in existing:
                    skipped_existing += 1
                    continue

                # Skip confidential docs
                if doc.get("confidentiality") == "confidential":
                    continue

                text = self._extract_text(doc)
                if text:
                    doc["text"] = text
                    yield doc
                    yielded += 1
                    if yielded % 50 == 0:
                        logger.info(f"Progress: {yielded} yielded, {skipped_existing} skipped (existing)")

                time.sleep(0.5)

            skip += PAGE_SIZE
            time.sleep(1)

        logger.info(f"Finished: {yielded} yielded, {skipped_existing} skipped (already in Neon)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents created after given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        where = {
            **ICC_FILTER,
            "dateCreated": {"gt": since_str},
        }

        total = self._get_count(where)
        logger.info(f"Documents since {since_str}: {total}")

        existing = self._get_existing_ids()
        skip = 0

        while skip < total:
            docs = self._fetch_page(skip, PAGE_SIZE, where=where)
            if not docs:
                break

            for doc in docs:
                doc_id = doc.get("externalId") or doc.get("slug") or str(doc.get("id", ""))
                if doc_id in existing:
                    continue

                text = self._extract_text(doc)
                if text:
                    doc["text"] = text
                    yield doc

                time.sleep(0.5)

            skip += PAGE_SIZE
            time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ICCLegalTools data fetcher")
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

    scraper = ICCLegalToolsScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            count = scraper._get_count()
            logger.info(f"OK: {count} ICC judicial documents")
            docs = scraper._fetch_page(0, 3)
            for d in docs:
                logger.info(f"  {d.get('externalId', 'N/A')}: {d.get('title', '')[:80]}")
                pdf_url = d.get("orignalPdfURL", "")
                logger.info(f"  PDF: {pdf_url[:100]}")
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
