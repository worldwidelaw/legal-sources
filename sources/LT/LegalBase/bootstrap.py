#!/usr/bin/env python3
"""
LT/LegalBase -- Lithuanian Legal Database (TAR) Data Fetcher

Fetches Lithuanian legislation from the data.gov.lt Open Data API.

Strategy:
  - Use the REST API at get.data.gov.lt/datasets/gov/lrsk/teises_aktai/Dokumentas
  - Paginate using cursor-based pagination (_page.next)
  - Full text is directly available in the tekstas_lt field

Endpoints:
  - Documents: https://get.data.gov.lt/datasets/gov/lrsk/teises_aktai/Dokumentas
  - Dataset info: https://data.gov.lt/datasets/2613/

Data:
  - Document types: Istatymas (Law), Isakymas (Order), Nutarimas (Resolution), etc.
  - Full text in Lithuanian (tekstas_lt field)
  - License: CC BY 4.0

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent documents)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LT.legalbase")

# Base URL for the data.gov.lt API
BASE_URL = "https://get.data.gov.lt"
DOCUMENTS_ENDPOINT = "/datasets/gov/lrsk/teises_aktai/Dokumentas"

# Default page size
DEFAULT_LIMIT = 100


class LegalBaseScraper(BaseScraper):
    """
    Scraper for LT/LegalBase -- Lithuanian Legal Database (TAR).
    Country: LT
    URL: https://data.gov.lt/datasets/2613/

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (EU Legal Research; contact@example.com)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _fetch_page(self, cursor: Optional[str] = None, limit: int = DEFAULT_LIMIT) -> Dict[str, Any]:
        """
        Fetch a page of documents from the API.

        Args:
            cursor: Pagination cursor (base64 encoded)
            limit: Number of records to fetch

        Returns:
            Dict with _data (list of documents) and _page (pagination info)
        """
        url = f"{BASE_URL}{DOCUMENTS_ENDPOINT}"
        params = {"_limit": limit}

        if cursor:
            # Cursor is passed as a query parameter
            url = f"{url}?_page={cursor}&_limit={limit}"
            params = None

        try:
            self.rate_limiter.wait()

            if params:
                resp = requests.get(url, params=params, timeout=60, headers={
                    "User-Agent": "WorldWideLaw/1.0",
                    "Accept": "application/json",
                })
            else:
                resp = requests.get(url, timeout=60, headers={
                    "User-Agent": "WorldWideLaw/1.0",
                    "Accept": "application/json",
                })

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return {"_data": [], "_page": {}}

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Lithuanian Legal Database.

        Iterates through all pages using cursor-based pagination.
        """
        cursor = None
        page_num = 0
        total_fetched = 0

        while True:
            page_num += 1
            logger.info(f"Fetching page {page_num}...")

            result = self._fetch_page(cursor=cursor)
            documents = result.get("_data", [])

            if not documents:
                logger.info(f"No more documents. Total fetched: {total_fetched}")
                break

            for doc in documents:
                total_fetched += 1
                yield doc

            # Check for next page
            page_info = result.get("_page", {})
            next_cursor = page_info.get("next")

            if not next_cursor:
                logger.info(f"Reached last page. Total fetched: {total_fetched}")
                break

            cursor = next_cursor

            # Safety limit for full bootstrap
            if page_num > 10000:
                logger.warning("Reached page limit (10000), stopping")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Note: The API doesn't have a direct date filter, so we fetch recent
        documents and filter by publication date.
        """
        cursor = None
        page_num = 0

        while True:
            page_num += 1
            result = self._fetch_page(cursor=cursor)
            documents = result.get("_data", [])

            if not documents:
                break

            for doc in documents:
                # Check if document was published after 'since'
                pub_date = doc.get("paskelbta_tar") or doc.get("priimtas")
                if pub_date:
                    try:
                        doc_date = datetime.strptime(pub_date[:10], '%Y-%m-%d')
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            # Documents are roughly chronological, so we can stop
                            # when we see older documents
                            continue
                    except ValueError:
                        pass

                yield doc

            # Check for next page
            page_info = result.get("_page", {})
            next_cursor = page_info.get("next")

            if not next_cursor:
                break

            cursor = next_cursor

            # Limit pages for updates (most recent data)
            if page_num > 100:
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        # Extract key fields
        doc_id = raw.get("dokumento_id", "")
        title = raw.get("pavadinimas", "")
        full_text = self._clean_text(raw.get("tekstas_lt", ""))
        doc_type = raw.get("rusis", "")
        url = raw.get("nuoroda", "")

        # Date handling - prefer priimtas (adoption date), fallback to paskelbta_tar
        date = raw.get("priimtas") or raw.get("paskelbta_tar") or ""
        if date:
            # Ensure ISO 8601 format
            date = date[:10]  # Take just YYYY-MM-DD

        # Validity status
        validity = raw.get("galioj_busena", "")
        is_valid = validity == "galioja"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LT/LegalBase",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "dokumento_id": doc_id,
            "tar_kodas": raw.get("tar_kodas", ""),
            "atv_dok_nr": raw.get("atv_dok_nr", ""),
            "document_type": doc_type,
            "adopting_institution": raw.get("priemusi_inst", ""),
            "preparing_institution": raw.get("parengusi_inst", ""),
            "validity_status": validity,
            "is_valid": is_valid,
            "entry_into_force": raw.get("isigalioja", ""),
            "lost_force": raw.get("negalioja", ""),
            "published_tar": raw.get("paskelbta_tar", ""),
            "document_group": raw.get("dok_grupe", ""),
            "language": "lt",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LT/LegalBase API endpoints...")

        # Test API endpoint
        print("\n1. Testing documents endpoint...")
        try:
            result = self._fetch_page(limit=3)
            docs = result.get("_data", [])
            print(f"   Found {len(docs)} documents")

            if docs:
                doc = docs[0]
                print(f"   Sample ID: {doc.get('dokumento_id', 'N/A')}")
                print(f"   Title: {doc.get('pavadinimas', 'N/A')[:80]}...")
                print(f"   Type: {doc.get('rusis', 'N/A')}")

                text = doc.get("tekstas_lt", "")
                print(f"   Full text length: {len(text)} characters")
                if text:
                    print(f"   Text preview: {text[:200]}...")

        except Exception as e:
            print(f"   ERROR: {e}")

        # Test pagination
        print("\n2. Testing pagination...")
        try:
            result = self._fetch_page(limit=2)
            page_info = result.get("_page", {})
            next_cursor = page_info.get("next")
            print(f"   Has next page: {bool(next_cursor)}")
            if next_cursor:
                print(f"   Next cursor: {next_cursor[:50]}...")

        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = LegalBaseScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
