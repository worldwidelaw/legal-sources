#!/usr/bin/env python3
"""
LT/ConstitutionalCourt -- Lithuanian Constitutional Court Data Fetcher

Fetches Constitutional Court decisions from the data.gov.lt Open Data API.

Strategy:
  - Use the REST API at get.data.gov.lt/datasets/gov/lrsk/teises_aktai/Dokumentas
  - Filter by priemusi_inst.contains("Konstitucinis") for CC documents
  - Paginate using vda_id-based keyset pagination (cursor pagination has API issues with filters)
  - Full text is directly available in the tekstas_lt field

Endpoints:
  - Documents: https://get.data.gov.lt/datasets/gov/lrsk/teises_aktai/Dokumentas
  - Dataset info: https://data.gov.lt/datasets/2613/

Data:
  - Document types: Nutarimas (Ruling), Sprendimas (Decision), Išvada (Conclusion),
    Pranešimas (Announcement), Potvarkis (Order), Atitaisymas (Correction)
  - Full text in Lithuanian (tekstas_lt field)
  - License: CC BY 4.0
  - Total: ~1,300 documents (as of 2026-02)
  - Note: data.gov.lt has fewer records than lrkt.lt archive (Cloudflare protected)

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

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
logger = logging.getLogger("legal-data-hunter.LT.constitutionalcourt")

# Base URL for the data.gov.lt API
BASE_URL = "https://get.data.gov.lt"
DOCUMENTS_ENDPOINT = "/datasets/gov/lrsk/teises_aktai/Dokumentas"

# Filter for Constitutional Court documents
# Use "Konstitu" to capture both:
# - "Lietuvos Respublikos Konstitucinis Teismas" (the Court itself - rulings, decisions)
# - "Lietuvos Respublikos Konstitucinio Teismo pirmininkas" (Chairman - orders, announcements)
CC_FILTER = 'priemusi_inst.contains("Konstitu")'

# Default page size
DEFAULT_LIMIT = 100


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for LT/ConstitutionalCourt -- Lithuanian Constitutional Court.
    Country: LT
    URL: https://www.lrkt.lt

    Data types: case_law
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.headers = {
            "User-Agent": "Legal-Data-Hunter/1.0 (EU Legal Research; contact@example.com)",
            "Accept": "application/json",
        }

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

    def _fetch_page(self, last_vda_id: Optional[str] = None, limit: int = DEFAULT_LIMIT) -> Dict[str, Any]:
        """
        Fetch a page of Constitutional Court documents from the API.

        Uses vda_id-based keyset pagination since cursor pagination has issues
        when combined with filters.

        Args:
            last_vda_id: The vda_id of the last document from previous page
            limit: Number of records to fetch

        Returns:
            Dict with _data (list of documents)
        """
        url = f"{BASE_URL}{DOCUMENTS_ENDPOINT}"

        # Build query with filter and pagination
        if last_vda_id:
            query = f'{CC_FILTER}&vda_id>"{last_vda_id}"&_sort=vda_id&_limit={limit}'
        else:
            query = f'{CC_FILTER}&_sort=vda_id&_limit={limit}'

        full_url = f"{url}?{query}"

        try:
            self.rate_limiter.wait()
            resp = requests.get(full_url, headers=self.headers, timeout=60)
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return {"_data": []}

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Constitutional Court documents.

        Iterates through all pages using vda_id-based keyset pagination.
        """
        last_vda_id = None
        page_num = 0
        total_fetched = 0

        while True:
            page_num += 1
            logger.info(f"Fetching page {page_num}...")

            result = self._fetch_page(last_vda_id=last_vda_id)
            documents = result.get("_data", [])

            if not documents:
                logger.info(f"No more documents. Total fetched: {total_fetched}")
                break

            for doc in documents:
                total_fetched += 1
                yield doc

            # Update pagination cursor
            last_vda_id = documents[-1].get("vda_id", "")

            # Check if we got a full page (more may exist)
            if len(documents) < DEFAULT_LIMIT:
                logger.info(f"Reached last page. Total fetched: {total_fetched}")
                break

            # Safety limit
            if page_num > 100:
                logger.warning("Reached page limit (100), stopping")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield Constitutional Court documents updated since the given date.

        Note: Uses the same pagination but filters by publication date.
        """
        last_vda_id = None
        page_num = 0
        found_old = False

        while not found_old:
            page_num += 1
            result = self._fetch_page(last_vda_id=last_vda_id)
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
                            found_old = True
                            continue
                    except ValueError:
                        pass

                yield doc

            # Update pagination cursor
            last_vda_id = documents[-1].get("vda_id", "")

            if len(documents) < DEFAULT_LIMIT:
                break

            # Limit pages for updates
            if page_num > 20:
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        # Extract key fields
        doc_id = raw.get("dokumento_id", "")
        vda_id = raw.get("vda_id", "")
        title = raw.get("pavadinimas", "")
        full_text = self._clean_text(raw.get("tekstas_lt", ""))
        doc_type = raw.get("rusis", "")
        url = raw.get("nuoroda", "")

        # Date handling - prefer priimtas (decision date), fallback to paskelbta_tar
        date = raw.get("priimtas") or raw.get("paskelbta_tar") or ""
        if date:
            # Ensure ISO 8601 format
            date = date[:10]  # Take just YYYY-MM-DD

        # Map Lithuanian document types to English
        doc_type_map = {
            "Nutarimas": "ruling",
            "Sprendimas": "decision",
            "Išvada": "conclusion",
            "Pranešimas": "announcement",
            "Atitaisymas": "correction",
            "Potvarkis": "order",
        }
        doc_type_en = doc_type_map.get(doc_type, doc_type.lower() if doc_type else "unknown")

        # Validity status
        validity = raw.get("galioj_busena", "")
        is_valid = validity == "galioja"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LT/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Court-specific fields
            "document_type": doc_type_en,
            "document_type_lt": doc_type,
            "court": "Lietuvos Respublikos Konstitucinis Teismas",
            "court_en": "Constitutional Court of the Republic of Lithuania",
            # Additional metadata
            "dokumento_id": doc_id,
            "vda_id": vda_id,
            "tar_kodas": raw.get("tar_kodas", ""),
            "atv_dok_nr": raw.get("atv_dok_nr", ""),
            "validity_status": validity,
            "is_valid": is_valid,
            "entry_into_force": raw.get("isigalioja", ""),
            "lost_force": raw.get("negalioja", ""),
            "published_tar": raw.get("paskelbta_tar", ""),
            "language": "lt",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LT/ConstitutionalCourt API endpoints...")

        # Test API endpoint with filter
        print("\n1. Testing Constitutional Court filter...")
        try:
            result = self._fetch_page(limit=5)
            docs = result.get("_data", [])
            print(f"   Found {len(docs)} documents")

            if docs:
                doc = docs[0]
                print(f"   Sample ID: {doc.get('dokumento_id', 'N/A')}")
                print(f"   Title: {doc.get('pavadinimas', 'N/A')[:80]}...")
                print(f"   Type: {doc.get('rusis', 'N/A')}")
                print(f"   Institution: {doc.get('priemusi_inst', 'N/A')}")

                text = doc.get("tekstas_lt", "")
                print(f"   Full text length: {len(text)} characters")
                if text:
                    print(f"   Text preview: {text[:200]}...")

        except Exception as e:
            print(f"   ERROR: {e}")

        # Test pagination
        print("\n2. Testing pagination...")
        try:
            result = self._fetch_page(limit=5)
            docs = result.get("_data", [])
            if docs:
                last_vda = docs[-1].get("vda_id", "")
                print(f"   Last vda_id: {last_vda}")

                # Fetch second page
                result2 = self._fetch_page(last_vda_id=last_vda, limit=5)
                docs2 = result2.get("_data", [])
                print(f"   Page 2 documents: {len(docs2)}")

                if docs2:
                    # Verify documents are different
                    first_ids = {d.get("dokumento_id") for d in docs}
                    second_ids = {d.get("dokumento_id") for d in docs2}
                    overlap = first_ids & second_ids
                    print(f"   ID overlap (should be 0): {len(overlap)}")

        except Exception as e:
            print(f"   ERROR: {e}")

        # Count total documents
        print("\n3. Counting total documents...")
        try:
            total = 0
            last_vda = None
            pages = 0
            while pages < 20:  # Safety limit for test
                result = self._fetch_page(last_vda_id=last_vda, limit=100)
                docs = result.get("_data", [])
                if not docs:
                    break
                total += len(docs)
                last_vda = docs[-1].get("vda_id", "")
                pages += 1
                if len(docs) < 100:
                    break
            print(f"   Total Constitutional Court documents: {total}")

        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = ConstitutionalCourtScraper()

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
