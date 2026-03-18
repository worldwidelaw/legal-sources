#!/usr/bin/env python3
"""
AT/Findok -- Austrian Tax Doctrine (Finanzdokumentation) Fetcher

Fetches BMF tax doctrine from the Findok JSON API.

Strategy:
  - Bootstrap: Paginates through documents using Findok API.
  - Update: Fetches new documents using the neuInFindok endpoint.
  - Sample: Fetches 10+ records for validation.

API Endpoints:
  - /findok/api/neuInFindok/sync - Search/list documents by type
  - /findok/api/volltext - Get full text content
  - /findok/api/richtlinien/materieGruppe - Get guidelines by subject

Document types (suchtypen):
  - RICHTLINIEN - Guidelines
  - AMTLICHE_VEROEFFENTLICHUNGEN - Official publications
  - BFG - Federal Finance Court decisions
  - UFS - Independent Tax Senate decisions
  - EAS - Express Reply Service (international tax)
  - ERLAESSE - Decrees

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AT.Findok")

# Findok API base URL
API_BASE = "https://findok.bmf.gv.at/findok"

# Document types to fetch
DOC_TYPES = [
    "RICHTLINIEN",
    "AMTLICHE_VEROEFFENTLICHUNGEN",
    "BFG",
    "UFS",
    "EAS",
    "ERLAESSE",
]


class FindokScraper(BaseScraper):
    """
    Scraper for AT/Findok -- Austrian Tax Doctrine.
    Country: AT
    URL: https://findok.bmf.gv.at/findok

    Data types: doctrine
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _paginate(
        self,
        suchtypen: list = None,
        max_pages: int = None,
        page_size: int = 100,
    ):
        """
        Generator that paginates through Findok documents.

        Yields individual document references (raw dicts from the API).
        """
        if suchtypen is None:
            suchtypen = DOC_TYPES

        for suchtyp in suchtypen:
            page = 1
            total_pages = None

            while True:
                if max_pages and page > max_pages:
                    logger.info(f"Reached max_pages={max_pages} for {suchtyp}, moving to next type")
                    break

                self.rate_limiter.wait()

                try:
                    resp = self.client.get(
                        "/api/neuInFindok/sync",
                        params={
                            "page": str(page),
                            "size": str(page_size),
                            "suchtypen": suchtyp,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    logger.error(f"API error on {suchtyp} page {page}: {e}")
                    # Retry once after a pause
                    time.sleep(5)
                    try:
                        resp = self.client.get(
                            "/api/neuInFindok/sync",
                            params={
                                "page": str(page),
                                "size": str(page_size),
                                "suchtypen": suchtyp,
                            },
                        )
                        resp.raise_for_status()
                        data = resp.json()
                    except Exception as e2:
                        logger.error(f"Retry failed for {suchtyp}: {e2}")
                        break

                # The API returns an array, take first element
                if isinstance(data, list) and data:
                    result = data[0]
                else:
                    logger.warning(f"Unexpected response format for {suchtyp}")
                    break

                page_results = result.get("pageResults", {})

                # Parse total pages on first page
                if total_pages is None:
                    total_pages = page_results.get("totalPages", 0)
                    total_size = page_results.get("totalSize", 0)
                    logger.info(f"{suchtyp}: {total_size} total documents, {total_pages} pages")
                    if total_pages == 0:
                        break

                # Extract documents
                docs = page_results.get("searchResults", [])
                if not docs:
                    logger.info(f"No more documents on page {page} for {suchtyp}")
                    break

                for doc in docs:
                    # Add suchtyp to the document for reference
                    doc["_suchtyp"] = suchtyp
                    yield doc

                # Check if we've fetched all pages
                if page >= total_pages:
                    logger.info(f"Fetched all {total_size} {suchtyp} records")
                    break

                page += 1
                logger.info(f"  {suchtyp} page {page}/{total_pages}")

    def _fetch_volltext(self, dokument_id: str, segment_id: str = None) -> dict:
        """
        Fetch full text content for a document.

        Returns the volltext response as a dict with 'content' and metadata.
        """
        params = {"dokumentId": dokument_id}
        if segment_id:
            params["segmentId"] = segment_id

        self.rate_limiter.wait()

        try:
            resp = self.client.get("/api/volltext", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug(f"Failed to fetch volltext for {dokument_id}: {e}")
            return {}

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content to plain text."""
        if not html_content:
            return ""

        # Remove script and style tags
        text = re.sub(r"<script[^>]*>.*?</script>", "", html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)

        # Convert common block elements to newlines
        text = re.sub(r"</?(p|div|br|tr|li|h[1-6])[^>]*>", "\n", text, flags=re.IGNORECASE)

        # Remove remaining HTML tags
        text = re.sub(r"<[^>]+>", " ", text)

        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Findok documents.

        Full fetch covers multiple document types.
        """
        logger.info("Fetching all Findok documents")
        for doc in self._paginate():
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield Findok documents added/modified recently.

        The neuInFindok endpoint already returns documents sorted by recency.
        We'll paginate and check dates.
        """
        logger.info(f"Fetching Findok updates since {since.isoformat()}")

        # Fetch from each document type, limit to recent pages
        for doc in self._paginate(max_pages=5):
            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw Findok API response into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from volltext API.
        """
        dokument_id = raw.get("dokumentId", "")
        segment_id = raw.get("segmentId", "")
        konseh_id = raw.get("konsehId", "")

        # Generate unique ID
        doc_id = segment_id or dokument_id or konseh_id
        if not doc_id:
            doc_id = f"findok-{hash(json.dumps(raw, sort_keys=True))}"

        # Fetch full text
        volltext_data = self._fetch_volltext(dokument_id, segment_id)
        content_html = volltext_data.get("content", "")
        full_text = self._clean_html(content_html)

        # If no content from API, try to get any available text from raw
        if not full_text:
            # Try title as fallback (but mark as incomplete)
            full_text = raw.get("title", "")
            if full_text:
                logger.debug(f"Using title as fallback text for {doc_id}")

        # Parse date from gueltigAbString (e.g., "gültig ab 11.03.2026")
        date_str = raw.get("gueltigAbString", "") or raw.get("inFindokSeit", "")
        date = ""
        date_match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
        if date_match:
            day, month, year = date_match.groups()
            date = f"{year}-{month}-{day}"

        # Construct URL
        url = ""
        if dokument_id:
            url = f"https://findok.bmf.gv.at/findok/?dokumentId={dokument_id}"
        elif segment_id:
            url = f"https://findok.bmf.gv.at/findok/?segmentId={segment_id}"

        # Extract document type
        suchtyp = raw.get("_suchtyp", "")
        dokumenttyp_text = raw.get("dokumenttyp", "")

        # Map suchtyp to standard type
        type_mapping = {
            "RICHTLINIEN": "doctrine",
            "AMTLICHE_VEROEFFENTLICHUNGEN": "doctrine",
            "BFG": "case_law",
            "UFS": "case_law",
            "EAS": "doctrine",
            "ERLAESSE": "doctrine",
        }
        doc_type = type_mapping.get(suchtyp, "doctrine")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "AT/Findok",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": self._clean_html(raw.get("title", "")),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Findok-specific fields
            "dokument_id": dokument_id,
            "segment_id": segment_id,
            "konseh_id": konseh_id,
            "dokumenttyp": self._clean_html(dokumenttyp_text),
            "suchtyp": suchtyp,
            "gueltig_ab": date_str,
            "in_findok_seit": raw.get("inFindokSeit", ""),
            "intern": raw.get("intern", False),
            "extern": raw.get("externesDokument", False),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Findok API...")

        # Test neuInFindok endpoint
        resp = self.client.get(
            "/api/neuInFindok/sync",
            params={"page": "1", "size": "5", "suchtypen": "RICHTLINIEN"},
        )
        data = resp.json()

        if isinstance(data, list) and data:
            result = data[0]
            page_results = result.get("pageResults", {})
            total = page_results.get("totalSize", 0)
            print(f"  RICHTLINIEN: {total} documents")
        else:
            print("  Unexpected response format")
            return

        # Test volltext endpoint with first document
        docs = page_results.get("searchResults", [])
        if docs:
            doc = docs[0]
            dokument_id = doc.get("dokumentId", "")
            segment_id = doc.get("segmentId", "")
            print(f"  Testing volltext for dokumentId={dokument_id}")

            volltext_resp = self.client.get(
                "/api/volltext",
                params={"dokumentId": dokument_id, "segmentId": segment_id},
            )
            volltext_data = volltext_resp.json()
            content = volltext_data.get("content", "")
            if content:
                text_preview = self._clean_html(content)[:200]
                print(f"  Volltext preview: {text_preview}...")
            else:
                print("  No content in volltext response")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = FindokScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

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
