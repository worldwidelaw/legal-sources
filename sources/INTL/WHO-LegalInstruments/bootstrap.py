#!/usr/bin/env python3
"""
INTL/WHO-LegalInstruments — WHO Governing Bodies Resolutions & Decisions

Fetches World Health Assembly and Executive Board resolutions, decisions, and
related legal instruments from the WHO IRIS DSpace 7 REST API (~6,800 English
documents from 1948–present).

Strategy:
  - Search the Governing Bodies community (scope 347f287d...) in IRIS
  - Filter: publisher="World Health Organization" (English), subject="Resolutions and decisions"
  - Paginate through results (100 per page)
  - For each item, fetch TEXT bundle bitstreams for extracted full text
  - Falls back to ORIGINAL PDF filename if no TEXT bundle exists

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap            # Full fetch all documents
    python bootstrap.py test                 # Quick connectivity test
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WHO-LegalInstruments")

BASE_URL = "https://iris.who.int/server/api"
GOV_BODIES_SCOPE = "347f287d-d8af-4e21-85cc-0fba3730380c"
SEARCH_URL = f"{BASE_URL}/discover/search/objects"
PAGE_SIZE = 100
RATE_LIMIT = 1.5


class WHOLegalInstrumentsScraper(BaseScraper):
    """Scraper for INTL/WHO-LegalInstruments via WHO IRIS DSpace 7 API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _search_items(self, page: int = 0) -> dict:
        """Search IRIS for English resolutions in Governing Bodies scope."""
        params = {
            "scope": GOV_BODIES_SCOPE,
            "query": "*",
            "dsoType": "ITEM",
            "f.subject": "Resolutions and decisions,equals",
            "f.publisher": "World Health Organization,equals",
            "size": PAGE_SIZE,
            "page": page,
            "sort": "dc.date.issued,DESC",
        }
        resp = self.session.get(SEARCH_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _get_text_content(self, item_id: str) -> Optional[str]:
        """Fetch extracted text from an item's TEXT bundle."""
        # Get bundles for the item
        bundles_url = f"{BASE_URL}/core/items/{item_id}/bundles"
        try:
            resp = self.session.get(bundles_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Error fetching bundles for {item_id}: {e}")
            return None

        bundles = resp.json().get("_embedded", {}).get("bundles", [])
        text_bundle = None
        for b in bundles:
            if b.get("name") == "TEXT":
                text_bundle = b
                break

        if not text_bundle:
            return None

        # Get bitstreams from TEXT bundle
        bs_url = text_bundle.get("_links", {}).get("bitstreams", {}).get("href")
        if not bs_url:
            return None

        time.sleep(RATE_LIMIT)
        try:
            resp = self.session.get(bs_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Error fetching TEXT bitstreams for {item_id}: {e}")
            return None

        bitstreams = resp.json().get("_embedded", {}).get("bitstreams", [])
        if not bitstreams:
            return None

        # Get the largest text file (some items have multiple)
        best_bs = max(bitstreams, key=lambda b: b.get("sizeBytes", 0))
        content_url = best_bs.get("_links", {}).get("content", {}).get("href")
        if not content_url:
            return None

        time.sleep(RATE_LIMIT)
        try:
            resp = self.session.get(content_url, timeout=60)
            resp.raise_for_status()
            text = resp.text.strip()
            # Clean up common PDF extraction artifacts
            text = re.sub(r'\n{4,}', '\n\n\n', text)
            return text if len(text) > 50 else None
        except requests.RequestException as e:
            logger.warning(f"Error fetching text content for {item_id}: {e}")
            return None

    def _extract_metadata(self, item: dict) -> dict:
        """Extract structured metadata from an IRIS item."""
        meta = item.get("metadata", {})

        def first_val(field: str) -> Optional[str]:
            vals = meta.get(field, [])
            return vals[0].get("value") if vals else None

        def all_vals(field: str) -> list:
            return [v.get("value", "") for v in meta.get(field, [])]

        title = first_val("dc.title") or "Untitled"
        date_issued = first_val("dc.date.issued")
        identifier = first_val("dc.identifier.govdoc") or first_val("dc.identifier")
        uri = first_val("dc.identifier.uri") or ""
        description = first_val("dc.description") or ""
        subjects = all_vals("dc.subject")
        mesh_terms = all_vals("dc.subject.mesh")
        authors = all_vals("dc.contributor.author")
        spatial = first_val("dc.coverage.spatial") or ""
        doc_type = first_val("dc.type") or "Governing Bodies Documents"
        publisher = first_val("dc.publisher") or ""

        return {
            "item_id": item.get("id", ""),
            "title": title,
            "date_issued": date_issued,
            "identifier": identifier,
            "uri": uri,
            "description": description,
            "subjects": subjects,
            "mesh_terms": mesh_terms,
            "authors": authors,
            "spatial": spatial,
            "doc_type": doc_type,
            "publisher": publisher,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all English resolutions/decisions from WHO Governing Bodies."""
        page = 0
        total_pages = None

        while True:
            logger.info(f"Fetching page {page}" + (f"/{total_pages}" if total_pages else ""))
            try:
                data = self._search_items(page=page)
            except requests.RequestException as e:
                logger.error(f"Search request failed on page {page}: {e}")
                break

            sr = data.get("_embedded", {}).get("searchResult", {})
            page_info = sr.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            total_elements = page_info.get("totalElements", 0)

            if page == 0:
                logger.info(f"Total items to fetch: {total_elements} across {total_pages} pages")

            objects = sr.get("_embedded", {}).get("objects", [])
            if not objects:
                break

            for obj in objects:
                item = obj.get("_embedded", {}).get("indexableObject", {})
                if not item:
                    continue

                metadata = self._extract_metadata(item)

                # Fetch full text from TEXT bundle
                time.sleep(RATE_LIMIT)
                text = self._get_text_content(metadata["item_id"])

                if text:
                    metadata["text"] = text
                    yield metadata
                else:
                    logger.debug(f"No text available for: {metadata['title'][:80]}")

            page += 1
            if page >= total_pages:
                break

            time.sleep(RATE_LIMIT)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents added/modified since the given date."""
        since_str = since.strftime("%Y-%m-%d")
        page = 0

        while True:
            params = {
                "scope": GOV_BODIES_SCOPE,
                "query": f"dc.date.accessioned:[{since_str} TO *]",
                "dsoType": "ITEM",
                "f.subject": "Resolutions and decisions,equals",
                "f.publisher": "World Health Organization,equals",
                "size": PAGE_SIZE,
                "page": page,
                "sort": "dc.date.accessioned,DESC",
            }
            try:
                resp = self.session.get(SEARCH_URL, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                logger.error(f"Update search failed on page {page}: {e}")
                break

            sr = data.get("_embedded", {}).get("searchResult", {})
            objects = sr.get("_embedded", {}).get("objects", [])
            if not objects:
                break

            for obj in objects:
                item = obj.get("_embedded", {}).get("indexableObject", {})
                if not item:
                    continue
                metadata = self._extract_metadata(item)
                time.sleep(RATE_LIMIT)
                text = self._get_text_content(metadata["item_id"])
                if text:
                    metadata["text"] = text
                    yield metadata

            total_pages = sr.get("page", {}).get("totalPages", 0)
            page += 1
            if page >= total_pages:
                break
            time.sleep(RATE_LIMIT)

    def normalize(self, raw: dict) -> dict:
        """Transform IRIS metadata + text into standard schema."""
        item_id = raw.get("item_id", "")
        identifier = raw.get("identifier") or item_id
        uri = raw.get("uri", "")

        # Build a clean _id from the govdoc identifier or item UUID
        doc_id = identifier.replace("/", "-").replace(" ", "_") if identifier else item_id

        # Parse date
        date_str = raw.get("date_issued")
        date_iso = None
        if date_str:
            # Handle YYYY-MM-DD, YYYY-MM, or YYYY formats
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                date_iso = date_str
            elif re.match(r'^\d{4}-\d{2}$', date_str):
                date_iso = f"{date_str}-01"
            elif re.match(r'^\d{4}$', date_str):
                date_iso = f"{date_str}-01-01"

        text = raw.get("text", "")

        return {
            "_id": f"WHO-{doc_id}",
            "_source": "INTL/WHO-LegalInstruments",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_iso,
            "url": uri,
            "identifier": identifier,
            "doc_type": raw.get("doc_type", ""),
            "authors": raw.get("authors", []),
            "subjects": raw.get("subjects", []),
            "mesh_terms": raw.get("mesh_terms", []),
            "spatial": raw.get("spatial", ""),
            "publisher": raw.get("publisher", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WHO-LegalInstruments bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = WHOLegalInstrumentsScraper()

    if args.command == "test":
        logger.info("Testing IRIS API connectivity...")
        try:
            data = scraper._search_items(page=0)
            sr = data.get("_embedded", {}).get("searchResult", {})
            total = sr.get("page", {}).get("totalElements", 0)
            logger.info(f"Connection OK. {total} items available.")
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        sample_size = 15 if sample_mode else 999999
        logger.info(f"Starting bootstrap (sample={sample_mode}, size={sample_size})")
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
