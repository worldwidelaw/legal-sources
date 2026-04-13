#!/usr/bin/env python3
"""
PE/INDECOPI -- Peru INDECOPI Tribunal Resolutions

Fetches resolutions from INDECOPI's DSpace 7 institutional repository at
repositorio.indecopi.gob.pe. Covers competition, IP, consumer protection,
bureaucratic barriers, dumping/subsidies, and bankruptcy resolutions.

Strategy:
  - Use DSpace 7 REST API (HAL+JSON) to paginate through all items
  - Scope searches to legal-resolution communities
  - Extract full text from TEXT bundle (pre-extracted by DSpace)
  - Fall back to PDF extraction via PyMuPDF if TEXT bundle is empty/missing
  - Metadata from dc.* Dublin Core fields

Source: https://repositorio.indecopi.gob.pe/
Rate limit: 2 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (all items)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import io
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.INDECOPI")

API_BASE = "https://repositorio.indecopi.gob.pe/backend/api"

# Legal-resolution communities to fetch from
LEGAL_COMMUNITIES = {
    "3ea92fce-0da0-4686-98aa-296632875060": "Resoluciones",
    "8b388f73-cb7d-4051-8e2d-22929ab5d73d": "Sala Defensa Competencia",
    "0eedbf5e-05bb-4c9d-a8eb-af1ae6f12577": "Sala Procedimientos Concursales",
    "d86adb99-fc98-4368-b3fa-9aef14e1f2ec": "Sala Propiedad Intelectual",
    "8b729e23-8b6a-4f0d-8c2f-735df7d547d3": "Sala Proteccion Consumidor",
    "163ab264-e6d1-45bb-a295-f2aed5c9a029": "Competencia desleal",
    "d58b5464-25da-4a18-9fb9-3077f313228d": "Libre competencia",
    "1e3846c4-c7e2-4430-88f9-063b8459dd40": "Eliminación barreras burocráticas",
    "3ebbb7b6-1245-441b-9a60-af488c6692fd": "Signos Distintivos",
    "68947adb-1b54-4613-98dd-36ec09d4d66e": "Derecho de Autor",
    "6bf8ff16-491d-4d5e-abed-4ee2a08dfa96": "Invenciones y Nuevas Tecnologías",
    "4c4e1fdf-4634-4cb9-98ce-531e2b0c5c06": "Procedimientos concursales",
    "2b0d618a-76a1-468b-b101-c57ca8801214": "Protección al Consumidor",
    "05c92cab-8ecf-404d-a0ea-bd58e88afc35": "Dumping y subsidios",
}

PAGE_SIZE = 100


def _try_import_fitz():
    """Lazy import PyMuPDF for PDF text extraction fallback."""
class INDECOPIScraper(BaseScraper):
    """
    Scraper for PE/INDECOPI -- Peru INDECOPI Resolutions via DSpace 7 API.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )
        self._seen_uuids = set()

    @staticmethod
    def _clean_html(text: str) -> str:
        """Strip HTML tags, decode entities, and normalize whitespace."""
        if not text:
            return ""
        text = re.sub(r"<[^>]+>", " ", text)
        text = html_mod.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _get_json(self, url: str, params: dict = None) -> Optional[dict]:
        """GET a JSON endpoint, return parsed dict or None on error."""
        try:
            resp = self.client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"HTTP {resp.status_code} from {url}")
            return None
        except Exception as e:
            logger.warning(f"Request error for {url}: {e}")
            return None

    def _get_item_text(self, item_uuid: str) -> str:
        """
        Extract full text for an item:
        1. Try the TEXT bundle (DSpace pre-extracted text)
        2. Fall back to downloading PDF and extracting with PyMuPDF
        """
        bundles_url = f"{API_BASE}/core/items/{item_uuid}/bundles"
        data = self._get_json(bundles_url)
        if not data:
            return ""

        bundles = data.get("_embedded", {}).get("bundles", [])
        text_bundle = None
        original_bundle = None

        for b in bundles:
            if b["name"] == "TEXT":
                text_bundle = b
            elif b["name"] == "ORIGINAL":
                original_bundle = b

        # Try TEXT bundle first
        if text_bundle:
            text = self._download_text_bundle(text_bundle["uuid"])
            if text and len(text.strip()) > 50:
                return text.strip()

        # Fall back to PDF extraction
        if original_bundle:
            return self._extract_pdf_text(original_bundle["uuid"])

        return ""

    def _download_text_bundle(self, bundle_uuid: str) -> str:
        """Download the pre-extracted text from a TEXT bundle."""
        bs_url = f"{API_BASE}/core/bundles/{bundle_uuid}/bitstreams"
        data = self._get_json(bs_url)
        if not data:
            return ""

        bitstreams = data.get("_embedded", {}).get("bitstreams", [])
        if not bitstreams:
            return ""

        bs = bitstreams[0]
        size = bs.get("sizeBytes", 0)
        if size < 20:
            return ""

        content_url = f"{API_BASE}/core/bitstreams/{bs['uuid']}/content"
        try:
            resp = self.client.get(content_url)
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.debug(f"Error downloading text bundle: {e}")
        return ""

    def _extract_pdf_text(self, bundle_uuid: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="PE/INDECOPI",
            source_id="",
            pdf_bytes=bundle_uuid,
            table="case_law",
        ) or ""

    def _extract_metadata(self, item: dict) -> dict:
        """Extract Dublin Core metadata from a DSpace item."""
        meta = item.get("metadata", {})

        def first(key):
            vals = meta.get(key, [])
            return vals[0].get("value", "") if vals else ""

        def all_vals(key):
            return [v.get("value", "") for v in meta.get(key, [])]

        title = first("dc.title") or item.get("name", "")
        date_issued = first("dc.date.issued")
        identifier = first("dc.identifier.other") or first("dc.identifier.citation")
        abstract = first("dc.description.abstract")
        publisher = first("dc.publisher")
        subjects = all_vals("dc.subject")
        uri = first("dc.identifier.uri")

        return {
            "uuid": item.get("uuid", ""),
            "title": title,
            "date_issued": date_issued,
            "identifier": identifier,
            "abstract": abstract,
            "publisher": publisher,
            "subjects": subjects,
            "uri": uri,
        }

    def _paginate_community(self, community_uuid: str) -> Generator[dict, None, None]:
        """Paginate through all items in a community via the search API."""
        page = 0
        while True:
            url = f"{API_BASE}/discover/search/objects"
            params = {
                "size": PAGE_SIZE,
                "page": page,
                "dsoType": "item",
                "scope": community_uuid,
            }
            data = self._get_json(url, params)
            if not data:
                break

            search_result = data.get("_embedded", {}).get("searchResult", {})
            objects = search_result.get("_embedded", {}).get("objects", [])
            if not objects:
                break

            for obj in objects:
                item = obj.get("_embedded", {}).get("indexableObject", {})
                if item:
                    yield item

            page_info = search_result.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            page += 1
            if page >= total_pages:
                break

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all resolution items from legal communities."""
        for comm_uuid, comm_name in LEGAL_COMMUNITIES.items():
            logger.info(f"Fetching community: {comm_name}")
            count = 0
            for item in self._paginate_community(comm_uuid):
                uuid = item.get("uuid", "")
                if uuid in self._seen_uuids:
                    continue
                self._seen_uuids.add(uuid)

                meta = self._extract_metadata(item)

                # Get full text
                self.rate_limiter.wait()
                text = self._get_item_text(uuid)

                if not text:
                    logger.debug(f"No text for {uuid}: {meta['title'][:60]}")
                    continue

                meta["text"] = text
                meta["community"] = comm_name
                count += 1
                yield meta

            logger.info(f"  {comm_name}: yielded {count} items with text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch items updated since a given date using DSpace search."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        for comm_uuid, comm_name in LEGAL_COMMUNITIES.items():
            url = f"{API_BASE}/discover/search/objects"
            page = 0
            while True:
                params = {
                    "size": PAGE_SIZE,
                    "page": page,
                    "dsoType": "item",
                    "scope": comm_uuid,
                    "sort": "lastModified,desc",
                }
                data = self._get_json(url, params)
                if not data:
                    break

                search_result = data.get("_embedded", {}).get("searchResult", {})
                objects = search_result.get("_embedded", {}).get("objects", [])
                if not objects:
                    break

                found_old = False
                for obj in objects:
                    item = obj.get("_embedded", {}).get("indexableObject", {})
                    if not item:
                        continue
                    last_mod = item.get("lastModified", "")
                    if last_mod and last_mod < since_str:
                        found_old = True
                        break

                    uuid = item.get("uuid", "")
                    if uuid in self._seen_uuids:
                        continue
                    self._seen_uuids.add(uuid)

                    meta = self._extract_metadata(item)
                    self.rate_limiter.wait()
                    text = self._get_item_text(uuid)
                    if text:
                        meta["text"] = text
                        meta["community"] = comm_name
                        yield meta

                if found_old:
                    break
                page_info = search_result.get("page", {})
                total_pages = page_info.get("totalPages", 0)
                page += 1
                if page >= total_pages:
                    break

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw DSpace metadata + text into standard schema."""
        uuid = raw.get("uuid", "")
        title = raw.get("title", "")
        text = raw.get("text", "")

        # Clean HTML tags from text
        text = self._clean_html(text)

        if not text or len(text.strip()) < 50:
            return None

        # Parse date
        date_str = raw.get("date_issued", "")
        date_iso = None
        if date_str:
            # Handle various formats: YYYY, YYYY-MM, YYYY-MM-DD
            for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
                try:
                    date_iso = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        # Build URL
        uri = raw.get("uri", "")
        url = uri if uri else f"https://repositorio.indecopi.gob.pe/items/{uuid}"

        # Extract resolution number from identifier or title
        res_number = raw.get("identifier", "")
        if not res_number:
            match = re.search(r"[Rr]esoluc[ií][oó]n\s+N[°ºo]?\s*(\S+)", title)
            if match:
                res_number = match.group(1)

        # Determine area from community
        community = raw.get("community", "")

        return {
            "_id": f"PE-INDECOPI-{uuid}",
            "_source": "PE/INDECOPI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": url,
            "resolution_number": res_number,
            "area": community,
            "abstract": raw.get("abstract", ""),
            "subjects": raw.get("subjects", []),
            "publisher": raw.get("publisher", ""),
            "language": "es",
            "jurisdiction": "PE",
        }

    def test_api(self):
        """Test connectivity to the DSpace API."""
        logger.info("Testing DSpace API connectivity...")
        data = self._get_json(f"{API_BASE}")
        if not data:
            logger.error("Cannot reach DSpace API")
            return False
        logger.info(f"DSpace version: {data.get('dspaceVersion', '?')}")

        # Count items across legal communities
        total = 0
        for comm_uuid, comm_name in LEGAL_COMMUNITIES.items():
            data = self._get_json(
                f"{API_BASE}/discover/search/objects",
                params={"size": 1, "page": 0, "dsoType": "item", "scope": comm_uuid},
            )
            if data:
                n = (
                    data.get("_embedded", {})
                    .get("searchResult", {})
                    .get("page", {})
                    .get("totalElements", 0)
                )
                if n > 0:
                    total += n
                    logger.info(f"  {comm_name}: {n} items")
        logger.info(f"Total items across legal communities: {total}")
        return True


# ── CLI entry point ─────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="PE/INDECOPI bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Sample mode: fetch only 15 records",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (all records)",
    )
    args = parser.parse_args()

    scraper = INDECOPIScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample or not args.full:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
