#!/usr/bin/env python3
"""
VG/LawsOnline -- British Virgin Islands Laws Online

Fetches legislation from the official BVI government portal (laws.gov.vg),
a Drupal site with JSON:API access. Each law node has an attached PDF
containing the full text of the legislation.

Strategy:
  - Paginate Drupal JSON:API: /jsonapi/node/laws?include=field_upload_legislation
  - For each law, download the attached PDF
  - Extract full text via common.pdf_extract

Data:
  - ~200 acts, statutory instruments, and subsidiary legislation
  - Full text extracted from PDF attachments
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VG.LawsOnline")

BASE_URL = "https://laws.gov.vg"
API_URL = f"{BASE_URL}/jsonapi/node/laws"
PAGE_SIZE = 50
SOURCE_ID = "VG/LawsOnline"


class BVILawsOnlineScraper(BaseScraper):
    """Scraper for VG/LawsOnline -- BVI legislation via Drupal JSON:API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/vnd.api+json",
        })

    def _request_json(self, url: str, timeout: int = 120) -> Optional[dict]:
        """GET JSON with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 60s")
                    time.sleep(60)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(15)
        return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF bytes with retry."""
        full_url = url if url.startswith("http") else f"{BASE_URL}{url}"
        for attempt in range(3):
            try:
                time.sleep(3)
                resp = self.session.get(
                    full_url,
                    timeout=120,
                    headers={"Accept": "application/pdf,*/*"},
                )
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _build_file_map(self, included: List[dict]) -> Dict[str, dict]:
        """Build mapping from file UUID to file attributes."""
        file_map = {}
        for inc in included or []:
            if inc.get("type", "").startswith("file"):
                attrs = inc.get("attributes", {})
                uri = attrs.get("uri", {})
                file_map[inc["id"]] = {
                    "filename": attrs.get("filename", ""),
                    "url": uri.get("url", ""),
                    "size": attrs.get("filesize", 0),
                }
        return file_map

    def _extract_year(self, title: str, reference: str) -> Optional[str]:
        """Extract year from title or reference."""
        for text in [reference, title]:
            if not text:
                continue
            m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
            if m:
                return m.group(1)
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all BVI laws with full text from PDFs."""
        offset = 0
        total = 0
        while True:
            url = (
                f"{API_URL}?include=field_upload_legislation"
                f"&page%5Blimit%5D={PAGE_SIZE}&page%5Boffset%5D={offset}"
            )
            logger.info(f"Fetching page offset={offset}")
            data = self._request_json(url)
            if not data:
                logger.error(f"Failed to fetch page at offset {offset}")
                break

            nodes = data.get("data", [])
            if not nodes:
                break

            file_map = self._build_file_map(data.get("included", []))

            for node in nodes:
                record = self._process_node(node, file_map)
                if record:
                    total += 1
                    yield record

            if len(nodes) < PAGE_SIZE or "next" not in data.get("links", {}):
                break
            offset += PAGE_SIZE

        logger.info(f"Fetched {total} laws total")

    def _process_node(self, node: dict, file_map: Dict[str, dict]) -> Optional[Dict[str, Any]]:
        """Process a single law node into a normalized record."""
        attrs = node.get("attributes", {})
        title = (attrs.get("title") or "").strip()
        if not title:
            return None

        node_id = node.get("id", "")
        reference = attrs.get("field_reference") or ""
        nid = attrs.get("drupal_internal__nid", "")

        # Get file info
        rel = node.get("relationships", {}).get("field_upload_legislation", {})
        rel_data = rel.get("data")
        if not rel_data:
            logger.debug(f"No PDF attachment for: {title}")
            return None

        file_entries = rel_data if isinstance(rel_data, list) else [rel_data]
        file_info = None
        for fe in file_entries:
            fid = fe.get("id", "")
            if fid in file_map:
                file_info = file_map[fid]
                break

        if not file_info or not file_info.get("url"):
            logger.debug(f"No file URL for: {title}")
            return None

        # Download and extract PDF text
        pdf_bytes = self._download_pdf(file_info["url"])
        if not pdf_bytes:
            logger.warning(f"Failed to download PDF for: {title}")
            return None

        doc_id = f"vg-law-{nid}" if nid else f"vg-law-{node_id[:12]}"
        text = extract_pdf_markdown(
            SOURCE_ID, doc_id, pdf_bytes=pdf_bytes, table="legislation"
        )
        if not text or len(text.strip()) < 50:
            logger.warning(f"Insufficient text for: {title} ({len(text or '')} chars)")
            return None

        year = self._extract_year(title, reference)
        created = attrs.get("created")
        path_alias = (attrs.get("path") or {}).get("alias", "")
        doc_url = f"{BASE_URL}{path_alias}" if path_alias else f"{BASE_URL}/node/{nid}"

        return {
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "reference": reference,
            "year": year,
            "date": year if year else None,
            "url": doc_url,
            "created": created,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "reference": raw.get("reference"),
        }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch laws updated since a given date."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        data = self._request_json(f"{API_URL}?page%5Blimit%5D=1")
        if not data or not data.get("data"):
            logger.error("API returned no data")
            return False
        logger.info(f"API OK — got {len(data['data'])} record(s)")
        return True


if __name__ == "__main__":
    scraper = BVILawsOnlineScraper()
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    sample = "--sample" in sys.argv
    scraper.bootstrap(sample_mode=sample)
