#!/usr/bin/env python3
"""
NF/Legislation -- Norfolk Island Continued Laws Fetcher

Fetches Norfolk Island legislation from Australia's Federal Register of
Legislation REST API.  Norfolk Island laws are stored under the
"ContinuedLaw" collection after the 2015 governance reforms.

Strategy:
  - Lists titles with OData filter: collection eq 'ContinuedLaw'
  - For each title, gets latest version metadata
  - Downloads EPUB (preferred) or Word document
  - Extracts full text from the archive

Endpoints:
  - Titles: /v1/titles?$filter=collection eq 'ContinuedLaw'
  - Versions: /v1/versions?$filter=titleId eq 'X' and isLatest eq true
  - Documents: /v1/Documents?$filter=titleId eq 'X' and type eq 'Primary' and format eq 'Epub'
  - Download: /v1/documents(titleid='X',start=...,format='Epub')

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import json
import logging
import zipfile
import io
import re
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NF.Legislation")

API_BASE_URL = "https://api.prod.legislation.gov.au"
WEBSITE_URL = "https://www.legislation.gov.au"
COLLECTION_FILTER = "collection eq 'ContinuedLaw'"
PAGE_SIZE = 100


class NorfolkIslandLegislationScraper(BaseScraper):
    """
    Scraper for NF/Legislation -- Norfolk Island Continued Laws.
    Country: NF
    URL: https://www.legislation.gov.au/norfolk-island-legislation

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=120,
        )

    def _get_titles_page(self, skip: int = 0, top: int = PAGE_SIZE) -> List[Dict[str, Any]]:
        """Fetch a page of ContinuedLaw titles."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                f"/v1/titles?$top={top}&$skip={skip}&$filter={COLLECTION_FILTER}"
            )
            resp.raise_for_status()
            return resp.json().get("value", [])
        except Exception as e:
            logger.error(f"Failed to fetch titles page (skip={skip}): {e}")
            return []

    def _get_total_count(self) -> int:
        """Get total count of ContinuedLaw titles."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/v1/titles/$count?$filter={COLLECTION_FILTER}")
            resp.raise_for_status()
            return int(resp.text.strip())
        except Exception as e:
            logger.error(f"Failed to get title count: {e}")
            return 0

    def _get_latest_version(self, title_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest version for a title."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                f"/v1/versions?$filter=titleId eq '{title_id}' and isLatest eq true&$top=1"
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            versions = resp.json().get("value", [])
            return versions[0] if versions else None
        except Exception as e:
            logger.warning(f"Failed to get latest version for {title_id}: {e}")
            return None

    def _get_document_info(self, title_id: str) -> Optional[Dict[str, Any]]:
        """Get document metadata, preferring EPUB format."""
        try:
            self.rate_limiter.wait()
            # Try EPUB first
            resp = self.client.get(
                f"/v1/Documents?$filter=titleId eq '{title_id}' and type eq 'Primary' "
                f"and format eq 'Epub'&$top=1&$orderby=start desc"
            )
            if resp.status_code == 200:
                docs = resp.json().get("value", [])
                if docs:
                    return docs[0]

            # Fall back to Word
            self.rate_limiter.wait()
            resp = self.client.get(
                f"/v1/Documents?$filter=titleId eq '{title_id}' and type eq 'Primary' "
                f"and format eq 'Word'&$top=1&$orderby=start desc"
            )
            if resp.status_code == 200:
                docs = resp.json().get("value", [])
                if docs:
                    return docs[0]

            return None
        except Exception as e:
            logger.warning(f"Failed to get document info for {title_id}: {e}")
            return None

    def _download_document(self, doc_info: Dict[str, Any]) -> Optional[bytes]:
        """Download a document using OData composite key URL."""
        try:
            self.rate_limiter.wait()
            url = (
                f"/v1/documents("
                f"titleid='{doc_info['titleId']}',"
                f"start={doc_info['start']},"
                f"retrospectivestart={doc_info['retrospectiveStart']},"
                f"rectificationversionnumber={doc_info['rectificationVersionNumber']},"
                f"type='{doc_info['type']}',"
                f"uniqueTypeNumber={doc_info['uniqueTypeNumber']},"
                f"volumeNumber={doc_info['volumeNumber']},"
                f"format='{doc_info['format']}')"
            )
            resp = self.client.get(url)
            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content
            return None
        except Exception as e:
            logger.warning(f"Failed to download document: {e}")
            return None

    def _extract_text(self, archive_bytes: bytes, fmt: str = "Epub") -> str:
        """Extract text from EPUB or Word (.docx) archive."""
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as zf:
                if fmt == "Epub":
                    html_files = [
                        f for f in zf.namelist()
                        if f.endswith(".html") or f.endswith(".xhtml")
                    ]
                    if not html_files:
                        return ""
                    text_parts = []
                    for hf in html_files:
                        content = zf.read(hf).decode("utf-8", errors="ignore")
                        text = re.sub(r"<[^>]+>", " ", content)
                        text = unescape(text)
                        text = text.replace("\xa0", " ")
                        text_parts.append(text)
                    full_text = " ".join(text_parts)
                else:
                    # Word docx
                    if "word/document.xml" not in zf.namelist():
                        return ""
                    xml_content = zf.read("word/document.xml")
                    root = ET.fromstring(xml_content)
                    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                    texts = root.findall(".//w:t", ns)
                    full_text = " ".join(t.text for t in texts if t.text)

                return re.sub(r"\s+", " ", full_text).strip()

        except (zipfile.BadZipFile, ET.ParseError) as e:
            logger.warning(f"Archive extraction error: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Norfolk Island continued law documents."""
        total = self._get_total_count()
        logger.info(f"Total Norfolk Island continued laws: {total}")
        documents_yielded = 0
        skip = 0

        while skip < total:
            titles = self._get_titles_page(skip=skip)
            if not titles:
                break

            for title in titles:
                title_id = title.get("id")
                if not title_id:
                    continue

                version = self._get_latest_version(title_id)
                if not version:
                    logger.debug(f"No version for {title_id}")
                    continue

                doc_info = self._get_document_info(title_id)
                if not doc_info:
                    logger.debug(f"No document for {title_id}")
                    continue

                doc_bytes = self._download_document(doc_info)
                if not doc_bytes:
                    logger.debug(f"Download failed for {title_id}")
                    continue

                fmt = doc_info.get("format", "Epub")
                full_text = self._extract_text(doc_bytes, fmt)
                if not full_text or len(full_text) < 100:
                    logger.debug(
                        f"Insufficient text for {title_id} "
                        f"({len(full_text) if full_text else 0} chars)"
                    )
                    continue

                yield {
                    "title": title,
                    "version": version,
                    "register_id": version.get("registerId", title_id),
                    "full_text": full_text,
                }
                documents_yielded += 1

                if documents_yielded % 25 == 0:
                    logger.info(f"Progress: {documents_yielded} documents fetched")

            skip += PAGE_SIZE

        logger.info(f"Fetch complete: {documents_yielded} documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents registered/modified since a given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Fetching updates since {since_str}")
        documents_yielded = 0

        filter_str = (
            f"registeredAt ge {since_str} and "
            f"titleId in (select titleId from titles where {COLLECTION_FILTER})"
        )
        # Simpler approach: iterate titles and check version dates
        total = self._get_total_count()
        skip = 0

        while skip < total:
            titles = self._get_titles_page(skip=skip)
            if not titles:
                break

            for title in titles:
                title_id = title.get("id")
                if not title_id:
                    continue

                version = self._get_latest_version(title_id)
                if not version:
                    continue

                registered_at = version.get("registeredAt", "")
                if registered_at and registered_at >= since_str:
                    doc_info = self._get_document_info(title_id)
                    if not doc_info:
                        continue

                    doc_bytes = self._download_document(doc_info)
                    if not doc_bytes:
                        continue

                    fmt = doc_info.get("format", "Epub")
                    full_text = self._extract_text(doc_bytes, fmt)
                    if not full_text or len(full_text) < 100:
                        continue

                    yield {
                        "title": title,
                        "version": version,
                        "register_id": version.get("registerId", title_id),
                        "full_text": full_text,
                    }
                    documents_yielded += 1

            skip += PAGE_SIZE

        logger.info(f"Update complete: {documents_yielded} documents")

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        title = raw.get("title", {})
        version = raw.get("version", {})
        register_id = raw.get("register_id", "")
        full_text = raw.get("full_text", "")

        title_id = title.get("id", "")
        name = title.get("name", "") or version.get("name", "")
        status = title.get("status", "") or version.get("status", "")

        making_date = title.get("makingDate", "")
        if making_date:
            making_date = making_date[:10]

        start_date = version.get("start", "")
        if start_date:
            start_date = start_date[:10]

        date = start_date or making_date

        return {
            "_id": register_id,
            "_source": "NF/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": name,
            "text": full_text,
            "date": date,
            "url": f"{WEBSITE_URL}/Details/{register_id}",
            "register_id": register_id,
            "title_id": title_id,
            "collection": "ContinuedLaw",
            "status": status,
            "making_date": making_date,
            "compilation_number": version.get("compilationNumber", ""),
            "is_principal": title.get("isPrincipal", False),
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Norfolk Island Legislation API...")

        print("\n1. Counting ContinuedLaw titles...")
        total = self._get_total_count()
        print(f"   Total: {total}")

        print("\n2. Fetching sample title...")
        titles = self._get_titles_page(top=1)
        if titles:
            t = titles[0]
            print(f"   {t['id']} - {t['name']}")

            print("\n3. Getting latest version...")
            version = self._get_latest_version(t["id"])
            if version:
                print(f"   Register ID: {version.get('registerId')}")

            print("\n4. Downloading document...")
            doc_info = self._get_document_info(t["id"])
            if doc_info:
                print(f"   Format: {doc_info.get('format')}, size: {doc_info.get('sizeInBytes')} bytes")
                doc_bytes = self._download_document(doc_info)
                if doc_bytes:
                    fmt = doc_info.get("format", "Epub")
                    text = self._extract_text(doc_bytes, fmt)
                    print(f"   Text: {len(text)} chars")
                    print(f"   Sample: {text[:200]}...")

        print("\nTest complete!")


def main():
    scraper = NorfolkIslandLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
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
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
