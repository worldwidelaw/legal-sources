#!/usr/bin/env python3
"""
AU/FederalRegister -- Australian Federal Register of Legislation Fetcher

Fetches Australian Commonwealth legislation from the official REST API.

Strategy:
  - Uses the official REST API at https://api.prod.legislation.gov.au/v1/
  - Lists titles with OData pagination ($top, $skip)
  - For each title, finds the current version
  - Downloads Word document and extracts full text from XML

Endpoints:
  - Titles listing: /v1/titles?$top=100&$skip=0
  - Version find: /v1/versions?$filter=titleId eq 'X' and isLatest eq true
  - Document download: /v1/documents/find(registerId='X',type='Primary',format='Word',...)

Data:
  - Acts from 1901 to present
  - Legislative Instruments, Notifiable Instruments, etc.
  - Language: English
  - Rate limit: conservative 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import zipfile
import io
from html import unescape
import re
import xml.etree.ElementTree as ET
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
logger = logging.getLogger("legal-data-hunter.AU.FederalRegister")

# Base URL for Australian legislation API
API_BASE_URL = "https://api.prod.legislation.gov.au"
WEBSITE_URL = "https://www.legislation.gov.au"

# Collections to fetch (in order of priority)
COLLECTIONS = [
    "Act",
    "LegislativeInstrument",
    "NotifiableInstrument",
    "Constitution",
    "AdministrativeArrangementsOrder",
    "ContinuedLaw",
    "PrerogativeInstrument",
]

# Page size for OData queries
PAGE_SIZE = 100


class AustraliaFederalRegisterScraper(BaseScraper):
    """
    Scraper for AU/FederalRegister -- Australian Federal Register of Legislation.
    Country: AU
    URL: https://www.legislation.gov.au

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
            timeout=120,  # Long timeout for document downloads
        )
        # Separate client for binary document downloads (no Accept header)
        self.doc_client = HttpClient(
            base_url=API_BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            },
            timeout=120,
        )

    def _get_titles_page(
        self, skip: int = 0, top: int = PAGE_SIZE, filter_str: str = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a page of titles from the API.

        Args:
            skip: Number of records to skip (OData $skip)
            top: Number of records to return (OData $top)
            filter_str: Optional OData filter string

        Returns:
            List of title objects
        """
        try:
            self.rate_limiter.wait()

            params = f"$top={top}&$skip={skip}"
            if filter_str:
                params += f"&$filter={filter_str}"

            resp = self.client.get(f"/v1/titles?{params}")
            resp.raise_for_status()

            data = resp.json()
            titles = data.get("value", [])
            logger.debug(f"Fetched {len(titles)} titles (skip={skip})")
            return titles

        except Exception as e:
            logger.error(f"Failed to fetch titles page (skip={skip}): {e}")
            return []

    def _get_total_count(self, filter_str: str = None) -> int:
        """Get total count of titles matching the filter."""
        try:
            self.rate_limiter.wait()

            url = "/v1/titles/$count"
            if filter_str:
                url += f"?$filter={filter_str}"

            resp = self.client.get(url)
            resp.raise_for_status()
            return int(resp.text.strip())

        except Exception as e:
            logger.error(f"Failed to get title count: {e}")
            return 0

    def _get_latest_version(self, title_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest (current) version for a title.

        Returns version object with registerId needed for document download.
        """
        try:
            self.rate_limiter.wait()

            # URL encode the filter
            filter_str = f"titleId eq '{title_id}' and isLatest eq true"
            resp = self.client.get(f"/v1/versions?$filter={filter_str}&$top=1")

            if resp.status_code == 404:
                return None

            resp.raise_for_status()
            data = resp.json()
            versions = data.get("value", [])

            if versions:
                return versions[0]
            return None

        except Exception as e:
            logger.warning(f"Failed to get latest version for {title_id}: {e}")
            return None

    def _get_document_info(self, title_id: str) -> Optional[Dict[str, Any]]:
        """
        Get document metadata for a title, preferring EPUB format for the Primary document.

        Returns the document object with all parameters needed for download.
        """
        try:
            self.rate_limiter.wait()

            # Query Documents for this title, filtering for Primary type and Epub format
            filter_str = f"titleId eq '{title_id}' and type eq 'Primary' and format eq 'Epub'"
            resp = self.client.get(f"/v1/Documents?$filter={filter_str}&$top=1&$orderby=start desc")

            if resp.status_code != 200:
                return None

            data = resp.json()
            docs = data.get("value", [])

            if docs:
                return docs[0]

            # Fall back to Word if no EPUB
            filter_str = f"titleId eq '{title_id}' and type eq 'Primary' and format eq 'Word'"
            resp = self.client.get(f"/v1/Documents?$filter={filter_str}&$top=1&$orderby=start desc")

            if resp.status_code != 200:
                return None

            data = resp.json()
            docs = data.get("value", [])
            return docs[0] if docs else None

        except Exception as e:
            logger.warning(f"Failed to get document info for {title_id}: {e}")
            return None

    def _download_document(self, doc_info: Dict[str, Any]) -> Optional[bytes]:
        """
        Download a document using exact parameters from document info.

        Returns raw bytes of the file, or None if failed.
        """
        try:
            self.rate_limiter.wait()

            # Build URL using exact document parameters
            title_id = doc_info.get("titleId", "")
            start = doc_info.get("start", "")
            retro_start = doc_info.get("retrospectiveStart", "")
            rect_ver = doc_info.get("rectificationVersionNumber", 0)
            doc_type = doc_info.get("type", "Primary")
            unique_num = doc_info.get("uniqueTypeNumber", 0)
            vol_num = doc_info.get("volumeNumber", 0)
            fmt = doc_info.get("format", "Epub")

            url = (
                f"/v1/documents("
                f"titleid='{title_id}',"
                f"start={start},"
                f"retrospectivestart={retro_start},"
                f"rectificationversionnumber={rect_ver},"
                f"type='{doc_type}',"
                f"uniqueTypeNumber={unique_num},"
                f"volumeNumber={vol_num},"
                f"format='{fmt}')"
            )

            resp = self.doc_client.get(url)

            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content

            return None

        except Exception as e:
            logger.warning(f"Failed to download document: {e}")
            return None

    def _extract_text_from_archive(self, archive_bytes: bytes, fmt: str = "Epub") -> str:
        """
        Extract text from a document archive (EPUB or Word .docx).

        Both formats are ZIP archives containing XML/HTML content.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as zf:
                file_list = zf.namelist()

                if fmt == "Epub":
                    # EPUB: look for HTML files in OEBPS folder
                    html_files = [f for f in file_list if f.endswith(".html") or f.endswith(".xhtml")]
                    if not html_files:
                        logger.warning("No HTML files in EPUB")
                        return ""

                    text_parts = []
                    for html_file in html_files:
                        html_content = zf.read(html_file).decode("utf-8", errors="ignore")
                        # Strip HTML tags
                        text = re.sub(r"<[^>]+>", " ", html_content)
                        # Decode HTML entities
                        text = unescape(text)
                        text = text.replace("\xa0", " ")  # Non-breaking space
                        text_parts.append(text)

                    full_text = " ".join(text_parts)

                else:
                    # Word: read word/document.xml
                    if "word/document.xml" not in file_list:
                        logger.warning("No word/document.xml in docx")
                        return ""

                    xml_content = zf.read("word/document.xml")
                    root = ET.fromstring(xml_content)

                    # Word namespace
                    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                    texts = root.findall(".//w:t", ns)
                    text_parts = [t.text for t in texts if t.text]
                    full_text = " ".join(text_parts)

                # Clean up whitespace
                full_text = re.sub(r"\s+", " ", full_text).strip()
                return full_text

        except zipfile.BadZipFile:
            logger.warning("Invalid ZIP archive")
            return ""
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return ""
        except Exception as e:
            logger.warning(f"Error extracting text: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from the Federal Register.

        Iterates through all collections and pages, fetching current
        versions and their full text.
        """
        documents_yielded = 0

        # Build filter for in-force legislation
        # We'll fetch all statuses but prioritize InForce
        for collection in COLLECTIONS:
            logger.info(f"Processing collection: {collection}")

            # Count total in this collection
            filter_str = f"collection eq '{collection}'"
            total = self._get_total_count(filter_str)
            logger.info(f"  Total titles in {collection}: {total}")

            if total == 0:
                continue

            # Paginate through titles
            skip = 0
            while skip < total:
                titles = self._get_titles_page(skip=skip, top=PAGE_SIZE, filter_str=filter_str)

                if not titles:
                    break

                for title in titles:
                    title_id = title.get("id")
                    if not title_id:
                        continue

                    # Get the latest version
                    version = self._get_latest_version(title_id)
                    if not version:
                        logger.debug(f"No version found for {title_id}, skipping")
                        continue

                    register_id = version.get("registerId")
                    if not register_id:
                        # Some titles don't have a registerId in version
                        # Try using title's id
                        register_id = title_id
                        logger.debug(f"Using title_id as register_id for {title_id}")

                    # Get document info (prefer EPUB)
                    doc_info = self._get_document_info(title_id)
                    if not doc_info:
                        logger.debug(f"No document info for {title_id}, skipping")
                        continue

                    # Download document
                    doc_bytes = self._download_document(doc_info)
                    if not doc_bytes:
                        logger.debug(f"No document for {title_id}, skipping")
                        continue

                    # Extract full text
                    fmt = doc_info.get("format", "Epub")
                    full_text = self._extract_text_from_archive(doc_bytes, fmt)

                    if not full_text or len(full_text) < 100:
                        logger.debug(f"Insufficient text for {register_id} ({len(full_text) if full_text else 0} chars)")
                        continue

                    yield {
                        "title": title,
                        "version": version,
                        "register_id": register_id,
                        "full_text": full_text,
                    }

                    documents_yielded += 1

                    # Log progress periodically
                    if documents_yielded % 50 == 0:
                        logger.info(f"Progress: {documents_yielded} documents fetched")

                skip += PAGE_SIZE

        logger.info(f"Fetch complete: {documents_yielded} total documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents registered/modified since the given date.

        Uses the registeredAt field in versions to find recently updated titles.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Fetching updates since {since_str}")

        # OData filter for recently registered versions
        # Note: The API may have limited support for date filtering
        filter_str = f"registeredAt ge {since_str}"

        documents_yielded = 0

        try:
            skip = 0
            while True:
                self.rate_limiter.wait()

                url = f"/v1/versions?$filter={filter_str}&$top={PAGE_SIZE}&$skip={skip}"
                resp = self.client.get(url)

                if resp.status_code != 200:
                    logger.warning(f"Version fetch failed: {resp.status_code}")
                    break

                data = resp.json()
                versions = data.get("value", [])

                if not versions:
                    break

                for version in versions:
                    title_id = version.get("titleId")
                    register_id = version.get("registerId")

                    if not register_id:
                        continue

                    # Fetch title info
                    self.rate_limiter.wait()
                    title_resp = self.client.get(f"/v1/titles/{title_id}")

                    if title_resp.status_code != 200:
                        continue

                    title = title_resp.json()

                    # Get document info and download
                    doc_info = self._get_document_info(title_id)
                    if not doc_info:
                        continue

                    doc_bytes = self._download_document(doc_info)
                    if not doc_bytes:
                        continue

                    fmt = doc_info.get("format", "Epub")
                    full_text = self._extract_text_from_archive(doc_bytes, fmt)

                    if not full_text or len(full_text) < 100:
                        continue

                    yield {
                        "title": title,
                        "version": version,
                        "register_id": register_id,
                        "full_text": full_text,
                    }

                    documents_yielded += 1

                skip += PAGE_SIZE

        except Exception as e:
            logger.error(f"Error fetching updates: {e}")

        logger.info(f"Update complete: {documents_yielded} documents")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        title = raw.get("title", {})
        version = raw.get("version", {})
        register_id = raw.get("register_id", "")
        full_text = raw.get("full_text", "")

        title_id = title.get("id", "")
        name = title.get("name", "") or version.get("name", "")
        collection = title.get("collection", "")
        status = title.get("status", "") or version.get("status", "")

        # Parse dates
        making_date = title.get("makingDate", "")
        if making_date:
            making_date = making_date[:10]  # ISO date only

        start_date = version.get("start", "")
        if start_date:
            start_date = start_date[:10]

        # Use start_date as primary date, fall back to making_date
        date = start_date or making_date

        # Build URL to legislation
        url = f"{WEBSITE_URL}/Details/{register_id}"

        return {
            # Required base fields
            "_id": register_id,
            "_source": "AU/FederalRegister",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": name,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "register_id": register_id,
            "title_id": title_id,
            "collection": collection,
            "status": status,
            "making_date": making_date,
            "compilation_number": version.get("compilationNumber", ""),
            "year": title.get("year"),
            "number": title.get("number"),
            "is_principal": title.get("isPrincipal", False),
            "series_type": title.get("seriesType", ""),
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Australian Federal Register API endpoints...")

        # Test titles endpoint
        print("\n1. Testing titles endpoint...")
        try:
            resp = self.client.get("/v1/titles?$top=3")
            print(f"   Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                titles = data.get("value", [])
                print(f"   Found {len(titles)} titles")
                if titles:
                    print(f"   Sample: {titles[0].get('id')} - {titles[0].get('name', '')[:50]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test total count
        print("\n2. Testing titles count...")
        try:
            resp = self.client.get("/v1/titles/$count")
            print(f"   Status: {resp.status_code}")
            if resp.status_code == 200:
                print(f"   Total titles: {resp.text.strip()}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test version lookup
        print("\n3. Testing version lookup...")
        try:
            version = self._get_latest_version("C1901A00002")
            if version:
                print(f"   Version found: {version.get('registerId')}")
                print(f"   Start date: {version.get('start', '')[:10]}")
                print(f"   Compilation: {version.get('compilationNumber')}")
            else:
                print("   No version found")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document download
        print("\n4. Testing document download...")
        try:
            doc_info = self._get_document_info("C1901A00002")
            if doc_info:
                print(f"   Found document: {doc_info.get('format')} format")
                doc_bytes = self._download_document(doc_info)
                if doc_bytes:
                    print(f"   Document size: {len(doc_bytes)} bytes")
                    fmt = doc_info.get("format", "Epub")
                    text = self._extract_text_from_archive(doc_bytes, fmt)
                    print(f"   Text length: {len(text)} characters")
                    print(f"   Sample: {text[:150]}...")
                else:
                    print("   No document downloaded")
            else:
                print("   No document info found")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = AustraliaFederalRegisterScraper()

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
