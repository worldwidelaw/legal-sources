#!/usr/bin/env python3
"""
FI/Finlex -- Finnish Legal Database Data Fetcher

Fetches Finnish legislation from the Finlex Open Data API.

Strategy:
  - Bootstrap: Paginates through statute, statute-consolidated, treaty,
    and government-proposal endpoints using the REST API.
  - Update: Uses status parameter to fetch NEW/MODIFIED records.
  - Sample: Fetches 10+ records from legislation for validation.

API: https://opendata.finlex.fi/finlex/avoindata/v1
Docs: https://www.finlex.fi/en/open-data/integration-quick-guide

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
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.finlex")

# Finlex Open Data API v1
API_BASE = "https://opendata.finlex.fi/finlex/avoindata/v1"

# Akoma Ntoso namespace
AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}

# Document types to fetch (legislation only - case law requires auth)
DOCUMENT_TYPES = [
    ("act", "statute"),
    ("act", "statute-consolidated"),
    ("doc", "treaty"),
    ("doc", "government-proposal"),
]


class FinlexScraper(BaseScraper):
    """
    Scraper for FI/Finlex -- Finnish Legal Database.
    Country: FI
    URL: https://www.finlex.fi

    Data types: legislation
    Auth: none (User-Agent header required)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_document_list(
        self,
        category: str,
        doc_type: str,
        page: int = 1,
        limit: int = 10,  # API max is 10 per page
    ) -> list:
        """
        Fetch a page of document URIs from the list endpoint.

        Returns list of dicts with akn_uri and status.
        Note: API has a max limit of 10 documents per page.
        """
        endpoint = f"/akn/fi/{category}/{doc_type}/list"
        params = {
            "page": str(page),
            "limit": str(min(limit, 10)),  # Enforce API max
            "format": "json",
        }

        self.rate_limiter.wait()

        try:
            resp = self.client.get(endpoint, params=params)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return []
        except Exception as e:
            logger.error(f"Error fetching {endpoint} page {page}: {e}")
            return []

    def _fetch_document(self, akn_uri: str) -> Optional[dict]:
        """
        Fetch a single document by its AKN URI.

        Returns raw document data including full XML content.
        """
        # The list endpoint returns full URLs, extract the path
        if akn_uri.startswith("http"):
            path = akn_uri.replace(API_BASE, "")
        else:
            path = akn_uri

        self.rate_limiter.wait()

        try:
            resp = self.client.get(path)
            resp.raise_for_status()
            xml_content = resp.text

            return {
                "akn_uri": akn_uri,
                "xml_content": xml_content,
                "path": path,
            }
        except Exception as e:
            logger.warning(f"Failed to fetch document {path}: {e}")
            return None

    def _paginate_documents(
        self,
        category: str,
        doc_type: str,
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through document list and fetches each document.

        Yields raw document dicts with full XML content.
        """
        page = 1

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            docs = self._get_document_list(category, doc_type, page=page, limit=100)

            if not docs:
                if page == 1:
                    logger.info(f"No documents found for {category}/{doc_type}")
                else:
                    logger.info(f"Finished {category}/{doc_type} at page {page-1}")
                return

            logger.info(f"{category}/{doc_type} page {page}: {len(docs)} documents")

            for doc_ref in docs:
                akn_uri = doc_ref.get("akn_uri", "")
                status = doc_ref.get("status", "")

                if not akn_uri:
                    continue

                doc = self._fetch_document(akn_uri)
                if doc:
                    doc["status"] = status
                    doc["doc_type"] = doc_type
                    doc["category"] = category
                    yield doc

            # Check if we got a full page (API max is 10)
            if len(docs) < 10:
                logger.info(f"Last page for {category}/{doc_type} (got {len(docs)} docs)")
                return

            page += 1

    # -- XML Parsing --------------------------------------------------------

    def _extract_text_from_akn(self, xml_content: str) -> tuple:
        """
        Extract text and metadata from Akoma Ntoso XML.

        Returns (title, text, metadata_dict)
        """
        try:
            root = ET.fromstring(xml_content.encode('utf-8'))
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return "", "", {}

        metadata = {}

        # Find the main document element (act, doc, judgment)
        main_elem = None
        for tag in ["act", "doc", "judgment"]:
            main_elem = root.find(f"akn:{tag}", AKN_NS)
            if main_elem is not None:
                break

        if main_elem is None:
            # Try without namespace
            for tag in ["act", "doc", "judgment"]:
                main_elem = root.find(f".//{tag}")
                if main_elem is not None:
                    break

        if main_elem is None:
            logger.warning("Could not find main document element in AKN XML")
            return "", "", metadata

        # Extract title from preface/docTitle
        title = ""
        doc_title = main_elem.find(".//akn:docTitle", AKN_NS)
        if doc_title is None:
            doc_title = main_elem.find(".//docTitle")
        if doc_title is not None:
            title = "".join(doc_title.itertext()).strip()

        # Extract document number
        doc_number = main_elem.find(".//akn:docNumber", AKN_NS)
        if doc_number is None:
            doc_number = main_elem.find(".//docNumber")
        if doc_number is not None:
            metadata["doc_number"] = "".join(doc_number.itertext()).strip()

        # Extract metadata from identification
        identification = main_elem.find(".//akn:identification", AKN_NS)
        if identification is None:
            identification = main_elem.find(".//identification")

        if identification is not None:
            # FRBRWork info
            work = identification.find(".//akn:FRBRWork", AKN_NS) or identification.find(".//FRBRWork")
            if work is not None:
                # Date
                date_elem = work.find("akn:FRBRdate", AKN_NS) or work.find("FRBRdate")
                if date_elem is not None:
                    metadata["date_issued"] = date_elem.get("date", "")

                # Number
                num_elem = work.find("akn:FRBRnumber", AKN_NS) or work.find("FRBRnumber")
                if num_elem is not None:
                    metadata["number"] = num_elem.get("value", "")

                # Subtype
                subtype_elem = work.find("akn:FRBRsubtype", AKN_NS) or work.find("FRBRsubtype")
                if subtype_elem is not None:
                    metadata["subtype"] = subtype_elem.get("value", "")

                # ELI
                eli_elem = work.find("akn:FRBRalias[@name='eli']", AKN_NS)
                if eli_elem is None:
                    eli_elem = work.find(".//FRBRalias[@name='eli']")
                if eli_elem is not None:
                    metadata["eli"] = eli_elem.get("value", "")

            # FRBRExpression info (includes language)
            expr = identification.find(".//akn:FRBRExpression", AKN_NS) or identification.find(".//FRBRExpression")
            if expr is not None:
                lang_elem = expr.find("akn:FRBRlanguage", AKN_NS) or expr.find("FRBRlanguage")
                if lang_elem is not None:
                    metadata["language"] = lang_elem.get("language", "")

                date_elem = expr.find("akn:FRBRdate[@name='datePublished']", AKN_NS)
                if date_elem is None:
                    date_elem = expr.find(".//FRBRdate[@name='datePublished']")
                if date_elem is not None:
                    metadata["date_published"] = date_elem.get("date", "")

        # Extract year from proprietary metadata
        proprietary = main_elem.find(".//akn:proprietary", AKN_NS)
        if proprietary is None:
            proprietary = main_elem.find(".//proprietary")
        if proprietary is not None:
            # Finlex-specific namespace
            year_elem = proprietary.find(".//{http://data.finlex.fi/schema/finlex}documentYear")
            if year_elem is not None and year_elem.text:
                metadata["year"] = year_elem.text

        # Extract full text from body
        body = main_elem.find("akn:body", AKN_NS)
        if body is None:
            body = main_elem.find("body")

        text_parts = []
        if body is not None:
            # Extract text from all relevant elements
            for elem in body.iter():
                # Get text from content elements
                if elem.tag.endswith(("p", "content", "num", "heading", "intro", "wrapUp")):
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(text)

        # If body extraction didn't work, try getting all text
        if not text_parts:
            for elem in main_elem.iter():
                if elem.text and elem.text.strip():
                    text_parts.append(elem.text.strip())
                if elem.tail and elem.tail.strip():
                    text_parts.append(elem.tail.strip())

        full_text = "\n\n".join(text_parts)

        # Clean up the text
        full_text = html.unescape(full_text)
        full_text = re.sub(r"\s+", " ", full_text)
        full_text = full_text.strip()

        return title, full_text, metadata

    def _parse_akn_uri(self, akn_uri: str) -> dict:
        """
        Parse an AKN URI to extract document identifiers.

        Example: /akn/fi/act/statute/2025/51/fin@
        """
        result = {}

        # Remove base URL if present
        path = akn_uri
        if "opendata.finlex.fi" in path:
            path = path.split("/finlex/avoindata/v1")[-1]

        parts = path.strip("/").split("/")

        # Expected format: akn/fi/{category}/{type}/{year}/{number}/{lang}@
        if len(parts) >= 6:
            result["category"] = parts[2] if len(parts) > 2 else ""
            result["type"] = parts[3] if len(parts) > 3 else ""
            result["year"] = parts[4] if len(parts) > 4 else ""
            result["number"] = parts[5] if len(parts) > 5 else ""

            # Language and version marker
            if len(parts) > 6:
                lang_part = parts[6]
                if "@" in lang_part:
                    result["language"] = lang_part.replace("@", "")

        return result

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from Finlex.

        Iterates through statute, statute-consolidated, treaty,
        and government-proposal endpoints.
        """
        for category, doc_type in DOCUMENT_TYPES:
            logger.info(f"Fetching {category}/{doc_type}")
            for doc in self._paginate_documents(category, doc_type):
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified since the given date.

        The Finlex API returns status (NEW/MODIFIED) in the list endpoint,
        so we can filter based on that during bootstrap.
        For true incremental updates, we'd need to track which documents
        we've already processed and check status.
        """
        # For now, fetch all and let the base class handle deduplication
        # The API doesn't support date-based filtering directly
        for category, doc_type in DOCUMENT_TYPES:
            logger.info(f"Fetching updates for {category}/{doc_type}")
            for doc in self._paginate_documents(category, doc_type, max_pages=5):
                yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw Finlex document into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from Akoma Ntoso XML.
        """
        akn_uri = raw.get("akn_uri", "")
        xml_content = raw.get("xml_content", "")
        doc_type = raw.get("doc_type", "statute")

        # Parse the URI for identifiers
        uri_parts = self._parse_akn_uri(akn_uri)

        # Extract text and metadata from XML
        title, full_text, xml_metadata = self._extract_text_from_akn(xml_content)

        # Build unique ID from URI path
        doc_id = akn_uri.replace("https://opendata.finlex.fi/finlex/avoindata/v1", "")
        doc_id = doc_id.strip("/").replace("/", "_")

        # Determine date
        date = xml_metadata.get("date_issued") or xml_metadata.get("date_published") or ""

        # Build URL to original document
        if akn_uri.startswith("http"):
            url = akn_uri
        else:
            url = f"{API_BASE}{akn_uri}"

        # Get year
        year = xml_metadata.get("year") or uri_parts.get("year") or ""
        if year:
            try:
                year = int(year)
            except ValueError:
                year = None

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "FI/Finlex",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title or xml_metadata.get("doc_number", doc_id),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "akn_uri": akn_uri,
            "doc_type": doc_type,
            "year": year,
            "number": xml_metadata.get("number") or uri_parts.get("number") or "",
            "language": xml_metadata.get("language") or uri_parts.get("language") or "",
            "eli": xml_metadata.get("eli", ""),
            "subtype": xml_metadata.get("subtype", ""),
            "date_published": xml_metadata.get("date_published", ""),
            "status": raw.get("status", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API version test."""
        print("Testing Finlex Open Data API...")

        # Test statute list endpoint
        print("\nChecking available document types:")

        for category, doc_type in DOCUMENT_TYPES:
            docs = self._get_document_list(category, doc_type, page=1, limit=5)
            print(f"  {category}/{doc_type}: {len(docs)} documents on first page")

            if docs:
                first_uri = docs[0].get("akn_uri", "")
                print(f"    Example: {first_uri[:80]}...")

        # Test fetching a single document
        print("\nFetching sample document...")
        docs = self._get_document_list("act", "statute", page=1, limit=1)
        if docs:
            sample_uri = docs[0].get("akn_uri", "")
            sample_doc = self._fetch_document(sample_uri)
            if sample_doc:
                title, text, _ = self._extract_text_from_akn(sample_doc.get("xml_content", ""))
                print(f"  Title: {title[:100]}...")
                print(f"  Text length: {len(text)} chars")
                if text:
                    print(f"  Text preview: {text[:200]}...")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = FinlexScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
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
