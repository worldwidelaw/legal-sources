#!/usr/bin/env python3
"""
UK/LegislationGovUK -- UK National Archives Legislation Portal Fetcher

Fetches UK legislation from legislation.gov.uk ATOM feed API.

Strategy:
  - Bootstrap: Paginates through ATOM feed, downloads XML for full text
  - Update: Uses feed ordering (most recent first) or date filtering
  - Sample: Fetches 15+ recent acts for validation

API: https://www.legislation.gov.uk/
Docs: https://www.legislation.gov.uk/developer

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
logger = logging.getLogger("legal-data-hunter.UK.LegislationGovUK")

# legislation.gov.uk base URL
BASE_URL = "https://www.legislation.gov.uk"

# ATOM namespace
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
LEG_NS = {"leg": "http://www.legislation.gov.uk/namespaces/legislation"}
UKM_NS = {"ukm": "http://www.legislation.gov.uk/namespaces/metadata"}
DC_NS = {"dc": "http://purl.org/dc/elements/1.1/"}
DCT_NS = {"dct": "http://purl.org/dc/terms/"}

# Legislation types to fetch (primary UK legislation)
LEGISLATION_TYPES = [
    "ukpga",  # UK Public General Acts
]

# For full coverage, can add:
# "uksi",  # UK Statutory Instruments
# "wsi",   # Welsh Statutory Instruments
# "asp",   # Acts of Scottish Parliament
# "ssi",   # Scottish Statutory Instruments


class LegislationGovUKScraper(BaseScraper):
    """
    Scraper for UK/LegislationGovUK -- UK National Archives Legislation Portal.
    Country: UK
    URL: https://www.legislation.gov.uk

    Data types: legislation
    Auth: none (Open Government Licence v3.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/atom+xml, application/xml, text/xml",
            },
            timeout=60,
        )

    # -- Feed parsing ----------------------------------------------------------

    def _paginate_feed(
        self,
        leg_type: str,
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Paginate through the ATOM feed for a legislation type.
        Yields raw entry dicts with document metadata.
        """
        page = 1
        feed_url = f"/{leg_type}/data.feed"

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping")
                return

            self.rate_limiter.wait()
            url = f"{feed_url}?page={page}" if page > 1 else feed_url

            try:
                resp = self.client.get(url)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Feed error on page {page}: {e}")
                return

            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError as e:
                logger.error(f"XML parse error on page {page}: {e}")
                return

            # Find all entries
            entries = root.findall("atom:entry", ATOM_NS)
            if not entries:
                logger.info(f"No more entries on page {page}")
                return

            logger.info(f"{leg_type} page {page}: {len(entries)} entries")

            for entry in entries:
                parsed = self._parse_entry(entry, leg_type)
                if parsed:
                    yield parsed

            # Check for next page
            next_link = root.find("atom:link[@rel='next']", ATOM_NS)
            if next_link is None:
                logger.info(f"No more pages for {leg_type}")
                return

            page += 1

    def _parse_entry(self, entry: ET.Element, leg_type: str) -> Optional[dict]:
        """Parse a single ATOM entry into a raw document dict."""
        try:
            title = entry.findtext("atom:title", "", ATOM_NS)

            # Get all links and filter manually (ElementTree XPath is limited)
            links = entry.findall("atom:link", ATOM_NS)

            doc_id = ""
            doc_url = ""
            xml_url = ""

            for link in links:
                rel = link.get("rel", "")
                link_type = link.get("type", "")
                href = link.get("href", "")

                if rel == "self":
                    doc_id = href
                elif link_type == "application/xml":
                    xml_url = href
                elif not rel and not doc_url:
                    # First link without rel attribute
                    doc_url = href

            # Get updated date
            updated = entry.findtext("atom:updated", "", ATOM_NS)

            # Extract year and number from doc_id (e.g., "/id/ukpga/2020/1")
            year = ""
            number = ""
            if doc_id:
                parts = doc_id.rstrip("/").split("/")
                if len(parts) >= 2:
                    try:
                        number = parts[-1]
                        year = parts[-2]
                    except (IndexError, ValueError):
                        pass

            # Extract <content> and <summary> as fallback text (#151)
            atom_content = ""
            content_elem = entry.find("atom:content", ATOM_NS)
            if content_elem is not None:
                # Content may be HTML or plain text
                atom_content = "".join(content_elem.itertext()).strip()

            atom_summary = ""
            summary_elem = entry.find("atom:summary", ATOM_NS)
            if summary_elem is not None:
                atom_summary = "".join(summary_elem.itertext()).strip()

            return {
                "doc_id": doc_id,
                "title": title,
                "doc_url": doc_url,
                "xml_url": xml_url,
                "updated": updated,
                "leg_type": leg_type,
                "year": year,
                "number": number,
                "atom_content": atom_content,
                "atom_summary": atom_summary,
            }
        except Exception as e:
            logger.warning(f"Error parsing entry: {e}")
            return None

    # -- Full text extraction --------------------------------------------------

    def _download_full_text(self, xml_url: str) -> tuple[str, dict]:
        """
        Download XML and extract full text plus metadata.
        Returns (full_text, metadata_dict).
        """
        if not xml_url:
            return "", {}

        self.rate_limiter.wait()

        try:
            resp = self.client.get(xml_url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch XML from {xml_url}: {e}")
            return "", {}

        try:
            # Register namespaces to avoid ns0: prefixes
            ET.register_namespace("", "http://www.legislation.gov.uk/namespaces/legislation")
            ET.register_namespace("ukm", "http://www.legislation.gov.uk/namespaces/metadata")
            ET.register_namespace("dc", "http://purl.org/dc/elements/1.1/")

            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.warning(f"XML parse error for {xml_url}: {e}")
            return "", {}

        # Extract metadata from ukm:Metadata
        metadata = self._extract_metadata(root)

        # Extract full text from body
        text_parts = []

        # Define namespace map for XPath
        ns = {
            "leg": "http://www.legislation.gov.uk/namespaces/legislation",
            "": "http://www.legislation.gov.uk/namespaces/legislation",
        }

        # Try to find text in various elements
        # Legislation structure: Body > Part > Chapter > Section > Subsection > Text
        for text_elem in root.iter():
            # Skip metadata elements
            if "Metadata" in text_elem.tag or "ukm:" in text_elem.tag:
                continue

            # Extract text from content elements
            tag_local = text_elem.tag.split("}")[-1] if "}" in text_elem.tag else text_elem.tag

            if tag_local in ["Title", "Text", "Para", "P", "Pnumber", "Heading",
                            "Number", "Subtitle", "LongTitle", "EnactingText",
                            "IntroductoryText", "SignatureText"]:
                elem_text = "".join(text_elem.itertext()).strip()
                if elem_text:
                    text_parts.append(elem_text)

        full_text = "\n\n".join(text_parts)

        # Clean up the text
        full_text = html.unescape(full_text)
        # Normalize whitespace but keep paragraph breaks
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        full_text = full_text.strip()

        return full_text, metadata

    def _extract_metadata(self, root: ET.Element) -> dict:
        """Extract metadata from legislation XML."""
        metadata = {}

        ns = {
            "ukm": "http://www.legislation.gov.uk/namespaces/metadata",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dct": "http://purl.org/dc/terms/",
        }

        # Find metadata section
        meta_elem = root.find(".//{http://www.legislation.gov.uk/namespaces/metadata}Metadata")
        if meta_elem is None:
            return metadata

        # Dublin Core metadata
        for dc_elem in meta_elem.findall(".//{http://purl.org/dc/elements/1.1/}*"):
            tag = dc_elem.tag.split("}")[-1]
            metadata[f"dc_{tag}"] = dc_elem.text or ""

        for dct_elem in meta_elem.findall(".//{http://purl.org/dc/terms/}*"):
            tag = dct_elem.tag.split("}")[-1]
            metadata[f"dct_{tag}"] = dct_elem.text or ""

        # UK-specific metadata
        year_elem = meta_elem.find(".//{http://www.legislation.gov.uk/namespaces/metadata}Year")
        if year_elem is not None:
            metadata["year"] = year_elem.get("Value", "")

        number_elem = meta_elem.find(".//{http://www.legislation.gov.uk/namespaces/metadata}Number")
        if number_elem is not None:
            metadata["number"] = number_elem.get("Value", "")

        # Get RestrictExtent (territorial application)
        extent = root.get("RestrictExtent", "")
        metadata["extent"] = extent

        # Get document URI
        doc_uri = root.get("DocumentURI", "")
        metadata["document_uri"] = doc_uri

        return metadata

    # -- Abstract method implementations ---------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from legislation.gov.uk.
        Iterates through ATOM feeds for each legislation type.
        """
        for leg_type in LEGISLATION_TYPES:
            logger.info(f"Fetching legislation type: {leg_type}")
            for doc in self._paginate_feed(leg_type):
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified since the given date.

        The feed is ordered by most recent first, so we fetch pages until
        we hit documents older than 'since'.
        """
        for leg_type in LEGISLATION_TYPES:
            logger.info(f"Fetching updates for {leg_type} since {since}")

            for doc in self._paginate_feed(leg_type, max_pages=10):
                # Parse the updated date
                updated_str = doc.get("updated", "")
                if updated_str:
                    try:
                        # Parse ISO format with timezone
                        updated_dt = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                        if updated_dt < since:
                            logger.info(f"Reached documents older than {since}, stopping")
                            return
                    except ValueError:
                        pass

                yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw entry dict into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from XML endpoint.
        """
        doc_id = raw.get("doc_id", "")
        xml_url = raw.get("xml_url", "")

        # Download full text and additional metadata
        full_text, xml_metadata = self._download_full_text(xml_url)

        # Fallback: use ATOM <content> or <summary> when XML download fails (#151)
        if not full_text:
            atom_content = raw.get("atom_content", "")
            atom_summary = raw.get("atom_summary", "")
            fallback = atom_content or atom_summary
            if fallback:
                # Clean HTML entities and tags from ATOM content
                fallback = html.unescape(fallback)
                fallback = re.sub(r"<[^>]+>", " ", fallback)
                fallback = re.sub(r"\s+", " ", fallback).strip()
                if len(fallback) > 50:
                    full_text = fallback
                    logger.info(f"Used ATOM fallback text for {doc_id} ({len(full_text)} chars)")

        # Build unique ID from doc_id
        # Format: UK/LegislationGovUK/ukpga/2020/1
        clean_id = doc_id.replace("http://www.legislation.gov.uk/id/", "")
        unique_id = f"UK/LegislationGovUK/{clean_id}"

        # Determine date - use dct_valid (in force date) or year
        date = xml_metadata.get("dct_valid", "")
        if not date:
            year = raw.get("year") or xml_metadata.get("year", "")
            if year:
                date = f"{year}-01-01"

        # Get version date from updated field or dc_modified
        version_date = raw.get("updated", "") or xml_metadata.get("dc_modified", "")

        # Build document URL (human-readable page)
        doc_url = raw.get("doc_url", "")
        if not doc_url and doc_id:
            doc_url = doc_id.replace("/id/", "/")

        return {
            # Required base fields
            "_id": unique_id,
            "_source": "UK/LegislationGovUK",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", "") or xml_metadata.get("dc_title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": doc_url,
            # UK-specific fields
            "doc_id": doc_id,
            "doc_type": raw.get("leg_type", "ukpga"),
            "year": raw.get("year") or xml_metadata.get("year", ""),
            "number": raw.get("number") or xml_metadata.get("number", ""),
            "version_date": version_date,
            "extent": xml_metadata.get("extent", ""),
            "document_uri": xml_metadata.get("document_uri", ""),
            "description": xml_metadata.get("dc_description", ""),
            "language": xml_metadata.get("dc_language", "en"),
        }

    # -- Custom commands -------------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing legislation.gov.uk API...")

        # Test feed access
        resp = self.client.get("/ukpga/data.feed")
        print(f"  Feed status: {resp.status_code}")

        # Parse to count entries
        root = ET.fromstring(resp.content)
        entries = root.findall("atom:entry", ATOM_NS)
        print(f"  Entries on first page: {len(entries)}")

        # Get total count from facets if available
        facets = root.find(".//leg:facetTypes", {"leg": "http://www.legislation.gov.uk/namespaces/legislation"})
        if facets is not None:
            for facet in facets:
                value = facet.get("value", "")
                print(f"  Total UKPGA acts: {value}")
                break

        # Test XML download
        if entries:
            xml_link = entries[0].find("atom:link[@type='application/xml']", ATOM_NS)
            if xml_link is not None:
                xml_url = xml_link.get("href", "")
                print(f"  Testing XML download: {xml_url}")
                try:
                    xml_resp = self.client.get(xml_url)
                    print(f"  XML status: {xml_resp.status_code}")
                    print(f"  XML size: {len(xml_resp.content)} bytes")
                except Exception as e:
                    print(f"  XML download error: {e}")

        print("\nAPI test passed!")


# -- CLI Entry Point -----------------------------------------------------------


def main():
    scraper = LegislationGovUKScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test-api":
        scraper.test_api()
    elif cmd == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_size = 15  # Default sample size

        for i, arg in enumerate(sys.argv):
            if arg == "--sample-size" and i + 1 < len(sys.argv):
                sample_size = int(sys.argv[i + 1])

        if sample_mode:
            print(f"Running bootstrap in sample mode (n={sample_size})...")
            stats = scraper.bootstrap(sample_mode=True, sample_size=sample_size)
        else:
            print("Running full bootstrap...")
            stats = scraper.bootstrap()

        print(f"\nBootstrap complete:")
        print(json.dumps(stats, indent=2, default=str))
    elif cmd == "update":
        print("Running incremental update...")
        stats = scraper.update()
        print(f"\nUpdate complete:")
        print(json.dumps(stats, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
