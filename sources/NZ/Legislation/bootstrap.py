#!/usr/bin/env python3
"""
NZ/Legislation -- New Zealand Legislation Fetcher

Fetches New Zealand legislation from the official legislation.govt.nz website.
Uses the public sitemap to discover all legislation and fetches XML full text.

Strategy:
  - Bootstrap: Parse sitemap.xml to get all legislation URLs
  - Append ".xml" to each URL to get full text XML
  - Extract text content from XML elements
  - Handle all types: acts, bills, secondary-legislation, amendment-papers

URL pattern:
  https://www.legislation.govt.nz/{type}/{subtype}/{year}/{number}/{lang}/{version}/
  XML version: append ".xml" to the URL

Data:
  - Acts: ~17,500 (public, local, private, imperial, provincial)
  - Bills: ~1,800
  - Secondary legislation: ~20,000
  - Amendment papers: ~2,700
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (not yet implemented)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.Legislation")

# Base URL for NZ legislation
BASE_URL = "https://www.legislation.govt.nz"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"


class NZLegislationScraper(BaseScraper):
    """
    Scraper for NZ/Legislation -- New Zealand Legislation.
    Country: NZ
    URL: https://www.legislation.govt.nz

    Data types: legislation
    Auth: none (Open Data - XML files publicly accessible)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/xml, text/xml, */*",
            },
            timeout=120,  # Long timeout for large XML files
        )

    def _fetch_sitemap(self) -> Generator[Dict[str, str], None, None]:
        """
        Parse the sitemap and yield URL entries with metadata.

        Yields dicts with: url, lastmod
        """
        logger.info(f"Fetching sitemap from {SITEMAP_URL}")
        self.rate_limiter.wait()

        try:
            resp = self.client.get("/sitemap.xml")
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch sitemap: {e}")
            return

        # Parse sitemap XML (namespace-aware)
        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            return

        # Handle sitemap namespace
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        for url_elem in root.findall("sm:url", ns):
            loc = url_elem.find("sm:loc", ns)
            lastmod = url_elem.find("sm:lastmod", ns)

            if loc is not None and loc.text:
                yield {
                    "url": loc.text.strip(),
                    "lastmod": lastmod.text.strip() if lastmod is not None and lastmod.text else None,
                }

    def _parse_url_path(self, url: str) -> Optional[Dict[str, str]]:
        """
        Extract metadata from URL path.

        URL format: /type/subtype/year/number/lang/version/
        Example: /act/public/1991/69/en/latest/

        Returns dict with: legislation_type, legislation_subtype, year, number, lang, version
        """
        # Remove base URL and trailing slash
        path = url.replace(BASE_URL, "").strip("/")
        parts = path.split("/")

        if len(parts) < 4:
            return None

        # Determine type and validate
        leg_type = parts[0]
        if leg_type not in ("act", "bill", "secondary-legislation", "amendment-paper"):
            return None

        return {
            "legislation_type": leg_type,
            "legislation_subtype": parts[1] if len(parts) > 1 else None,
            "year": parts[2] if len(parts) > 2 else None,
            "number": parts[3] if len(parts) > 3 else None,
            "lang": parts[4] if len(parts) > 4 else "en",
            "version": parts[5] if len(parts) > 5 else "latest",
        }

    def _fetch_xml(self, url: str) -> Optional[str]:
        """
        Fetch the XML content for a legislation URL.

        Appends .xml to the URL if not already present.
        """
        xml_url = url.rstrip("/") + ".xml" if not url.endswith(".xml") else url

        self.rate_limiter.wait()

        try:
            # Convert to relative path for client
            relative_path = xml_url.replace(BASE_URL, "")
            resp = self.client.get(relative_path)

            if resp.status_code == 404:
                logger.debug(f"XML not found: {xml_url}")
                return None

            resp.raise_for_status()
            return resp.text

        except Exception as e:
            logger.warning(f"Failed to fetch XML {xml_url}: {e}")
            return None

    def _extract_text_from_xml(self, xml_content: str) -> str:
        """
        Extract plain text content from NZ legislation XML.

        The XML uses various elements containing text:
        - <text>: Main text content
        - <para>: Paragraphs
        - <heading>: Section headings
        - <label>: Section numbers
        - <title>: Document title

        Returns concatenated plain text with structure preserved.
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse XML: {e}")
            return ""

        text_parts = []

        def extract_text_recursive(elem, indent=0):
            """Recursively extract text from element tree."""
            # Get tag name without namespace
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            # Add heading/title with formatting
            if tag in ("title", "heading"):
                text = "".join(elem.itertext()).strip()
                if text:
                    text_parts.append(f"\n{text}\n")

            # Add label (section numbers)
            elif tag == "label":
                text = "".join(elem.itertext()).strip()
                if text:
                    text_parts.append(f"{text}. ")

            # Add text content
            elif tag == "text":
                text = "".join(elem.itertext()).strip()
                if text:
                    text_parts.append(text + "\n")

            # Recursively process children
            for child in elem:
                extract_text_recursive(child, indent + 1)

        extract_text_recursive(root)

        # Clean up the text
        full_text = "".join(text_parts)
        # Normalize whitespace
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        return full_text.strip()

    def _extract_metadata_from_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Extract metadata from NZ legislation XML.

        Returns dict with: title, assent_date, commencement, long_title, ministry, reprint_date
        """
        metadata = {}

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            return metadata

        # Get root element attributes
        for attr in root.attrib:
            if attr == "year":
                metadata["year"] = root.get(attr)
            elif attr == "date.assent":
                metadata["assent_date"] = root.get(attr)
            elif attr == "date.as.at":
                metadata["reprint_date"] = root.get(attr)
            elif attr in ("act.no", "bill.no"):
                metadata["number"] = root.get(attr)
            elif attr in ("act.type", "bill.type"):
                metadata["subtype"] = root.get(attr)

        # Find cover element (contains title, assent date, etc.)
        cover = root.find(".//cover")
        if cover is not None:
            # Title
            title_elem = cover.find("title")
            if title_elem is not None:
                metadata["title"] = "".join(title_elem.itertext()).strip()

            # Assent date
            assent_elem = cover.find("assent")
            if assent_elem is not None:
                metadata["assent_date"] = assent_elem.text.strip() if assent_elem.text else None

            # Commencement
            commence_elem = cover.find("commencement")
            if commence_elem is not None:
                metadata["commencement"] = commence_elem.text.strip() if commence_elem.text else None

            # Reprint date
            reprint_elem = cover.find("reprint-date")
            if reprint_elem is not None:
                metadata["reprint_date"] = reprint_elem.text.strip() if reprint_elem.text else None

            # Administering ministry
            ministry_elem = cover.find(".//ministry")
            if ministry_elem is not None:
                metadata["administering_ministry"] = "".join(ministry_elem.itertext()).strip()

        # Find long-title
        long_title = root.find(".//long-title")
        if long_title is not None:
            metadata["long_title"] = "".join(long_title.itertext()).strip()

        # For bills, get title from billdetail
        billdetail = root.find(".//billdetail")
        if billdetail is not None:
            title_elem = billdetail.find("title")
            if title_elem is not None:
                metadata["title"] = "".join(title_elem.itertext()).strip()

        return metadata

    def _generate_id(self, url: str, path_info: Dict[str, str]) -> str:
        """
        Generate a unique document ID from URL components.

        Format: {type}_{subtype}_{year}_{number}
        Example: act_public_1991_69
        """
        parts = [
            path_info.get("legislation_type", "unknown"),
            path_info.get("legislation_subtype", "unknown"),
            path_info.get("year", "0000"),
            path_info.get("number", "0"),
        ]
        return "_".join(parts)

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from NZ.

        Parses sitemap, fetches XML for each entry, extracts full text.
        """
        logger.info("Starting full fetch of NZ legislation")
        count = 0

        for entry in self._fetch_sitemap():
            url = entry["url"]
            lastmod = entry.get("lastmod")

            # Parse URL to get metadata
            path_info = self._parse_url_path(url)
            if path_info is None:
                continue

            # Fetch XML content
            xml_content = self._fetch_xml(url)
            if xml_content is None:
                continue

            count += 1
            if count % 100 == 0:
                logger.info(f"Processed {count} documents")

            yield {
                "url": url,
                "lastmod": lastmod,
                "path_info": path_info,
                "xml_content": xml_content,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified since the given date.

        Uses lastmod from sitemap to filter entries.
        """
        logger.info(f"Fetching updates since {since.isoformat()}")

        for entry in self._fetch_sitemap():
            url = entry["url"]
            lastmod = entry.get("lastmod")

            # Skip if no lastmod or if older than since
            if lastmod:
                try:
                    mod_date = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
                    if mod_date < since:
                        continue
                except ValueError:
                    pass  # Can't parse date, include anyway

            path_info = self._parse_url_path(url)
            if path_info is None:
                continue

            xml_content = self._fetch_xml(url)
            if xml_content is None:
                continue

            yield {
                "url": url,
                "lastmod": lastmod,
                "path_info": path_info,
                "xml_content": xml_content,
            }

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform raw document into standardized schema.

        Required fields:
          - _id: unique identifier
          - _source: "NZ/Legislation"
          - _type: "legislation"
          - _fetched_at: ISO timestamp
          - title: document title
          - text: FULL TEXT (mandatory)
          - url: source URL
        """
        url = raw.get("url", "")
        path_info = raw.get("path_info", {})
        xml_content = raw.get("xml_content", "")

        if not xml_content:
            return None

        # Extract text content
        text = self._extract_text_from_xml(xml_content)
        if not text or len(text) < 50:
            logger.debug(f"Skipping {url}: insufficient text content")
            return None

        # Extract metadata from XML
        metadata = self._extract_metadata_from_xml(xml_content)

        # Generate ID
        doc_id = self._generate_id(url, path_info)

        # Get title
        title = metadata.get("title", "")
        if not title:
            # Fallback: construct from path info
            title = f"{path_info.get('legislation_type', 'Unknown')} {path_info.get('year', '')}/{path_info.get('number', '')}"

        # Parse dates
        assent_date = metadata.get("assent_date")
        reprint_date = metadata.get("reprint_date")

        # Determine the primary date
        primary_date = None
        if assent_date:
            # Try to parse assent date
            try:
                if len(assent_date) == 10:  # YYYY-MM-DD format
                    primary_date = assent_date
                elif "-" in assent_date:
                    primary_date = assent_date
            except Exception:
                pass

        # Year from path or metadata
        year = None
        try:
            year = int(path_info.get("year") or metadata.get("year") or 0)
            if year == 0:
                year = None
        except (ValueError, TypeError):
            pass

        return {
            "_id": doc_id,
            "_source": "NZ/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "url": url,
            "date": primary_date,
            "legislation_type": path_info.get("legislation_type"),
            "legislation_subtype": path_info.get("legislation_subtype"),
            "year": year,
            "number": path_info.get("number"),
            "version_date": reprint_date,
            "administering_ministry": metadata.get("administering_ministry"),
            "commencement": metadata.get("commencement"),
            "long_title": metadata.get("long_title"),
            "lastmod": raw.get("lastmod"),
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="NZ/Legislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Sample mode: fetch only 10-15 records",
    )
    args = parser.parse_args()

    scraper = NZLegislationScraper()

    if args.command == "test":
        # Quick connectivity test
        logger.info("Testing connectivity to legislation.govt.nz...")
        try:
            # Test sitemap
            entries = list(scraper._fetch_sitemap())
            logger.info(f"Sitemap contains {len(entries)} entries")

            # Test one XML fetch
            if entries:
                test_entry = entries[0]
                path_info = scraper._parse_url_path(test_entry["url"])
                if path_info:
                    xml = scraper._fetch_xml(test_entry["url"])
                    if xml:
                        text = scraper._extract_text_from_xml(xml)
                        logger.info(f"Sample document text length: {len(text)} chars")
                        logger.info(f"Test passed! URL: {test_entry['url']}")
                    else:
                        logger.error("Failed to fetch XML")
                        sys.exit(1)
        except Exception as e:
            logger.error(f"Test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "update":
        logger.info("Running incremental update")
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
