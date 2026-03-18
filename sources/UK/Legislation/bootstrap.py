#!/usr/bin/env python3
"""
UK/Legislation -- legislation.gov.uk Data Fetcher

Fetches UK legislation from the official legislation.gov.uk Atom/XML API.

Strategy:
  - Bootstrap: Paginates through Atom feeds for each legislation type/year
  - Update: Uses date filters to fetch recently modified records
  - Sample: Fetches 10+ records from recent primary legislation for validation

API: https://www.legislation.gov.uk/developer
Docs: https://www.legislation.gov.uk/developer/formats

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
logger = logging.getLogger("legal-data-hunter.UK.Legislation")

# API base URL
API_BASE = "https://www.legislation.gov.uk"

# Legislation types to scrape
PRIMARY_TYPES = ["ukpga", "asp", "asc", "nia"]  # Primary legislation
SECONDARY_TYPES = ["uksi", "ssi", "wsi", "nisr"]  # Secondary legislation

# XML namespaces
NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "leg": "http://www.legislation.gov.uk/namespaces/legislation",
    "ukm": "http://www.legislation.gov.uk/namespaces/metadata",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dct": "http://purl.org/dc/terms/",
}


class UKLegislationScraper(BaseScraper):
    """
    Scraper for UK/Legislation -- legislation.gov.uk.
    Country: UK
    URL: https://www.legislation.gov.uk

    Data types: legislation
    Auth: none (Open Government Licence)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/atom+xml, application/xml",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_feed(self, leg_type: str, year: int, page: int = 1) -> Optional[ET.Element]:
        """Fetch an Atom feed page for a legislation type/year."""
        url = f"/{leg_type}/{year}/data.feed"
        params = {"page": str(page)} if page > 1 else {}
        
        self.rate_limiter.wait()
        
        try:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Error fetching feed {url}: {e}")
            return None

    def _get_document_xml(self, doc_uri: str) -> Optional[ET.Element]:
        """Fetch full XML for a specific legislation document."""
        # Remove any trailing /data.xml if already included
        doc_uri = doc_uri.rstrip("/")
        if doc_uri.endswith("/data.xml"):
            doc_uri = doc_uri[:-9]

        url = f"/{doc_uri}/data.xml"

        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Error fetching document {url}: {e}")
            return None

    def _extract_text_from_xml(self, root: ET.Element) -> str:
        """Extract full text content from legislation XML."""
        text_parts = []

        # Get the title from metadata
        ns = "{http://purl.org/dc/elements/1.1/}"
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "title" and elem.text:
                text_parts.append(elem.text.strip())
                break

        # Get Long Title if available
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "LongTitle":
                title_text = "".join(elem.itertext()).strip()
                if title_text:
                    text_parts.append(title_text)
                break

        # Extract only from <Text> elements which contain the actual content
        # Avoid nested duplicates by only getting direct text from Text elements
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "Text":
                # Get all text content from this Text element
                text = "".join(elem.itertext()).strip()
                if text and text not in text_parts:  # Avoid duplicates
                    text_parts.append(text)

        full_text = "\n\n".join(text_parts)

        # Clean up whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' {2,}', ' ', full_text)

        return full_text.strip()

    def _parse_feed_entry(self, entry: ET.Element) -> dict:
        """Parse an Atom feed entry into metadata dict."""
        ns = NAMESPACES

        # Extract basic metadata from feed entry
        title = entry.findtext("atom:title", default="", namespaces=ns)

        # Get document URI from link - prefer the 'alternate' XML link
        doc_uri = None
        for link in entry.findall("atom:link", ns):
            rel = link.get("rel", "")
            href = link.get("href", "")
            link_type = link.get("type", "")
            # Prefer the XML data link
            if rel == "alternate" and link_type == "application/xml" and href.endswith("/data.xml"):
                doc_uri = href.replace("http://www.legislation.gov.uk/", "")
                doc_uri = doc_uri.replace("https://www.legislation.gov.uk/", "")
                doc_uri = doc_uri.replace("/data.xml", "")  # Remove suffix, we add it back
                break

        # Fallback: get the empty rel link (without /id/)
        if not doc_uri:
            for link in entry.findall("atom:link", ns):
                href = link.get("href", "")
                rel = link.get("rel", "")
                if rel == "" and "/id/" not in href and href.startswith("http"):
                    doc_uri = href.replace("http://www.legislation.gov.uk/", "")
                    doc_uri = doc_uri.replace("https://www.legislation.gov.uk/", "")
                    break

        updated = entry.findtext("atom:updated", default="", namespaces=ns)

        return {
            "title": title,
            "doc_uri": doc_uri,
            "updated": updated,
        }

    def _paginate_feed(
        self,
        leg_type: str,
        year: int,
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Paginate through a legislation type/year feed."""
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                return
            
            root = self._get_feed(leg_type, year, page)
            if root is None:
                return
            
            entries = root.findall("atom:entry", NAMESPACES)
            if not entries:
                return
            
            logger.info(f"{leg_type}/{year} page {page}: {len(entries)} entries")
            
            for entry in entries:
                metadata = self._parse_feed_entry(entry)
                if metadata.get("doc_uri"):
                    yield metadata
            
            # Check for next page
            has_next = False
            for link in root.findall("atom:link", NAMESPACES):
                if link.get("rel") == "next":
                    has_next = True
                    break
            
            if not has_next:
                return
            
            page += 1

    # -- Public API ---------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all UK legislation (full bootstrap)."""
        current_year = datetime.now().year
        
        for leg_type in PRIMARY_TYPES + SECONDARY_TYPES:
            logger.info(f"Fetching {leg_type} legislation...")
            
            # Iterate backwards from current year
            for year in range(current_year, 1900, -1):
                for metadata in self._paginate_feed(leg_type, year):
                    doc_uri = metadata["doc_uri"]
                    
                    # Fetch full document
                    doc_xml = self._get_document_xml(doc_uri)
                    if doc_xml is None:
                        continue
                    
                    # Extract full text
                    text = self._extract_text_from_xml(doc_xml)
                    if not text:
                        logger.warning(f"No text extracted from {doc_uri}")
                        continue
                    
                    # Parse document URI for type/year/number
                    parts = doc_uri.split("/")
                    doc_type = parts[0] if parts else leg_type
                    doc_year = parts[1] if len(parts) > 1 else str(year)
                    doc_number = parts[2] if len(parts) > 2 else "1"
                    
                    record = {
                        "doc_id": doc_uri,
                        "title": metadata["title"],
                        "text": text,
                        "url": f"{API_BASE}/{doc_uri}",
                        "legislation_type": doc_type,
                        "year": int(doc_year) if doc_year.isdigit() else year,
                        "number": doc_number,
                        "date": metadata.get("updated", ""),
                        "_raw_metadata": metadata,
                    }
                    
                    yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch legislation updated since a given date."""
        # For now, just fetch recent years
        current_year = datetime.now().year
        for year in range(current_year, current_year - 2, -1):
            for leg_type in PRIMARY_TYPES:
                for record in self._fetch_year(leg_type, year):
                    # Check if updated after 'since'
                    if record.get("date"):
                        try:
                            record_date = datetime.fromisoformat(record["date"].replace("Z", "+00:00"))
                            if record_date >= since:
                                yield record
                        except:
                            yield record
                    else:
                        yield record

    def _fetch_year(self, leg_type: str, year: int) -> Generator[dict, None, None]:
        """Helper to fetch all records for a specific type/year."""
        for metadata in self._paginate_feed(leg_type, year):
            doc_uri = metadata["doc_uri"]
            doc_xml = self._get_document_xml(doc_uri)
            if doc_xml is None:
                continue
            
            text = self._extract_text_from_xml(doc_xml)
            if not text:
                continue
            
            parts = doc_uri.split("/")
            doc_type = parts[0] if parts else leg_type
            doc_year = parts[1] if len(parts) > 1 else str(year)
            doc_number = parts[2] if len(parts) > 2 else "1"
            
            yield {
                "doc_id": doc_uri,
                "title": metadata["title"],
                "text": text,
                "url": f"{API_BASE}/{doc_uri}",
                "legislation_type": doc_type,
                "year": int(doc_year) if doc_year.isdigit() else year,
                "number": doc_number,
                "date": metadata.get("updated", ""),
                "_raw_metadata": metadata,
            }

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch sample records for validation (10+ from recent legislation)."""
        current_year = datetime.now().year
        count = 0
        target = 12
        
        # Sample from primary legislation in recent years
        for year in range(current_year, current_year - 3, -1):
            for leg_type in ["ukpga", "asp", "uksi"]:
                for metadata in self._paginate_feed(leg_type, year, max_pages=1):
                    doc_uri = metadata["doc_uri"]
                    doc_xml = self._get_document_xml(doc_uri)
                    if doc_xml is None:
                        continue
                    
                    text = self._extract_text_from_xml(doc_xml)
                    if not text or len(text) < 100:
                        continue
                    
                    parts = doc_uri.split("/")
                    doc_type = parts[0] if parts else leg_type
                    doc_year = parts[1] if len(parts) > 1 else str(year)
                    doc_number = parts[2] if len(parts) > 2 else "1"
                    
                    record = {
                        "doc_id": doc_uri,
                        "title": metadata["title"],
                        "text": text,
                        "url": f"{API_BASE}/{doc_uri}",
                        "legislation_type": doc_type,
                        "year": int(doc_year) if doc_year.isdigit() else year,
                        "number": doc_number,
                        "date": metadata.get("updated", ""),
                    }
                    
                    yield record
                    count += 1
                    
                    if count >= target:
                        return

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to the standard schema."""
        # Defensive check: if raw is not a dict, skip it
        if not isinstance(raw, dict):
            logger.error(f"normalize() received non-dict: {type(raw).__name__}")
            return None

        doc_id = raw.get("doc_id", "")
        if not doc_id:
            logger.warning("normalize() received record without doc_id")
            return None

        text = raw.get("text", "")
        if not text:
            logger.warning(f"normalize() received record without text: {doc_id}")
            return None

        return {
            "_id": doc_id,
            "_source": "UK/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "doc_id": doc_id,  # Required by schema
            "legislation_type": raw.get("legislation_type", ""),
            "year": raw.get("year", ""),
            "number": raw.get("number", ""),
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing legislation.gov.uk API...")
        
        try:
            root = self._get_feed("ukpga", 2024, 1)
            if root is None:
                logger.error("Failed to fetch test feed")
                return False
            
            entries = root.findall("atom:entry", NAMESPACES)
            logger.info(f"API test successful: {len(entries)} entries found")
            
            if entries:
                # Test fetching one document
                metadata = self._parse_feed_entry(entries[0])
                if metadata.get("doc_uri"):
                    doc = self._get_document_xml(metadata["doc_uri"])
                    if doc is not None:
                        text = self._extract_text_from_xml(doc)
                        logger.info(f"Document fetch successful: {len(text)} chars")
                        return True
            
            return True
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False


def main():
    """CLI entry point."""
    scraper = UKLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py <command> [options]")
        print("Commands: bootstrap, bootstrap-fast, update, test-api")
        print("Options: --sample, --sample-size N, --workers N, --batch-size N")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_size = 15

        for i, arg in enumerate(sys.argv):
            if arg == "--sample-size" and i + 1 < len(sys.argv):
                sample_size = int(sys.argv[i + 1])

        if sample_mode:
            logger.info(f"Running in SAMPLE mode (n={sample_size})")
            stats = scraper.bootstrap(sample_mode=True, sample_size=sample_size)
        else:
            logger.info("Running FULL bootstrap via BaseScraper")
            stats = scraper.bootstrap()

        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")

    elif command == "bootstrap-fast":
        workers = 5
        batch_size = 100

        for i, arg in enumerate(sys.argv):
            if arg == "--workers" and i + 1 < len(sys.argv):
                workers = int(sys.argv[i + 1])
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])

        logger.info(f"Running FAST bootstrap (workers={workers}, batch_size={batch_size})")
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")
    
    elif command == "update":
        logger.info("Running incremental update via BaseScraper")
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2, default=str)}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
