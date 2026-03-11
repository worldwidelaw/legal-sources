#!/usr/bin/env python3
"""
CA/FederalLegislation -- Justice Laws Website (laws-lois.justice.gc.ca) Data Fetcher

Fetches Canadian federal legislation (Acts and Regulations) from the official
Justice Laws Website XML endpoints.

Strategy:
  - Bootstrap: Parse XML catalog (Legis.xml) for all Acts and Regulations,
    then fetch full XML content for each document
  - Update: Uses CurrentToDate field to identify recently modified documents
  - Sample: Fetches 10+ English Acts for validation

API: https://laws-lois.justice.gc.ca/eng/XML/Legis.xml (catalog)
     https://laws-lois.justice.gc.ca/eng/XML/{UniqueId}.xml (individual docs)

Docs: https://laws-lois.justice.gc.ca/eng/XML/Legis.xml (self-documenting)
GitHub mirror: https://github.com/justicecanada/laws-lois-xml

Data notes:
  - ~956 Acts + ~4834 Regulations (English + French versions = 1912 + 9669)
  - Updated bi-weekly
  - Full text in XML with detailed structure (sections, paragraphs, etc.)
  - Bilingual: English (eng) and French (fra)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
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
logger = logging.getLogger("legal-data-hunter.CA.FederalLegislation")

# API base URL
API_BASE = "https://laws-lois.justice.gc.ca"
CATALOG_URL = f"{API_BASE}/eng/XML/Legis.xml"


class CAFederalLegislationScraper(BaseScraper):
    """
    Scraper for CA/FederalLegislation -- Justice Laws Website.
    Country: CA
    URL: https://laws-lois.justice.gc.ca

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/xml, text/xml",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_catalog(self) -> Optional[ET.Element]:
        """Fetch the XML catalog of all Acts and Regulations."""
        self.rate_limiter.wait()

        try:
            resp = self.client.get(CATALOG_URL)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Error fetching catalog: {e}")
            return None

    def _parse_catalog(self, root: ET.Element) -> Generator[dict, None, None]:
        """Parse the catalog XML and yield metadata for each document."""
        # Parse Acts
        for act in root.findall(".//Act"):
            yield self._parse_catalog_entry(act, "act")

        # Parse Regulations
        for reg in root.findall(".//Regulation"):
            yield self._parse_catalog_entry(reg, "regulation")

    def _parse_catalog_entry(self, elem: ET.Element, doc_type: str) -> dict:
        """Parse a single Act or Regulation entry from the catalog."""
        unique_id = elem.findtext("UniqueId", "")
        official_number = elem.findtext("OfficialNumber", "")
        language = elem.findtext("Language", "")
        link_to_xml = elem.findtext("LinkToXML", "")
        link_to_html = elem.findtext("LinkToHTMLToC", "")
        title = elem.findtext("Title", "")
        current_to_date = elem.findtext("CurrentToDate", "")

        return {
            "unique_id": unique_id,
            "official_number": official_number,
            "language": language,
            "link_to_xml": link_to_xml,
            "link_to_html": link_to_html,
            "title": title,
            "current_to_date": current_to_date,
            "doc_type": doc_type,
        }

    def _get_document_xml(self, xml_url: str) -> Optional[ET.Element]:
        """Fetch full XML for a specific legislation document."""
        self.rate_limiter.wait()

        # Ensure HTTPS
        xml_url = xml_url.replace("http://", "https://")

        try:
            resp = self.client.get(xml_url)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Error fetching document {xml_url}: {e}")
            return None

    def _extract_text_from_xml(self, root: ET.Element) -> str:
        """Extract full text content from legislation XML."""
        text_parts = []

        # Extract title from Identification section
        for elem in root.iter():
            tag = self._get_tag_name(elem)
            if tag == "LongTitle" and elem.text:
                text_parts.append(elem.text.strip())
                break

        # Extract all text content from Text, P, Label elements
        # These contain the actual legislative text
        seen_text = set()

        for elem in root.iter():
            tag = self._get_tag_name(elem)

            # Get text from relevant elements
            if tag in ("Text", "P", "FormulaParagraph", "MarginalNote"):
                # Get all text including nested elements
                text = "".join(elem.itertext()).strip()
                if text and text not in seen_text:
                    seen_text.add(text)
                    text_parts.append(text)

        full_text = "\n\n".join(text_parts)

        # Clean up whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' {2,}', ' ', full_text)

        return full_text.strip()

    def _get_tag_name(self, elem: ET.Element) -> str:
        """Get tag name without namespace prefix."""
        tag = elem.tag
        if "}" in tag:
            return tag.split("}")[-1]
        return tag

    def _extract_metadata_from_xml(self, root: ET.Element) -> dict:
        """Extract additional metadata from the document XML."""
        metadata = {}

        # Get short title
        for elem in root.iter():
            tag = self._get_tag_name(elem)
            if tag == "ShortTitle":
                text = "".join(elem.itertext()).strip()
                if text:
                    metadata["short_title"] = text
                break

        # Get consolidated number
        for elem in root.iter():
            tag = self._get_tag_name(elem)
            if tag == "ConsolidatedNumber":
                text = "".join(elem.itertext()).strip()
                if text:
                    metadata["consolidated_number"] = text
                break

        # Get in-force status
        if root.get("in-force"):
            metadata["in_force"] = root.get("in-force")

        # Get last amended date
        lims_last_amended = root.get("{http://justice.gc.ca/lims}lastAmendedDate")
        if lims_last_amended:
            metadata["last_amended_date"] = lims_last_amended

        return metadata

    # -- Public API ---------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Canadian federal legislation (full bootstrap)."""
        logger.info("Fetching catalog from Justice Laws Website...")

        catalog = self._get_catalog()
        if catalog is None:
            logger.error("Failed to fetch catalog")
            return

        # Count entries for logging
        acts = catalog.findall(".//Act")
        regs = catalog.findall(".//Regulation")
        logger.info(f"Catalog contains {len(acts)} Acts and {len(regs)} Regulations")

        count = 0
        for entry in self._parse_catalog(catalog):
            xml_url = entry.get("link_to_xml")
            if not xml_url:
                continue

            # Fetch full document
            doc_xml = self._get_document_xml(xml_url)
            if doc_xml is None:
                continue

            # Extract full text
            text = self._extract_text_from_xml(doc_xml)
            if not text:
                logger.warning(f"No text extracted from {entry['unique_id']}")
                continue

            # Extract additional metadata
            extra_metadata = self._extract_metadata_from_xml(doc_xml)

            record = {
                **entry,
                "text": text,
                **extra_metadata,
            }

            count += 1
            if count % 100 == 0:
                logger.info(f"Processed {count} documents...")

            yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch legislation updated since a given date."""
        logger.info(f"Fetching updates since {since.isoformat()}")

        catalog = self._get_catalog()
        if catalog is None:
            return

        since_str = since.strftime("%Y-%m-%d")

        for entry in self._parse_catalog(catalog):
            current_to_date = entry.get("current_to_date", "")

            # Only process if current_to_date is after 'since'
            if current_to_date and current_to_date >= since_str:
                xml_url = entry.get("link_to_xml")
                if not xml_url:
                    continue

                doc_xml = self._get_document_xml(xml_url)
                if doc_xml is None:
                    continue

                text = self._extract_text_from_xml(doc_xml)
                if not text:
                    continue

                extra_metadata = self._extract_metadata_from_xml(doc_xml)

                yield {
                    **entry,
                    "text": text,
                    **extra_metadata,
                }

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch sample records for validation (10+ from English Acts)."""
        logger.info("Fetching sample records...")

        catalog = self._get_catalog()
        if catalog is None:
            return

        count = 0
        target = 12

        # Sample from English Acts
        for act in catalog.findall(".//Act"):
            entry = self._parse_catalog_entry(act, "act")

            # Only English
            if entry.get("language") != "eng":
                continue

            xml_url = entry.get("link_to_xml")
            if not xml_url:
                continue

            doc_xml = self._get_document_xml(xml_url)
            if doc_xml is None:
                continue

            text = self._extract_text_from_xml(doc_xml)
            if not text or len(text) < 100:
                continue

            extra_metadata = self._extract_metadata_from_xml(doc_xml)

            record = {
                **entry,
                "text": text,
                **extra_metadata,
            }

            yield record
            count += 1

            if count >= target:
                return

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to the standard schema."""
        # Build URL
        html_url = raw.get("link_to_html", "")
        if not html_url:
            # Construct from unique_id
            lang = raw.get("language", "eng")
            doc_type = raw.get("doc_type", "act")
            unique_id = raw.get("unique_id", "")
            if doc_type == "act":
                html_url = f"{API_BASE}/{lang}/acts/{unique_id}/index.html"
            else:
                html_url = f"{API_BASE}/{lang}/regulations/{unique_id}/index.html"

        # Parse date
        current_to_date = raw.get("current_to_date", "")
        if current_to_date:
            # Format: YYYY-MM-DD
            date = current_to_date
        else:
            date = ""

        return {
            "_id": f"{raw['unique_id']}_{raw.get('language', 'eng')}",
            "_source": "CA/FederalLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": html_url,
            "unique_id": raw.get("unique_id", ""),
            "official_number": raw.get("official_number", ""),
            "language": raw.get("language", ""),
            "doc_type": raw.get("doc_type", ""),
            "current_to_date": current_to_date,
            "short_title": raw.get("short_title", ""),
            "consolidated_number": raw.get("consolidated_number", ""),
            "in_force": raw.get("in_force", ""),
            "last_amended_date": raw.get("last_amended_date", ""),
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing Justice Laws Website API...")

        try:
            catalog = self._get_catalog()
            if catalog is None:
                logger.error("Failed to fetch catalog")
                return False

            acts = catalog.findall(".//Act")
            regs = catalog.findall(".//Regulation")
            logger.info(f"API test successful: {len(acts)} Acts, {len(regs)} Regulations")

            # Test fetching one document
            if acts:
                entry = self._parse_catalog_entry(acts[0], "act")
                xml_url = entry.get("link_to_xml")
                if xml_url:
                    doc = self._get_document_xml(xml_url)
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
    scraper = CAFederalLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py <command> [options]")
        print("Commands: bootstrap, update, test-api")
        print("Options: --sample")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv

        if sample_mode:
            logger.info("Running in SAMPLE mode (10+ records)")
            records = list(scraper.fetch_sample())
        else:
            logger.info("Running FULL bootstrap")
            records = list(scraper.fetch_all())

        # Save records
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        for i, raw in enumerate(records):
            normalized = scraper.normalize(raw)
            # Sanitize filename
            safe_id = normalized['_id'].replace('/', '_').replace(',', '_').replace(' ', '_')
            filename = f"{safe_id}.json"
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved: {filename} ({len(normalized.get('text', ''))} chars)")

        logger.info(f"Total records saved: {len(records)}")

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        logger.info(f"Fetching updates since {since.isoformat()}")

        records = list(scraper.fetch_updates(since))
        logger.info(f"Found {len(records)} updated records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
