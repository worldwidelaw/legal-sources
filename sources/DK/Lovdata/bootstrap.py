#!/usr/bin/env python3
"""
DK/Lovdata -- Danish Legislation Database (Retsinformation) Data Fetcher

Fetches Danish legislation from Retsinformation using ELI XML endpoints.

Strategy:
  - Discovery: Iterate through years and document numbers via ELI URIs
  - Full text: Download XML from /eli/lta/{year}/{number}/xml
  - The XML contains structured content with full legal text

Document types covered:
  - LOV: Laws (Lov)
  - LBK: Consolidated laws (Lovbekendtgørelse)
  - BEK: Executive orders (Bekendtgørelse)
  - CIR: Circulars (Cirkulære)
  - VEJ: Guidelines (Vejledning)

API endpoints:
  - XML: https://www.retsinformation.dk/eli/lta/{year}/{number}/xml
  - Harvest API: https://api.retsinformation.dk/v1/Documents (03:00-23:45 CET)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("legal-data-hunter.DK.Lovdata")

# API endpoints
RETSINFORMATION_BASE = "https://www.retsinformation.dk"
HARVEST_API_BASE = "https://api.retsinformation.dk"

# Years to scan (legislation from ~1990 to present)
START_YEAR = 2020  # For initial bootstrap, start from recent years
END_YEAR = datetime.now().year


class RetsinformationScraper(BaseScraper):
    """
    Scraper for DK/Lovdata -- Danish Legislation (Retsinformation).
    Country: DK
    URL: https://www.retsinformation.dk

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.xml_client = HttpClient(
            base_url=RETSINFORMATION_BASE,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

        self.api_client = HttpClient(
            base_url=HARVEST_API_BASE,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _fetch_xml_document(self, year: int, number: int) -> Optional[dict]:
        """
        Fetch a single document via ELI XML endpoint.

        Returns parsed document dict or None if document doesn't exist.
        """
        url = f"/eli/lta/{year}/{number}/xml"

        try:
            self.rate_limiter.wait()
            resp = self.xml_client.get(url)

            if resp.status_code == 404:
                return None

            resp.raise_for_status()

            # Parse XML
            root = ET.fromstring(resp.content)

            return {
                "_raw_xml": resp.content,
                "_root": root,
                "_year": year,
                "_number": number,
                "_eli_uri": f"/eli/lta/{year}/{number}",
            }

        except ET.ParseError as e:
            logger.warning(f"XML parse error for {url}: {e}")
            return None
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                return None
            logger.warning(f"Error fetching {url}: {e}")
            return None

    def _extract_text_from_xml(self, root: ET.Element) -> str:
        """
        Extract full text content from Retsinformation XML structure.

        The XML uses elements like:
          - <Char> for text content
          - <Linea> for lines
          - <Paragraf>, <Stk>, <Kapitel> for structure
        """
        text_parts = []

        # Extract title
        for titel in root.iter("Titel"):
            title_text = "".join(titel.itertext()).strip()
            if title_text:
                text_parts.append(title_text)
                text_parts.append("")  # Empty line after title

        # Extract all <Char> elements which contain the actual text
        for char_elem in root.iter("Char"):
            text = char_elem.text or ""
            text = text.strip()
            if text:
                text_parts.append(text)

        # If no <Char> elements, try extracting from other text containers
        if not text_parts:
            for elem in root.iter():
                if elem.tag in ["Exitus", "Linea", "text", "absatz"]:
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(text)

        # Join and clean up
        full_text = "\n".join(text_parts)

        # Clean up HTML entities
        full_text = html.unescape(full_text)

        # Normalize whitespace within lines (but keep paragraph breaks)
        lines = []
        for line in full_text.split("\n"):
            cleaned = re.sub(r"\s+", " ", line).strip()
            lines.append(cleaned)

        full_text = "\n".join(lines)

        # Remove excessive blank lines
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)

        return full_text.strip()

    def _parse_meta(self, root: ET.Element) -> dict:
        """Extract metadata from XML <Meta> section."""
        meta = {}
        meta_elem = root.find(".//Meta")

        if meta_elem is None:
            return meta

        # Direct child elements
        field_map = {
            "DocumentType": "document_type_raw",
            "AccessionNumber": "accession_number",
            "DocumentId": "document_id",
            "UniqueDocumentId": "unique_document_id",
            "DocumentTitle": "title",
            "Year": "year",
            "Number": "number",
            "DiesSigni": "signature_date",  # Date signed
            "DiesEdicti": "publication_date",  # Date published in gazette
            "Status": "status",
            "AnnouncedIn": "announced_in",
            "Ministry": "ministry",
            "AdministrativeAuthority": "administrative_authority",
            "JournalNumber": "journal_number",
            "Rank": "rank",
        }

        for xml_field, dict_field in field_map.items():
            elem = meta_elem.find(xml_field)
            if elem is not None and elem.text:
                meta[dict_field] = elem.text.strip()

        # Extract subjects (can be multiple)
        subjects = []
        for subj in meta_elem.findall("Subject"):
            if subj.text:
                subjects.append(subj.text.strip())
        if subjects:
            meta["subjects"] = subjects

        # Extract signatures
        signatures = []
        for sig in meta_elem.findall(".//Signature"):
            if sig.text:
                signatures.append(sig.text.strip())
        if signatures:
            meta["signatures"] = signatures

        return meta

    def _get_document_type(self, raw_type: str) -> str:
        """Normalize document type from raw XML format."""
        if not raw_type:
            return "unknown"

        # Extract main type code from format like "BEK H#LOKDOK04"
        raw_type = raw_type.upper()

        if raw_type.startswith("LOV"):
            return "LOV"  # Law
        elif raw_type.startswith("LBK"):
            return "LBK"  # Consolidated law
        elif raw_type.startswith("BEK"):
            return "BEK"  # Executive order
        elif raw_type.startswith("CIR"):
            return "CIR"  # Circular
        elif raw_type.startswith("VEJ"):
            return "VEJ"  # Guideline
        elif raw_type.startswith("SKR"):
            return "SKR"  # Written statement
        else:
            return raw_type.split()[0] if raw_type else "unknown"

    def _scan_year_for_documents(self, year: int, max_consecutive_404s: int = 50) -> Generator[dict, None, None]:
        """
        Scan a year for all documents by iterating through document numbers.

        Stops when encountering too many consecutive 404s (indicating end of range).
        """
        number = 1
        consecutive_404s = 0

        while consecutive_404s < max_consecutive_404s:
            doc = self._fetch_xml_document(year, number)

            if doc is None:
                consecutive_404s += 1
            else:
                consecutive_404s = 0
                yield doc

            number += 1

            # Safety limit per year
            if number > 3000:
                logger.info(f"Reached number limit for year {year}")
                break

        logger.info(f"Finished scanning year {year} (last number: {number - 1})")

    def _fetch_recent_changes(self, date: str) -> Generator[dict, None, None]:
        """
        Fetch documents changed on a specific date via harvest API.

        API is only available 03:00-23:45 CET.
        """
        try:
            self.rate_limiter.wait()
            resp = self.api_client.get(f"/v1/Documents", params={"date": date})

            if resp.status_code == 400:
                # API returns 400 for out-of-hours or invalid dates
                error_data = resp.json() if resp.content else {}
                logger.warning(f"Harvest API error: {error_data}")
                return

            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list):
                data = [data] if data else []

            for doc_ref in data:
                # Extract year and number from ELI URI
                eli_uri = doc_ref.get("href", "")
                # Format: /eli/accn/XYYYYYNNNNNN or similar
                accession = doc_ref.get("accessionsnummer", "")

                # Try to get year/number from various formats
                if accession:
                    # Accession format: B20240000105 -> Year 2024, number from last digits
                    match = re.search(r"(\d{4})(\d+)$", accession)
                    if match:
                        year = int(match.group(1))
                        # Fetch the document using accession-based approach
                        # Try common number patterns
                        for number in range(1, 100):
                            doc = self._fetch_xml_document(year, number)
                            if doc:
                                root = doc["_root"]
                                meta_accn = root.find(".//AccessionNumber")
                                if meta_accn is not None and meta_accn.text == accession:
                                    yield doc
                                    break

        except Exception as e:
            logger.warning(f"Harvest API error for {date}: {e}")

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from Retsinformation.

        Scans years from END_YEAR down to START_YEAR, iterating through
        document numbers for each year.
        """
        for year in range(END_YEAR, START_YEAR - 1, -1):
            logger.info(f"Scanning year {year}...")

            for doc in self._scan_year_for_documents(year):
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified since the given date.

        Uses the harvest API if available (03:00-23:45 CET), otherwise
        falls back to scanning recent documents.
        """
        # Try harvest API for each day since 'since'
        current = since.date()
        today = datetime.now(timezone.utc).date()

        while current <= today:
            date_str = current.strftime("%Y-%m-%d")
            logger.info(f"Fetching updates for {date_str}")

            for doc in self._fetch_recent_changes(date_str):
                yield doc

            current += timedelta(days=1)

        # Also scan the current year for any missed documents
        logger.info(f"Scanning current year {END_YEAR} for updates...")
        for doc in self._scan_year_for_documents(END_YEAR, max_consecutive_404s=20):
            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw XML document into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from XML content.
        """
        root = raw["_root"]
        year = raw["_year"]
        number = raw["_number"]
        eli_uri = raw["_eli_uri"]

        # Parse metadata
        meta = self._parse_meta(root)

        # Extract full text
        full_text = self._extract_text_from_xml(root)

        # Get document type
        doc_type_raw = meta.get("document_type_raw", "")
        doc_type = self._get_document_type(doc_type_raw)

        # Get title
        title = meta.get("title", "")
        if not title:
            # Try extracting from TitelGruppe
            titel_elem = root.find(".//Titel")
            if titel_elem is not None:
                title = "".join(titel_elem.itertext()).strip()

        # Get date (prefer signature date, fall back to publication)
        date = meta.get("signature_date") or meta.get("publication_date") or ""

        # Build accession number if not in meta
        accession = meta.get("accession_number", "")
        if not accession:
            accession = f"{doc_type}{year}{number:06d}"

        # Build canonical URL
        url = f"{RETSINFORMATION_BASE}{eli_uri}"

        return {
            # Required base fields
            "_id": accession or f"DK-{year}-{number}",
            "_source": "DK/Lovdata",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "accession_number": accession,
            "year": int(meta.get("year", year)),
            "number": int(meta.get("number", number)),
            "document_type": doc_type,
            "document_type_raw": doc_type_raw,
            "eli_uri": eli_uri,
            "ministry": meta.get("ministry", ""),
            "administrative_authority": meta.get("administrative_authority", ""),
            "status": meta.get("status", ""),
            "publication_date": meta.get("publication_date", ""),
            "signature_date": meta.get("signature_date", ""),
            "announced_in": meta.get("announced_in", ""),
            "subjects": meta.get("subjects", []),
            "signatures": meta.get("signatures", []),
            "journal_number": meta.get("journal_number", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Retsinformation endpoints...")

        # Test XML endpoint with a known document
        print("\n1. Testing ELI XML endpoint...")
        doc = self._fetch_xml_document(2024, 1)
        if doc:
            meta = self._parse_meta(doc["_root"])
            print(f"   Document found: {meta.get('title', 'Unknown')[:60]}...")
            text = self._extract_text_from_xml(doc["_root"])
            print(f"   Text length: {len(text)} characters")
            print(f"   First 200 chars: {text[:200]}...")
        else:
            print("   ERROR: Could not fetch document 2024/1")

        # Test a few more documents
        print("\n2. Scanning for documents in 2024...")
        count = 0
        for doc in self._scan_year_for_documents(2024, max_consecutive_404s=10):
            count += 1
            if count >= 5:
                break
        print(f"   Found {count} documents in quick scan")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = RetsinformationScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15  # Fetch more samples for better validation
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
