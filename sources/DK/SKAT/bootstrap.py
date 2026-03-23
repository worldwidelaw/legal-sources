#!/usr/bin/env python3
"""
DK/SKAT -- Danish Tax Authority Rulings (Skatterådet/Skattestyrelsen)

Fetches Danish tax rulings and binding answers (bindende svar) from Retsinformation.

Strategy:
  - Scan ELI URIs /eli/retsinfo/{year}/{number}/dan/xml
  - Filter for documents from Skatteministeriet
  - Include AFG (Afgørelser) document types - these are tax rulings

Document types covered:
  - AFG: Afgørelser (Decisions/Rulings from Skatterådet)
  - Includes: Binding rulings (bindende svar), tax decisions

API endpoint:
  - XML: https://www.retsinformation.dk/eli/retsinfo/{year}/{number}/dan/xml

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
logger = logging.getLogger("legal-data-hunter.DK.SKAT")

# API endpoint
RETSINFORMATION_BASE = "https://www.retsinformation.dk"

# Scan range - AFG documents for Skatteministeriet typically in this range
# The number range varies by year, so we scan broadly
NUMBER_START = 9000
NUMBER_END = 11500

# Years to scan
START_YEAR = 2020
END_YEAR = datetime.now().year


class SKATScraper(BaseScraper):
    """
    Scraper for DK/SKAT -- Danish Tax Authority Rulings.
    Country: DK
    URL: https://www.retsinformation.dk

    Data types: doctrine (tax rulings, binding answers)
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

    def _fetch_xml_document(self, year: int, number: int) -> Optional[dict]:
        """
        Fetch a single document via ELI retsinfo endpoint.

        Returns parsed document dict or None if document doesn't exist or
        is not from Skatteministeriet.
        """
        url = f"/eli/retsinfo/{year}/{number}/dan/xml"

        try:
            self.rate_limiter.wait()
            resp = self.xml_client.get(url)

            if resp.status_code == 404:
                return None

            resp.raise_for_status()

            # Parse XML
            root = ET.fromstring(resp.content)

            # Check if this is from Skatteministeriet
            ministry_elem = root.find(".//Ministry")
            ministry = ministry_elem.text if ministry_elem is not None else ""

            if ministry != "Skatteministeriet":
                return None

            # Check document type - we want AFG (Afgørelser)
            doc_type_elem = root.find(".//DocumentType")
            doc_type = doc_type_elem.text if doc_type_elem is not None else ""

            if not doc_type.upper().startswith("AFG"):
                return None

            return {
                "_raw_xml": resp.content,
                "_root": root,
                "_year": year,
                "_number": number,
                "_eli_uri": f"/eli/retsinfo/{year}/{number}",
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
          - <Resume>, <TekstGruppe> for content sections
        """
        text_parts = []

        # Extract title first
        for titel in root.iter("Titel"):
            title_text = "".join(titel.itertext()).strip()
            if title_text:
                text_parts.append(title_text)
                text_parts.append("")

        # Extract Resume (summary) section
        for resume in root.iter("Resume"):
            resume_text = "".join(resume.itertext()).strip()
            if resume_text:
                text_parts.append("RESUME:")
                text_parts.append(resume_text)
                text_parts.append("")

        # Extract main content from TekstGruppe
        for tekst in root.iter("TekstGruppe"):
            for elem in tekst.iter():
                if elem.tag == "Rubrica":
                    # Section header
                    header = "".join(elem.itertext()).strip()
                    if header:
                        text_parts.append("")
                        text_parts.append(header.upper())
                elif elem.tag == "Char":
                    text = elem.text or ""
                    text = text.strip()
                    if text:
                        text_parts.append(text)

        # If limited content found, also try direct Char extraction
        if len([p for p in text_parts if p]) < 10:
            for char_elem in root.iter("Char"):
                text = char_elem.text or ""
                text = text.strip()
                if text and text not in text_parts:
                    text_parts.append(text)

        # Join and clean up
        full_text = "\n".join(text_parts)

        # Clean up HTML entities
        full_text = html.unescape(full_text)

        # Normalize whitespace within lines
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
            "DiesSigni": "signature_date",
            "DiesEdicti": "publication_date",
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

        # Extract references to laws
        refs = []
        for ref in meta_elem.findall(".//Ref_Text"):
            if ref.text:
                refs.append(ref.text.strip())
        if refs:
            meta["references"] = refs

        return meta

    def _scan_year_for_documents(
        self, year: int, number_start: int = NUMBER_START, number_end: int = NUMBER_END
    ) -> Generator[dict, None, None]:
        """
        Scan a year for tax ruling documents.

        Iterates through number range looking for Skatteministeriet AFG documents.
        """
        found_count = 0
        consecutive_misses = 0

        for number in range(number_start, number_end + 1):
            doc = self._fetch_xml_document(year, number)

            if doc is not None:
                found_count += 1
                consecutive_misses = 0
                yield doc
            else:
                consecutive_misses += 1

            # If we've found documents but hit a long streak of misses, we may be past the range
            # However, AFG documents are scattered, so we need to keep scanning
            if found_count > 0 and consecutive_misses > 500:
                logger.info(f"Stopping year {year} scan after {consecutive_misses} consecutive misses")
                break

        logger.info(f"Year {year}: found {found_count} tax rulings")

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all tax rulings from Retsinformation.

        Scans years from END_YEAR down to START_YEAR.
        """
        for year in range(END_YEAR, START_YEAR - 1, -1):
            logger.info(f"Scanning year {year} for tax rulings...")

            for doc in self._scan_year_for_documents(year):
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from the current year.

        Since we don't have a reliable update API, re-scan current year.
        """
        logger.info(f"Scanning current year {END_YEAR} for updates...")
        for doc in self._scan_year_for_documents(END_YEAR):
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

        # Get title
        title = meta.get("title", "")
        if not title:
            titel_elem = root.find(".//Titel")
            if titel_elem is not None:
                title = "".join(titel_elem.itertext()).strip()

        # Get date (prefer signature date, fall back to publication)
        date = meta.get("signature_date") or meta.get("publication_date") or ""

        # Build ID
        accession = meta.get("accession_number", "")
        doc_id = accession or f"DK-SKAT-{year}-{number}"

        # Build canonical URL
        url = f"{RETSINFORMATION_BASE}{eli_uri}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "DK/SKAT",
            "_type": "doctrine",  # tax doctrine/rulings - not legislation or case_law
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "accession_number": accession,
            "document_id": meta.get("document_id", ""),
            "unique_document_id": meta.get("unique_document_id", ""),
            "year": int(meta.get("year", year)),
            "number": int(meta.get("number", number)),
            "document_type": "AFG",
            "document_type_raw": meta.get("document_type_raw", ""),
            "eli_uri": eli_uri,
            "ministry": meta.get("ministry", "Skatteministeriet"),
            "administrative_authority": meta.get("administrative_authority", ""),
            "status": meta.get("status", ""),
            "publication_date": meta.get("publication_date", ""),
            "signature_date": meta.get("signature_date", ""),
            "journal_number": meta.get("journal_number", ""),
            "references": meta.get("references", []),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Retsinformation endpoints for tax rulings...")

        # Test known tax ruling
        print("\n1. Testing ELI retsinfo endpoint...")
        doc = self._fetch_xml_document(2024, 9335)
        if doc:
            meta = self._parse_meta(doc["_root"])
            print(f"   Document found: {meta.get('title', 'Unknown')[:60]}...")
            print(f"   Ministry: {meta.get('ministry', 'Unknown')}")
            print(f"   Authority: {meta.get('administrative_authority', 'Unknown')}")
            text = self._extract_text_from_xml(doc["_root"])
            print(f"   Text length: {len(text)} characters")
            print(f"   First 300 chars: {text[:300]}...")
        else:
            print("   ERROR: Could not fetch known tax ruling 2024/9335")

        # Quick scan
        print("\n2. Quick scan for tax rulings in 2024...")
        count = 0
        for num in range(9250, 9500, 25):
            doc = self._fetch_xml_document(2024, num)
            if doc:
                count += 1
                if count <= 3:
                    meta = self._parse_meta(doc["_root"])
                    print(f"   Found: {num} - {meta.get('title', 'Unknown')[:50]}...")

        print(f"   Found {count} tax rulings in sample range")
        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = SKATScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
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
