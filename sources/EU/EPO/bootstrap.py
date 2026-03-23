#!/usr/bin/env python3
"""
EU/EPO -- European Patent Office Boards of Appeal Decisions

Fetches decisions from the EPO Bulk Data Download Service (BDDS).

Strategy:
  - Download bulk ZIP from BDDS API (product ID 21)
  - Stream-parse the large XML file (~1.1GB, ~51,000 decisions)
  - Extract full text from structured elements

Data:
  - All Boards of Appeal decisions from 1979 to present
  - Full text including headnotes, catchwords, summary of facts, reasons, and orders
  - License: Open Data (free public access since Jan 2025)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Check for new deliveries and update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import os
import re
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from xml.etree import ElementTree as ET

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.EPO")

# BDDS API endpoints
BDDS_API_BASE = "https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api"
PRODUCT_ID = 21  # EPO Boards of Appeal decisions


class EPOBoardsOfAppealScraper(BaseScraper):
    """
    Scraper for EU/EPO -- European Patent Office Boards of Appeal.
    Country: EU
    URL: https://www.epo.org/en/law-practice/case-law-appeals

    Data types: case_law
    Auth: none (Open Data since Jan 2025)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BDDS_API_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=120,
        )

        self.data_dir = source_dir / "data"
        self.data_dir.mkdir(exist_ok=True)

    def _get_product_info(self) -> Dict[str, Any]:
        """Get product information and available deliveries from BDDS API."""
        url = f"/products/{PRODUCT_ID}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to get product info: {e}")
            raise

    def _get_latest_delivery(self) -> Optional[Dict[str, Any]]:
        """Get the most recent delivery (ZIP file) info."""
        product_info = self._get_product_info()
        deliveries = product_info.get("deliveries", [])

        # Filter out schema/DTD deliveries, keep only actual data deliveries
        data_deliveries = [
            d for d in deliveries
            if isinstance(d, dict)
            and "Schema" not in d.get("deliveryName", "")
            and "DTD" not in d.get("deliveryName", "")
        ]

        if not data_deliveries:
            return None

        # Sort by publication date, most recent first
        data_deliveries.sort(
            key=lambda x: x.get("deliveryPublicationDatetime", ""),
            reverse=True
        )

        return data_deliveries[0]

    def _download_delivery(self, delivery: Dict[str, Any]) -> Path:
        """Download the ZIP file for a delivery."""
        files = delivery.get("files", [])
        if not files:
            raise ValueError("No files in delivery")

        file_info = files[0]
        file_id = file_info.get("fileId")
        delivery_id = delivery.get("deliveryId")
        file_name = file_info.get("fileName", "decisions.zip")

        # Check if we already have this file
        local_path = self.data_dir / file_name
        if local_path.exists():
            logger.info(f"Using cached file: {local_path}")
            return local_path

        download_url = f"{BDDS_API_BASE}/products/{PRODUCT_ID}/delivery/{delivery_id}/file/{file_id}/download"

        logger.info(f"Downloading {file_name} ({file_info.get('fileSize', 'unknown')})...")

        # Stream download to avoid memory issues
        resp = requests.get(download_url, stream=True, timeout=600)
        resp.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded to {local_path}")
        return local_path

    def _extract_xml_path(self, zip_path: Path) -> Path:
        """Extract XML file from ZIP if not already extracted."""
        # Check for existing extracted XML
        for xml_file in self.data_dir.glob("*.xml"):
            if "Decisions" in xml_file.name or "decisions" in xml_file.name.lower():
                logger.info(f"Using existing XML file: {xml_file}")
                return xml_file

        # Extract from ZIP
        with zipfile.ZipFile(zip_path, 'r') as zf:
            xml_files = [n for n in zf.namelist() if n.endswith('.xml')]
            if not xml_files:
                raise ValueError("No XML file found in ZIP")

            xml_name = xml_files[0]
            logger.info(f"Extracting {xml_name}...")
            zf.extract(xml_name, self.data_dir)
            return self.data_dir / xml_name

    def _extract_text_content(self, element: ET.Element, include_children: bool = True) -> str:
        """Extract text content from an XML element, handling nested elements."""
        if element is None:
            return ""

        texts = []

        # Handle text directly in element
        if element.text:
            texts.append(element.text.strip())

        if include_children:
            # Process child elements
            for child in element:
                # Handle headings
                if child.tag == "heading":
                    heading_text = self._extract_text_content(child, include_children=True)
                    if heading_text:
                        texts.append(f"\n## {heading_text}\n")
                # Handle paragraphs
                elif child.tag == "p":
                    p_text = self._extract_text_content(child, include_children=True)
                    if p_text:
                        texts.append(p_text)
                # Handle inline formatting (b, i, u, sup, sub)
                elif child.tag in ("b", "i", "u", "sup", "sub", "smallcaps"):
                    if child.text:
                        texts.append(child.text.strip())
                    # Also get tail text
                    for subchild in child:
                        texts.append(self._extract_text_content(subchild, True))
                else:
                    # Recursively extract from other elements
                    texts.append(self._extract_text_content(child, include_children=True))

                # Don't forget tail text
                if child.tail:
                    texts.append(child.tail.strip())

        return " ".join(filter(None, texts)).strip()

    def _parse_decision(self, decision_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse a single ep-appeal-decision element into a raw document."""
        try:
            # Get basic attributes
            lang = decision_elem.get("lang", "")
            procedure_lang = decision_elem.get("procedure-lang", "")
            appeal_type = decision_elem.get("appeal-type", "")

            # Get bibliographic data
            bib_data = decision_elem.find("ep-appeal-bib-data")
            if bib_data is None:
                return None

            # Case number
            case_num_elem = bib_data.find("ep-case-num")
            case_number = ""
            year = ""
            if case_num_elem is not None:
                appeal_num = case_num_elem.findtext("ep-appeal-num", "")
                year = case_num_elem.findtext("ep-year", "")
                code = case_num_elem.get("code", "")
                case_number = f"{code}{appeal_num}/{year}" if code else f"{appeal_num}/{year}"

            # ECLI
            ecli = bib_data.findtext("ep-ecli", "")

            # Decision date
            date_elem = bib_data.find("ep-date-of-decision/date")
            decision_date = ""
            if date_elem is not None and date_elem.text:
                raw_date = date_elem.text.strip()
                # Convert YYYYMMDD to YYYY-MM-DD
                if len(raw_date) == 8:
                    decision_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

            # Board code
            board_code = bib_data.findtext("ep-board-of-appeal-code", "")

            # Reference number (for URL construction)
            reference = bib_data.get("reference", "")

            # Invention title (if present)
            invention_title = bib_data.findtext("invention-title", "")

            # Headword
            headword = decision_elem.findtext("ep-headword", "")

            # Keywords
            keywords_elem = decision_elem.find("ep-keywords")
            keywords = []
            if keywords_elem is not None:
                for kw in keywords_elem.findall("keyword"):
                    if kw.text:
                        keywords.append(kw.text.strip())

            # === FULL TEXT SECTIONS ===
            text_parts = []

            # Headnote
            headnote_elem = decision_elem.find("ep-headnote")
            if headnote_elem is not None:
                headnote_text = self._extract_text_content(headnote_elem)
                if headnote_text:
                    text_parts.append(f"HEADNOTE:\n{headnote_text}")

            # Catchword
            catchword_elem = decision_elem.find("ep-catchword")
            if catchword_elem is not None:
                catchword_text = self._extract_text_content(catchword_elem)
                if catchword_text:
                    text_parts.append(f"CATCHWORD:\n{catchword_text}")

            # Summary of Facts
            facts_elem = decision_elem.find("ep-summary-of-facts")
            if facts_elem is not None:
                facts_text = self._extract_text_content(facts_elem)
                if facts_text:
                    text_parts.append(f"SUMMARY OF FACTS:\n{facts_text}")

            # Reasons for Decision
            reasons_elem = decision_elem.find("ep-reasons-for-decision")
            if reasons_elem is not None:
                reasons_text = self._extract_text_content(reasons_elem)
                if reasons_text:
                    text_parts.append(f"REASONS FOR DECISION:\n{reasons_text}")

            # Order
            order_elem = decision_elem.find("ep-appeal-order")
            if order_elem is not None:
                order_text = self._extract_text_content(order_elem)
                if order_text:
                    text_parts.append(f"ORDER:\n{order_text}")

            full_text = "\n\n".join(text_parts)

            # Skip if no meaningful text content
            if not full_text or len(full_text) < 100:
                return None

            # Build title
            title = ""
            if case_number:
                title = f"Decision {case_number}"
                if headword:
                    title += f" - {headword[:100]}"
            elif headword:
                title = headword[:150]
            else:
                title = f"EPO Board of Appeal Decision"

            return {
                "ecli": ecli,
                "case_number": case_number,
                "decision_date": decision_date,
                "year": year,
                "board_code": board_code,
                "reference": reference,
                "appeal_type": appeal_type,
                "language": lang or procedure_lang,
                "invention_title": invention_title,
                "headword": headword,
                "keywords": keywords,
                "title": title,
                "full_text": full_text,
            }

        except Exception as e:
            logger.warning(f"Error parsing decision: {e}")
            return None

    def _stream_decisions(self, xml_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Stream parse the large XML file, yielding one decision at a time."""
        logger.info(f"Streaming decisions from {xml_path}...")

        count = 0
        context = ET.iterparse(xml_path, events=("end",))

        for event, elem in context:
            if elem.tag == "ep-appeal-decision":
                raw_doc = self._parse_decision(elem)
                if raw_doc:
                    count += 1
                    if count % 1000 == 0:
                        logger.info(f"Processed {count} decisions...")
                    yield raw_doc

                # Clear the element to free memory
                elem.clear()

        logger.info(f"Total decisions parsed: {count}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions from the EPO Boards of Appeal database.

        Downloads the bulk file if needed, then streams the XML.
        """
        # Get latest delivery info
        delivery = self._get_latest_delivery()
        if not delivery:
            raise RuntimeError("No delivery found for EPO BoA decisions")

        logger.info(f"Latest delivery: {delivery.get('deliveryName')}")

        # Download if needed
        zip_path = self._download_delivery(delivery)

        # Extract XML if needed
        xml_path = self._extract_xml_path(zip_path)

        # Stream decisions
        yield from self._stream_decisions(xml_path)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Check for new deliveries since last run.

        The EPO updates twice yearly (March, September).
        If there's a new delivery, re-download and yield all new decisions.
        """
        # Get latest delivery info
        delivery = self._get_latest_delivery()
        if not delivery:
            return

        # Check delivery date
        pub_date_str = delivery.get("deliveryPublicationDatetime", "")
        if pub_date_str:
            try:
                # Parse ISO format datetime
                pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
                if pub_date.replace(tzinfo=timezone.utc) <= since:
                    logger.info(f"No new deliveries since {since}")
                    return
            except:
                pass

        logger.info(f"New delivery available: {delivery.get('deliveryName')}")

        # Download the new delivery
        zip_path = self._download_delivery(delivery)

        # Extract and stream
        xml_path = self._extract_xml_path(zip_path)
        yield from self._stream_decisions(xml_path)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        ecli = raw.get("ecli", "")
        case_number = raw.get("case_number", "")

        # Build unique ID (prefer ECLI, fall back to case number)
        doc_id = ecli if ecli else f"EPO-BoA-{case_number}"

        # Build URL for decision lookup
        url = "https://www.epo.org/en/law-practice/case-law-appeals"
        if case_number:
            # EPO has a case lookup, but requires interactive search
            url = f"https://www.epo.org/en/boards-of-appeal/decisions/recent/{case_number.replace('/', '_')}"

        full_text = raw.get("full_text", "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "EU/EPO",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get("decision_date", ""),
            "url": url,
            # Additional metadata
            "ecli": ecli,
            "case_number": case_number,
            "board_code": raw.get("board_code", ""),
            "appeal_type": raw.get("appeal_type", ""),
            "language": raw.get("language", ""),
            "year": raw.get("year", ""),
            "invention_title": raw.get("invention_title", ""),
            "headword": raw.get("headword", ""),
            "keywords": raw.get("keywords", []),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing EPO BDDS API endpoints...")

        # Test API
        print("\n1. Testing BDDS API...")
        try:
            product_info = self._get_product_info()
            print(f"   Product: {product_info.get('name')}")
            deliveries = product_info.get('deliveries', [])
            print(f"   Available deliveries: {len(deliveries)}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test latest delivery
        print("\n2. Getting latest delivery...")
        try:
            delivery = self._get_latest_delivery()
            if delivery:
                print(f"   Name: {delivery.get('deliveryName')}")
                print(f"   Published: {delivery.get('deliveryPublicationDatetime')}")
                files = delivery.get('files', [])
                if files:
                    print(f"   File: {files[0].get('fileName')} ({files[0].get('fileSize')})")
            else:
                print("   No delivery found!")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test XML parsing (if file exists)
        print("\n3. Testing XML parsing...")
        try:
            xml_files = list(self.data_dir.glob("*Decisions*.xml"))
            if xml_files:
                xml_path = xml_files[0]
                print(f"   Found XML: {xml_path.name}")

                # Parse first 5 decisions
                count = 0
                for raw_doc in self._stream_decisions(xml_path):
                    count += 1
                    if count == 1:
                        print(f"   Sample decision:")
                        print(f"     ECLI: {raw_doc.get('ecli', 'N/A')}")
                        print(f"     Case: {raw_doc.get('case_number', 'N/A')}")
                        print(f"     Date: {raw_doc.get('decision_date', 'N/A')}")
                        print(f"     Lang: {raw_doc.get('language', 'N/A')}")
                        print(f"     Text length: {len(raw_doc.get('full_text', ''))} chars")
                        text_preview = raw_doc.get('full_text', '')[:300].replace('\n', ' ')
                        print(f"     Preview: {text_preview}...")
                    if count >= 5:
                        break
                print(f"   Parsed {count} sample decisions successfully")
            else:
                print("   No XML file found in data/ - run 'bootstrap --sample' first")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = EPOBoardsOfAppealScraper()

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
