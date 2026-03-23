#!/usr/bin/env python3
"""
FI/Eduskunta -- Finnish Parliament Open Data Fetcher

Fetches parliamentary documents from the Eduskunta Open Data API.

Strategy:
  - Bootstrap: Fetches government proposals (HE) and committee reports
    from the VaskiData table using batch pagination.
  - Update: Uses Created/Imported timestamps to fetch recent documents.
  - Sample: Fetches 12+ records with full text for validation.

API: https://avoindata.eduskunta.fi/api/v1
Docs: https://avoindata.eduskunta.fi/swagger/apidocs.html

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
logger = logging.getLogger("legal-data-hunter.FI.Eduskunta")

# Eduskunta Open Data API
API_BASE = "https://avoindata.eduskunta.fi/api/v1"

# Document type keywords to search for in VaskiData
# HE = Hallituksen esitys (Government Proposal)
DOCUMENT_TYPES = [
    "HE",      # Government proposals
    "LaVM",    # Law committee report
    "MmVM",    # Agriculture committee report
    "PeVM",    # Constitutional committee report
    "StVM",    # Social affairs committee report
    "TaVM",    # Finance committee report
    "TyVM",    # Employment committee report
    "VaVM",    # Finance committee report
    "YmVM",    # Environment committee report
]


class EduskuntaScraper(BaseScraper):
    """
    Scraper for FI/Eduskunta -- Finnish Parliament Open Data.
    Country: FI
    URL: https://avoindata.eduskunta.fi

    Data types: legislation (government proposals, committee reports)
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _get_tables(self) -> list:
        """Get list of available tables."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/tables/")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching tables: {e}")
            return []

    def _get_table_counts(self) -> list:
        """Get row counts for all tables."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/tables/counts")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching table counts: {e}")
            return []

    def _get_rows(
        self,
        table_name: str,
        page: int = 0,
        per_page: int = 10,
        column_name: Optional[str] = None,
        column_value: Optional[str] = None,
    ) -> dict:
        """
        Fetch rows from a table with optional filtering.

        Returns dict with columnNames, rowData, hasMore, etc.
        """
        self.rate_limiter.wait()

        params = {
            "page": str(page),
            "perPage": str(per_page),
        }

        if column_name and column_value:
            params["columnName"] = column_name
            params["columnValue"] = column_value

        try:
            resp = self.client.get(f"/tables/{table_name}/rows", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching {table_name} page {page}: {e}")
            return {"rowData": [], "hasMore": False}

    def _get_single_row(self, table_name: str, row_id: str) -> dict:
        """Fetch a single row by ID."""
        self.rate_limiter.wait()

        try:
            resp = self.client.get(f"/tables/{table_name}/rows/{row_id}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error fetching {table_name}/{row_id}: {e}")
            return {"rowData": []}

    def _paginate_vaski_data(
        self,
        doc_type_filter: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through VaskiData documents.

        Args:
            doc_type_filter: Filter by document type prefix (e.g., "HE" for gov proposals)
            max_pages: Maximum pages to fetch (None for all)

        Yields raw row data dicts with columns and values.
        """
        page = 0
        per_page = 10  # API seems to limit response size

        while True:
            if max_pages and page >= max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping")
                return

            # Use column filter if doc type specified
            if doc_type_filter:
                data = self._get_rows(
                    "VaskiData",
                    page=page,
                    per_page=per_page,
                    column_name="Eduskuntatunnus",
                    column_value=f"%{doc_type_filter}%",
                )
            else:
                data = self._get_rows("VaskiData", page=page, per_page=per_page)

            rows = data.get("rowData", [])
            columns = data.get("columnNames", [])
            has_more = data.get("hasMore", False)

            if not rows:
                logger.info(f"No more rows at page {page}")
                return

            logger.info(f"VaskiData page {page}: {len(rows)} documents")

            for row in rows:
                # Convert row array to dict
                row_dict = dict(zip(columns, row))
                yield row_dict

            if not has_more:
                logger.info(f"Finished VaskiData at page {page}")
                return

            page += 1

    # -- XML Parsing --------------------------------------------------------

    def _extract_text_from_vaski_xml(self, xml_content: str) -> tuple:
        """
        Extract text and metadata from Eduskunta VaskiData XML.

        Returns (title, text, metadata_dict)
        """
        if not xml_content:
            return "", "", {}

        metadata = {}
        text_parts = []
        title = ""

        try:
            # Clean up XML for parsing
            xml_content = xml_content.strip()
            root = ET.fromstring(xml_content.encode('utf-8'))
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            # Fall back to regex extraction
            return self._extract_text_with_regex(xml_content)

        # Find all text content elements
        # The XML uses Finnish government/parliament schemas
        text_patterns = [
            ".//{http://www.vn.fi/skeemat/sisaltokooste/2010/04/27}KappaleKooste",
            ".//{http://www.vn.fi/skeemat/metatietoelementit/2010/04/27}NimekeTeksti",
            ".//{http://www.vn.fi/skeemat/sisaltoelementit/2010/04/27}ValiotsikkoTeksti",
            ".//KappaleKooste",
            ".//NimekeTeksti",
            ".//ValiotsikkoTeksti",
        ]

        for pattern in text_patterns:
            for elem in root.findall(pattern):
                text = "".join(elem.itertext()).strip()
                if text:
                    # NimekeTeksti is the title
                    if "NimekeTeksti" in pattern or "NimekeTeksti" in (elem.tag or ""):
                        if not title:
                            title = text
                    else:
                        text_parts.append(text)

        # Extract metadata
        # Document type
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

            # Get attributes with metadata
            for attr_name, attr_val in elem.attrib.items():
                if "asiakirjatyyppiNimi" in attr_name:
                    metadata["doc_type_name"] = attr_val
                elif "eduskuntaTunnus" in attr_name:
                    metadata["eduskunta_id"] = attr_val
                elif "kieliKoodi" in attr_name:
                    metadata["language"] = attr_val
                elif "laadintaPvm" in attr_name:
                    metadata["date_drafted"] = attr_val
                elif "identifiointiTunnus" in attr_name:
                    metadata["uuid"] = attr_val

        full_text = "\n\n".join(text_parts)

        # Clean up text
        full_text = html.unescape(full_text)
        full_text = re.sub(r"<[^>]+>", "", full_text)  # Remove any remaining tags
        full_text = re.sub(r"\s+", " ", full_text)
        full_text = full_text.strip()

        title = html.unescape(title)
        title = re.sub(r"\s+", " ", title).strip()

        return title, full_text, metadata

    def _extract_text_with_regex(self, xml_content: str) -> tuple:
        """
        Fallback regex extraction for malformed XML.

        Returns (title, text, metadata_dict)
        """
        metadata = {}
        text_parts = []
        title = ""

        # Extract KappaleKooste content
        kappale_matches = re.findall(
            r"<[^>]*KappaleKooste[^>]*>(.*?)</[^>]*KappaleKooste[^>]*>",
            xml_content,
            re.DOTALL | re.IGNORECASE,
        )
        for match in kappale_matches:
            # Remove inner tags
            text = re.sub(r"<[^>]+>", "", match)
            text = html.unescape(text).strip()
            if text:
                text_parts.append(text)

        # Extract ValiotsikkoTeksti (subtitles)
        subtitle_matches = re.findall(
            r"<[^>]*ValiotsikkoTeksti[^>]*>(.*?)</[^>]*ValiotsikkoTeksti[^>]*>",
            xml_content,
            re.DOTALL | re.IGNORECASE,
        )
        for match in subtitle_matches:
            text = re.sub(r"<[^>]+>", "", match)
            text = html.unescape(text).strip()
            if text:
                text_parts.append(text)

        # Extract title from NimekeTeksti
        title_match = re.search(
            r"<[^>]*NimekeTeksti[^>]*>(.*?)</[^>]*NimekeTeksti[^>]*>",
            xml_content,
            re.DOTALL | re.IGNORECASE,
        )
        if title_match:
            title = re.sub(r"<[^>]+>", "", title_match.group(1))
            title = html.unescape(title).strip()

        # Extract metadata from attributes
        doc_type_match = re.search(r'asiakirjatyyppiNimi="([^"]+)"', xml_content)
        if doc_type_match:
            metadata["doc_type_name"] = doc_type_match.group(1)

        eduskunta_id_match = re.search(r'eduskuntaTunnus="([^"]+)"', xml_content)
        if eduskunta_id_match:
            metadata["eduskunta_id"] = eduskunta_id_match.group(1)

        lang_match = re.search(r'kieliKoodi="([^"]+)"', xml_content)
        if lang_match:
            metadata["language"] = lang_match.group(1)

        date_match = re.search(r'laadintaPvm="([^"]+)"', xml_content)
        if date_match:
            metadata["date_drafted"] = date_match.group(1)

        full_text = "\n\n".join(text_parts)
        full_text = re.sub(r"\s+", " ", full_text).strip()

        return title, full_text, metadata

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all parliamentary documents from VaskiData.

        Focuses on government proposals (HE) and committee reports.
        """
        # Fetch government proposals first (most important)
        logger.info("Fetching government proposals (HE)")
        for row in self._paginate_vaski_data(doc_type_filter="HE"):
            yield row

        # Then fetch committee reports
        for doc_type in ["LaVM", "MmVM", "PeVM", "StVM", "TaVM", "TyVM", "VaVM", "YmVM"]:
            logger.info(f"Fetching {doc_type} committee reports")
            for row in self._paginate_vaski_data(doc_type_filter=doc_type, max_pages=10):
                yield row

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents created/imported since the given date.

        Currently fetches recent pages of government proposals.
        """
        logger.info(f"Fetching updates since {since}")
        for row in self._paginate_vaski_data(doc_type_filter="HE", max_pages=5):
            # Check if the document is newer than since
            imported = row.get("Imported", "")
            if imported:
                try:
                    imported_dt = datetime.fromisoformat(imported.replace(" ", "T"))
                    if imported_dt.replace(tzinfo=timezone.utc) >= since:
                        yield row
                except ValueError:
                    yield row  # Include if we can't parse the date
            else:
                yield row

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw VaskiData row into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from XML.
        """
        row_id = raw.get("Id", "")
        xml_content = raw.get("XmlData", "")
        eduskunta_id = raw.get("Eduskuntatunnus", "")
        created = raw.get("Created", "")
        imported = raw.get("Imported", "")

        # Extract text and metadata from XML
        title, full_text, xml_metadata = self._extract_text_from_vaski_xml(xml_content)

        # Build unique ID
        doc_id = f"FI_Eduskunta_{row_id}"
        if eduskunta_id:
            # Clean up the ID for use as a filename
            clean_id = re.sub(r"[^\w\d]", "_", eduskunta_id)
            doc_id = f"FI_Eduskunta_{clean_id}_{row_id}"

        # Determine date (prefer drafted date, then created)
        date = xml_metadata.get("date_drafted", "")
        if not date and created:
            # Parse the created timestamp
            try:
                dt = datetime.fromisoformat(created.replace(" ", "T"))
                date = dt.date().isoformat()
            except ValueError:
                date = created[:10] if len(created) >= 10 else ""

        # Build URL to view document
        url = f"https://avoindata.eduskunta.fi/#/fi/vaski/{row_id}"

        # Determine document type from Eduskuntatunnus
        doc_type = "legislation"  # Default
        doc_subtype = ""
        if eduskunta_id:
            if eduskunta_id.startswith("HE"):
                doc_subtype = "government_proposal"
            elif any(eduskunta_id.startswith(t) for t in ["LaVM", "MmVM", "PeVM", "StVM", "TaVM", "TyVM", "VaVM", "YmVM"]):
                doc_subtype = "committee_report"

        # Use title from XML, fallback to Eduskuntatunnus
        if not title:
            title = xml_metadata.get("doc_type_name", eduskunta_id or f"Document {row_id}")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "FI/Eduskunta",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "eduskunta_id": eduskunta_id,
            "row_id": row_id,
            "doc_subtype": doc_subtype,
            "doc_type_name": xml_metadata.get("doc_type_name", ""),
            "language": xml_metadata.get("language", "fi"),
            "created": created,
            "imported": imported,
            "attachment_group_id": raw.get("AttachmentGroupId", ""),
            "status": raw.get("Status", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Eduskunta Open Data API...")

        # Test tables endpoint
        print("\nAvailable tables:")
        tables = self._get_tables()
        for table in tables:
            print(f"  - {table}")

        # Get table counts
        print("\nTable row counts:")
        counts = self._get_table_counts()
        for item in counts:
            print(f"  - {item['tableName']}: {item['rowCount']:,} rows")

        # Test VaskiData with HE filter
        print("\nFetching sample government proposal (HE)...")
        data = self._get_rows(
            "VaskiData",
            page=0,
            per_page=1,
            column_name="Eduskuntatunnus",
            column_value="%HE%",
        )

        rows = data.get("rowData", [])
        columns = data.get("columnNames", [])

        if rows:
            row_dict = dict(zip(columns, rows[0]))
            eduskunta_id = row_dict.get("Eduskuntatunnus", "")
            xml_content = row_dict.get("XmlData", "")

            print(f"  Eduskuntatunnus: {eduskunta_id}")
            print(f"  XML length: {len(xml_content)} chars")

            # Extract text
            title, text, metadata = self._extract_text_from_vaski_xml(xml_content)
            print(f"  Title: {title[:100]}..." if len(title) > 100 else f"  Title: {title}")
            print(f"  Text length: {len(text)} chars")
            if text:
                print(f"  Text preview: {text[:300]}...")
            print(f"  Metadata: {json.dumps(metadata, indent=4, ensure_ascii=False)}")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = EduskuntaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
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
