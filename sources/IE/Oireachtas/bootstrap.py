#!/usr/bin/env python3
"""
IE/Oireachtas -- Houses of the Oireachtas (Irish Parliament) Data Fetcher

Fetches Irish parliamentary debates and proceedings from the Oireachtas Open Data API.

Strategy:
  - Uses the official Oireachtas Open Data API at api.oireachtas.ie
  - Fetches debate records which include full XML transcripts (Akoma Ntoso format)
  - Covers both Dáil Éireann (lower house) and Seanad Éireann (upper house)

Endpoints:
  - API base: https://api.oireachtas.ie/v1
  - Debates list: /v1/debates?limit=20&date_start=...&date_end=...
  - Data files: https://data.oireachtas.ie/akn/ie/debateRecord/{chamber}/{date}/debate/mul@/main.xml

Data:
  - Parliamentary debates from 1919 to present
  - Includes: speeches, questions, votes, bills discussion
  - Format: Akoma Ntoso XML (full text available)
  - License: Oireachtas (Open Data) PSI Licence (CC-BY 4.0 equivalent)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("legal-data-hunter.IE.Oireachtas")

# API endpoints
API_BASE = "https://api.oireachtas.ie"
DATA_BASE = "https://data.oireachtas.ie"

# Chambers
CHAMBERS = ["dail", "seanad"]

# Years to scrape (most recent first for sample mode)
CURRENT_YEAR = datetime.now().year
START_YEAR = 1919  # Debates available from 1919


class OireachtasScraper(BaseScraper):
    """
    Scraper for IE/Oireachtas -- Houses of the Oireachtas (Irish Parliament).
    Country: IE
    URL: https://www.oireachtas.ie

    Data types: parliamentary_proceedings
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.api_client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

        self.data_client = HttpClient(
            base_url=DATA_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/xml",
            },
            timeout=120,
        )

    def _get_debates_for_period(
        self, date_start: str, date_end: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Fetch debate records from the API for a given date range.

        Args:
            date_start: Start date (YYYY-MM-DD)
            date_end: End date (YYYY-MM-DD)
            limit: Maximum results per request

        Returns:
            List of debate record metadata
        """
        debates = []
        skip = 0

        while True:
            try:
                self.rate_limiter.wait()
                params = f"?date_start={date_start}&date_end={date_end}&limit={limit}&skip={skip}"
                resp = self.api_client.get(f"/v1/debates{params}")

                if resp.status_code != 200:
                    logger.warning(f"API returned {resp.status_code} for debates")
                    break

                data = resp.json()
                results = data.get("results", [])

                if not results:
                    break

                for item in results:
                    debate = item.get("debateRecord", {})
                    if debate:
                        debates.append(debate)

                # Check if more results available
                head = data.get("head", {})
                counts = head.get("counts", {})
                total = counts.get("resultCount", 0)

                skip += limit
                if skip >= total:
                    break

            except Exception as e:
                logger.warning(f"Error fetching debates: {e}")
                break

        return debates

    def _fetch_debate_xml(self, xml_uri: str) -> Optional[str]:
        """
        Fetch full XML content of a debate from data.oireachtas.ie.

        Args:
            xml_uri: The URI fragment (after data.oireachtas.ie)

        Returns:
            XML content as string, or None if failed
        """
        try:
            # URI comes as full URL, need to extract path
            if xml_uri.startswith("https://data.oireachtas.ie"):
                path = xml_uri.replace("https://data.oireachtas.ie", "")
            else:
                path = xml_uri

            self.rate_limiter.wait()
            resp = self.data_client.get(path)

            if resp.status_code == 404:
                logger.debug(f"XML not found: {path}")
                return None

            if resp.status_code != 200:
                logger.warning(f"Failed to fetch XML: {resp.status_code}")
                return None

            return resp.text

        except Exception as e:
            logger.warning(f"Error fetching debate XML: {e}")
            return None

    def _extract_text_from_akn_xml(self, xml_content: str) -> str:
        """
        Extract clean text from Akoma Ntoso XML content.

        Extracts all speeches, questions, and narrative text.
        """
        if not xml_content:
            return ""

        try:
            # Parse XML
            root = ET.fromstring(xml_content.encode('utf-8'))

            # Akoma Ntoso namespace
            ns = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"}

            text_parts = []

            # Extract all text from the debate body
            # Look for speeches, questions, and other elements
            for elem in root.iter():
                # Skip metadata elements
                if elem.tag.endswith('}meta') or 'meta' in (elem.tag or ''):
                    continue
                if elem.tag.endswith('}identification'):
                    continue
                if elem.tag.endswith('}references'):
                    continue

                # Get text content
                if elem.text:
                    text = elem.text.strip()
                    if text and len(text) > 1:
                        text_parts.append(text)
                if elem.tail:
                    text = elem.tail.strip()
                    if text and len(text) > 1:
                        text_parts.append(text)

            full_text = "\n".join(text_parts)

            # Clean up the text
            full_text = re.sub(r'\s+', ' ', full_text)
            full_text = html.unescape(full_text)

            return full_text.strip()

        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            # Fallback: regex extraction
            return self._extract_text_fallback(xml_content)
        except Exception as e:
            logger.warning(f"Error extracting text: {e}")
            return self._extract_text_fallback(xml_content)

    def _extract_text_fallback(self, xml_content: str) -> str:
        """Fallback text extraction using regex."""
        # Remove XML tags
        text = re.sub(r'<[^>]+>', ' ', xml_content)
        # Unescape HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _get_main_xml_uri(self, debate: Dict[str, Any]) -> Optional[str]:
        """
        Get the main XML URI for a debate record.

        Tries to get the full debate XML, falling back to section XMLs.
        """
        # Try to get the main debate XML
        # Format: /akn/ie/debateRecord/{chamber}/{date}/debate/mul@/main.xml
        uri = debate.get("uri", "")
        if uri:
            # Convert to main XML path
            main_xml = uri.replace("https://data.oireachtas.ie", "")
            if not main_xml.endswith(".xml"):
                main_xml = main_xml + "/mul@/main.xml"
            return main_xml

        return None

    def _get_section_xml_uris(self, debate: Dict[str, Any]) -> List[str]:
        """
        Get XML URIs for individual debate sections.

        Returns list of XML URIs that have content.
        """
        uris = []

        sections = debate.get("debateSections", [])
        for section in sections:
            sec_data = section.get("debateSection", {})
            formats = sec_data.get("formats", {})
            xml_info = formats.get("xml", {})

            if xml_info and xml_info.get("uri"):
                uris.append(xml_info["uri"])

        return uris

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all debate records from the Oireachtas.

        Iterates through date ranges (newest first), fetching debates
        and their full XML transcripts.
        """
        documents_yielded = 0

        # Process year by year, most recent first
        for year in range(CURRENT_YEAR, START_YEAR - 1, -1):
            logger.info(f"Processing debates for year {year}...")

            date_start = f"{year}-01-01"
            date_end = f"{year}-12-31"

            debates = self._get_debates_for_period(date_start, date_end, limit=100)
            logger.info(f"Found {len(debates)} debate records for {year}")

            for debate in debates:
                doc = self._process_debate(debate)
                if doc:
                    yield doc
                    documents_yielded += 1

    def _process_debate(self, debate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single debate record and fetch its full text.

        Returns raw document dict or None if failed.
        """
        # Extract metadata
        uri = debate.get("uri", "")

        # Parse chamber and date from URI
        # Format: /akn/ie/debateRecord/{chamber}/{date}/debate/main
        chamber = "unknown"
        date_str = ""

        if uri:
            # Extract from URI like https://data.oireachtas.ie/akn/ie/debateRecord/dail/2026-02-12/debate/main
            # or https://data.oireachtas.ie/akn/ie/debateRecord/joint_committee_on_agriculture_and_food/2026-02-05/debate/main
            match = re.search(r'/debateRecord/([^/]+)/(\d{4}-\d{2}-\d{2})/', uri)
            if match:
                chamber = match.group(1)
                date_str = match.group(2)

        if not date_str:
            logger.warning(f"Could not extract date from debate URI: {uri}")
            return None

        # Create document ID
        doc_id = f"{chamber}/{date_str}"

        # Fetch full text
        full_text = ""

        # Try to get main XML
        main_xml_uri = self._get_main_xml_uri(debate)
        if main_xml_uri:
            xml_content = self._fetch_debate_xml(main_xml_uri)
            if xml_content:
                full_text = self._extract_text_from_akn_xml(xml_content)

        # If main XML failed, try section XMLs
        if not full_text or len(full_text) < 500:
            section_uris = self._get_section_xml_uris(debate)
            section_texts = []

            for xml_uri in section_uris[:20]:  # Limit sections to avoid too many requests
                xml_content = self._fetch_debate_xml(xml_uri)
                if xml_content:
                    text = self._extract_text_from_akn_xml(xml_content)
                    if text:
                        section_texts.append(text)

            if section_texts:
                full_text = "\n\n".join(section_texts)

        if not full_text:
            logger.warning(f"No full text for debate {doc_id}, skipping")
            return None

        # Get counts
        counts = debate.get("counts", {})

        # Build title
        if chamber == "dail":
            chamber_name = "Dáil Éireann"
        elif chamber == "seanad":
            chamber_name = "Seanad Éireann"
        else:
            # Committee debates - format the name nicely
            chamber_name = chamber.replace("_", " ").title()
        title = f"{chamber_name} Debate - {date_str}"

        return {
            "doc_id": doc_id,
            "chamber": chamber,
            "chamber_name": chamber_name,
            "date": date_str,
            "title": title,
            "full_text": full_text,
            "uri": uri,
            "contributor_count": counts.get("contributorCount", 0),
            "speech_count": counts.get("debateSectionCount", 0),
            "question_count": counts.get("questionCount", 0),
        }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield debate records updated since the given date.
        """
        since_str = since.strftime("%Y-%m-%d")
        today_str = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching debates from {since_str} to {today_str}")

        debates = self._get_debates_for_period(since_str, today_str, limit=100)
        logger.info(f"Found {len(debates)} debate records since {since_str}")

        for debate in debates:
            doc = self._process_debate(debate)
            if doc:
                yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw debate data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("doc_id", "")
        chamber = raw.get("chamber", "unknown")
        date_str = raw.get("date", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        uri = raw.get("uri", "")

        # Build URL
        if uri.startswith("https://"):
            url = uri
        else:
            url = f"{DATA_BASE}{uri}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IE/Oireachtas",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "chamber": chamber,
            "chamber_name": raw.get("chamber_name", ""),
            "contributor_count": raw.get("contributor_count", 0),
            "speech_count": raw.get("speech_count", 0),
            "question_count": raw.get("question_count", 0),
            "language": "en",  # Primary language (also contains Irish)
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Oireachtas Open Data API...")

        # Test API endpoint
        print("\n1. Testing debates API endpoint...")
        try:
            today = datetime.now()
            week_ago = today - timedelta(days=7)
            date_start = week_ago.strftime("%Y-%m-%d")
            date_end = today.strftime("%Y-%m-%d")

            resp = self.api_client.get(f"/v1/debates?date_start={date_start}&date_end={date_end}&limit=2")
            print(f"   Status: {resp.status_code}")

            if resp.status_code == 200:
                data = resp.json()
                results = data.get("results", [])
                print(f"   Found {len(results)} recent debates")

                if results:
                    debate = results[0].get("debateRecord", {})
                    uri = debate.get("uri", "")
                    print(f"   Sample URI: {uri[:80]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test XML fetching
        print("\n2. Testing XML data endpoint...")
        try:
            # Get a recent debate
            resp = self.api_client.get(f"/v1/debates?limit=1")
            if resp.status_code == 200:
                data = resp.json()
                results = data.get("results", [])

                if results:
                    debate = results[0].get("debateRecord", {})
                    section_uris = self._get_section_xml_uris(debate)

                    if section_uris:
                        xml_uri = section_uris[0]
                        print(f"   Fetching: {xml_uri[:80]}...")

                        xml_content = self._fetch_debate_xml(xml_uri)
                        if xml_content:
                            print(f"   XML length: {len(xml_content)} bytes")

                            text = self._extract_text_from_akn_xml(xml_content)
                            print(f"   Extracted text: {len(text)} characters")
                            if text:
                                print(f"   Sample: {text[:200]}...")
                        else:
                            print("   ERROR: Could not fetch XML")
                    else:
                        print("   No section XMLs found")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test processing a full debate
        print("\n3. Testing full debate processing...")
        try:
            resp = self.api_client.get(f"/v1/debates?limit=1")
            if resp.status_code == 200:
                data = resp.json()
                results = data.get("results", [])

                if results:
                    debate = results[0].get("debateRecord", {})
                    doc = self._process_debate(debate)

                    if doc:
                        print(f"   Doc ID: {doc['doc_id']}")
                        print(f"   Title: {doc['title']}")
                        print(f"   Text length: {len(doc['full_text'])} characters")
                        print(f"   Contributors: {doc['contributor_count']}")
                    else:
                        print("   Failed to process debate")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = OireachtasScraper()

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
