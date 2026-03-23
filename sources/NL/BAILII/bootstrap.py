#!/usr/bin/env python3
"""
NL/BAILII -- Dutch Case Law Data Fetcher

Fetches Dutch court decisions from the Rechtspraak.nl Open Data API.
This source provides access to 900K+ court decisions from all Dutch courts
using the European Case Law Identifier (ECLI) system.

Strategy:
  - Bootstrap: Paginates through decisions using Atom feed search API
  - Update: Uses date modified filter to fetch only recently changed records
  - Sample: Fetches 10+ recent decisions for validation with full text

API Documentation:
  - Open Data: https://www.rechtspraak.nl/Uitspraken/Paginas/Open-Data.aspx
  - Technical docs: https://www.rechtspraak.nl/SiteCollectionDocuments/Technische-documentatie-Open-Data-van-de-Rechtspraak.pdf
  - Search endpoint: https://data.rechtspraak.nl/uitspraken/zoeken
  - Content endpoint: https://data.rechtspraak.nl/uitspraken/content?id={ECLI}

Rate limit: max 10 requests per second

Usage:
  python bootstrap.py bootstrap          # Full initial pull (900K+ records)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent modifications)
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
import time

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.BAILII")

# API endpoints
SEARCH_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

# XML namespaces
NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
    "psi": "http://psi.rechtspraak.nl/",
    "ecli": "https://e-justice.europa.eu/ecli",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0",
}


class RechtspraakScraper(BaseScraper):
    """
    Scraper for NL/BAILII -- Dutch Case Law from Rechtspraak.nl.
    Country: NL
    URL: https://data.rechtspraak.nl

    Data types: case_law
    Auth: none (Open Data, public service)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

        # Rate limit: max 10 requests/second -> 0.1s between requests
        self._last_request_time = 0
        self._min_request_interval = 0.15  # Slightly conservative

    def _rate_limit(self):
        """Enforce rate limit of max 10 requests per second."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    # -- Search API helpers --------------------------------------------------------

    def _search_decisions(
        self,
        max_results: int = 100,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        modified_from: Optional[str] = None,
        sort: str = "DESC",
        return_type: str = "DOC",
    ) -> Optional[ET.Element]:
        """
        Search for decisions using the Rechtspraak.nl Atom feed API.

        Args:
            max_results: Maximum number of results (default 100)
            date_from: Filter by decision date (YYYY-MM-DD)
            date_to: Filter by decision date (YYYY-MM-DD)
            modified_from: Filter by modification date (YYYY-MM-DD)
            sort: Sort order (ASC or DESC)
            return_type: Return type (DOC for full, ECLI for IDs only)

        Returns:
            XML Element tree of the Atom feed response
        """
        params = {
            "max": str(max_results),
            "sort": sort,
            "return": return_type,
        }

        if date_from:
            params["date"] = f">={date_from}"
        if date_to:
            if "date" in params:
                params["date"] = f"{params['date']}&date<={date_to}"
            else:
                params["date"] = f"<={date_to}"
        if modified_from:
            params["modified"] = f">={modified_from}"

        self._rate_limit()

        try:
            resp = self.client.get(SEARCH_URL, params=params)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Search API error: {e}")
            return None

    def _get_decision_content(self, ecli: str) -> Optional[ET.Element]:
        """
        Fetch full content of a decision by ECLI.

        Args:
            ecli: European Case Law Identifier

        Returns:
            XML Element tree of the decision
        """
        self._rate_limit()

        try:
            resp = self.client.get(CONTENT_URL, params={"id": ecli})
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Content API error for {ecli}: {e}")
            return None

    def _parse_feed_entries(self, root: ET.Element) -> list[dict]:
        """
        Parse Atom feed entries into list of ECLI metadata dicts.
        """
        entries = []
        for entry in root.findall(".//atom:entry", NAMESPACES):
            ecli = entry.find("atom:id", NAMESPACES)
            title = entry.find("atom:title", NAMESPACES)
            updated = entry.find("atom:updated", NAMESPACES)
            link = entry.find("atom:link", NAMESPACES)
            summary = entry.find("atom:summary", NAMESPACES)

            if ecli is not None and ecli.text:
                entries.append({
                    "ecli": ecli.text,
                    "title": title.text if title is not None else "",
                    "updated": updated.text if updated is not None else "",
                    "url": link.get("href", "") if link is not None else "",
                    "summary": summary.text if summary is not None else "",
                })

        return entries

    def _extract_full_text(self, root: ET.Element) -> str:
        """
        Extract full text from decision XML.

        The decision text is in <uitspraak> element with nested elements like:
        - <section> - major sections
        - <para> - paragraphs
        - <parablock> - paragraph blocks
        - <paragroup> - grouped paragraphs
        """
        text_parts = []

        # Try to find the uitspraak (decision) element
        uitspraak = root.find(".//rs:uitspraak", NAMESPACES)
        if uitspraak is None:
            # Try without namespace
            uitspraak = root.find(".//uitspraak")

        if uitspraak is not None:
            # Extract all text from the decision
            text_parts = self._extract_text_from_element(uitspraak)

        # Also check for inhoudsindicatie (summary/abstract)
        summary = root.find(".//rs:inhoudsindicatie", NAMESPACES)
        if summary is None:
            summary = root.find(".//inhoudsindicatie")

        if summary is not None:
            summary_text = self._extract_text_from_element(summary)
            if summary_text:
                text_parts = summary_text + text_parts

        full_text = "\n\n".join(text_parts)

        # Clean up the text
        full_text = html.unescape(full_text)
        # Remove excessive whitespace while preserving paragraph breaks
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        full_text = full_text.strip()

        return full_text

    def _extract_text_from_element(self, elem: ET.Element) -> list[str]:
        """
        Recursively extract text from an XML element, handling
        the Rechtspraak.nl document structure.
        """
        text_parts = []

        # Tags that typically contain meaningful text
        text_tags = {
            "para", "parablock", "title", "nr", "emphasis",
            "bridgehead", "subtitle", "intitule", "al"
        }

        # Get tag name without namespace
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

        # If this is a text-containing element, extract its full text
        if tag in text_tags:
            text = "".join(elem.itertext()).strip()
            if text:
                text_parts.append(text)
        else:
            # Recursively process children
            for child in elem:
                text_parts.extend(self._extract_text_from_element(child))

        return text_parts

    def _parse_decision_metadata(self, root: ET.Element) -> dict:
        """
        Parse RDF metadata from decision XML.
        """
        metadata = {}

        # Find the RDF Description element
        desc = root.find(".//rdf:Description", NAMESPACES)
        if desc is None:
            return metadata

        # Extract standard Dublin Core metadata
        mappings = {
            "dcterms:identifier": "ecli",
            "dcterms:creator": "court",
            "dcterms:date": "decision_date",
            "dcterms:issued": "publication_date",
            "dcterms:modified": "modified_date",
            "dcterms:subject": "legal_area",
            "dcterms:language": "language",
            "dcterms:spatial": "location",
            "dcterms:type": "decision_type",
        }

        for xpath, key in mappings.items():
            elem = desc.find(f".//{xpath}", NAMESPACES)
            if elem is not None:
                # Check for rdfs:label attribute (human-readable label)
                label = elem.get(f"{{{NAMESPACES['rdfs']}}}label", "")
                value = elem.text or ""
                metadata[key] = value
                if label:
                    metadata[f"{key}_label"] = label
                # Check for resourceIdentifier attribute
                resource_id = elem.get("resourceIdentifier", "")
                if resource_id:
                    metadata[f"{key}_uri"] = resource_id

        # Extract case number
        case_num = desc.find(".//psi:zaaknummer", NAMESPACES)
        if case_num is not None:
            metadata["case_number"] = case_num.text or ""

        # Extract procedure type
        procedure = desc.find(".//psi:procedure", NAMESPACES)
        if procedure is not None:
            metadata["procedure"] = procedure.text or ""
            proc_uri = procedure.get("resourceIdentifier", "")
            if proc_uri:
                metadata["procedure_uri"] = proc_uri

        # Extract relations (appeals, etc.)
        relations = []
        for relation in desc.findall(".//dcterms:relation", NAMESPACES):
            rel_ecli = relation.get(f"{{{NAMESPACES['ecli']}}}resourceIdentifier", "")
            rel_type = relation.get(f"{{{NAMESPACES['psi']}}}type", "")
            rel_label = relation.get(f"{{{NAMESPACES['rdfs']}}}label", "")
            if rel_ecli:
                relations.append({
                    "ecli": rel_ecli,
                    "type": rel_type,
                    "label": rel_label,
                })
        if relations:
            metadata["relations"] = relations

        # Extract vindplaatsen (citations)
        vindplaatsen = desc.find(".//dcterms:hasVersion", NAMESPACES)
        if vindplaatsen is not None:
            citations = []
            for li in vindplaatsen.findall(".//rdf:li", NAMESPACES):
                if li.text:
                    citations.append(li.text)
            if citations:
                metadata["citations"] = citations

        return metadata

    def _paginate_decisions(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        modified_from: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through decisions.

        Note: The Rechtspraak API doesn't support offset pagination directly.
        Instead, we paginate by date ranges.
        """
        page = 1
        batch_size = 100

        # Get total count first
        root = self._search_decisions(max_results=1)
        if root is None:
            return

        # Parse subtitle for total count
        subtitle = root.find(".//atom:subtitle", NAMESPACES)
        total_count = 0
        if subtitle is not None and subtitle.text:
            match = re.search(r"(\d+)", subtitle.text)
            if match:
                total_count = int(match.group(1))

        logger.info(f"Total decisions available: {total_count}")

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping")
                return

            root = self._search_decisions(
                max_results=batch_size,
                date_from=date_from,
                date_to=date_to,
                modified_from=modified_from,
            )

            if root is None:
                logger.error("Failed to fetch search results")
                return

            entries = self._parse_feed_entries(root)
            if not entries:
                logger.info("No more entries in feed")
                return

            logger.info(f"Page {page}: fetched {len(entries)} entries")

            for entry in entries:
                yield entry

            # For sample mode, we don't need to paginate further
            if max_pages == 1:
                return

            # Note: True pagination would require tracking last ECLI and
            # using from/to parameters. For now, we just do one batch.
            page += 1

            # Stop after first page for now (use date ranges for full fetch)
            return

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Dutch court decisions.

        Uses the search API to iterate through all decisions.
        """
        logger.info("Starting full Dutch case law fetch...")
        yield from self._paginate_decisions()

    def run_sample(self, n: int = 12) -> dict:
        """
        Custom sample mode that fetches recent decisions with full text.
        """
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        sample_records = []
        logger.info(f"Fetching {n} recent Dutch court decisions for sampling...")

        try:
            # Fetch recent decisions
            root = self._search_decisions(max_results=n * 2)  # Fetch extra in case some fail
            if root is None:
                raise Exception("Failed to fetch search results")

            entries = self._parse_feed_entries(root)
            logger.info(f"Found {len(entries)} decisions in search results")

            for entry in entries:
                if len(sample_records) >= n:
                    break

                ecli = entry.get("ecli", "")
                if not ecli:
                    continue

                logger.info(f"  Fetching content for {ecli}...")

                try:
                    content_root = self._get_decision_content(ecli)
                    if content_root is None:
                        logger.warning(f"  Skipped {ecli}: failed to fetch content")
                        stats["errors"] += 1
                        continue

                    # Parse metadata and full text
                    metadata = self._parse_decision_metadata(content_root)
                    full_text = self._extract_full_text(content_root)

                    if not full_text:
                        logger.warning(f"  Skipped {ecli}: no full text")
                        stats["records_skipped"] += 1
                        continue

                    # Normalize the record
                    raw = {
                        "ecli": ecli,
                        "feed_entry": entry,
                        "metadata": metadata,
                        "full_text": full_text,
                    }

                    record = self.normalize(raw)
                    sample_records.append(record)

                    logger.info(f"  Collected: {ecli} ({len(full_text)} chars)")
                    stats["records_fetched"] += 1

                except Exception as e:
                    logger.warning(f"  Error processing {ecli}: {e}")
                    stats["errors"] += 1
                    continue

        except Exception as e:
            logger.error(f"Sample error: {e}")
            stats["error_message"] = str(e)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._save_samples(sample_records)
        stats["sample_records_saved"] = len(sample_records)

        return stats

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions modified since the given date.
        """
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching decisions modified since {since_str}...")
        yield from self._paginate_decisions(modified_from=since_str)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes FULL TEXT of the decision.
        """
        ecli = raw.get("ecli", "")
        feed_entry = raw.get("feed_entry", {})
        metadata = raw.get("metadata", {})
        full_text = raw.get("full_text", "")

        # Determine decision date
        decision_date = metadata.get("decision_date", "")
        if not decision_date:
            # Try to extract from ECLI (format: ECLI:NL:COURT:YYYY:NUMBER)
            parts = ecli.split(":")
            if len(parts) >= 4:
                year = parts[3]
                if year.isdigit():
                    decision_date = year

        # Build deeplink URL
        url = feed_entry.get("url", "")
        if not url and ecli:
            url = f"https://uitspraken.rechtspraak.nl/details?id={ecli}"

        return {
            # Required base fields
            "_id": ecli,
            "_source": "NL/BAILII",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": feed_entry.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": decision_date,
            "url": url,
            # Source-specific fields
            "ecli": ecli,
            "court": metadata.get("court", ""),
            "court_uri": metadata.get("court_uri", ""),
            "case_number": metadata.get("case_number", ""),
            "procedure": metadata.get("procedure", ""),
            "procedure_uri": metadata.get("procedure_uri", ""),
            "legal_area": metadata.get("legal_area", ""),
            "legal_area_uri": metadata.get("legal_area_uri", ""),
            "decision_type": metadata.get("decision_type", ""),
            "location": metadata.get("location", ""),
            "language": metadata.get("language", "nl"),
            "publication_date": metadata.get("publication_date", ""),
            "modified_date": metadata.get("modified_date", ""),
            "summary": feed_entry.get("summary", ""),
            "relations": metadata.get("relations", []),
            "citations": metadata.get("citations", []),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Dutch Rechtspraak.nl Open Data API...")

        # Test search endpoint
        root = self._search_decisions(max_results=1)
        if root is None:
            print("  ERROR: Failed to connect to search API")
            return

        subtitle = root.find(".//atom:subtitle", NAMESPACES)
        if subtitle is not None:
            print(f"  {subtitle.text}")

        entries = self._parse_feed_entries(root)
        if entries:
            entry = entries[0]
            ecli = entry.get("ecli", "")
            print(f"  Sample ECLI: {ecli}")
            print(f"  Title: {entry.get('title', '')[:80]}...")

            # Test content endpoint
            print(f"\nFetching full content for {ecli}...")
            content_root = self._get_decision_content(ecli)
            if content_root is not None:
                metadata = self._parse_decision_metadata(content_root)
                full_text = self._extract_full_text(content_root)

                print(f"  Court: {metadata.get('court', 'N/A')}")
                print(f"  Case number: {metadata.get('case_number', 'N/A')}")
                print(f"  Legal area: {metadata.get('legal_area', 'N/A')}")
                print(f"  Full text length: {len(full_text)} characters")
                if full_text:
                    print(f"  Text preview: {full_text[:200]}...")
            else:
                print("  WARNING: Could not fetch full content")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = RechtspraakScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12  # Default to 12 for validation
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
