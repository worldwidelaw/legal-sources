#!/usr/bin/env python3
"""
NL/Staatsblad -- Dutch Official Gazette Data Fetcher

Fetches Dutch national legislation from the Staatsblad van het Koninkrijk der Nederlanden
using the SRU 2.0 API provided by KOOP (Kennis- en Exploitatiecentrum Officiële Overheidspublicaties).

Strategy:
  - Bootstrap: Paginates through Staatsblad publications using SRU search
  - Update: Uses date modified filter to fetch only recently changed records
  - Sample: Fetches 10+ records for validation with full text

API Documentation:
  - SRU 2.0: https://www.loc.gov/standards/sru/
  - Repository: https://repository.overheid.nl
  - Search endpoint: https://repository.overheid.nl/sru
  - Full text XML: https://repository.overheid.nl/frbr/officielepublicaties/stb/{year}/{id}/1/xml/{id}.xml

Usage:
  python bootstrap.py bootstrap          # Full initial pull (49K+ Staatsblad records)
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.Staatsblad")

# SRU API endpoint
SRU_ENDPOINT = "https://repository.overheid.nl/sru"

# XML namespaces used in responses
NAMESPACES = {
    "sru": "http://docs.oasis-open.org/ns/search-ws/sruResponse",
    "gzd": "http://standaarden.overheid.nl/sru",
    "dc": "http://purl.org/dc/terms/",
    "overheidwetgeving": "http://standaarden.overheid.nl/wetgeving/",
    "c": "http://standaarden.overheid.nl/collectie/",
    "cd": "http://standaarden.overheid.nl/cup/data",
}


class StaatsbladScraper(BaseScraper):
    """
    Scraper for NL/Staatsblad -- Dutch Official Gazette.
    Country: NL
    URL: https://www.staatsblad.nl

    Data types: legislation
    Auth: none (Open Government Data, CC0 license)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=SRU_ENDPOINT,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- SRU API helpers --------------------------------------------------------

    def _sru_search(
        self,
        query: str,
        start_record: int = 1,
        maximum_records: int = 100,
    ) -> Optional[ET.Element]:
        """
        Execute an SRU searchRetrieve query.

        Args:
            query: CQL query string
            start_record: Starting record number (1-based)
            maximum_records: Maximum records per page (max 100)

        Returns:
            XML Element tree of the response, or None on error
        """
        params = {
            "operation": "searchRetrieve",
            "version": "2.0",
            "query": query,
            "startRecord": str(start_record),
            "maximumRecords": str(maximum_records),
        }

        self.rate_limiter.wait()

        try:
            resp = self.client.get("", params=params)
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"SRU search error: {e}")
            return None

    def _paginate_staatsblad(
        self,
        extra_query: str = "",
        max_pages: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through Staatsblad publications.

        Yields individual document metadata dicts with content URLs.
        """
        # Base query for Staatsblad publications
        base_query = "w.publicatienaam==Staatsblad"
        if extra_query:
            query = f"({base_query}) AND ({extra_query})"
        else:
            query = base_query

        page = 1
        start_record = 1
        records_per_page = 100
        total_records = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            root = self._sru_search(
                query=query,
                start_record=start_record,
                maximum_records=records_per_page,
            )

            if root is None:
                logger.error("Failed to fetch SRU response")
                return

            # Parse total number of records on first page
            if total_records is None:
                num_records_elem = root.find(".//sru:numberOfRecords", NAMESPACES)
                if num_records_elem is not None:
                    try:
                        total_records = int(num_records_elem.text)
                    except (ValueError, TypeError):
                        total_records = 0
                else:
                    total_records = 0
                logger.info(f"Staatsblad search: {total_records} total records")

                if total_records == 0:
                    return

            # Extract records
            records = root.findall(".//sru:record", NAMESPACES)
            if not records:
                logger.info(f"No more records on page {page}")
                return

            for record in records:
                doc_data = self._parse_sru_record(record)
                if doc_data:
                    yield doc_data

            # Check if we've fetched all records
            fetched_so_far = start_record + len(records) - 1
            if fetched_so_far >= total_records:
                logger.info(f"Fetched all {total_records} Staatsblad records")
                return

            page += 1
            start_record = fetched_so_far + 1
            logger.info(f"  Page {page} ({fetched_so_far}/{total_records} fetched)")

    def _parse_sru_record(self, record: ET.Element) -> Optional[dict]:
        """
        Parse an SRU record element into a dict with metadata and content URLs.
        """
        try:
            record_data = record.find(".//sru:recordData", NAMESPACES)
            if record_data is None:
                return None

            gzd = record_data.find(".//gzd:gzd", NAMESPACES)
            if gzd is None:
                return None

            original_data = gzd.find(".//gzd:originalData", NAMESPACES)
            enriched_data = gzd.find(".//gzd:enrichedData", NAMESPACES)

            if original_data is None:
                return None

            # Parse metadata
            meta = original_data.find(".//overheidwetgeving:meta", NAMESPACES)
            if meta is None:
                return None

            owmskern = meta.find(".//overheidwetgeving:owmskern", NAMESPACES)
            owmsmantel = meta.find(".//overheidwetgeving:owmsmantel", NAMESPACES)
            tpmeta = meta.find(".//overheidwetgeving:tpmeta", NAMESPACES)

            # Extract core metadata
            doc_id = self._get_text(owmskern, "dc:identifier")
            title = self._get_text(owmskern, "dc:title")
            doc_type_raw = self._get_text(owmskern, "dc:type")
            language = self._get_text(owmskern, "dc:language")
            creator = self._get_text(owmskern, "dc:creator")
            date_modified = self._get_text(owmskern, "dc:modified")

            # Extract mantel metadata
            date_available = self._get_text(owmsmantel, "dc:available")
            date_issued = self._get_text(owmsmantel, "dc:issued")
            source = self._get_text(owmsmantel, "dc:source")
            subject = self._get_text(owmsmantel, "dc:subject")
            publisher = self._get_text(owmsmantel, "dc:publisher")
            alternative_title = self._get_text(owmsmantel, "dc:alternative")

            # Extract technical metadata
            jaargang = self._get_text(tpmeta, "overheidwetgeving:jaargang")
            publicatienummer = self._get_text(tpmeta, "overheidwetgeving:publicatienummer")
            datum_ondertekening = self._get_text(tpmeta, "overheidwetgeving:datumOndertekening")
            product_area = self._get_text(tpmeta, "c:product-area")

            # Extract content URLs from enriched data
            content_urls = {}
            if enriched_data is not None:
                for item_url in enriched_data.findall(".//gzd:itemUrl", NAMESPACES):
                    manifestation = item_url.get("manifestation", "")
                    url = item_url.text or ""
                    if manifestation and url:
                        content_urls[manifestation] = url

                preferred_url = enriched_data.find(".//gzd:preferredUrl", NAMESPACES)
                if preferred_url is not None and preferred_url.text:
                    content_urls["preferred"] = preferred_url.text

            return {
                "doc_id": doc_id,
                "title": title,
                "alternative_title": alternative_title,
                "doc_type_raw": doc_type_raw,
                "language": language,
                "creator": creator,
                "publisher": publisher,
                "subject": subject,
                "source": source,
                "date_modified": date_modified,
                "date_available": date_available,
                "date_issued": date_issued,
                "jaargang": jaargang,
                "publicatienummer": publicatienummer,
                "datum_ondertekening": datum_ondertekening,
                "product_area": product_area,
                "content_urls": content_urls,
            }

        except Exception as e:
            logger.warning(f"Error parsing SRU record: {e}")
            return None

    def _get_text(self, parent: Optional[ET.Element], path: str) -> str:
        """Get text content from an XML element, handling namespaces."""
        if parent is None:
            return ""
        elem = parent.find(f".//{path}", NAMESPACES)
        if elem is not None and elem.text:
            return elem.text.strip()
        return ""

    def _download_full_text(self, content_urls: dict) -> str:
        """
        Download and extract full text from content URLs.

        Prefers XML format for structured text extraction.
        Returns cleaned plain text.
        """
        # Try XML first (cleanest for text extraction)
        xml_url = content_urls.get("xml")
        if xml_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(xml_url, timeout=60)
                resp.raise_for_status()

                # Parse XML and extract text
                root = ET.fromstring(resp.content)
                text_parts = []

                # Extract text from all relevant elements
                # Staatsblad XML structure includes:
                # - <intitule> - Document title/description
                # - <considerans> - Preamble
                # - <wettekst> - Law text
                # - <artikel> - Articles
                # - <lid> - Paragraphs
                # - <al> - Text paragraphs
                # - <nota-toelichting> - Explanatory notes

                # Get all text from al (alinea/paragraph) elements
                for elem in root.iter():
                    if elem.tag in ["intitule", "al", "titel", "lidnr"]:
                        text = "".join(elem.itertext()).strip()
                        if text:
                            text_parts.append(text)

                # If no al elements, try to get all text
                if not text_parts:
                    for elem in root.iter():
                        if elem.text and elem.text.strip():
                            text_parts.append(elem.text.strip())
                        if elem.tail and elem.tail.strip():
                            text_parts.append(elem.tail.strip())

                full_text = "\n\n".join(text_parts)

                # Clean up
                full_text = html.unescape(full_text)
                # Remove processing instructions
                full_text = re.sub(r"<\?[^>]+\?>", "", full_text)
                # Normalize whitespace while preserving paragraph breaks
                full_text = re.sub(r"[ \t]+", " ", full_text)
                full_text = re.sub(r"\n{3,}", "\n\n", full_text)

                if full_text.strip():
                    return full_text.strip()

            except Exception as e:
                logger.warning(f"Failed to fetch XML content from {xml_url}: {e}")

        # Try HTML as fallback
        html_url = content_urls.get("html")
        if html_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(html_url, timeout=60)
                resp.raise_for_status()

                # Simple HTML tag stripping
                text = resp.text
                # Remove script and style tags
                text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
                # Remove HTML tags
                text = re.sub(r"<[^>]+>", " ", text)
                # Clean up entities
                text = html.unescape(text)
                # Normalize whitespace
                text = re.sub(r"\s+", " ", text)
                text = text.strip()

                if text:
                    return text

            except Exception as e:
                logger.warning(f"Failed to fetch HTML content from {html_url}: {e}")

        logger.warning(f"Could not fetch full text from any URL: {content_urls}")
        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Staatsblad documents.

        Uses SRU search with w.publicatienaam==Staatsblad query.
        """
        logger.info("Starting full Staatsblad fetch...")
        yield from self._paginate_staatsblad()

    def run_sample(self, n: int = 12) -> dict:
        """
        Custom sample mode that fetches recent documents with full text.

        Overrides base class to filter for documents from 2010+ which
        have XML full text available (older docs are PDF-only).
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
        logger.info(f"Fetching {n} recent Staatsblad documents for sampling...")

        try:
            # Filter for recent documents (2010+) which have XML full text
            for raw in self._paginate_staatsblad(extra_query="w.jaargang>=2010"):
                self.rate_limiter.wait()

                try:
                    record = self.normalize(raw)
                except Exception as e:
                    logger.warning(f"Normalization error: {e}")
                    stats["errors"] += 1
                    continue

                stats["records_fetched"] += 1

                # Only include records with actual text content
                if record.get("text"):
                    sample_records.append(record)
                    logger.info(f"  Collected: {record.get('doc_id')} ({len(record.get('text', ''))} chars)")
                    if len(sample_records) >= n:
                        break
                else:
                    logger.warning(f"  Skipped {record.get('doc_id')}: no full text")

        except Exception as e:
            logger.error(f"Sample error: {e}")
            stats["error_message"] = str(e)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._save_samples(sample_records)
        stats["sample_records_saved"] = len(sample_records)

        return stats

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield Staatsblad records modified since the given date.

        Uses dt.modified filter in SRU query.
        """
        since_str = since.strftime("%Y-%m-%d")
        extra_query = f"dt.modified>={since_str}"
        logger.info(f"Fetching Staatsblad updates since {since_str}...")
        yield from self._paginate_staatsblad(extra_query=extra_query)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw SRU record into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from content URLs.
        """
        # Download full text
        content_urls = raw.get("content_urls", {})
        full_text = ""
        if content_urls:
            full_text = self._download_full_text(content_urls)

        # Determine document URL
        url = content_urls.get("preferred", "")
        if not url:
            url = content_urls.get("html", content_urls.get("xml", ""))

        # Determine date (prefer ondertekening, then available, then issued)
        date = raw.get("datum_ondertekening") or raw.get("date_available") or raw.get("date_issued") or ""

        # Parse document type from raw type
        doc_type_raw = raw.get("doc_type_raw", "")
        # Staatsblad types: Wet, AMvB, Klein Koninklijk Besluit, etc.
        # All are legislation
        doc_type = "legislation"

        return {
            # Required base fields
            "_id": raw.get("doc_id", ""),
            "_source": "NL/Staatsblad",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "doc_id": raw.get("doc_id", ""),
            "alternative_title": raw.get("alternative_title", ""),
            "doc_type_raw": doc_type_raw,
            "language": raw.get("language", ""),
            "creator": raw.get("creator", ""),
            "publisher": raw.get("publisher", ""),
            "subject": raw.get("subject", ""),
            "source": raw.get("source", ""),
            "jaargang": raw.get("jaargang", ""),
            "publicatienummer": raw.get("publicatienummer", ""),
            "datum_ondertekening": raw.get("datum_ondertekening", ""),
            "date_available": raw.get("date_available", ""),
            "date_issued": raw.get("date_issued", ""),
            "date_modified": raw.get("date_modified", ""),
            "content_urls": content_urls,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing NL Staatsblad SRU API...")

        # Test basic search
        root = self._sru_search(query="w.publicatienaam==Staatsblad", maximum_records=1)
        if root is None:
            print("  ERROR: Failed to connect to SRU API")
            return

        num_records_elem = root.find(".//sru:numberOfRecords", NAMESPACES)
        if num_records_elem is not None:
            print(f"  Total Staatsblad records: {num_records_elem.text}")
        else:
            print("  Warning: Could not parse total record count")

        # Test fetching one record
        records = root.findall(".//sru:record", NAMESPACES)
        if records:
            doc_data = self._parse_sru_record(records[0])
            if doc_data:
                print(f"  Sample document: {doc_data.get('doc_id')}")
                print(f"    Title: {doc_data.get('title', '')[:80]}...")
                print(f"    Type: {doc_data.get('doc_type_raw')}")
                print(f"    Year: {doc_data.get('jaargang')}")

                # Test full text fetch
                content_urls = doc_data.get("content_urls", {})
                if content_urls.get("xml"):
                    print(f"    XML URL: {content_urls['xml']}")
                    text = self._download_full_text(content_urls)
                    if text:
                        print(f"    Full text length: {len(text)} characters")
                        print(f"    Text preview: {text[:200]}...")
                    else:
                        print("    WARNING: Could not fetch full text")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = StaatsbladScraper()

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
