#!/usr/bin/env python3
"""
NL/wetten.overheid.nl -- Dutch Consolidated Legislation Fetcher

Fetches consolidated Dutch legislation from the Basis Wetten Bestand (BWB)
using the SRU 1.2 API provided by KOOP (Kennis- en Exploitatiecentrum
Officiële Overheidspublicaties).

Key difference from NL/Staatsblad:
  - Staatsblad: Official gazette publications (as-published amendments)
  - wetten.overheid.nl: CONSOLIDATED legislation (integrated current law)

Strategy:
  - Bootstrap: Paginates through all regulations using SRU with x-connection=BWB
  - Update: Uses dcterms.modified filter to fetch only recently changed records
  - Sample: Fetches 12+ records for validation with full text

API Documentation:
  - SRU 1.2: https://www.loc.gov/standards/sru/
  - BWB endpoint: https://zoekservice.overheid.nl/sru/Search?x-connection=BWB
  - Full text XML: https://repository.officiele-overheidspublicaties.nl/bwb/{id}/{date}/xml/{file}.xml

Usage:
  python bootstrap.py bootstrap           # Full initial pull (45K+ regulations)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent modifications)
  python bootstrap.py test-api            # Quick API connectivity test
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

import requests
from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.wetten_overheid_nl")

# SRU API endpoint for BWB (Basis Wetten Bestand)
SRU_ENDPOINT = "https://zoekservice.overheid.nl/sru/Search"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# XML namespaces used in SRU responses
NAMESPACES = {
    "srw": "http://www.loc.gov/zing/srw/",
    "gzd": "http://standaarden.overheid.nl/sru",
    "dc": "http://purl.org/dc/terms/",
    "overheid": "http://standaarden.overheid.nl/owms/terms/",
    "overheidbwb": "http://standaarden.overheid.nl/bwb/terms/",
}


class WettenOverheidScraper(BaseScraper):
    """
    Scraper for NL/wetten.overheid.nl -- Dutch Consolidated Legislation.
    Country: NL
    URL: https://wetten.overheid.nl

    Data types: legislation (consolidated)
    Auth: none (Open Government Data, CC0 license)

    Data Coverage:
      - ~45,000 regulations with 100,000+ versions
      - Laws (wetten), AMvBs, ministerial regulations, etc.
      - All consolidated/current text since May 2002
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=SRU_ENDPOINT,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- SRU API helpers --------------------------------------------------------

    def _sru_search(
        self,
        query: str,
        start_record: int = 1,
        maximum_records: int = 100,
        max_retries: int = 10,
    ) -> Optional[ET.Element]:
        """
        Execute an SRU searchRetrieve query against the BWB database.

        Args:
            query: CQL query string
            start_record: Starting record number (1-based)
            maximum_records: Maximum records per page (max 100)
            max_retries: Maximum number of retry attempts for connection errors

        Returns:
            XML Element tree of the response, or None on error
        """
        import time
        from http.client import RemoteDisconnected
        from urllib3.exceptions import ProtocolError

        params = {
            "operation": "searchRetrieve",
            "version": "1.2",
            "x-connection": "BWB",
            "query": query,
            "startRecord": str(start_record),
            "maximumRecords": str(maximum_records),
        }

        # Create fresh session for each search to avoid stale connections
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        for attempt in range(max_retries):
            self.rate_limiter.wait()

            try:
                resp = session.get(
                    SRU_ENDPOINT,
                    params=params,
                    timeout=120,  # Increased timeout to 2 minutes
                )
                resp.raise_for_status()
                return ET.fromstring(resp.content)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                    RemoteDisconnected,
                    ProtocolError) as e:
                # Connection errors - retry with exponential backoff
                # Use longer waits to let the server recover
                wait_time = min(600, 30 * (2 ** attempt))  # Max 10 minutes
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
                # Create fresh session after connection error
                session = requests.Session()
                session.headers.update({"User-Agent": USER_AGENT})
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code >= 500:
                    # Server errors - retry with backoff
                    wait_time = min(300, 30 * (2 ** attempt))
                    logger.warning(f"Server error {e.response.status_code} (attempt {attempt + 1}/{max_retries})")
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"HTTP error: {e}")
                    return None
            except Exception as e:
                logger.error(f"SRU search error: {e}")
                # On unexpected errors, also retry with backoff
                if attempt < max_retries - 1:
                    wait_time = min(300, 30 * (2 ** attempt))
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    return None

        logger.error(f"Failed after {max_retries} attempts for startRecord={start_record}")
        return None

    def _load_checkpoint(self) -> dict:
        """Load pagination checkpoint if it exists."""
        checkpoint_file = self.source_dir / "checkpoint.json"
        if checkpoint_file.exists():
            try:
                return json.load(open(checkpoint_file))
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return {}

    def _save_checkpoint(self, data: dict) -> None:
        """Save pagination checkpoint."""
        checkpoint_file = self.source_dir / "checkpoint.json"
        with open(checkpoint_file, "w") as f:
            json.dump(data, f)

    def _clear_checkpoint(self) -> None:
        """Clear checkpoint after successful completion."""
        checkpoint_file = self.source_dir / "checkpoint.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()

    def _paginate_bwb(
        self,
        query: str = "cql.allRecords=1",
        max_pages: Optional[int] = None,
        use_checkpoint: bool = True,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through BWB (consolidated legislation) records.

        Yields individual document metadata dicts with content URLs.

        Args:
            query: CQL query string. Default fetches all BWB records (145K+).
                   Valid queries use == relation (e.g., "dcterms.type==wet").
            max_pages: Optional limit on pages to fetch
            use_checkpoint: If True, resume from checkpoint if available
        """
        page = 1
        start_record = 1
        records_per_page = 100
        total_records = None

        # Try to resume from checkpoint
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            if checkpoint.get("query") == query and checkpoint.get("start_record"):
                start_record = checkpoint["start_record"]
                page = checkpoint.get("page", (start_record - 1) // records_per_page + 1)
                logger.info(f"Resuming from checkpoint: page {page}, record {start_record}")

        consecutive_failures = 0
        max_consecutive_failures = 5

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
                consecutive_failures += 1
                # Save checkpoint on every failure (not just final)
                if use_checkpoint:
                    self._save_checkpoint({
                        "query": query,
                        "start_record": start_record,
                        "page": page,
                        "total_records": total_records,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "failure_count": consecutive_failures,
                    })
                    logger.info(f"Checkpoint saved at page {page} (failure {consecutive_failures})")

                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({consecutive_failures}), stopping")
                    return
                logger.warning(f"Failed to fetch, retrying ({consecutive_failures}/{max_consecutive_failures})...")
                continue

            consecutive_failures = 0  # Reset on success

            # Parse total number of records on first page
            if total_records is None:
                num_records_elem = root.find(".//srw:numberOfRecords", NAMESPACES)
                if num_records_elem is not None:
                    try:
                        total_records = int(num_records_elem.text)
                    except (ValueError, TypeError):
                        total_records = 0
                else:
                    total_records = 0
                logger.info(f"BWB search: {total_records} total records")

                if total_records == 0:
                    return

            # Extract records
            records = root.findall(".//srw:record", NAMESPACES)
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
                logger.info(f"Fetched all {total_records} BWB records")
                # Clear checkpoint on successful completion
                if use_checkpoint:
                    self._clear_checkpoint()
                return

            page += 1
            start_record = fetched_so_far + 1

            # Save checkpoint every 10 pages (more frequent for resilience)
            if use_checkpoint and page % 10 == 0:
                self._save_checkpoint({
                    "query": query,
                    "start_record": start_record,
                    "page": page,
                    "total_records": total_records,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
                logger.info(f"Checkpoint saved at page {page} ({fetched_so_far}/{total_records})")

            if page % 10 == 0:
                logger.info(f"  Page {page} ({fetched_so_far}/{total_records} fetched)")

    def _parse_sru_record(self, record: ET.Element) -> Optional[dict]:
        """
        Parse an SRU BWB record element into a dict with metadata and content URLs.
        """
        try:
            record_data = record.find(".//srw:recordData", NAMESPACES)
            if record_data is None:
                return None

            gzd = record_data.find(".//gzd:gzd", NAMESPACES)
            if gzd is None:
                # Try without namespace
                gzd = record_data.find(".//{http://standaarden.overheid.nl/sru}gzd")
            if gzd is None:
                return None

            original_data = gzd.find(".//{http://standaarden.overheid.nl/sru}originalData")
            enriched_data = gzd.find(".//{http://standaarden.overheid.nl/sru}enrichedData")

            if original_data is None:
                return None

            # Parse metadata from overheidbwb:meta
            meta = original_data.find(".//{http://standaarden.overheid.nl/bwb/terms/}meta")
            if meta is None:
                return None

            # Extract core metadata from owmskern
            owmskern = meta.find(".//owmskern")
            if owmskern is None:
                # Find anywhere in meta
                for elem in meta.iter():
                    if 'owmskern' in elem.tag.lower() or elem.tag.endswith('owmskern'):
                        owmskern = elem
                        break

            # Direct element extraction with namespace handling
            doc_id = self._get_text_any_ns(meta, ["identifier"])
            title = self._get_text_any_ns(meta, ["title"])
            doc_type = self._get_text_any_ns(meta, ["type"])
            language = self._get_text_any_ns(meta, ["language"])
            authority = self._get_text_any_ns(meta, ["authority"])
            creator = self._get_text_any_ns(meta, ["creator"])
            modified = self._get_text_any_ns(meta, ["modified"])

            # Extract BWB-specific metadata
            rechtsgebied = []
            overheidsdomein = []
            for elem in meta.iter():
                if 'rechtsgebied' in elem.tag.lower() and elem.text:
                    rechtsgebied.append(elem.text.strip())
                if 'overheidsdomein' in elem.tag.lower() and elem.text:
                    overheidsdomein.append(elem.text.strip())

            # Get validity dates
            geldig_start = self._get_text_any_ns(meta, ["geldigheidsperiode_startdatum"])
            geldig_end = self._get_text_any_ns(meta, ["geldigheidsperiode_einddatum"])

            # Extract content URLs from enriched data
            xml_url = ""
            wti_url = ""
            manifest_url = ""
            if enriched_data is not None:
                for elem in enriched_data.iter():
                    if 'locatie_toestand' in elem.tag.lower() and elem.text:
                        xml_url = elem.text.strip()
                    if 'locatie_wti' in elem.tag.lower() and elem.text:
                        wti_url = elem.text.strip()
                    if 'locatie_manifest' in elem.tag.lower() and elem.text:
                        manifest_url = elem.text.strip()

            return {
                "doc_id": doc_id,
                "title": title,
                "doc_type": doc_type,
                "language": language,
                "authority": authority,
                "creator": creator,
                "date_modified": modified,
                "rechtsgebied": rechtsgebied,
                "overheidsdomein": overheidsdomein,
                "geldig_start": geldig_start,
                "geldig_end": geldig_end,
                "xml_url": xml_url,
                "wti_url": wti_url,
                "manifest_url": manifest_url,
            }

        except Exception as e:
            logger.warning(f"Error parsing SRU record: {e}")
            return None

    def _get_text_any_ns(self, parent: ET.Element, local_names: list) -> str:
        """Get text content from an XML element, trying multiple local names and ignoring namespace."""
        if parent is None:
            return ""
        for elem in parent.iter():
            tag_local = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            if tag_local.lower() in [n.lower() for n in local_names]:
                if elem.text and elem.text.strip():
                    return elem.text.strip()
        return ""

    def _download_full_text(self, xml_url: str, max_retries: int = 5) -> str:
        """
        Download and extract full text from BWB XML.

        Returns cleaned plain text of the legislation.
        """
        import time
        from http.client import RemoteDisconnected
        from urllib3.exceptions import ProtocolError

        if not xml_url:
            return ""

        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                resp = self.client.session.get(xml_url, timeout=60)
                resp.raise_for_status()

                # Parse XML and extract text
                root = ET.fromstring(resp.content)
                break  # Success, exit retry loop
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                    RemoteDisconnected,
                    ProtocolError) as e:
                wait_time = min(120, 10 * (2 ** attempt))
                logger.warning(f"XML download error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying XML download in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download XML after {max_retries} attempts: {xml_url}")
                    return ""
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code >= 500:
                    wait_time = min(60, 10 * (2 ** attempt))
                    logger.warning(f"Server error {e.response.status_code} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                    else:
                        return ""
                else:
                    logger.warning(f"HTTP error downloading XML: {e}")
                    return ""
            except Exception as e:
                logger.warning(f"Unexpected error downloading XML: {e}")
                return ""

        try:
            # Parse and extract text (already have 'root' from successful download)
            text_parts = []

            # BWB XML structure includes:
            # - <intitule> - Document title/description
            # - <citeertitel> - Citation title
            # - <aanhef> - Preamble (wij, considerans)
            # - <wettekst> - Law text body
            # - <artikel> - Articles
            # - <lid> - Article paragraphs
            # - <al> - Text paragraphs
            # - <nota-toelichting> - Explanatory notes

            # Extract text from all relevant text elements
            text_tags = ['intitule', 'citeertitel', 'al', 'titel', 'wij',
                        'considerans.al', 'tussenkop', 'lidnr', 'li.nr']

            for elem in root.iter():
                tag_local = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                if tag_local.lower() in [t.lower() for t in text_tags]:
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(text)

            # If no specific elements found, try to get all text content
            if not text_parts:
                for elem in root.iter():
                    if elem.text and elem.text.strip():
                        text_parts.append(elem.text.strip())
                    if elem.tail and elem.tail.strip():
                        text_parts.append(elem.tail.strip())

            full_text = "\n\n".join(text_parts)

            # Clean up
            full_text = html.unescape(full_text)
            # Normalize whitespace while preserving paragraph breaks
            full_text = re.sub(r"[ \t]+", " ", full_text)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)

            return full_text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch XML content from {xml_url}: {e}")
            return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all BWB consolidated legislation records.

        Uses SRU search with x-connection=BWB.
        """
        logger.info("Starting full BWB (consolidated legislation) fetch...")
        yield from self._paginate_bwb()

    def run_sample(self, n: int = 12) -> dict:
        """
        Custom sample mode that fetches recent documents with full text.

        Filters for laws (wetten) modified in the last 5 years for testing.
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
        logger.info(f"Fetching {n} recent consolidated legislation documents for sampling...")

        try:
            # Filter for recently modified laws
            since_date = (datetime.now() - timedelta(days=365*2)).strftime("%Y-%m-%d")
            query = f"dcterms.type==wet AND dcterms.modified>={since_date}"

            for raw in self._paginate_bwb(query=query, max_pages=5):
                self.rate_limiter.wait()

                try:
                    record = self.normalize(raw)
                except Exception as e:
                    logger.warning(f"Normalization error: {e}")
                    stats["errors"] += 1
                    continue

                stats["records_fetched"] += 1

                # Only include records with actual text content
                if record.get("text") and len(record.get("text", "")) > 100:
                    sample_records.append(record)
                    logger.info(f"  Collected: {record.get('_id')} ({len(record.get('text', ''))} chars)")
                    if len(sample_records) >= n:
                        break
                else:
                    logger.warning(f"  Skipped {record.get('_id')}: insufficient full text")

        except Exception as e:
            logger.error(f"Sample error: {e}")
            stats["error_message"] = str(e)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        self._save_samples(sample_records)
        stats["sample_records_saved"] = len(sample_records)

        return stats

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield BWB records modified since the given date.

        Uses dcterms.modified filter in SRU query.
        """
        since_str = since.strftime("%Y-%m-%d")
        query = f"dcterms.modified>={since_str}"
        logger.info(f"Fetching BWB updates since {since_str}...")
        yield from self._paginate_bwb(query=query)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw SRU record into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from content URLs.
        """
        # Download full text
        xml_url = raw.get("xml_url", "")
        full_text = ""
        if xml_url:
            full_text = self._download_full_text(xml_url)

        # Construct wetten.overheid.nl URL
        doc_id = raw.get("doc_id", "")
        url = f"https://wetten.overheid.nl/{doc_id}" if doc_id else ""

        # Determine effective date
        date = raw.get("geldig_start") or raw.get("date_modified") or ""

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "NL/wetten.overheid.nl",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "doc_id": doc_id,
            "doc_type": raw.get("doc_type", ""),
            "language": raw.get("language", ""),
            "authority": raw.get("authority", ""),
            "creator": raw.get("creator", ""),
            "date_modified": raw.get("date_modified", ""),
            "rechtsgebied": raw.get("rechtsgebied", []),
            "overheidsdomein": raw.get("overheidsdomein", []),
            "geldig_start": raw.get("geldig_start", ""),
            "geldig_end": raw.get("geldig_end", ""),
            "xml_url": xml_url,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing NL wetten.overheid.nl BWB SRU API...")

        # Test basic search
        root = self._sru_search(query="dcterms.type==wet", maximum_records=1)
        if root is None:
            print("  ERROR: Failed to connect to SRU API")
            return

        num_records_elem = root.find(".//srw:numberOfRecords", NAMESPACES)
        if num_records_elem is not None:
            print(f"  Total 'wet' (laws) records: {num_records_elem.text}")
        else:
            print("  Warning: Could not parse total record count")

        # Test fetching one record with full text
        records = root.findall(".//srw:record", NAMESPACES)
        if records:
            doc_data = self._parse_sru_record(records[0])
            if doc_data:
                print(f"  Sample document: {doc_data.get('doc_id')}")
                print(f"    Title: {doc_data.get('title', '')[:80]}...")
                print(f"    Type: {doc_data.get('doc_type')}")

                # Test full text fetch
                xml_url = doc_data.get("xml_url", "")
                if xml_url:
                    print(f"    XML URL: {xml_url}")
                    text = self._download_full_text(xml_url)
                    if text:
                        print(f"    Full text length: {len(text)} characters")
                        print(f"    Text preview: {text[:200]}...")
                    else:
                        print("    WARNING: Could not fetch full text")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = WettenOverheidScraper()

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
