#!/usr/bin/env python3
"""
XK/GazetaZyrtare -- Kosovo Official Gazette (Gazeta Zyrtare) Fetcher

Fetches Kosovo legislation with full text from the official gazette portal.

Strategy:
  - Discovery: Iterate through Official Gazette issues (GZID 1-600+)
  - Full text: HTML extraction from ActDocumentDetail.aspx?ActID={id}

Data access method:
  - HTML scraping (no API available)
  - Full text embedded in MainContent_txtDocument span

Coverage:
  - Kosovo legislation since independence (2008)
  - Laws, Decrees, Administrative Instructions, Regulations
  - Available in Albanian, English, Serbian, Turkish, Bosnian

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import html
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://gzk.rks-gov.net"
GAZETTE_DETAIL_URL = f"{BASE_URL}/OfficialGazetteDetail.aspx"
ACT_DOCUMENT_URL = f"{BASE_URL}/ActDocumentDetail.aspx"
ACT_DETAIL_URL = f"{BASE_URL}/ActDetail.aspx"

REQUEST_DELAY = 1.0  # seconds between requests
MAX_GAZETTE_ID = 700  # Upper bound for gazette issue IDs
REQUEST_TIMEOUT = 30

# HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) WorldWideLaw/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,sq;q=0.8",
}


class KosovoGazetteFetcher:
    """Fetcher for Kosovo Official Gazette legislation."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._discovered_act_ids: Set[int] = set()

    def _get(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        """Make an HTTP GET request with rate limiting."""
        try:
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    def discover_act_ids_from_gazette(self, gzid: int) -> Set[int]:
        """Discover ActIDs from a gazette issue page."""
        act_ids = set()
        url = f"{GAZETTE_DETAIL_URL}?GZID={gzid}"
        resp = self._get(url)
        if not resp:
            return act_ids

        # Extract ActIDs from links
        for match in re.findall(r'ActID=(\d+)', resp.text):
            act_ids.add(int(match))

        return act_ids

    def discover_all_act_ids(self, max_gzid: int = MAX_GAZETTE_ID, sample_mode: bool = False) -> Set[int]:
        """Discover all ActIDs by scanning gazette issues."""
        all_act_ids = set()
        empty_count = 0

        for gzid in range(1, max_gzid + 1):
            act_ids = self.discover_act_ids_from_gazette(gzid)

            if act_ids:
                all_act_ids.update(act_ids)
                empty_count = 0
                logger.info(f"GZID {gzid}: found {len(act_ids)} acts (total: {len(all_act_ids)})")
            else:
                empty_count += 1

            # In sample mode, stop after finding enough
            if sample_mode and len(all_act_ids) >= 150:
                logger.info(f"Sample mode: stopping with {len(all_act_ids)} ActIDs")
                break

            # Stop if we've hit many consecutive empty gazettes (beyond range)
            if empty_count > 20 and gzid > 100:
                logger.info(f"Stopping discovery: {empty_count} consecutive empty gazettes")
                break

        logger.info(f"Discovery complete: {len(all_act_ids)} unique ActIDs")
        return all_act_ids

    def fetch_act_metadata(self, act_id: int) -> Optional[Dict[str, Any]]:
        """Fetch metadata for an act from the detail page."""
        url = f"{ACT_DETAIL_URL}?ActID={act_id}"
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract title from page title or header
        title = ""
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)

        # Extract metadata fields
        metadata = {
            "act_id": act_id,
            "title": title,
            "act_type": "",
            "act_number": "",
            "institution": "",
            "publication_date": "",
            "gazette_reference": "",
        }

        # Parse metadata table
        for span_id, field in [
            ("MainContent_lblDActCategoryName_1", "act_type"),
            ("MainContent_lblDActNo", "act_number"),
            ("MainContent_lblDInstSpons", "institution"),
            ("MainContent_lblDPubDate", "publication_date"),
        ]:
            span = soup.find("span", id=span_id)
            if span:
                metadata[field] = span.get_text(strip=True)

        # Gazette reference from link
        gazette_link = soup.find("a", id="MainContent_lblDGZK")
        if gazette_link:
            metadata["gazette_reference"] = gazette_link.get_text(strip=True)

        return metadata

    def fetch_act_fulltext(self, act_id: int) -> Optional[str]:
        """Fetch full text for an act from the document detail page."""
        url = f"{ACT_DOCUMENT_URL}?ActID={act_id}"
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Find the text content span
        text_span = soup.find("span", id="MainContent_txtDocument")
        if not text_span:
            return None

        # Get the HTML content and clean it
        raw_html = str(text_span)

        # Convert <br> tags to newlines
        text = re.sub(r'<br\s*/?>', '\n', raw_html)

        # Remove remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = text.strip()

        # Skip if text is too short (empty or just metadata)
        if len(text) < 100:
            return None

        return text

    def fetch_document(self, act_id: int) -> Optional[Dict[str, Any]]:
        """Fetch complete document (metadata + full text)."""
        metadata = self.fetch_act_metadata(act_id)
        if not metadata:
            logger.warning(f"Failed to fetch metadata for ActID {act_id}")
            return None

        fulltext = self.fetch_act_fulltext(act_id)
        if not fulltext:
            logger.warning(f"No full text available for ActID {act_id}")
            return None

        # Parse publication date
        pub_date = metadata.get("publication_date", "")
        iso_date = None
        if pub_date:
            # Format: DD.MM.YYYY
            try:
                dt = datetime.strptime(pub_date, "%d.%m.%Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                iso_date = None

        return {
            "_id": f"XK-GZK-{act_id}",
            "_source": "XK/GazetaZyrtare",
            "_type": "legislation",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "act_id": act_id,
            "title": metadata.get("title", ""),
            "text": fulltext,
            "date": iso_date,
            "publication_date": pub_date,
            "act_type": metadata.get("act_type", ""),
            "act_number": metadata.get("act_number", ""),
            "institution": metadata.get("institution", ""),
            "gazette_reference": metadata.get("gazette_reference", ""),
            "url": f"{ACT_DETAIL_URL}?ActID={act_id}",
            "language": "sq",  # Albanian
        }

    def fetch_all(self, sample_mode: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all documents from the Kosovo Official Gazette."""
        # First discover all ActIDs
        act_ids = self.discover_all_act_ids(sample_mode=sample_mode)

        # Sort to process in order
        sorted_ids = sorted(act_ids, reverse=True)  # Newest first

        # In sample mode, limit to first 150
        if sample_mode:
            sorted_ids = sorted_ids[:150]

        # Fetch each document
        for act_id in sorted_ids:
            doc = self.fetch_document(act_id)
            if doc:
                yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a document to standard schema."""
        return raw  # Already normalized in fetch_document


def test_api():
    """Quick connectivity test."""
    fetcher = KosovoGazetteFetcher()

    # Test gazette discovery
    logger.info("Testing gazette discovery...")
    act_ids = fetcher.discover_act_ids_from_gazette(50)  # Older gazette with text
    logger.info(f"Found {len(act_ids)} acts in GZID 50")

    # Test document fetch with a known working ActID
    # ActID 8667 is confirmed to have embedded text
    test_ids = [8667, 2703, 2436] + list(act_ids)[:5]

    for test_id in test_ids:
        logger.info(f"Testing document fetch for ActID {test_id}...")
        doc = fetcher.fetch_document(test_id)
        if doc and len(doc.get('text', '')) > 500:
            logger.info(f"Title: {doc['title'][:80]}...")
            logger.info(f"Text length: {len(doc['text'])} chars")
            logger.info("API test PASSED")
            return True
        else:
            logger.info(f"ActID {test_id}: no text or too short, trying next...")

    logger.error("API test FAILED - no documents with full text found")
    return False


def bootstrap_sample(sample_dir: Path, count: int = 100):
    """Fetch sample documents for validation."""
    fetcher = KosovoGazetteFetcher()

    sample_dir.mkdir(parents=True, exist_ok=True)
    all_samples = []
    fetched = 0

    for doc in fetcher.fetch_all(sample_mode=True):
        # Save individual sample
        filename = f"{doc['_id']}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)

        all_samples.append(doc)
        fetched += 1
        logger.info(f"[{fetched}] Saved: {doc['title'][:60]}... ({len(doc['text'])} chars)")

        if fetched >= count:
            break

    # Save combined samples file
    all_samples_path = sample_dir / "all_samples.json"
    with open(all_samples_path, 'w', encoding='utf-8') as f:
        json.dump(all_samples, f, indent=2, ensure_ascii=False)

    logger.info(f"Bootstrap complete: {fetched} samples saved to {sample_dir}")

    # Print statistics
    if all_samples:
        avg_len = sum(len(s['text']) for s in all_samples) / len(all_samples)
        logger.info(f"Average text length: {avg_len:.0f} chars")
        logger.info(f"Document types: {set(s['act_type'] for s in all_samples)}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [test-api|bootstrap|bootstrap --sample|update]")
        sys.exit(1)

    command = sys.argv[1]
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        if sample_mode:
            bootstrap_sample(sample_dir, count=100)
        else:
            # Full bootstrap - stream all documents
            fetcher = KosovoGazetteFetcher()
            count = 0
            for doc in fetcher.fetch_all(sample_mode=False):
                print(json.dumps(doc, ensure_ascii=False))
                count += 1
            logger.info(f"Full bootstrap complete: {count} documents")

    elif command == "update":
        # For updates, just fetch recent gazettes
        logger.info("Update mode: fetching recent gazettes...")
        fetcher = KosovoGazetteFetcher()
        # Only scan last 50 gazette issues for updates
        for gzid in range(MAX_GAZETTE_ID, MAX_GAZETTE_ID - 50, -1):
            act_ids = fetcher.discover_act_ids_from_gazette(gzid)
            for act_id in act_ids:
                doc = fetcher.fetch_document(act_id)
                if doc:
                    print(json.dumps(doc, ensure_ascii=False))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
