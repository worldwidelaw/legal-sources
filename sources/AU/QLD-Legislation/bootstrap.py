#!/usr/bin/env python3
"""
AU/QLD-Legislation -- Queensland Legislation Fetcher

Fetches Queensland Acts and subordinate legislation from the OQPC portal.

Strategy:
  - Discover document IDs via Atom feeds and systematic enumeration
  - Fetch full text XML via /view/whole/xml/inforce/current/{id}
  - Extract text from QuILLS DTD XML (act/part/clause/heading/txt elements)
  - No auth required; CC BY 4.0 license

Data:
  - ~3,000+ Acts and subordinate legislation
  - Full text in structured XML
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Check Atom feeds for updates
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, Set
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.QLD-Legislation")

BASE_URL = "https://www.legislation.qld.gov.au"
XML_URL_PATTERN = f"{BASE_URL}/view/whole/xml/inforce/current/{{doc_id}}"
ATOM_FEEDS = [
    f"{BASE_URL}/feed?id=whatsnew",
    f"{BASE_URL}/feed?id=newinforce",
    f"{BASE_URL}/feed?id=newlegislation",
]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "application/xml, text/xml, application/atom+xml",
}


def _fetch_url(url: str, accept: str = "application/xml") -> Optional[bytes]:
    """Fetch a URL with error handling."""
    headers = dict(HEADERS)
    headers["Accept"] = accept
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=60) as resp:
            return resp.read()
    except (URLError, HTTPError) as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None


def _extract_text_from_xml(xml_bytes: bytes) -> tuple:
    """Extract title and full text from QuILLS XML.

    Returns (title, text, date, doc_type).
    """
    try:
        xml_str = xml_bytes.decode("utf-8", errors="replace")

        # Extract title from root element attribute (e.g., <act title="...">)
        title = ""
        title_match = re.search(r'<(?:act|sl|regulation)\s[^>]*title="([^"]+)"', xml_str[:3000])
        if title_match:
            title = title_match.group(1).strip()

        if not title:
            title_match = re.search(
                r'<heading[^>]*>\s*<txt[^>]*>(.*?)</txt>', xml_str[:5000], re.DOTALL
            )
            if title_match:
                title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()

        # Extract date from assent.date or publication.date attribute
        date = None
        date_match = re.search(r'assent\.date="(\d{4}-\d{2}-\d{2})', xml_str[:3000])
        if date_match:
            date = date_match.group(1)
        else:
            date_match = re.search(r'publication\.date="(\d{4}-\d{2}-\d{2})', xml_str[:3000])
            if date_match:
                date = date_match.group(1)

        # Determine doc type from root element
        doc_type = "act"
        root_match = re.match(r'.*?<(act|sl|regulation)\s', xml_str[:2000], re.DOTALL)
        if root_match:
            root_tag = root_match.group(1)
            if root_tag == "sl":
                doc_type = "subordinate_legislation"
            elif root_tag == "regulation":
                doc_type = "regulation"

        # Extract text: remove XML tags, comments, DOCTYPE, processing instructions
        text = re.sub(r'<!--.*?-->', ' ', xml_str, flags=re.DOTALL)
        text = re.sub(r'<\?.*?\?>', ' ', text)
        text = re.sub(r'<!DOCTYPE[^>]+>', ' ', text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()

        if len(text) > 200:
            return title, text, date, doc_type

        return title, text, date, doc_type

    except Exception as e:
        logger.warning(f"XML parse error: {e}")
        return "", "", None, "unknown"


class QueenslandLegislationScraper(BaseScraper):
    """
    Scraper for AU/QLD-Legislation -- Queensland Legislation.
    Country: AU
    URL: https://www.legislation.qld.gov.au/

    Data types: legislation
    Auth: none (Open Data, CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _discover_ids_from_feeds(self) -> Set[str]:
        """Discover document IDs from Atom feeds."""
        doc_ids = set()

        for feed_url in ATOM_FEEDS:
            logger.info(f"Fetching Atom feed: {feed_url}")
            data = _fetch_url(feed_url, accept="application/atom+xml")
            if not data:
                continue

            text = data.decode("utf-8", errors="replace")

            # Extract IDs from feed links
            # Links look like: /view/html/inforce/current/act-1899-009
            id_matches = re.findall(
                r'/(?:view|browse)/[^"]*?/((?:act|sl|reg)-\d{4}-\d{3,4})',
                text
            )
            for doc_id in id_matches:
                doc_ids.add(doc_id)

            # Also extract from <id> or <link> elements
            id_matches = re.findall(
                r'((?:act|sl)-\d{4}-\d{3,4})', text
            )
            for doc_id in id_matches:
                doc_ids.add(doc_id)

            time.sleep(1)

        logger.info(f"Discovered {len(doc_ids)} document IDs from Atom feeds")
        return doc_ids

    def _enumerate_ids(self) -> Generator[str, None, None]:
        """Systematically enumerate document IDs."""
        # Acts: act-YYYY-NNN where YYYY is ~1860-2026, NNN is 001-200+
        # Start with recent years and well-known acts for better hit rate
        current_year = datetime.now().year

        # Recent years first (more likely to exist)
        for year in range(current_year, 1860, -1):
            for num in range(1, 300):
                yield f"act-{year}-{num:03d}"
            # Subordinate legislation
            for num in range(1, 500):
                yield f"sl-{year}-{num:04d}"

    def _fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single document by ID and return raw data."""
        url = XML_URL_PATTERN.format(doc_id=doc_id)
        data = _fetch_url(url)
        if not data:
            return None

        # Check if we got actual XML content (not an error page)
        if len(data) < 500:
            return None
        if b'<!DOCTYPE html' in data[:200].lower() or b'<html' in data[:200].lower():
            return None

        title, text, date, doc_type = _extract_text_from_xml(data)

        if not text or len(text) < 200:
            return None

        return {
            "doc_id": doc_id,
            "title": title or doc_id,
            "text": text,
            "date": date,
            "doc_type": doc_type,
            "url": f"{BASE_URL}/view/whole/html/inforce/current/{doc_id}",
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": "AU/QLD-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", "act"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all QLD legislation documents."""
        # Phase 1: Feed-discovered documents
        feed_ids = self._discover_ids_from_feeds()
        seen = set()

        for doc_id in sorted(feed_ids):
            if doc_id in seen:
                continue
            seen.add(doc_id)

            doc = self._fetch_document(doc_id)
            if doc:
                yield doc
            time.sleep(1)

        # Phase 2: Systematic enumeration
        logger.info("Starting systematic ID enumeration...")
        miss_streak = 0
        for doc_id in self._enumerate_ids():
            if doc_id in seen:
                continue
            seen.add(doc_id)

            doc = self._fetch_document(doc_id)
            if doc:
                miss_streak = 0
                yield doc
            else:
                miss_streak += 1

            # If we get 50 consecutive misses for a year, skip ahead
            if miss_streak >= 50:
                miss_streak = 0

            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently updated documents from Atom feeds."""
        feed_ids = self._discover_ids_from_feeds()
        for doc_id in sorted(feed_ids):
            doc = self._fetch_document(doc_id)
            if doc:
                yield doc
            time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/QLD-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = QueenslandLegislationScraper()

    if args.command == "test":
        logger.info("Testing XML full text access...")
        doc = scraper._fetch_document("act-1899-009")
        if doc:
            logger.info(f"OK — '{doc['title']}' ({len(doc['text'])} chars)")
        else:
            logger.error("FAILED — could not fetch act-1899-009")
            sys.exit(1)

        logger.info("Testing Atom feed...")
        ids = scraper._discover_ids_from_feeds()
        logger.info(f"OK — discovered {len(ids)} IDs from feeds")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
