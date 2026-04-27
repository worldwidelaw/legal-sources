#!/usr/bin/env python3
"""
AU/TAS-Legislation -- Tasmania Legislation Fetcher

Fetches Tasmanian Acts and Statutory Rules from legislation.tas.gov.au.

Strategy:
  - Index documents via JSON projectdata API (year-by-year enumeration)
  - Fetch full text HTML from /view/whole/html/inforce/current/{id}
  - Extract text from HTML content div
  - No auth required; CC BY 4.0 license

Data:
  - Tasmanian Acts (from 1839) and Statutory Rules
  - Full text in HTML
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Check Atom feed for updates
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.TAS-Legislation")

BASE_URL = "https://www.legislation.tas.gov.au"
PROJECTDATA_URL = f"{BASE_URL}/projectdata"
HTML_URL_PATTERN = f"{BASE_URL}/view/whole/html/inforce/current/{{doc_id}}"

# Document types to fetch
DOC_TYPES = [
    ("act.reprint", "act"),      # Acts
    ("reprint", "sr"),           # Statutory Rules
]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "text/html, application/json",
}

# Year range for enumeration
START_YEAR = 1839
CURRENT_YEAR = datetime.now().year


def _fetch_url(url: str, timeout: int = 60) -> Optional[bytes]:
    """Fetch a URL with error handling."""
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except (URLError, HTTPError) as e:
        logger.debug(f"Failed to fetch {url}: {e}")
        return None


def _extract_text_from_html(html_bytes: bytes) -> str:
    """Extract legislation text from Tasmania HTML page."""
    try:
        html_str = html_bytes.decode("utf-8", errors="replace")
    except Exception:
        html_str = html_bytes.decode("latin-1", errors="replace")

    # Find the 'content' div which contains the legislation body
    content_match = re.search(r'<div\s+class="content">(.*)', html_str, re.DOTALL)
    if not content_match:
        # Fallback: try body
        content_match = re.search(r'<body[^>]*>(.*?)</body>', html_str, re.DOTALL)
    if not content_match:
        return ""

    text = content_match.group(1)

    # Remove scripts, styles, nav elements
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<nav[^>]*>.*?</nav>', ' ', text, flags=re.DOTALL)
    # Remove footer/copyright area
    text = re.sub(r'<footer[^>]*>.*?</footer>', ' ', text, flags=re.DOTALL)
    # Remove all HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = html_mod.unescape(text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Remove trailing copyright notice
    copyright_idx = text.find("©The State of Tasmania")
    if copyright_idx > 0:
        text = text[:copyright_idx].strip()

    # Remove trailing disclaimer text
    disclaimer_idx = text.find("The URL of this page may change")
    if disclaimer_idx > 0:
        text = text[:disclaimer_idx].strip()
    disclaimer_idx = text.find("This version is not published under")
    if disclaimer_idx > 0:
        text = text[:disclaimer_idx].strip()

    return text


def _get_val(obj: Any) -> str:
    """Extract value from Tasmania's JSON format (handles UniString wrapper)."""
    if isinstance(obj, dict) and "__value__" in obj:
        return obj["__value__"]
    if isinstance(obj, str):
        return obj
    return str(obj) if obj else ""


class TasmaniaLegislationScraper(BaseScraper):
    """
    Scraper for AU/TAS-Legislation -- Tasmania Legislation.
    Country: AU
    URL: https://www.legislation.tas.gov.au/

    Data types: legislation
    Auth: none (Open Data, CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_index(self, print_type: str, year: int) -> List[Dict[str, Any]]:
        """Fetch the index for a given type and year."""
        pit = datetime.now().strftime("%Y%m%d%H%M%S")
        expression = (
            f"PrintType={print_type}+AND+Year={year}?+"
            f"AND+PitValid=@pointInTime({pit})"
        )
        url = (
            f"{PROJECTDATA_URL}?ds=EnAct-BrowseDataSource"
            f"&start=1&count=5000"
            f"&sortField=sort.title&sortDirection=asc"
            f"&expression={quote(expression, safe='+@()=?')}"
            f"&collection="
        )

        data = _fetch_url(url)
        if not data:
            return []

        try:
            result = json.loads(data)
            entries = result.get("data", [])
            # API returns a single dict when only 1 result, list when multiple
            if isinstance(entries, dict):
                entries = [entries]
            return entries if isinstance(entries, list) else []
        except json.JSONDecodeError:
            return []

    def _fetch_document(self, doc_id: str, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch a single document by ID and return raw data."""
        url = HTML_URL_PATTERN.format(doc_id=doc_id)
        html_data = _fetch_url(url, timeout=90)
        if not html_data:
            return None

        text = _extract_text_from_html(html_data)
        if not text or len(text) < 100:
            return None

        title = _get_val(metadata.get("title", ""))
        date = None
        assent = metadata.get("assent.date")
        if assent and isinstance(assent, str):
            date = assent[:10]  # Extract YYYY-MM-DD from ISO datetime
        if not date:
            pub = metadata.get("publication.date")
            if pub and isinstance(pub, str):
                date = pub[:10]

        return {
            "doc_id": doc_id,
            "title": title or doc_id,
            "text": text,
            "date": date,
            "year": _get_val(metadata.get("year", "")),
            "number": _get_val(metadata.get("no", "")),
            "doc_type": _get_val(metadata.get("type", "")),
            "repealed": _get_val(metadata.get("repealed", "N")),
            "url": url,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": "AU/TAS-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", ""),
            "year": raw.get("year"),
            "number": raw.get("number"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all Tasmanian legislation documents."""
        seen = set()

        for print_type, id_prefix in DOC_TYPES:
            logger.info(f"Indexing {print_type} documents...")

            for year in range(CURRENT_YEAR, START_YEAR - 1, -1):
                entries = self._fetch_index(print_type, year)
                if not entries:
                    continue

                logger.info(f"  {year}: {len(entries)} {print_type} entries")

                for entry in entries:
                    doc_id = _get_val(entry.get("id", ""))
                    if not doc_id or doc_id in seen:
                        continue

                    # Skip repealed legislation
                    if _get_val(entry.get("repealed", "N")) == "Y":
                        continue

                    seen.add(doc_id)

                    doc = self._fetch_document(doc_id, entry)
                    if doc:
                        yield doc

                    time.sleep(1)

                time.sleep(0.5)

        logger.info(f"Total unique documents: {len(seen)}")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently updated documents from Atom feed."""
        feed_url = f"{BASE_URL}/feed?id=crawler"
        data = _fetch_url(feed_url)
        if not data:
            return

        text = data.decode("utf-8", errors="replace")
        # Extract document IDs from feed links
        id_matches = re.findall(
            r'/(?:view|browse)/[^"]*?/((?:act|sr)-\d{4}-\d{3,4})', text
        )

        seen = set()
        for doc_id in id_matches:
            if doc_id in seen:
                continue
            seen.add(doc_id)

            doc = self._fetch_document(doc_id, {"title": "", "id": doc_id})
            if doc:
                yield doc
            time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/TAS-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TasmaniaLegislationScraper()

    if args.command == "test":
        logger.info("Testing JSON index API...")
        entries = scraper._fetch_index("act.reprint", 2020)
        if entries:
            logger.info(f"OK — {len(entries)} acts found for 2020")
            doc_id = _get_val(entries[0].get("id", ""))
            title = _get_val(entries[0].get("title", ""))
            logger.info(f"First: {doc_id} — {title}")

            logger.info("Testing HTML full text...")
            doc = scraper._fetch_document(doc_id, entries[0])
            if doc:
                logger.info(f"OK — '{doc['title']}' ({len(doc['text'])} chars)")
            else:
                logger.error("FAILED — could not fetch full text")
                sys.exit(1)
        else:
            logger.error("FAILED — no index results")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
