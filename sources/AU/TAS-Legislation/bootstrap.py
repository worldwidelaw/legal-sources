#!/usr/bin/env python3
"""
AU/TAS-Legislation -- Tasmania Legislation Fetcher

Fetches Tasmanian Acts and Statutory Rules from legislation.tas.gov.au.

Strategy:
  - Index documents via the JSON projectdata API (the same EnAct-BrowseDataSource
    the site's own /browse pages call), paginated with start/count
  - Fetch full text HTML from /view/whole/html/inforce/current/{id}
  - Extract text from the page's content div
  - No auth required; CC BY 4.0 license

Notes:
  The index API rejects `sortField=sort.title` ("E3701-AS: The sort operation
  failed / Cannot find a sort index"), so no sort is requested — paging is done
  purely with start/count, which the datasource returns in a stable order.
  Full text is downloaded inside normalize() so bootstrap_fast() can overlap
  downloads across worker threads.

Data:
  - In-force Tasmanian Acts (from 1839) and Statutory Rules
  - Full text in HTML
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull -> data/records.jsonl
  python bootstrap.py update             # Recently published documents
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

# (PrintType value in the datasource, human label)
DOC_TYPES = [
    ("act.reprint", "act"),      # Acts (consolidated reprints)
    ("reprint", "sr"),           # Statutory Rules (consolidated reprints)
]

# The datasource truncates large JSON responses (urllib raises IncompleteRead
# at ~65 KB), so keep pages small enough to always come back whole.
PAGE_SIZE = 50

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "text/html, application/json",
}


def _fetch_url(url: str, timeout: int = 60, attempts: int = 3) -> Optional[bytes]:
    """Fetch a URL with retries."""
    for attempt in range(attempts):
        req = Request(url, headers=HEADERS)
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except HTTPError as e:
            if e.code in (404, 410):
                return None
            logger.debug(f"HTTP {e.code} for {url} (attempt {attempt + 1})")
        except URLError as e:
            logger.debug(f"Failed to fetch {url}: {e} (attempt {attempt + 1})")
        except Exception as e:  # socket timeouts, incomplete reads
            logger.debug(f"Error fetching {url}: {e} (attempt {attempt + 1})")
        if attempt < attempts - 1:
            time.sleep(2 * (attempt + 1))
    return None


def _extract_text_from_html(html_bytes: bytes) -> str:
    """Extract legislation text from a Tasmania HTML page."""
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
        return obj["__value__"] or ""
    if isinstance(obj, str):
        return obj
    return str(obj) if obj else ""


def _point_in_time() -> str:
    """Server-time style 14-digit stamp used by the PitValid predicate."""
    return datetime.now().strftime("%Y%m%d%H%M%S")


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

    # ── Index ─────────────────────────────────────────────────────────

    def _index_url(self, print_type: str, start: int, count: int) -> str:
        expression = (
            f"PrintType={print_type} AND Repealed<>Y "
            f"AND PitValid=@pointInTime({_point_in_time()})"
        )
        return (
            f"{PROJECTDATA_URL}?ds=EnAct-BrowseDataSource"
            f"&start={start}&count={count}"
            f"&expression={quote(expression, safe='@()=<>')}"
            f"&collection="
        )

    def _fetch_index_page(
        self, print_type: str, start: int, count: int = PAGE_SIZE
    ) -> tuple:
        """Fetch one index page. Returns (entries, total_count)."""
        data = _fetch_url(self._index_url(print_type, start, count))
        if not data:
            return [], 0

        body = data.decode("utf-8", errors="replace")
        try:
            result = json.loads(body)
        except json.JSONDecodeError:
            logger.warning(
                f"Index for {print_type} start={start} was not JSON: {body[:200]!r}"
            )
            return [], 0

        total = result.get("totalCount")
        if isinstance(total, dict):
            total = total.get("__value__", 0)
        try:
            total = int(total or 0)
        except (TypeError, ValueError):
            total = 0

        entries = result.get("data", [])
        # API returns a single dict when only 1 result, list when multiple
        if isinstance(entries, dict):
            entries = [entries]
        if not isinstance(entries, list):
            entries = []
        return entries, total

    def _iter_index(self, print_type: str) -> Generator[Dict[str, Any], None, None]:
        """Page through the whole index for a PrintType."""
        start = 1
        total = None
        while True:
            entries, page_total = self._fetch_index_page(print_type, start)
            if total is None:
                total = page_total
                logger.info(f"{print_type}: {total} documents in index")
            if not entries:
                break
            for entry in entries:
                yield entry
            start += len(entries)
            if total and start > total:
                break
            time.sleep(0.5)

    # ── Fetch ─────────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield index metadata for all in-force Tasmanian legislation."""
        seen = set()
        for print_type, label in DOC_TYPES:
            logger.info(f"Indexing {print_type} ({label}) documents...")
            for entry in self._iter_index(print_type):
                doc_id = _get_val(entry.get("id", ""))
                if not doc_id or doc_id in seen:
                    continue
                if _get_val(entry.get("repealed", "N")) == "Y":
                    continue
                seen.add(doc_id)
                entry["_doc_id"] = doc_id
                entry["_label"] = label
                yield entry
        logger.info(f"Total unique documents indexed: {len(seen)}")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Yield documents published/reprinted on or after `since`."""
        cutoff = since.date().isoformat()
        for entry in self.fetch_all():
            pub = entry.get("publication.date") or entry.get("first.valid.date") or ""
            if isinstance(pub, str) and pub[:10] >= cutoff:
                yield entry

    # ── Normalize (downloads full text) ───────────────────────────────

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch the full text for an indexed document and normalize it."""
        doc_id = raw.get("_doc_id") or _get_val(raw.get("id", ""))
        if not doc_id:
            return None

        url = HTML_URL_PATTERN.format(doc_id=doc_id)
        html_data = _fetch_url(url, timeout=90)
        if not html_data:
            return None

        text = _extract_text_from_html(html_data)
        if not text or len(text) < 100:
            logger.debug(f"No usable text for {doc_id} ({len(text)} chars)")
            return None

        date = None
        for key in ("assent.date", "publication.date", "first.valid.date"):
            val = raw.get(key)
            if isinstance(val, str) and len(val) >= 10:
                date = val[:10]
                break

        title = _get_val(raw.get("title", "")) or doc_id

        return {
            "_id": doc_id,
            "_source": "AU/TAS-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "doc_type": _get_val(raw.get("type", "")) or raw.get("_label", ""),
            "year": _get_val(raw.get("year", "")),
            "number": _get_val(raw.get("no", "")),
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/TAS-Legislation data fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"]
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TasmaniaLegislationScraper()

    if args.command == "test":
        logger.info("Testing JSON index API...")
        entries, total = scraper._fetch_index_page("act.reprint", 1, 5)
        if entries:
            logger.info(f"OK — {total} in-force acts indexed")
            entries[0]["_doc_id"] = _get_val(entries[0].get("id", ""))
            logger.info(f"First: {entries[0]['_doc_id']} — {_get_val(entries[0].get('title'))}")

            logger.info("Testing HTML full text...")
            doc = scraper.normalize(entries[0])
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

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
