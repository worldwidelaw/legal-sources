#!/usr/bin/env python3
"""
INTL/AU-PAP -- African Union Pan-African Parliament

Fetches resolutions, recommendations, model laws, activity reports,
and hansards from the PAP Open Data Portal (opendata.pap.au.int).

Strategy:
  - Paginate listing pages /doc/{type}?page=N to collect AKN URIs.
  - For each document, fetch HTML and extract full text from the
    <la-akoma-ntoso> tag (Akoma Ntoso markup rendered by PeachJam).
  - Extract metadata (title, date, language) from the HTML.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # High-throughput full pull (VPS)
  python bootstrap.py update             # Re-scan listing
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html as html_mod
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.AU-PAP")

BASE = "https://opendata.pap.au.int"

DOC_TYPES = [
    ("resolution", "legislation"),
    ("recommendation", "doctrine"),
    ("model-law", "legislation"),
    ("activity-report", "doctrine"),
    ("hansard", "doctrine"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
}

MIN_TEXT_CHARS = 200


class AUPAPScraper(BaseScraper):
    """
    Scraper for INTL/AU-PAP.
    Country: INTL
    URL: https://opendata.pap.au.int/
    Data types: legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── listing ────────────────────────────────────────────────────
    def _list_docs(self, doc_type: str, max_pages: int = 20) -> list[str]:
        """Paginate /doc/{doc_type}?page=N and return AKN URIs."""
        all_uris: list[str] = []
        seen: set[str] = set()
        for page in range(1, max_pages + 1):
            url = f"{BASE}/doc/{doc_type}?page={page}"
            try:
                r = self.session.get(url, timeout=60)
                if r.status_code == 404:
                    break
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Listing {doc_type} page {page} failed: {e}")
                break

            links = re.findall(r'href="(/akn/[^"]+)"', r.text)
            new_links = []
            for link in links:
                if link not in seen:
                    seen.add(link)
                    new_links.append(link)
                    all_uris.append(link)

            logger.info(f"  {doc_type} page {page}: {len(new_links)} new URIs")
            if not new_links:
                break
            time.sleep(1.5)
        else:
            # Loop ran to max_pages without an empty page, so the listing may
            # hold more. Say so rather than reporting a truncated corpus as
            # the whole thing.
            logger.warning(
                f"{doc_type}: hit the {max_pages}-page cap while still finding new "
                f"URIs — listing may be truncated, raise max_pages"
            )

        logger.info(f"Collected {len(all_uris)} {doc_type} URIs")
        return all_uris

    # ── document fetch ─────────────────────────────────────────────
    def _fetch_document(self, akn_uri: str) -> Optional[dict]:
        """Fetch a single document and extract full text + metadata."""
        url = BASE + akn_uri
        try:
            r = self.session.get(url, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Document fetch failed {akn_uri}: {e}")
            return None

        html = r.text

        # Extract title from <title> tag
        title = ""
        m = re.search(r"<title>\s*(.*?)\s*</title>", html, re.DOTALL)
        if m:
            title = m.group(1).strip()
            # Remove site suffix
            title = re.sub(r"\s*[-–—]\s*PAP Open Data Portal\s*$", "", title).strip()
            title = html_mod.unescape(title)

        # Extract full text from <la-akoma-ntoso> tag
        text = ""
        m = re.search(r"<la-akoma-ntoso[^>]*>(.*?)</la-akoma-ntoso>", html, re.DOTALL)
        if m:
            content = m.group(1)
            text = re.sub(r"<[^>]+>", " ", content)
            text = html_mod.unescape(text)
            text = re.sub(r"\s+", " ", text).strip()

        if len(text) < MIN_TEXT_CHARS:
            logger.info(f"  insufficient text ({len(text)} chars) for {akn_uri}")
            return None

        # Extract date from AKN URI pattern: ...@YYYY-MM-DD
        date = None
        dm = re.search(r"@(\d{4}-\d{2}-\d{2})$", akn_uri)
        if dm:
            date = dm.group(1)

        # Determine doc type from URI path
        doc_type_str = "doctrine"
        if "/resolution/" in akn_uri or "/model-law/" in akn_uri:
            doc_type_str = "legislation"

        return {
            "akn_uri": akn_uri,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_type": doc_type_str,
        }

    # ── normalize ──────────────────────────────────────────────────
    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None

        akn_uri = raw.get("akn_uri", "")
        title = raw.get("title", "").strip()
        _type = raw.get("doc_type", "doctrine")

        _id = "AU-PAP-" + re.sub(r"[^0-9A-Za-z]+", "-", akn_uri).strip("-")

        return {
            "_id": _id,
            "_source": "INTL/AU-PAP",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "akn_uri": akn_uri,
            "institution": "Pan-African Parliament",
            "jurisdiction": "African Union",
        }

    # ── fetch ──────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        total_yielded = 0
        for doc_type, default_type in DOC_TYPES:
            uris = self._list_docs(doc_type)
            yielded = 0
            for i, uri in enumerate(uris):
                logger.info(f"[{doc_type} {i+1}/{len(uris)}] {uri}")
                doc = self._fetch_document(uri)
                if not doc:
                    time.sleep(1)
                    continue
                if "doc_type" not in doc or not doc["doc_type"]:
                    doc["doc_type"] = default_type
                yield doc
                yielded += 1
                total_yielded += 1
                logger.info(f"  yielded ({len(doc['text'])} chars)")
                time.sleep(1.5)
            logger.info(f"{doc_type}: {yielded}/{len(uris)} with full text")
        logger.info(f"Total yielded: {total_yielded}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/AU-PAP fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of samples")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    bf = sub.add_parser("bootstrap-fast", help="High-throughput full fetch (VPS)")
    bf.add_argument("--full", action="store_true", default=True)

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = AUPAPScraper()

    if args.command == "test":
        for doc_type, _ in DOC_TYPES[:2]:
            uris = scraper._list_docs(doc_type, max_pages=1)
            if uris:
                doc = scraper._fetch_document(uris[0])
                if doc:
                    logger.info(f"OK: {doc_type} — {doc['title'][:80]} ({len(doc['text'])} chars)")
                else:
                    logger.warning(f"FAIL: could not fetch {uris[0]}")
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=getattr(args, "sample", False),
            sample_size=getattr(args, "sample_size", 15),
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command in ("bootstrap-fast", "bootstrap_fast"):
        stats = scraper.bootstrap_fast()
        logger.info(f"Fast bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
