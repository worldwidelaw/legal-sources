#!/usr/bin/env python3
"""
US/MA-SessionLaws -- Massachusetts Session Laws (Acts & Resolves), 1692-present

Fetches the FULL TEXT of the Massachusetts session laws — the chronological
Acts and Resolves enacted by the General Court of Massachusetts since 1692 —
from the State Library of Massachusetts' open DSpace repository.

This is distinct from US/MA-Legislation, which covers only the *current*
consolidated General Laws (malegislature.gov). The session laws are the
as-enacted chapter-by-chapter record (each "Chap. NNNN. An Act ..." / "Resolve
..." is one document), a far larger historical legislation corpus.

Source / access
---------------
State Library of Massachusetts DSpace 7 REST API at
``https://archives.lib.state.ma.us/server/api`` (open, no auth, no WAF).

Enumeration: the Discover search endpoint
``/discover/search/objects?query=dc.subject.lcsh:"Session Laws - Massachusetts"``
returns ~108k items. To stay well under DSpace's deep-pagination ceiling the
scan is partitioned by year (``AND dc.date.issued:YYYY``), each year paged at
size 100. ``embed=bundles/bitstreams`` returns each item's files inline so the
text bitstream URL is known without an extra round-trip per item.

Full text: each item carries a DSpace-extracted plain-text bitstream in its
``TEXT`` bundle (and frequently an ``ORIGINAL`` ``.txt`` sibling). Modern acts
(roughly 1990s-2010) are born-digital and extract cleanly; older volumes are
OCR of print (readable, with period-typical OCR noise). The text bitstream is
downloaded directly — no local PDF/OCR step needed.

Usage:
  python3 bootstrap.py bootstrap            # Full pull (all years)
  python3 bootstrap.py bootstrap --sample   # Sample (newest acts first)
  python3 bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python3 bootstrap.py test-api             # Connectivity / extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MA-SessionLaws")

SOURCE_ID = "US/MA-SessionLaws"
API_BASE = "https://archives.lib.state.ma.us/server/api"
SUBJECT = "Session Laws - Massachusetts"
OLDEST_YEAR = 1692
# Upper bound for the year scan; harmless if a year has no items.
NEWEST_YEAR_GUESS = 2015

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)


def clean_text(text: str) -> str:
    """Normalize whitespace; strip OCR (cid:N) artefacts and stray control chars."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\(cid:\d+\)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _meta_first(md: dict, key: str) -> Optional[str]:
    vals = md.get(key) or []
    if vals and isinstance(vals, list):
        return vals[0].get("value")
    return None


def parse_title(title: str) -> tuple[Optional[str], Optional[str]]:
    """Return (year, chapter-number-as-string) parsed from the item title.

    e.g. "2010 Chap. 0292. An Act Relative To Inhalant Abuse." -> ("2010", "0292")
    """
    year = None
    chap = None
    ym = re.match(r"\s*(\d{4})", title or "")
    if ym:
        year = ym.group(1)
    cm = re.search(r"Chap\.?\s*0*(\d+)", title or "", re.I)
    if cm:
        chap = cm.group(1)
    return year, chap


class MASessionLawsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={"User-Agent": _UA, "Accept": "application/json,*/*;q=0.8"},
            timeout=90,
        )
        self.delay = 0.5

    # ── HTTP helpers ──────────────────────────────────────────────────
    def _get_json(self, url: str, retries: int = 3) -> Optional[dict]:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"JSON fetch error {url} (try {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_text(self, url: str, retries: int = 3) -> Optional[str]:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    return None
            except Exception as e:
                logger.warning(f"text fetch error {url} (try {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ── DSpace traversal ─────────────────────────────────────────────
    @staticmethod
    def _best_text_href(indexable: dict) -> Optional[str]:
        """Pick the largest non-empty text bitstream from embedded bundles.

        Prefers the TEXT bundle; falls back to ``.txt`` files in ORIGINAL.
        """
        bundles = (
            indexable.get("_embedded", {})
            .get("bundles", {})
            .get("_embedded", {})
            .get("bundles", [])
        )
        text_candidates = []   # (size, href) from TEXT bundle
        orig_candidates = []   # (size, href) from ORIGINAL .txt
        for b in bundles:
            name = b.get("name")
            bitstreams = (
                b.get("_embedded", {})
                .get("bitstreams", {})
                .get("_embedded", {})
                .get("bitstreams", [])
            )
            for x in bitstreams:
                size = x.get("sizeBytes") or 0
                href = x.get("_links", {}).get("content", {}).get("href")
                bname = (x.get("name") or "").lower()
                if not href or size <= 0:
                    continue
                if name == "TEXT":
                    text_candidates.append((size, href))
                elif name == "ORIGINAL" and bname.endswith(".txt"):
                    orig_candidates.append((size, href))
        pool = text_candidates or orig_candidates
        if not pool:
            return None
        pool.sort(reverse=True)
        return pool[0][1]

    def _year_items(self, year: int) -> Generator[dict, None, None]:
        """Yield raw item dicts (with resolved text href) for one year."""
        page = 0
        size = 100
        while True:
            url = (
                f"{API_BASE}/discover/search/objects?"
                f"query=dc.subject.lcsh:%22{SUBJECT.replace(' ', '%20')}%22"
                f"%20AND%20dc.date.issued:{year}"
                f"&dsoType=item&size={size}&page={page}"
                f"&embed=bundles/bitstreams"
            )
            data = self._get_json(url)
            if not data:
                return
            res = data.get("_embedded", {}).get("searchResult", {})
            objs = res.get("_embedded", {}).get("objects", [])
            if not objs:
                return
            for o in objs:
                r = o.get("_embedded", {}).get("indexableObject", {})
                yield r
            pageinfo = res.get("page", {})
            total_pages = pageinfo.get("totalPages", 0)
            page += 1
            if page >= total_pages:
                return

    def _build_raw(self, indexable: dict) -> Optional[dict]:
        md = indexable.get("metadata", {})
        title = indexable.get("name") or _meta_first(md, "dc.title")
        if not title:
            return None
        href = self._best_text_href(indexable)
        if not href:
            return None
        text = clean_text(self._get_text(href) or "")
        if not text or len(text) < 120:
            return None
        year, chap = parse_title(title)
        issued = _meta_first(md, "dc.date.issued") or year
        handle = _meta_first(md, "dc.identifier.uri")
        uuid = indexable.get("uuid")
        if handle:
            ident = handle.rstrip("/").split("/")[-1]
        else:
            ident = uuid
        url = handle or f"https://archives.lib.state.ma.us/items/{uuid}"
        date = f"{issued}-01-01" if issued and re.fullmatch(r"\d{4}", str(issued)) else None
        return {
            "ident": ident,
            "title": title.strip(),
            "text": text,
            "date": date,
            "url": url,
            "year": issued,
            "chapter": chap,
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"{SOURCE_ID}/{raw['ident']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MA",
        }

    # ── Iteration ─────────────────────────────────────────────────────
    def _newest_year(self) -> int:
        url = (
            f"{API_BASE}/discover/search/objects?"
            f"query=dc.subject.lcsh:%22{SUBJECT.replace(' ', '%20')}%22"
            f"&dsoType=item&sort=dc.date.issued,DESC&size=1"
        )
        data = self._get_json(url)
        try:
            o = data["_embedded"]["searchResult"]["_embedded"]["objects"][0]
            r = o["_embedded"]["indexableObject"]
            y = _meta_first(r.get("metadata", {}), "dc.date.issued")
            return int(y)
        except Exception:
            return NEWEST_YEAR_GUESS

    def fetch_all(self) -> Generator[dict, None, None]:
        newest = self._newest_year()
        for year in range(newest, OLDEST_YEAR - 1, -1):
            count = 0
            for indexable in self._year_items(year):
                raw = self._build_raw(indexable)
                if raw:
                    count += 1
                    yield raw
            if count:
                logger.info(f"Year {year}: {count} session-law documents")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing Massachusetts Session Laws DSpace API...")
        try:
            newest = self._newest_year()
            logger.info(f"  Newest session-law year: {newest}")
            emitted = 0
            for indexable in self._year_items(newest):
                raw = self._build_raw(indexable)
                if raw:
                    logger.info(
                        f"  Full text OK: {raw['title'][:60]} "
                        f"({len(raw['text'])} chars)"
                    )
                    emitted += 1
                    if emitted >= 3:
                        logger.info("API test PASSED")
                        return True
            logger.error("  No usable full-text documents extracted")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MA-SessionLaws bootstrap")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MASessionLawsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
