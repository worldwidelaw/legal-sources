#!/usr/bin/env python3
"""
US/MA-AGOpinions -- Opinions of the Attorney General of Massachusetts (doctrine)

Fetches the FULL TEXT of the official opinions of the Massachusetts Attorney
General — the AG's authoritative (advisory) interpretations of Massachusetts law,
issued to state officers, boards and the legislature. From 1832 through 2006 the
opinions were published in the AG's *annual report*; an early run was also issued
as the bound multi-year series "Official opinions of the Attorney-General of the
Commonwealth of Massachusetts" (Volumes 1-8, 1891-1929). Both are digitized,
full-text, by the State Library of Massachusetts.

Source / access
---------------
State Library of Massachusetts DSpace 7 REST API at
``https://archives.lib.state.ma.us/server/api`` (open, no auth, no WAF) — the
same repository used by US/MA-SessionLaws.

Enumeration: every AG-opinion item carries the LCSH subject
``Attorneys general's opinions -- Massachusetts.`` (181 items: 173 annual
reports 1832-2006 + 8 bound opinion volumes). The Discover search endpoint
``/discover/search/objects?query=dc.subject.lcsh:"Attorneys general's opinions
-- Massachusetts."`` returns the whole set, well under DSpace's deep-paging
ceiling, so no year-partitioning is needed. ``embed=bundles/bitstreams`` returns
each item's files inline so the text-bitstream URL is known without a per-item
round-trip.

Full text: each item exposes a DSpace-extracted plain-text bitstream in its
``TEXT`` bundle (with an occasional ``.txt`` sibling in ORIGINAL). Most volumes
are OCR of print (readable, period-typical OCR noise); recent reports are clean.
The text bitstream is downloaded directly — no local PDF/OCR step.

Usage:
  python3 bootstrap.py bootstrap            # Full pull (all opinions)
  python3 bootstrap.py bootstrap --sample   # Sample (newest first)
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
logger = logging.getLogger("legal-data-hunter.US.MA-AGOpinions")

SOURCE_ID = "US/MA-AGOpinions"
API_BASE = "https://archives.lib.state.ma.us/server/api"
# LCSH subject shared by every AG-opinion item (annual reports + bound volumes).
SUBJECT = "Attorneys general's opinions -- Massachusetts."

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# Months used in the "year ending <Month> DD, YYYY" report titles.
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


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


def parse_date(title: str, issued: Optional[str]) -> Optional[str]:
    """Derive an ISO date from the item title or dc.date.issued.

    Annual reports: "Report of the attorney general for the year ending
    December 31, 1862" -> 1862-12-31.
    Bound volumes: "... Volume 7 (1923-1925)" -> 1923-01-01.
    Falls back to <issued>-01-01.
    """
    if title:
        m = re.search(
            r"year ending\s+([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})", title, re.I
        )
        if m:
            mon = _MONTHS.get(m.group(1).lower())
            day = int(m.group(2))
            yr = m.group(3)
            if mon and 1 <= day <= 31:
                return f"{yr}-{mon:02d}-{day:02d}"
        m = re.search(r"\((\d{4})\s*-\s*\d{4}\)", title)
        if m:
            return f"{m.group(1)}-01-01"
    if issued and re.fullmatch(r"\d{4}", str(issued)):
        return f"{issued}-01-01"
    return None


class MAAGOpinionsScraper(BaseScraper):

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

    def _all_items(self) -> Generator[dict, None, None]:
        """Yield raw indexable item dicts for the AG-opinions subject."""
        subj = SUBJECT.replace(" ", "%20").replace("'", "%27")
        page = 0
        size = 100
        while True:
            url = (
                f"{API_BASE}/discover/search/objects?"
                f"query=dc.subject.lcsh:%22{subj}%22"
                f"&dsoType=item&size={size}&page={page}"
                f"&sort=dc.date.issued,DESC"
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
        if not text or len(text) < 200:
            return None
        issued = _meta_first(md, "dc.date.issued")
        handle = _meta_first(md, "dc.identifier.uri")
        uuid = indexable.get("uuid")
        if handle:
            ident = handle.rstrip("/").split("/")[-1]
        else:
            ident = uuid
        url = handle or f"https://archives.lib.state.ma.us/items/{uuid}"
        return {
            "ident": ident,
            "title": title.strip(),
            "text": text,
            "date": parse_date(title, issued),
            "url": url,
            "year": issued,
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"{SOURCE_ID}/{raw['ident']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MA",
        }

    # ── Iteration ─────────────────────────────────────────────────────
    def fetch_all(self) -> Generator[dict, None, None]:
        count = 0
        for indexable in self._all_items():
            raw = self._build_raw(indexable)
            if raw:
                count += 1
                yield raw
        logger.info(f"Emitted {count} AG-opinion documents with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing Massachusetts AG Opinions DSpace API...")
        try:
            emitted = 0
            for indexable in self._all_items():
                raw = self._build_raw(indexable)
                if raw:
                    logger.info(
                        f"  Full text OK: {raw['title'][:60]} "
                        f"({len(raw['text'])} chars) date={raw['date']}"
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

    parser = argparse.ArgumentParser(description="US/MA-AGOpinions bootstrap")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MAAGOpinionsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
