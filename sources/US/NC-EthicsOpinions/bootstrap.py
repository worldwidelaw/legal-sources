#!/usr/bin/env python3
"""
US/NC-EthicsOpinions -- North Carolina Ethics Commission -- Formal Advisory
Opinions.

Fetches the full text of the formal advisory opinions of the North Carolina
Ethics Commission construing the State Government Ethics Act (N.C.G.S. Chapter
138A), the legislative ethics provisions and the Lobbying Law (Chapter 120C).
A formal advisory opinion is the Commission's written interpretation of those
statutes, adopted by the Commission and published (with the requester's identity
edited out) = doctrine. Three published series:

  - Formal Ethics Advisory Opinions        (caption "E-YY-NNN")
  - Formal Legislative Advisory Opinions    (caption "L-YY-NNN")
  - Formal Lobbying Advisory Opinions       (caption "LB-YY-NNN")

Access (no JavaScript, no CAPTCHA, no auth):
  The site (ethics.nc.gov) is Drupal. Each series has a listing page

      https://ethics.nc.gov/advisory-opinions/{series-slug}

  whose rows are anchors whose text is the opinion number ("E-15-004") and whose
  href points at the born-digital opinion PDF, either a media entity
  /media/{id}/open or /aos/{slug}/download?attachment. Full text comes from that
  PDF (clean text layer, OCR fallback for the oldest scans).

Strategy:
  For each series, parse the listing page for (opinion-number, pdf-url) anchors,
  dedup by opinion number, download each PDF and extract full text via the shared
  common.pdf_extract._extract backend chain. The year is derived from the opinion
  number; the issue date is parsed from the PDF body when present. All records are
  doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
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
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-EthicsOpinions")

BASE_URL = "https://ethics.nc.gov"

# The three formal-opinion series, with the document label used in the record.
SERIES = [
    ("advisory-opinions/formal-ethics-advisory-opinions", "Formal Ethics Advisory Opinion"),
    ("advisory-opinions/formal-legislative-advisory-opinions", "Formal Legislative Advisory Opinion"),
    ("advisory-opinions/formal-lobbying-advisory-opinions", "Formal Lobbying Advisory Opinion"),
]

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

# Opinion-number captions: E-15-004, L-14-002, LB-16-001.
NUM_RE = re.compile(r"^([A-Z]{1,3})-(\d{2,4})-(\d{1,3})$")
# PDF hrefs on the listing: /media/{id}/open or /aos/{slug}/download...
PDF_HREF_RE = re.compile(r"^/(?:media/\d+/open|aos/[^\"']+/download)", re.I)

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _year_from_yy(token: str) -> Optional[int]:
    if len(token) == 4:
        return int(token)
    yy = int(token)
    return 1900 + yy if yy >= 50 else 2000 + yy


def _iso_from_body(text: str) -> Optional[str]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS.index(m.group(1)) + 1
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class NCEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _parse_series(self, slug: str, label: str) -> list[dict]:
        r = self._get(f"{BASE_URL}/{slug}")
        if r is None or r.status_code != 200:
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse listing")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: list[dict] = []
        for a in soup.find_all("a", href=True):
            caption = _clean(a.get_text(" ", strip=True))
            m = NUM_RE.match(caption)
            if not m:
                continue
            href = a["href"].split("?")[0] if a["href"].startswith("/aos/") else a["href"]
            # keep the query for /aos/ download links (they need ?attachment)
            raw_href = a["href"]
            if not PDF_HREF_RE.match(raw_href.split("?")[0]) and not PDF_HREF_RE.match(raw_href):
                continue
            rows.append({
                "number": caption,
                "prefix": m.group(1),
                "num_year": m.group(2),
                "num_seq": int(m.group(3)),
                "pdf_url": urljoin(BASE_URL, raw_href),
                "doc_type": label,
                "series_url": f"{BASE_URL}/{slug}",
            })
        return rows

    def _collect_index(self) -> list[dict]:
        by_num: dict[str, dict] = {}
        for slug, label in SERIES:
            rows = self._parse_series(slug, label)
            for row in rows:
                by_num.setdefault(row["number"], row)
            logger.info(f"{label}: {len(rows)} opinions ({len(by_num)} distinct so far)")
        ordered = sorted(
            by_num.values(),
            key=lambda r: (_year_from_yy(r["num_year"]) or 0, r["num_seq"]),
            reverse=True,
        )
        logger.info(f"Index collected: {len(ordered)} distinct opinions")
        return ordered

    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["pdf_url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row['number']}: download is not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row['number']}: thin text ({len(text)} chars) — skipped")
            return None
        year = _year_from_yy(row["num_year"])
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_from_body(text) or (f"{year:04d}-01-01" if year else None)
        out["pdf_final_url"] = r.url
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NC Ethics Commission formal advisory opinions...")
        rows = self._parse_series(*SERIES[0])
        if len(rows) < 5:
            logger.error(f"API test FAILED: ethics series too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/NC-EthicsOpinions/{raw['number']}",
            "_source": "US/NC-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": raw["number"],
            "document_type": raw.get("doc_type"),
            "issuer": "North Carolina Ethics Commission",
            "title": f"{raw.get('doc_type')} {raw['number']}",
            "text": raw["text"],
            "url": raw.get("pdf_final_url") or raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NC",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NC-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
