#!/usr/bin/env python3
"""
US/CIT -- U.S. Court of International Trade (Slip Opinions)

Fetches the full text of the U.S. Court of International Trade's published
slip opinions -- the decisions of a specialized Article III federal court
with nationwide jurisdiction over civil actions arising out of the customs
and international-trade laws of the United States (antidumping and
countervailing-duty determinations, customs classification and valuation
protests, trade-remedy and enforcement matters, etc.). Each slip opinion
resolves a specific case = case_law, and they are official works of the
U.S. federal government in the public domain (17 U.S.C. 105).

BUILD RECIPE (no auth, no CAPTCHA, builds locally): the court publishes its
opinions in per-year archive pages linked from

  https://www.cit.uscourts.gov/slip-opinions

The main page links each year's archive (mostly ``/content/slip-opinions-{YYYY}``,
but a couple of years use the shorter ``/slip-opinions-{YYYY}`` slug -- the
scraper harvests the links from the page rather than constructing them).
Each year page is a clean HTML table with columns Number, Caption, Date,
Court No., Judge, Jurisdiction; the first cell links the born-digital PDF at
``/sites/cit/files/{YY-NN}.pdf``. The scraper walks every year, parses the
table rows for metadata, downloads each PDF, and extracts full text with the
shared ``common.pdf_extract`` extractor (born-digital opinions have a clean
text layer; the rare scan falls back to OCR). ``record_id`` is the slip-op
number ``{YY-NN}`` (e.g. ``25-160``), which is the natural, unique citation.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import os
import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Make locally-installed OCR tools (tesseract/poppler) discoverable so that
# common.pdf_extract's OCR fallback works for any older scanned opinion.
os.environ["PATH"] = "/opt/homebrew/bin:" + os.environ.get("PATH", "")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CIT")

HOST = "https://www.cit.uscourts.gov"
LIST_PAGE = HOST + "/slip-opinions"

# Some US federal court sites reject requests with no descriptive UA.
UA = "Mozilla/5.0 (compatible; LegalDataHunter/1.0; research; zacharie@goodlegal.fr)"

YEAR_LINK_RE = re.compile(r'href="([^"]*?/slip-opinions?-((?:19|20)\d{2}))"',
                          re.IGNORECASE)
SLIP_RE = re.compile(r"^\s*(\d{2}-\d{1,4})\s*$")


class CITScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.8
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90), allow_redirects=True)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120), stream=True)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _iso(us_date: str) -> str | None:
        """Convert a 'MM/DD/YYYY' cell to ISO 8601, else None."""
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", us_date or "")
        if not m:
            return None
        mo, da, yr = m.group(1), m.group(2), m.group(3)
        try:
            return datetime(int(yr), int(mo), int(da)).strftime("%Y-%m-%d")
        except ValueError:
            return None

    # --------------------------------------------------------- discovery
    def _year_pages(self) -> list[str]:
        html = self._get_text(LIST_PAGE)
        if not html:
            return []
        seen: dict[str, str] = {}
        for href, year in YEAR_LINK_RE.findall(html):
            # Keep newest years first; de-dupe by year (last one wins is fine).
            seen[year] = urljoin(HOST, href)
        # Sort by year descending so sample mode captures recent opinions.
        return [seen[y] for y in sorted(seen, reverse=True)]

    def _rows_from_year(self, url: str) -> Generator[dict, None, None]:
        html = self._get_text(url)
        if not html:
            return
        soup = BeautifulSoup(html, "html.parser")
        for tr in soup.select("table tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            a = tr.find("a", href=lambda x: x and x.lower().endswith(".pdf"))
            if not a:
                continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            number = texts[0]
            m = SLIP_RE.match(number)
            if not m:
                # first cell isn't a slip number (e.g. header) — skip
                continue
            record_id = m.group(1)
            caption = texts[1] if len(texts) > 1 else ""
            date = self._iso(texts[2]) if len(texts) > 2 else None
            court_no = texts[3] if len(texts) > 3 else ""
            judge = texts[4] if len(texts) > 4 else ""
            basis = texts[5] if len(texts) > 5 else ""
            yield {
                "record_id": record_id,
                "caption": caption,
                "date": date,
                "court_no": court_no,
                "judge": judge,
                "jurisdiction_basis": basis,
                "pdf_url": urljoin(HOST, a["href"]),
            }

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for year_url in self._year_pages():
            for rec in self._rows_from_year(year_url):
                key = rec["record_id"].lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                yield rec
                if sample and len(seen) >= 25:
                    logger.info(f"Sample: stopped after {len(seen)} pointers")
                    return
        logger.info(f"Discovered {len(seen)} CIT slip-opinion pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        rid = entry["record_id"]
        if rid in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["pdf_url"])
        if not pdf_bytes:
            return None
        if not pdf_bytes[:5].startswith(b"%PDF"):
            logger.warning(f"{rid}: response is not a PDF — skipping")
            return None
        text = extract_pdf_markdown(
            "US/CIT", rid, pdf_bytes=pdf_bytes, table="case_law")
        if not text or len(text.strip()) < 300:
            logger.warning(f"No usable text for {rid} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        caption = entry.get("caption") or ""
        if caption:
            title = f"{caption} (CIT Slip Op. {rid})"
        else:
            title = f"U.S. Court of International Trade Slip Op. {rid}"
        return {
            "record_id": rid,
            "caption": caption or None,
            "court_no": entry.get("court_no") or None,
            "judge": entry.get("judge") or None,
            "jurisdiction_basis": entry.get("jurisdiction_basis") or None,
            "title": title[:500],
            "text": text,
            "date": entry.get("date"),
            "url": entry["pdf_url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CIT slip-opinions list...")
        try:
            years = self._year_pages()
            if not years:
                logger.error("  No year-archive links discovered")
                return False
            logger.info(f"  Discovered {len(years)} year-archive pages")
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No slip-opinion pointers discovered")
                return False
            logger.info(f"  Discovered {len(entries)} pointers (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 300:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} [{raw['date']}]")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/CIT/{raw['record_id']}",
            "_source": "US/CIT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "court": "U.S. Court of International Trade",
            "caption": raw.get("caption"),
            "court_no": raw.get("court_no"),
            "judge": raw.get("judge"),
            "jurisdiction_basis": raw.get("jurisdiction_basis"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/CIT", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CIT bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CITScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
