#!/usr/bin/env python3
"""
US/PBGC-OpinionLetters -- Pension Benefit Guaranty Corporation Opinion Letters

Fetches the full text of the published Opinion Letters of the Pension
Benefit Guaranty Corporation (PBGC), the U.S. federal agency that
administers Title IV of the Employee Retirement Income Security Act of
1974 (ERISA) and insures private-sector defined-benefit pension plans.
PBGC Opinion Letters are the agency's formal, published interpretive
determinations -- e.g. whether a particular plan is covered under Title
IV, how the guarantee and premium provisions apply, and final decisions
on reconsideration requests. They are the PBGC counterpart to DOL/IRS
advisory opinions and are the agency's authoritative interpretive
guidance = doctrine. They are official works of the U.S. federal
government and are in the public domain (17 U.S.C. 105).

BUILD RECIPE (no auth, no CAPTCHA, builds locally): the PBGC publishes
the entire Opinion Letter series on one server-rendered database page,

  https://www.pbgc.gov/employers-practitioners/legal-resources/opinion-letters/database

whose HTML links every letter as
  /documents/opinion-letter-{YY}-{NNN}
Each of those /documents/ URLs returns the letter's PDF directly (the
path content-negotiates to application/pdf, not an HTML wrapper). The
scraper reads the database page, collects every opinion-letter id, then
downloads each PDF and extracts full text with the shared
``common.pdf_extract`` extractor (older born-digital PDFs extract a clean
text layer; the rare scanned copy falls back to OCR). ``record_id`` is
the letter id (e.g. ``opinion-letter-96-001``), which is stable and
unique. The letter number (e.g. 96-1) and the decision date are parsed
from the PDF body; the year is derived from the id as a fallback.

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

import requests

# Make locally-installed OCR tools (tesseract/poppler) discoverable so that
# common.pdf_extract's OCR fallback works for any scanned Opinion Letter.
os.environ["PATH"] = "/opt/homebrew/bin:" + os.environ.get("PATH", "")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.PBGC-OpinionLetters")

HOST = "https://www.pbgc.gov"
INDEX_PAGE = HOST + "/employers-practitioners/legal-resources/opinion-letters/database"
DOC_URL = HOST + "/documents/{id}"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

ID_RE = re.compile(r"/documents/(opinion-letter-\d{2}-\d{3})", re.IGNORECASE)
# A full "Month DD, YYYY" date near the top of the letter.
MONTHS = ("January February March April May June July August September "
          "October November December").split()
MONTH_MAP = {m.lower(): i + 1 for i, m in enumerate(MONTHS)}
LONGDATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),\s+(\d{4})\b", re.IGNORECASE
)
# Letter number as printed in the body, e.g. "96-1", "94-8".
NUMBER_RE = re.compile(r"\b(\d{2}-\d{1,3})\b")


class PBGCOpinionScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.7
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
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
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @classmethod
    def _iso_from_body(cls, text: str) -> str | None:
        m = LONGDATE_RE.search(text or "")
        if not m:
            return None
        mm = MONTH_MAP[m.group(1).lower()]
        dd = int(m.group(2))
        yy = int(m.group(3))
        if 1 <= dd <= 31 and 1970 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _year_from_id(letter_id: str) -> int | None:
        m = re.search(r"opinion-letter-(\d{2})-\d{3}", letter_id)
        if not m:
            return None
        yy = int(m.group(1))
        # PBGC opinion letters run from the early 1990s to the present.
        return 1900 + yy if yy >= 90 else 2000 + yy

    @staticmethod
    def _number_from_id(letter_id: str) -> str:
        m = re.search(r"opinion-letter-(\d{2})-(\d{3})", letter_id)
        if not m:
            return ""
        return f"{m.group(1)}-{int(m.group(2))}"

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[str, None, None]:
        html = self._get_text(INDEX_PAGE)
        if not html:
            logger.error("Could not fetch PBGC opinion-letter database page")
            return
        seen: set[str] = set()
        for letter_id in ID_RE.findall(html):
            key = letter_id.lower()
            if key in seen:
                continue
            seen.add(key)
            yield key
            if sample and len(seen) >= 20:
                logger.info(f"Sample: stopped after {len(seen)} pointers")
                return
        logger.info(f"Discovered {len(seen)} PBGC opinion-letter pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, letter_id: str) -> dict | None:
        if letter_id in self._existing:
            return None
        pdf_bytes = self._get_bytes(DOC_URL.format(id=letter_id))
        if not pdf_bytes:
            return None
        if not pdf_bytes[:5].startswith(b"%PDF"):
            logger.warning(f"{letter_id}: response is not a PDF — skipping")
            return None
        text = extract_pdf_markdown(
            "US/PBGC-OpinionLetters", letter_id, pdf_bytes=pdf_bytes,
            table="doctrine",
        )
        if not text or len(text.strip()) < 300:
            logger.warning(f"No usable text for {letter_id} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        number = self._number_from_id(letter_id)
        date = self._iso_from_body(text)
        if not date:
            yr = self._year_from_id(letter_id)
            date = f"{yr:04d}-01-01" if yr else None
        title = f"PBGC Opinion Letter {number}" if number else f"PBGC {letter_id}"
        return {
            "record_id": letter_id,
            "number": number,
            "title": title,
            "text": text,
            "date": date,
            "url": DOC_URL.format(id=letter_id),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing PBGC Opinion Letter database...")
        try:
            ids = list(self.discover(sample=True))
            if not ids:
                logger.error("  No opinion-letter pointers discovered")
                return False
            logger.info(f"  Discovered {len(ids)} pointers (sample)")
            raw = None
            for lid in ids:
                raw = self._build_raw(lid)
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
            "_id": f"US/PBGC-OpinionLetters/{raw['record_id']}",
            "_source": "US/PBGC-OpinionLetters",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "Pension Benefit Guaranty Corporation (PBGC)",
            "number": raw.get("number") or None,
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
                self._existing = preload_existing_ids(
                    "US/PBGC-OpinionLetters", "doctrine")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for letter_id in self.discover(sample=sample):
            raw = self._build_raw(letter_id)
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

    parser = argparse.ArgumentParser(description="US/PBGC-OpinionLetters bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PBGCOpinionScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
