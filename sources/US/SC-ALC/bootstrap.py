#!/usr/bin/env python3
"""
US/SC-ALC -- South Carolina Administrative Law Court (ALC) Decisions

Fetches the full text of the orders and final decisions of the South
Carolina Administrative Law Court (ALC, formerly the Administrative Law
Judge Division), the state's centralized court of record that hears
contested cases and appeals from most South Carolina state agencies --
Department of Revenue (state tax appeals), Department of Health and
Environmental Control, Department of Insurance, Department of Motor
Vehicles, Department of Labor Licensing and Regulation, Department of
Social Services, alcoholic-beverage licensing, and many others. Each
order resolves a specific contested case = case_law and is an official
South Carolina state-government work in the public domain (government
edicts).

BUILD RECIPE (no auth, no CAPTCHA): the ALC publishes its decisions
through a Kendo/Telerik search portal at
https://www.decisions.scalc.net/ . The search grid itself is JavaScript-
driven, but every individual decision document is served DIRECTLY, by a
plain sequential integer document id, at:

    https://www.decisions.scalc.net/Home/ViewPdf/{id}

No cookie, session, or anti-forgery token is required for that endpoint.
The response is one of two shapes:

  * older decisions  -> an HTML fragment (UTF-16LE encoded, mislabelled
    ``Content-Type: application/pdf``) whose body is the full decision
    text: ``<center><b>ORDER OF DISMISSAL</b></center> ...``
  * newer decisions  -> a genuine born-digital ``%PDF`` document.

Both carry the complete decision body. The scraper walks id = 1 .. N
(N observed > 14,000, 2026), skipping the ``HTTP 500`` gaps, decodes the
HTML fragments (strip tags) or extracts the PDFs via the shared
``common.pdf_extract`` extractor, and pulls the order type, docket number
(``YY-ALJ-NN-NNNN-XX``) and decision date out of the body text.

Usage:
  python bootstrap.py bootstrap            # Full pull (id 1..ceiling)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SC-ALC")

HOST = "https://www.decisions.scalc.net"
VIEW = HOST + "/Home/ViewPdf/"

# Enumeration ceiling. Ids are dense from 1; the live ceiling was >14,000
# in 2026. Walk with an early stop after a long run of consecutive misses.
MAX_ID = 16000
MAX_CONSECUTIVE_MISSES = 600

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

TAG_RE = re.compile(r"<[^>]+>")
# ALC / ALJD docket number, e.g. "96-ALJ-07-0126-CC", "05-ALJ-17-0123-CC".
DOCKET_RE = re.compile(r"\b(\d{2,4}-ALJ-\d{2}-\d{3,5}-[A-Z]{1,3})\b")
# Common date forms inside the body, e.g. "September 24, 1996".
MONTHS = ("January February March April May June July August September "
          "October November December").split()
MONTH_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),\s+(\d{4})\b")
# Fallback numeric date MM/DD/YYYY.
NUM_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


class SCALCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.5
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_bytes(self, url: str) -> tuple[int, bytes | None]:
        """Return (status_code, body-or-None)."""
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120))
                if resp.status_code == 500:
                    return 500, None
                resp.raise_for_status()
                return resp.status_code, resp.content
            except requests.HTTPError as e:
                code = getattr(e.response, "status_code", 0)
                if code in (404, 500):
                    return code, None
                logger.warning(f"GET {url} attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.warning(f"GET {url} attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return 0, None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _decode_html_fragment(body: bytes) -> str:
        # Server sends UTF-16LE (null-interleaved) for the HTML fragments,
        # occasionally UTF-8. Detect the BOM / null density.
        if body[:2] in (b"\xff\xfe", b"\xfe\xff"):
            txt = body.decode("utf-16", errors="replace")
        elif body.count(b"\x00") > len(body) // 4:
            txt = body.decode("utf-16-le", errors="replace")
        else:
            txt = body.decode("utf-8", errors="replace")
        txt = _html.unescape(TAG_RE.sub("\n", txt))
        txt = txt.replace("\xa0", " ").replace("’", "'")
        # Collapse whitespace but keep paragraph breaks.
        lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in txt.splitlines()]
        return "\n".join(ln for ln in lines if ln).strip()

    @classmethod
    def _extract_date(cls, text: str) -> str | None:
        m = MONTH_RE.search(text)
        if m:
            mm = cls._month_num(m.group(1))
            dd, yy = int(m.group(2)), int(m.group(3))
            if mm and 1 <= dd <= 31 and 1900 <= yy <= 2100:
                return f"{yy:04d}-{mm:02d}-{dd:02d}"
        m = NUM_DATE_RE.search(text)
        if m:
            mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yy <= 2100:
                return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _month_num(name: str) -> int | None:
        try:
            return MONTHS.index(name) + 1
        except ValueError:
            return None

    @staticmethod
    def _title(text: str, docket: str | None, doc_id: int) -> str:
        # First non-empty line is the order caption ("ORDER OF DISMISSAL"
        # etc.). Keep it short and append the docket when present.
        first = ""
        for ln in text.splitlines():
            ln = ln.strip()
            if len(ln) >= 3:
                first = ln
                break
        first = re.sub(r"\s+", " ", first)[:180] or "ALC Decision"
        if docket:
            return f"{first} ({docket})"
        return f"{first} (ALC Doc {doc_id})"

    # ------------------------------------------------------- build record
    def _build_raw(self, doc_id: int) -> dict | None:
        source_id = str(doc_id)
        if source_id in self._existing:
            return None
        status, body = self._get_bytes(VIEW + source_id)
        if status != 200 or not body:
            return None
        if body[:4] == b"%PDF":
            text = extract_pdf_markdown(
                "US/SC-ALC", source_id, pdf_bytes=body, table="case_law")
        else:
            text = self._decode_html_fragment(body)
        if not text or len(text.strip()) < 200:
            return None
        text = text.strip()
        docket_m = DOCKET_RE.search(text)
        docket = docket_m.group(1) if docket_m else None
        date = self._extract_date(text)
        return {
            "record_id": source_id,
            "docket": docket,
            "title": self._title(text, docket, doc_id),
            "text": text,
            "date": date,
            "url": VIEW + source_id,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing SC ALC ViewPdf endpoint...")
        try:
            hits = 0
            raw = None
            for doc_id in range(1, 60):
                r = self._build_raw(doc_id)
                if r:
                    hits += 1
                    raw = raw or r
                    if hits >= 3:
                        break
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"doc {raw['record_id']} docket={raw['docket']} "
                            f"[{raw['date']}]")
                logger.info("API test PASSED")
                return True
            logger.error("  No usable decision text found in first ids")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/SC-ALC/{raw['record_id']}",
            "_source": "US/SC-ALC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "docket": raw.get("docket"),
            "issuer": "South Carolina Administrative Law Court",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-SC",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/SC-ALC", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        misses = 0
        for doc_id in range(1, MAX_ID + 1):
            raw = self._build_raw(doc_id)
            if raw:
                misses = 0
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
                if emitted % 250 == 0:
                    logger.info(f"...{emitted} decisions (at id {doc_id})")
            else:
                misses += 1
                # Only apply the consecutive-miss early stop deep in the range,
                # so the sparse early gaps don't abort the walk.
                if doc_id > 2000 and misses >= MAX_CONSECUTIVE_MISSES:
                    logger.info(f"Stopping: {misses} consecutive misses at "
                                f"id {doc_id}")
                    return
        logger.info(f"Done: {emitted} decisions")

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

    parser = argparse.ArgumentParser(description="US/SC-ALC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCALCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
