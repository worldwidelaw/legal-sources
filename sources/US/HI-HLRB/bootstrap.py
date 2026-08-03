#!/usr/bin/env python3
"""
US/HI-HLRB -- Hawaii Labor Relations Board (HLRB) Decisions and Orders

Fetches the full text of the decisions and orders of the Hawaii Labor
Relations Board (HLRB), the state's quasi-judicial agency that
administers Hawaii's public-sector collective-bargaining statutes
(HRS chapter 89) and the Hawaii Employment Relations Act (HRS chapter
377). The Board adjudicates prohibited-practice complaints, bargaining-
unit / representation and clarification petitions, declaratory rulings,
impasse and other contested cases. Each numbered Decision and Order
resolves a specific case = case_law, and they are official Hawaii state-
government works in the public domain (government edicts).

BUILD RECIPE (no auth, no CAPTCHA, builds locally): the Board publishes
its entire corpus on one server-rendered index page,

  https://labor.hawaii.gov/hlrb/decisions/

which contains HTML tables of Board *Decisions* (No. 1 in 1971 through
the most recent, ~530) and Board *Orders* (numbered in the thousands).
Each table row carries:

  Decision/Order No. | Case Name | Case No. | Date | [PDF] [ADA]

The PDF cells link directly to born-digital (older ones scanned) decision
PDFs served at
  https://labor.hawaii.gov/hlrb/files/{YYYY}/{MM}/Decision-No.-{N}.pdf
  https://labor.hawaii.gov/hlrb/files/{YYYY}/{MM}/Order-No.-{N}.pdf
(each entry usually has a standard PDF plus an "-ADA" accessible copy of
the same document -- the scraper prefers the standard copy and treats the
ADA copy as a fallback). The scraper reads every table row, downloads the
PDF, and extracts full text with the shared ``common.pdf_extract``
extractor (older scans go through OCR). Decision/order number, case name,
case number and date come from the index row; ``record_id`` is the PDF
filename stem (e.g. ``Decision-No.-530``), which is stable and unique.

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
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote, urljoin

import requests

# Make locally-installed OCR tools (tesseract/poppler) discoverable so that
# common.pdf_extract's OCR fallback works for the older scanned decisions.
os.environ["PATH"] = "/opt/homebrew/bin:" + os.environ.get("PATH", "")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.HI-HLRB")

HOST = "https://labor.hawaii.gov"
INDEX_PAGE = HOST + "/hlrb/decisions/"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
PDF_HREF_RE = re.compile(r'href="([^"]*?\.pdf)"', re.IGNORECASE)
DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")
NUM_RE = re.compile(r"^\d{1,5}[A-Za-z]?$")


class HLRBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
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
    @staticmethod
    def _clean(cell: str) -> str:
        txt = _html.unescape(TAG_RE.sub(" ", cell)).replace("\xa0", " ")
        return re.sub(r"\s+", " ", txt).strip()

    @classmethod
    def _iso_date(cls, s: str) -> str | None:
        m = DATE_RE.search(s or "")
        if not m:
            return None
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if yy < 100:
            yy += 1900 if yy >= 50 else 2000
        if 1 <= mm <= 12 and 1 <= dd <= 31 and 1970 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _slug(filename: str) -> str:
        core = re.sub(r"(?i)\.pdf$", "", filename)
        return re.sub(r"[^A-Za-z0-9]+", "-", core).strip("-")

    # --------------------------------------------------------- discovery
    def _pick_pdf(self, row_html: str) -> str | None:
        """Prefer the standard PDF; fall back to the ADA copy."""
        hrefs = PDF_HREF_RE.findall(row_html)
        if not hrefs:
            return None
        standard = [h for h in hrefs if "-ada" not in h.lower()]
        return (standard or hrefs)[0]

    def _rows_from_page(self, html: str) -> Generator[dict, None, None]:
        for raw_row in ROW_RE.findall(html):
            pdf = self._pick_pdf(raw_row)
            if not pdf:
                continue
            pdf_url = urljoin(INDEX_PAGE, pdf)
            filename = pdf_url.rsplit("/", 1)[-1]
            cells = [self._clean(c) for c in CELL_RE.findall(raw_row)]
            # Date = first cell that parses as a date.
            date = None
            for c in cells:
                iso = self._iso_date(c)
                if iso:
                    date = iso
                    break
            # Decision/Order number = a short numeric cell (e.g. "530", "507A").
            number = next((c for c in cells if NUM_RE.match(c)), "")
            # Case name = a party-caption cell. Prefer a cell that reads like
            # a case caption ("X v. Y"); otherwise fall back to the longest
            # non-number / non-date / non-case-no / non-link-label cell.
            skip = {"", "pdf", "ada", "n/a", number.lower()}
            cand = [c for c in cells
                    if c.lower() not in skip
                    and not DATE_RE.fullmatch(c)]
            # A party caption ("X v. Y") is preferred. A bare case-number cell
            # is only docket codes/commas (e.g. "24-CU-03-403a, 24-CU-04-403b"):
            # a 3+ letter word never appears on its own — deprioritise those.
            def has_word(c: str) -> bool:
                return bool(re.search(r"[A-Za-z]{3,}", re.sub(r"[0-9]", " ", c)))
            parties = [c for c in cand if re.search(r"\sv[s.]?\s", c, re.I)]
            named = [c for c in cand if has_word(c)]
            if parties:
                caption = max(parties, key=len)
            elif named:
                caption = max(named, key=len)
            elif cand:
                caption = max(cand, key=len)
            else:
                caption = ""
            yield {
                "filename": filename,
                "pdf_url": pdf_url,
                "number": number,
                "caption": caption,
                "date": date,
            }

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        html = self._get_text(INDEX_PAGE)
        if not html:
            logger.error("Could not fetch HLRB index page")
            return
        seen: set[str] = set()
        found = 0
        for rec in self._rows_from_page(html):
            key = rec["filename"].lower()
            if key in seen:
                continue
            seen.add(key)
            yield rec
            found += 1
            if sample and found >= 30:
                logger.info(f"Sample: stopped after {found} pointers")
                return
        logger.info(f"Discovered {len(seen)} HLRB decision/order pointers")

    # ------------------------------------------------------- build record
    def _doc_kind(self, filename: str) -> str:
        low = filename.lower()
        if low.startswith("order"):
            return "Order"
        return "Decision"

    def _build_raw(self, entry: dict) -> dict | None:
        source_id = self._slug(entry["filename"])
        if source_id in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/HI-HLRB", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 300:
            logger.warning(f"No usable text for {entry['filename']} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        kind = self._doc_kind(entry["filename"])
        num = entry.get("number") or ""
        caption = entry.get("caption") or ""
        if caption and num:
            title = f"{caption} ({kind} No. {num})"
        elif caption:
            title = caption
        elif num:
            title = f"HLRB {kind} No. {num}"
        else:
            title = f"HLRB {source_id}"
        return {
            "record_id": source_id,
            "filename": entry["filename"],
            "doc_kind": kind,
            "number": num,
            "title": title[:500],
            "text": text,
            "date": entry.get("date"),
            "url": entry["pdf_url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Hawaii HLRB decision index...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No decision pointers discovered")
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
            "_id": f"US/HI-HLRB/{raw['record_id']}",
            "_source": "US/HI-HLRB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "Hawaii Labor Relations Board (HLRB)",
            "doc_kind": raw.get("doc_kind"),
            "number": raw.get("number") or None,
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-HI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/HI-HLRB", "case_law")
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

    parser = argparse.ArgumentParser(description="US/HI-HLRB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HLRBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
