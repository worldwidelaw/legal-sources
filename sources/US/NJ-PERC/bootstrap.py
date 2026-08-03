#!/usr/bin/env python3
"""
US/NJ-PERC -- New Jersey Public Employment Relations Commission (PERC) Decisions

Fetches the full text of every published Commission decision of the New Jersey
Public Employment Relations Commission (PERC), the state's quasi-judicial agency
that administers the New Jersey Employer-Employee Relations Act (N.J.S.A. 34:13A).
PERC adjudicates public-sector labor-relations disputes -- scope-of-negotiations
determinations, restraints of binding arbitration, unfair-practice charges,
representation / unit-clarification matters, and related contested cases. Each
Commission decision resolves a specific contested case = case_law. These are
official New Jersey state-government works in the public domain (edicts of a
quasi-judicial government body).

The official decision database is a Lotus Domino application hosted at
``https://www.perc.state.nj.us/percdecisions.nsf``. Decisions are stored as PDF
attachments, one per decision, exposed through the ``IssuedDecisions`` view.
The view is categorized by calendar year, so the full corpus is enumerated by
restricting the view to each year:

  GET /percdecisions.nsf/IssuedDecisions?OpenView&RestrictToCategory={YEAR}&Count=2000

Each row links to the decision PDF:

  /percdecisions.nsf/IssuedDecisions/{UNID}/$File/{PERC citation}.pdf?OpenElement

where ``{UNID}`` is the Domino document universal id (a stable 32-hex key) and the
attachment filename encodes the citation (e.g. "PERC 90-125.pdf" -> P.E.R.C. NO.
90-125, "PERC 2026 46.pdf" -> P.E.R.C. NO. 2026-46). The corpus runs from 1980 to
the present (~100-160 decisions/year, ~5,000+ total).

The PDF host requires a browser User-Agent (plain requests receive HTTP 403), so
this scraper downloads the bytes itself and hands them to the shared
``common.pdf_extract`` extractor. Each decision's issued date is parsed from the
"ISSUED: <Month DD, YYYY>" line the Commission stamps on the final page.

Usage:
  python bootstrap.py bootstrap            # Full pull (~5,000+ decisions)
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
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NJ-PERC")

HOST = "https://www.perc.state.nj.us"
DB = "/percdecisions.nsf"
VIEW = DB + "/IssuedDecisions"
FIRST_YEAR = 1980  # DB holds decisions from 1980; earlier years return empty.

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# Link to a decision PDF attachment inside the Domino view.
#   /percdecisions.nsf/IssuedDecisions/{UNID}/$File/{name}.pdf?OpenElement
PDF_HREF_RE = re.compile(
    r'href="(/percdecisions\.nsf/IssuedDecisions/([0-9A-Fa-f]{16,40})/\$File/'
    r'([^"?]+?\.pdf))(?:\?[^"]*)?"',
    re.IGNORECASE,
)
ISSUED_RE = re.compile(r"ISSUED:\s*([A-Z][a-z]+\.?\s+\d{1,2},\s*\d{4})")
DATE_ANY_RE = re.compile(r"([A-Z][a-z]+\.?)\s+(\d{1,2}),\s*(\d{4})")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


class NJPERCScraper(BaseScraper):

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
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:80]}) attempt {attempt+1}: {e}")
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
                logger.warning(f"PDF GET failed ({url[:80]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _citation_from_name(name: str) -> str:
        """'PERC 90-125.pdf' -> 'P.E.R.C. NO. 90-125'; 'PERC 2026 46.pdf' -> '... 2026-46'."""
        core = re.sub(r"(?i)\.pdf$", "", name).strip()
        core = re.sub(r"(?i)^perc\s*", "", core).strip()
        core = re.sub(r"\s+", "-", core)
        core = core.strip("-")
        return f"P.E.R.C. NO. {core}" if core else "P.E.R.C. Decision"

    @classmethod
    def _iso_date(cls, month: str, day: str, year: str) -> str | None:
        mm = MONTHS.get(month.lower().rstrip("."))
        if not mm:
            return None
        try:
            dd, yy = int(day), int(year)
        except ValueError:
            return None
        if 1 <= dd <= 31 and 1970 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @classmethod
    def _parse_issued(cls, text: str, fallback_year: int | None) -> str | None:
        m = ISSUED_RE.search(text)
        if m:
            mm = DATE_ANY_RE.search(m.group(1))
            if mm:
                iso = cls._iso_date(mm.group(1), mm.group(2), mm.group(3))
                if iso:
                    return iso
        # Fall back to the last date-like token in the document tail.
        tail = text[-1500:]
        cands = DATE_ANY_RE.findall(tail)
        for mo, dd, yy in reversed(cands):
            iso = cls._iso_date(mo, dd, yy)
            if iso and (fallback_year is None or abs(int(yy) - fallback_year) <= 1):
                return iso
        if fallback_year:
            return f"{fallback_year:04d}-01-01"
        return None

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        this_year = datetime.now(timezone.utc).year
        seen: set[str] = set()
        found = 0
        for year in range(this_year, FIRST_YEAR - 1, -1):
            url = f"{HOST}{VIEW}?OpenView&RestrictToCategory={year}&Count=2000"
            page = self._get_text(url)
            if not page:
                continue
            rows = PDF_HREF_RE.findall(page)
            if not rows:
                continue
            n_year = 0
            for path, unid, fname in rows:
                unid = unid.upper()
                if unid in seen:
                    continue
                seen.add(unid)
                fname = _html.unescape(fname)
                pdf_url = HOST + quote(path, safe="/$")
                yield {
                    "unid": unid,
                    "filename": fname,
                    "pdf_url": pdf_url,
                    "year": year,
                }
                n_year += 1
                found += 1
                if sample and found >= 24:
                    logger.info(f"Sample: stopped after {found} pointers")
                    return
            logger.info(f"Year {year}: {n_year} decision PDFs")
        logger.info(f"Discovered {len(seen)} PERC decision pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        source_id = entry["unid"]
        if source_id in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NJ-PERC", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {entry['filename']} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        citation = self._citation_from_name(entry["filename"])
        date = self._parse_issued(text, entry.get("year"))
        return {
            "unid": source_id,
            "citation": citation,
            "title": citation,
            "text": text,
            "date": date,
            "url": entry["pdf_url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NJ PERC Domino decision database...")
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
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['citation']} [{raw['date']}]")
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
            "_id": f"US/NJ-PERC/{raw['unid']}",
            "_source": "US/NJ-PERC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["unid"],
            "citation": raw.get("citation"),
            "issuer": "New Jersey Public Employment Relations Commission (PERC)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NJ",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/NJ-PERC", "case_law")
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

    parser = argparse.ArgumentParser(description="US/NJ-PERC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJPERCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
