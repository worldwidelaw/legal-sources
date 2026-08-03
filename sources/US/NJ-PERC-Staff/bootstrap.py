#!/usr/bin/env python3
"""
US/NJ-PERC-Staff -- New Jersey PERC Director, Hearing Examiner & Appeal Board Decisions

Fetches the full text of the non-Commission adjudications issued under the New
Jersey Public Employment Relations Commission (PERC): decisions of the Director
of Representation (D.R.), the Director of Unfair Practices (C.O.), Hearing
Examiners / Hearing Officers (H.E. / H.O.), and the PERC Appeal Board (A.B.D.).
These staff- and appeal-board-level rulings each resolve a specific contested
case under the New Jersey Employer-Employee Relations Act (N.J.S.A. 34:13A) =
case_law. They complement the Commission's final decisions (source US/NJ-PERC).
Official New Jersey state-government works in the public domain (edicts of a
quasi-judicial government body).

Same Lotus Domino database as US/NJ-PERC (perc.state.nj.us/percdecisions.nsf),
but a different view -- "Issued Decisions Non PERC" -- which groups these
non-Commission decisions. The whole set (~920 decisions) is returned by a single
expanded view request:

  GET /percdecisions.nsf/Issued%20Decisions%20Non%20PERC?OpenView&Count=5000&ExpandView

Each row links to the decision PDF attachment (served under the sibling
"Issued Decisions" resource path):

  /percdecisions.nsf/Issued Decisions/{UNID}/$File/{name}.pdf?OpenElement

where {UNID} is the stable Domino document universal id (record key). The
citation (e.g. "D.R. NO. 2018-6", "A.B.D. NO. 2008-1", "H.E. NO. 95-12") is read
from the first line of the decision body; the issued date is parsed from the
"ISSUED: <Month DD, YYYY>" stamp. The PDF host requires a browser User-Agent
(plain requests get HTTP 403), so this scraper downloads the bytes itself and
hands them to common.pdf_extract.

Usage:
  python bootstrap.py bootstrap            # Full pull (~920 decisions)
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
logger = logging.getLogger("legal-data-hunter.US.NJ-PERC-Staff")

HOST = "https://www.perc.state.nj.us"
DB = "/percdecisions.nsf"
VIEW = DB + "/Issued%20Decisions%20Non%20PERC"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# Any decision PDF attachment inside the Domino view (view segment may contain
# spaces, e.g. "/Issued Decisions/").
PDF_HREF_RE = re.compile(
    r'href="(/percdecisions\.nsf/[^/"]+/([0-9A-Fa-f]{16,40})/\$File/'
    r'([^"?]+?\.pdf))(?:\?[^"]*)?"',
    re.IGNORECASE,
)
# Citation on the first line of the body, e.g. "D.R. NO. 2018-6", "A.B.D. NO. 2008-1".
CITATION_RE = re.compile(
    r"\b([A-Z](?:\.[A-Z]){1,3}\.?\s*NO\.\s*\d{2,4}[-–]\d+)", re.IGNORECASE
)
ISSUED_RE = re.compile(r"ISSUED:\s*([A-Z][a-z]+\.?\s+\d{1,2},\s*\d{4})")
DATE_ANY_RE = re.compile(r"([A-Z][a-z]+\.?)\s+(\d{1,2}),\s*(\d{4})")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


class NJPERCStaffScraper(BaseScraper):

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
                resp = self._session.get(url, timeout=(15, 120))
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
    def _citation_from_name(name: str) -> str | None:
        core = re.sub(r"(?i)\.pdf$", "", name).strip()
        if core.startswith("~"):
            return None  # Domino temp attachment name — no citation encoded
        m = re.match(r"(?i)^(ABD|AB|DR|CO|HE|HO|TO|EF)\s+(.+)$", core)
        if not m:
            return None
        series = m.group(1).upper()
        num = re.sub(r"\s+", "-", m.group(2).strip()).strip("-")
        dotted = {"ABD": "A.B.D.", "AB": "A.B.", "DR": "D.R.", "CO": "C.O.",
                  "HE": "H.E.", "HO": "H.O.", "TO": "T.O.", "EF": "E.F."}.get(series, series)
        return f"{dotted} NO. {num}" if num else None

    @staticmethod
    def _citation_from_text(text: str) -> str | None:
        m = CITATION_RE.search(text[:400])
        if not m:
            return None
        cit = re.sub(r"\s+", " ", m.group(1)).strip()
        cit = cit.replace("–", "-")
        return cit.upper()

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

    @staticmethod
    def _year_from_citation(citation: str | None) -> int | None:
        if not citation:
            return None
        m = re.search(r"NO\.\s*(\d{2,4})-", citation)
        if not m:
            return None
        y = int(m.group(1))
        if y < 100:  # two-digit PERC year: 84 -> 1984, 26 -> 2026
            y = 1900 + y if y >= 60 else 2000 + y
        if 1970 <= y <= 2100:
            return y
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
        tail = text[-1500:]
        for mo, dd, yy in reversed(DATE_ANY_RE.findall(tail)):
            iso = cls._iso_date(mo, dd, yy)
            if iso and (fallback_year is None or abs(int(yy) - fallback_year) <= 1):
                return iso
        if fallback_year:
            return f"{fallback_year:04d}-01-01"
        return None

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        count = 100 if sample else 5000
        url = f"{HOST}{VIEW}?OpenView&Count={count}&ExpandView"
        page = self._get_text(url)
        if not page:
            return
        seen: set[str] = set()
        n = 0
        for path, unid, fname in PDF_HREF_RE.findall(page):
            unid = unid.upper()
            if unid in seen:
                continue
            seen.add(unid)
            fname = _html.unescape(fname)
            pdf_url = HOST + quote(_html.unescape(path), safe="/$")
            yield {"unid": unid, "filename": fname, "pdf_url": pdf_url}
            n += 1
            if sample and n >= 24:
                return
        logger.info(f"Discovered {len(seen)} non-Commission decision pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        source_id = entry["unid"]
        if source_id in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NJ-PERC-Staff", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {entry['filename']} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        citation = (self._citation_from_text(text)
                    or self._citation_from_name(entry["filename"]))
        title = citation or f"NJ PERC decision {entry['unid'][:12]}"
        date = self._parse_issued(text, self._year_from_citation(citation))
        return {
            "unid": source_id,
            "citation": citation,
            "title": title,
            "text": text,
            "date": date,
            "url": entry["pdf_url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NJ PERC non-Commission decision view...")
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
            "_id": f"US/NJ-PERC-Staff/{raw['unid']}",
            "_source": "US/NJ-PERC-Staff",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["unid"],
            "citation": raw.get("citation"),
            "issuer": "New Jersey Public Employment Relations Commission (PERC) — "
                      "Director / Hearing Examiner / Appeal Board",
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
                self._existing = preload_existing_ids("US/NJ-PERC-Staff", "case_law")
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

    parser = argparse.ArgumentParser(description="US/NJ-PERC-Staff bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJPERCStaffScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
