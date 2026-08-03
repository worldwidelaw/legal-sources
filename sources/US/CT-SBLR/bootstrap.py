#!/usr/bin/env python3
"""
US/CT-SBLR -- Connecticut State Board of Labor Relations (SBLR) Decisions

Fetches the full text of every published decision of the Connecticut State
Board of Labor Relations (SBLR / CSBLR), the tribunal within the Connecticut
Department of Labor that adjudicates public- and private-sector labor-relations
disputes under the State Employees Relations Act (SERA), the Municipal Employee
Relations Act (MERA), and the Connecticut Labor Relations Act (CLRA). The Board
decides prohibited-practice complaints, representation / election petitions, and
bargaining-unit determinations. Each numbered decision resolves a specific
contested case = case_law, and they are official Connecticut state-government
works in the public domain (government edicts).

BUILD RECIPE (builds + validates LOCALLY via the Internet Archive):
The CT Department of Labor retired the old ctdol.state.ct.us website (every
/csblr/ path now 301-redirects to a portal.ct.gov 404) and moved the live
archive to https://dolpublicdocumentlibrary.ct.gov/ , which sits behind an
F5/Shape (TSPD) JavaScript bot-challenge and cannot be enumerated without a
browser. HOWEVER, the entire born-digital decision corpus -- decisions No. 1
(1945) through the 5300s (2025), ~5,600 numbered PDFs -- was fully crawled and
preserved by the Internet Archive Wayback Machine under the stable path
  http://www.ctdol.state.ct.us/csblr/decisions-pdf/{YEAR}/{name}.pdf
These are official Connecticut government works (public domain); the Wayback
Machine is a durable public mirror. We enumerate them with the Wayback CDX API
(filter statuscode:200, collapse=urlkey) and download each preserved PDF via the
`/web/{timestamp}id_/{original_url}` raw-replay endpoint, then extract full text
with common.pdf_extract. The decision number and year come from the file path;
the case number and decision date are parsed from the decision body. No auth,
no CAPTCHA, no JS challenge (the Wayback Machine serves the preserved bytes).

Usage:
  python bootstrap.py bootstrap            # Full pull
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
from urllib.parse import quote, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-SBLR")

# Original (now-retired) host whose /csblr/decisions-pdf tree the Wayback
# Machine preserved in full.
ORIG_PREFIX = "ctdol.state.ct.us/csblr/decisions-pdf"
CDX_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_RAW = "https://web.archive.org/web/{ts}id_/{url}"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# Path: /decisions-pdf/{YEAR}/{filename}.pdf
PATH_RE = re.compile(r"/decisions-pdf/(\d{4})/(.+?)\.pdf$", re.IGNORECASE)

# CT SBLR case numbers, e.g. "Case No. MPP-34,393", "Case Nos. SPP-31,001",
# "Case No. E-2345", "Case No. MDR-27,001".
CASE_RE = re.compile(
    r"Case\s+Nos?\.?\s*[:\s]*"
    r"([A-Z]{1,5}-[0-9][0-9,\-\s]*[0-9A-Z](?:\s*(?:&|and|,)\s*"
    r"[A-Z]{1,5}-[0-9][0-9,\-\s]*[0-9A-Z])*)",
    re.IGNORECASE)
DECNO_RE = re.compile(r"Decision\s+No\.?\s*([0-9]{1,4}[A-Z\-]*)", re.IGNORECASE)
LONGDATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},\s+(?:19|20)\d{2}\b")

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class SBLRScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.4
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_json(self, url: str, params: dict) -> list | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, params=params, timeout=(15, 180))
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"CDX GET failed attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 180), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _iso_from_longdate(s: str) -> str | None:
        m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", s.strip())
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        try:
            d, y = int(m.group(2)), int(m.group(3))
        except ValueError:
            return None
        if 1 <= d <= 31 and 1940 <= y <= 2100:
            return f"{y:04d}-{mon:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug_from_url(orig_url: str) -> tuple[str, str, str]:
        """Return (record_id, year, filename) from the decisions-pdf path."""
        path = re.sub(r"^https?://[^/]+", "", orig_url).split("?", 1)[0]
        m = PATH_RE.search(path)
        if m:
            year = m.group(1)
            fname = unquote(m.group(2))
            base = f"{year}-{fname}"
        else:
            year = ""
            fname = unquote(path.rsplit("/", 1)[-1])
            base = re.sub(r"(?i)\.pdf$", "", fname)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-").lower()
        return slug, year, fname

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        rows = self._get_json(CDX_URL, {
            "url": ORIG_PREFIX + "*",
            "output": "json",
            "collapse": "urlkey",
            "filter": "statuscode:200",
            "limit": "50000",
        })
        if not rows or len(rows) < 2:
            logger.error("CDX returned no decision snapshots")
            return
        header = rows[0]
        ts_i = header.index("timestamp")
        url_i = header.index("original")
        mime_i = header.index("mimetype")
        entries = []
        seen: set[str] = set()
        for r in rows[1:]:
            orig = r[url_i]
            if not PATH_RE.search(orig.split("?", 1)[0]):
                continue
            if "pdf" not in (r[mime_i] or "").lower() and not orig.lower().endswith(".pdf"):
                continue
            rid, year, fname = self._slug_from_url(orig)
            if rid in seen:
                continue
            seen.add(rid)
            entries.append({"ts": r[ts_i], "orig": orig,
                            "record_id": rid, "year": year, "fname": fname})
        # newest first (year desc) so samples show modern born-digital decisions
        entries.sort(key=lambda e: (e["year"], e["record_id"]), reverse=True)
        logger.info(f"CDX: {len(entries)} unique SBLR decision PDFs (1945-present)")
        for e in entries:
            yield e

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        source_id = entry["record_id"]
        if source_id in self._existing:
            return None
        raw_url = WAYBACK_RAW.format(ts=entry["ts"],
                                     url=quote(entry["orig"], safe=":/?&=%"))
        pdf_bytes = self._get_bytes(raw_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/CT-SBLR", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {entry['fname']} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        case_number = None
        m = CASE_RE.search(text)
        if m:
            case_number = re.sub(r"\s+", " ", m.group(1)).strip(" .,")[:80]

        # Decision number: prefer body "Decision No. NNNN", else numeric filename.
        decision_no = None
        dm = DECNO_RE.search(text)
        if dm:
            decision_no = dm.group(1).upper().strip("-")
        elif re.fullmatch(r"\d{1,4}[A-Za-z\-]*", entry["fname"]):
            decision_no = entry["fname"].upper()

        date = None
        spans = [mm.group(0) for mm in LONGDATE_RE.finditer(text[:2000])]
        if spans:
            date = self._iso_from_longdate(spans[0])  # caption/issue date
        if not date:
            allspans = [mm.group(0) for mm in LONGDATE_RE.finditer(text)]
            if allspans:
                date = self._iso_from_longdate(allspans[-1])

        # Title: try the first ALL-CAPS "IN THE MATTER OF ..." caption party.
        party = None
        pm = re.search(r"IN\s+THE\s+MATTER\s+OF\s+(.+?)(?:\n|-AND-|Decision|Case)",
                       text[:800], re.IGNORECASE | re.DOTALL)
        if pm:
            party = re.sub(r"\s+", " ", pm.group(1)).strip(" .,-")[:120]
        bits = ["Connecticut SBLR"]
        if decision_no:
            bits.append(f"Decision No. {decision_no}")
        if party:
            bits.append(party)
        elif case_number:
            bits.append(case_number)
        title = " — ".join(bits)

        return {
            "record_id": source_id,
            "decision_number": decision_no,
            "case_number": case_number,
            "year": entry.get("year") or None,
            "title": _html.unescape(title)[:500],
            "text": text,
            "date": date,
            "url": entry["orig"],
            "archive_url": WAYBACK_RAW.format(ts=entry["ts"], url=entry["orig"]),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Connecticut SBLR decisions (via Wayback CDX)...")
        try:
            gen = self.discover(sample=True)
            raw = None
            tried = 0
            for e in gen:
                raw = self._build_raw(e)
                tried += 1
                if raw:
                    break
                if tried >= 8:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} [{raw.get('date')}]")
                logger.info("API test PASSED")
                return True
            logger.error("  Text extraction failed")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/CT-SBLR/{raw['record_id']}",
            "_source": "US/CT-SBLR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "decision_number": raw.get("decision_number") or None,
            "case_number": raw.get("case_number") or None,
            "issuer": "Connecticut State Board of Labor Relations (SBLR)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "archive_url": raw.get("archive_url"),
            "date": raw.get("date") or None,
            "jurisdiction": "US-CT",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/CT-SBLR", "case_law")
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

    parser = argparse.ArgumentParser(description="US/CT-SBLR bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SBLRScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
