#!/usr/bin/env python3
"""
US/MI-MERC -- Michigan Employment Relations Commission (MERC) Decisions & Orders

Fetches the full text of every published decision of the Michigan Employment
Relations Commission (MERC), the state's quasi-judicial agency that
administers Michigan's public- and private-sector labor-relations statutes --
the Public Employment Relations Act (PERA, 1965 PA 379), the Labor Mediation
Act / Wisconsin... (Michigan Employment Relations Act), and Act 312 compulsory
arbitration for police and fire.  The Commission decides unfair-labor-practice
(ULP) charges, representation / election petitions, and unit-clarification
petitions; each decision resolves a specific contested case = case_law.  MERC
decisions are official Michigan state-government works in the public domain
(government edicts, 17 U.S.C. §105 analogue for state edicts).

BUILD RECIPE (builds + validates LOCALLY, no CAPTCHA / JS / auth):
MERC's historical decision corpus (1994/1998-2015) is published as born-digital
text-layer PDFs on an IIS directory-browse file store:

    https://gsaindexed.apps.lara.state.mi.us/MERC/

The store is a browsable directory tree.  Structure is INCONSISTENT across
years: some years hold PDFs directly under /MERC/{YEAR}/, some nest them in
/MERC/{YEAR}/{MM}/ month folders, and a few nest an extra /MERC/{YEAR}/{sub}/
level.  We therefore RECURSIVELY WALK the whole /MERC/ tree, following every
subdirectory link and collecting every *.pdf.  Each PDF is one MERC decision.

Full text is extracted with the shared ``common.pdf_extract`` extractor (PDFs
are born-digital with a real text layer).  Case number, parties, and (where
present) the decision date are parsed from the decision body:
  - header: "STATE OF MICHIGAN / EMPLOYMENT RELATIONS COMMISSION"
  - "In the Matter of:" caption block -> parties
  - "Case No. C10 I-220" -> case number (filename encodes it too:
    prefix c=ULP charge, cu/uc=unit clarification, r=representation election)
  - "Dated: Month DD, YYYY" (often left blank) -> decision date, else null.

The michigan.gov LEO listing page that indexes these (and any post-2015
decisions) is Akamai/WAF-gated (HTTP 403 to datacenter + residential clients),
but the gsaindexed file store itself serves 200 to any browser UA, so the
1994-2015 corpus is fully retrievable here.

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
logger = logging.getLogger("legal-data-hunter.US.MI-MERC")

HOST = "https://gsaindexed.apps.lara.state.mi.us"
ROOT = "/MERC/"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

HREF_RE = re.compile(r'HREF="([^"]+)"', re.IGNORECASE)

# Case number in body, e.g. "Case No. C10 I-220" / "Case Nos. R11 A-001"
CASE_RE = re.compile(r"Case\s+Nos?\.?\s*([A-Z0-9][A-Z0-9 \-\.,/&]{1,60})", re.IGNORECASE)
# "Dated: January 5, 2011" (frequently blank -> no match)
DATED_RE = re.compile(
    r"Dated:\s*([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})", re.IGNORECASE)
# Generic long-form date anywhere (fallback)
LONGDATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},\s+(19|20)\d{2}\b")

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class MERCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.5
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
                return resp.content.decode("latin-1", "replace")
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
    def _iso_from_longdate(s: str) -> str | None:
        m = re.match(r"([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})", s.strip())
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        try:
            d, y = int(m.group(2)), int(m.group(3))
        except ValueError:
            return None
        if 1 <= d <= 31 and 1970 <= y <= 2100:
            return f"{y:04d}-{mon:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug_from_path(pdf_path: str) -> str:
        """Stable id from the store path (unique per file across the tree)."""
        rel = pdf_path[len(ROOT):] if pdf_path.startswith(ROOT) else pdf_path
        rel = re.sub(r"(?i)\.pdf$", "", rel)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", rel).strip("-")
        return slug.lower() or re.sub(r"[^A-Za-z0-9]+", "-", pdf_path).strip("-")

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[str, None, None]:
        """Recursively walk the /MERC/ IIS directory tree, yield PDF paths."""
        seen_dirs: set[str] = set()
        seen_pdfs: set[str] = set()
        stack = [ROOT]
        found = 0
        while stack:
            d = stack.pop()
            if d in seen_dirs:
                continue
            seen_dirs.add(d)
            html = self._get_text(HOST + quote(d, safe="/"))
            if not html:
                continue
            subdirs: list[str] = []
            for href in HREF_RE.findall(html):
                if not href.startswith(ROOT) or href == d:
                    continue
                if href.lower().endswith(".pdf"):
                    if href in seen_pdfs:
                        continue
                    seen_pdfs.add(href)
                    yield href
                    found += 1
                    if sample and found >= 20:
                        logger.info(f"Sample: stopped after {found} PDF pointers")
                        return
                elif href.endswith("/") and href not in seen_dirs:
                    subdirs.append(href)
            # Depth-first but push in reverse so earliest years are walked first
            for sd in reversed(subdirs):
                stack.append(sd)
        logger.info(f"Discovered {len(seen_pdfs)} MERC decision PDFs "
                    f"across {len(seen_dirs)} directories")

    # ------------------------------------------------------- build record
    def _build_raw(self, pdf_path: str) -> dict | None:
        source_id = self._slug_from_path(pdf_path)
        if source_id in self._existing:
            return None
        pdf_url = HOST + quote(pdf_path, safe="/")
        pdf_bytes = self._get_bytes(pdf_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/MI-MERC", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {pdf_path.rsplit('/',1)[-1]} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        # --- case number ---
        case_number = None
        m = CASE_RE.search(text)
        if m:
            case_number = re.sub(r"\s+", " ", m.group(1)).strip(" .,-")
            case_number = case_number[:60]

        # --- parties (from "In the Matter of:" caption block) ---
        parties = None
        cap = re.search(
            r"In the Matter of[:\s]*(.+?)(?:APPEARANCES|DECISION AND ORDER|"
            r"DECISION\s*\n|ORDER\b)", text, re.IGNORECASE | re.DOTALL)
        if cap:
            block = cap.group(1)
            # Collapse to a clean one-line caption; drop the trailing "/" divider.
            block = re.sub(r"[_]{3,}", " ", block)
            lines = [ln.strip() for ln in block.splitlines() if ln.strip()
                     and ln.strip() != "/"]
            parties = " ".join(lines)
            parties = re.sub(r"\s+", " ", parties).strip()[:400] or None

        # --- decision date ---
        date = None
        m = DATED_RE.search(text)
        if m:
            date = self._iso_from_longdate(m.group(1))
        if not date:
            # last long-form date in the document (usually the issue date)
            all_dates = LONGDATE_RE.findall(text)
            if all_dates:
                # findall returns tuples of groups; re-search for full strings
                spans = [mm.group(0) for mm in LONGDATE_RE.finditer(text)]
                if spans:
                    date = self._iso_from_longdate(spans[-1])

        # --- title ---
        fname = pdf_path.rsplit("/", 1)[-1]
        if case_number and parties:
            title = f"MERC Case {case_number} — {parties}"
        elif parties:
            title = parties
        elif case_number:
            title = f"MERC Case {case_number}"
        else:
            title = f"MERC Decision {fname}"

        return {
            "record_id": source_id,
            "case_number": case_number,
            "parties": parties,
            "title": _html.unescape(title)[:500],
            "text": text,
            "date": date,
            "url": pdf_url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Michigan MERC IIS decision store...")
        try:
            pdfs = list(self.discover(sample=True))
            if not pdfs:
                logger.error("  No decision PDFs discovered")
                return False
            logger.info(f"  Discovered {len(pdfs)} PDF pointers (sample)")
            raw = None
            for p in pdfs:
                raw = self._build_raw(p)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} [{raw.get('date')}]")
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
            "_id": f"US/MI-MERC/{raw['record_id']}",
            "_source": "US/MI-MERC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "case_number": raw.get("case_number") or None,
            "issuer": "Michigan Employment Relations Commission (MERC)",
            "parties": raw.get("parties") or None,
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/MI-MERC", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for pdf_path in self.discover(sample=sample):
            raw = self._build_raw(pdf_path)
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

    parser = argparse.ArgumentParser(description="US/MI-MERC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MERCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
