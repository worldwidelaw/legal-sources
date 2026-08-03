#!/usr/bin/env python3
"""
US/WA-BTA -- Washington State Board of Tax Appeals (Decisions)

Fetches the full text of every decision of the Washington State Board of
Tax Appeals (BTA) — Washington's independent, quasi-judicial administrative
forum that hears appeals from decisions of county Boards of Equalization
and the Washington Department of Revenue (property tax valuation/exemption,
excise tax, use tax, forest-land and other state/local tax disputes;
taxpayer v. Department of Revenue / county assessor). Each "Final Decision"
or "Order" resolves a specific tax controversy, so the corpus is case_law.

The Board publishes its decisions as born-digital text-layer PDFs served
from apps.bta.wa.gov under "/Decision PDF/..." folders. The full corpus is
indexed by a dtSearch Web engine exposed at
  https://apps.bta.wa.gov/dtSearch/dtisapi6.dll
A single POST (cmd=search) against the "Decision PDF" index, using a term
present in every decision ("board") with a high maxFiles, returns the direct
PDF URL of every indexed decision (~19,500 documents, oldest dockets through
the current Formal Dockets). No JavaScript, no CAPTCHA, no auth.

Strategy:
  1. POST the dtSearch query once and parse every result row for its PDF
     URL, indexed filename (docket), file Title and file Date.
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Parse the decision date (latest "Month D, YYYY" in the text) and the
     appellant/case caption from the PDF; normalize into case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as html_lib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-BTA")

BASE_URL = "https://apps.bta.wa.gov"
DLL = f"{BASE_URL}/dtSearch/dtisapi6.dll"
FORM_URL = f"{BASE_URL}/dtSearch_form.html"
# dtSearch index alias (fingerprint may change if the index is rebuilt; the
# scraper re-reads it from the live form at runtime and falls back to this).
DEFAULT_INDEX = "*{aab2a9c61304abf0f6a18ba93db2d3c2} Decision PDF"
# A token present in every BTA decision ("the BOARD OF TAX APPEALS ...").
UNIVERSAL_TERM = "board"
FULL_MAXFILES = 25000
SAMPLE_MAXFILES = 40

MON = ["January", "February", "March", "April", "May", "June", "July",
       "August", "September", "October", "November", "December"]
DATE_RE = re.compile(r"\b(" + "|".join(MON) + r")\s+(\d{1,2}),\s+(\d{4})")

# Result row: <a ... href="<PDFURL>#xml=...">FILENAME.pdf</a> ... Date: M/D/YYYY ... Title: ...
ROW_RE = re.compile(r"<TR valign=top>.*?</TR>", re.S | re.I)
HREF_RE = re.compile(r'href="(https://apps\.bta\.wa\.gov/[^"#]+\.pdf)', re.I)
FNAME_RE = re.compile(r">([^<>]+\.pdf)</a>", re.I)
FDATE_RE = re.compile(r"<B>Date:</B>\s*([0-9/]+)", re.I)
TITLE_RE = re.compile(r"<B>Title:</B>\s*(.*?)<BR>", re.I | re.S)
INDEX_FIELD_RE = re.compile(
    r'name="index"[^>]*value="([^"]*)"', re.I)


class WABTAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.8
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        self._index = None

    # ---- HTTP helpers ----------------------------------------------------

    def _curl_get(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl GET failed for {url} (try {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_search(self, max_files: int) -> str | None:
        data = [
            ("cmd", "search"),
            ("request", UNIVERSAL_TERM),
            ("SearchForm", "/dtSearch_form.html"),
            ("OrigSearchForm", "/dtSearch_form.html"),
            ("index", self._get_index()),
            ("maxFiles", str(max_files)),
            ("autoStopLimit", "0"),
        ]
        args = ["curl", "-s", "-L", "--max-time", "180", "-A", self._ua, "-X", "POST"]
        for k, v in data:
            args += ["--data-urlencode", f"{k}={v}"]
        args.append(DLL)
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(args, capture_output=True, timeout=210)
                if out.returncode == 0 and out.stdout:
                    body = out.stdout.decode("utf-8", "replace")
                    if "Decision%20PDF" in body or "ResultsTable" in body:
                        return body
                    logger.warning("dtSearch returned no results page "
                                   f"(try {attempt + 1}); first 200: {body[:200]}")
            except Exception as e:
                logger.warning(f"dtSearch POST failed (try {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _get_index(self) -> str:
        if self._index:
            return self._index
        html = self._curl_get(FORM_URL)
        if html:
            m = INDEX_FIELD_RE.search(html.decode("utf-8", "replace"))
            if m and m.group(1).strip() and not m.group(1).startswith("%%"):
                self._index = html_lib.unescape(m.group(1)).strip()
                logger.info(f"dtSearch index: {self._index}")
                return self._index
        self._index = DEFAULT_INDEX
        logger.info(f"dtSearch index (default): {self._index}")
        return self._index

    # ---- parsing ---------------------------------------------------------

    @staticmethod
    def _docket_from_fname(fname: str) -> str:
        return re.sub(r"\.pdf$", "", fname, flags=re.I).strip()

    @staticmethod
    def _slug(pdf_url: str) -> str:
        # last path segment without extension, plus parent folder for uniqueness
        parts = [p for p in pdf_url.split("/") if p]
        name = re.sub(r"\.pdf$", "", parts[-1], flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return slug[:120] or "decision"

    @staticmethod
    def _fdate_iso(s: str | None) -> str | None:
        if not s:
            return None
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s.strip())
        if not m:
            return None
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1960 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    def discover_documents(self, sample: bool = False) -> list[dict]:
        body = self._curl_search(SAMPLE_MAXFILES if sample else FULL_MAXFILES)
        if not body:
            logger.error("dtSearch query returned no body")
            return []
        out: list[dict] = []
        seen: set[str] = set()
        for row in ROW_RE.findall(body):
            hm = HREF_RE.search(row)
            if not hm:
                continue
            pdf_url = html_lib.unescape(hm.group(1))
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            fn = FNAME_RE.search(row)
            fname = html_lib.unescape(fn.group(1)) if fn else pdf_url.rsplit("/", 1)[-1]
            tm = TITLE_RE.search(row)
            raw_title = re.sub(r"\s+", " ", html_lib.unescape(tm.group(1))).strip() if tm else ""
            dm = FDATE_RE.search(row)
            out.append({
                "pdf_url": pdf_url,
                "docket": self._docket_from_fname(fname),
                "slug": self._slug(pdf_url),
                "index_title": raw_title,
                "file_date": self._fdate_iso(dm.group(1) if dm else None),
                "formal": "Formal%20Dockets" in pdf_url or "Formal Dockets" in pdf_url,
            })
            if sample and len(out) >= 25:
                break
        logger.info(f"Discovered {len(out)} WA Board of Tax Appeals decisions")
        return out

    @staticmethod
    def _decision_date(text: str, fallback: str | None) -> str | None:
        # The decision is dated at/after every event it recites; the latest
        # in-document "Month D, YYYY" is the best proxy for the decision date.
        best = None
        for mo, d, y in DATE_RE.findall(text):
            try:
                iso = f"{int(y):04d}-{MON.index(mo) + 1:02d}-{int(d):02d}"
            except ValueError:
                continue
            if 1960 <= int(y) <= 2035 and (best is None or iso > best):
                best = iso
        return best or fallback

    @staticmethod
    def _clean_caption(name: str) -> str | None:
        # Older formatted decisions carry left-margin pleading line numbers and
        # ")" caption brackets around the parties; strip those artifacts.
        name = re.sub(r"\s+", " ", name).strip()
        name = re.sub(r"^(?:\d+\s+)+", "", name)        # leading line numbers
        name = name.replace(")", " ").replace("(", " ").replace("|", " ")
        name = re.sub(r"\s+", " ", name).strip().rstrip(",").strip()
        if len(name) < 3 or len(name) > 160:
            return None
        # Reject OCR garbage / line-number runs: require a reasonable letter ratio.
        letters = sum(c.isalpha() for c in name)
        if letters < max(3, 0.5 * len(name)):
            return None
        if "BOARD OF TAX APPEALS" in name.upper():
            return None
        return name

    @classmethod
    def _case_name(cls, text: str, index_title: str, docket: str) -> str | None:
        # Caption: "...BOARD OF TAX APPEALS\nSTATE OF WASHINGTON\n<APPELLANT>,\nDocket No...."
        m = re.search(r"STATE OF WASHINGTON\s*[,\n]?\s*(.+?)\s+Docket No", text, re.S | re.I)
        if m:
            name = cls._clean_caption(m.group(1))
            if name:
                return name
        # Fall back to the dtSearch index title minus boilerplate.
        t = re.sub(r"(?i)^(BEFORE\s+)?(THE\s+)?BOARD OF TAX APPEALS\s+STATE OF WASHINGTON\s*",
                   "", index_title).strip()
        return cls._clean_caption(t) if t else None

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_get(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/WA-BTA", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._decision_date(text, doc.get("file_date"))
        doc["case_name"] = self._case_name(text, doc.get("index_title", ""), doc["docket"])
        return doc

    def test_api(self) -> bool:
        logger.info("Testing WA Board of Tax Appeals dtSearch + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample query)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name')} [{raw.get('date')}]")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        case_name = (raw.get("case_name") or "").strip()
        docket = raw.get("docket") or ""
        if case_name and docket:
            title = f"{case_name} v. Washington Dept. of Revenue (BTA Docket No. {docket})"
        elif case_name:
            title = case_name
        else:
            title = f"Washington Board of Tax Appeals — Decision {docket}".strip()
        return {
            "_id": f"US/WA-BTA/{raw['slug']}",
            "_source": "US/WA-BTA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket": docket or None,
            "court": "Washington State Board of Tax Appeals",
            "decision_type": "Formal" if raw.get("formal") else "Informal/Other",
            "case_name": case_name or None,
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WA",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
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

    parser = argparse.ArgumentParser(description="US/WA-BTA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WABTAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
