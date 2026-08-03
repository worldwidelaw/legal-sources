#!/usr/bin/env python3
"""
US/PA-BFR -- Pennsylvania Board of Finance and Revenue (Decisions)

Fetches the full text of the published decisions of the Pennsylvania Board
of Finance and Revenue (BF&R) — the Commonwealth's independent administrative
tax-appeal tribunal that reviews petitions for refund/reassessment of state
taxes (Personal Income, Corporate/Franchise/Capital Stock, Sales & Use,
Realty Transfer, and other "Miscellaneous" taxes) appealed from the
Department of Revenue's Board of Appeals. Each redacted "Decision and Order"
resolves a specific petition (taxpayer v. Commonwealth Dept. of Revenue), so
the corpus is case_law. Decisions are published since April 1, 2014.

The Board publishes the decisions through a public search app at
  https://bfrcases.patreasury.gov/DecisionSearch
which is an ASP.NET WebForms page whose results load from a clean PageMethod
JSON endpoint. The docket-search PageMethod returns the WHOLE published
corpus when queried with an empty docket (a SQL LIKE '%%' match):
  POST /DecisionSearch.aspx/SearchDocket   body {"Docket":""}
Each result row carries the TransactionId, DocketNumber, PetitionerName,
TaxName, SubTaxName and the redacted-decision PDF FileName; the PDF itself is
served from OpenDocument.aspx?id={TransactionId}&fname={FileName}. No
JavaScript, no CAPTCHA, no auth.

Strategy:
  1. POST SearchDocket {"Docket":""} once and parse every result row.
  2. Download each decision PDF and extract its text layer via
     common.pdf_extract.
  3. Parse the decision date from the standard "AND NOW, <Month D, YYYY>,"
     ordering clause; normalize into the case_law schema.

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
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.US.PA-BFR")

BASE_URL = "https://bfrcases.patreasury.gov"
SEARCH_URL = f"{BASE_URL}/DecisionSearch.aspx/SearchDocket"
DOC_URL = f"{BASE_URL}/OpenDocument.aspx"

MON = ["January", "February", "March", "April", "May", "June", "July",
       "August", "September", "October", "November", "December"]
# Tolerate an embedded newline in "May 3,\n2013".
DATE_RE = re.compile(
    r"\b(" + "|".join(MON) + r")\s+(\d{1,2}),\s*(\d{4})")
# The BF&R decision is dated by its ordering clause: "AND NOW, <date>, ...".
ANDNOW_RE = re.compile(
    r"AND NOW,?\s+(" + "|".join(MON) + r")\s+(\d{1,2}),\s*(\d{4})", re.I)


class PABFRScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.8
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

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

    def _curl_search(self) -> list[dict]:
        args = ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua,
                "-H", "Content-Type: application/json; charset=utf-8",
                "-H", "X-Requested-With: XMLHttpRequest",
                "-X", "POST", "--data", '{"Docket":""}', SEARCH_URL]
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(args, capture_output=True, timeout=150)
                if out.returncode == 0 and out.stdout:
                    data = json.loads(out.stdout.decode("utf-8", "replace"))
                    rows = data.get("d", [])
                    if rows and rows[0].get("TransactionId"):
                        return rows
            except Exception as e:
                logger.warning(f"SearchDocket POST failed (try {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return []

    # ---- parsing ---------------------------------------------------------

    @staticmethod
    def _slug(tid: int, docket: str | None) -> str:
        base = f"{tid}-{docket}" if docket else str(tid)
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")
        return slug[:120] or f"decision-{tid}"

    def discover_documents(self, sample: bool = False) -> list[dict]:
        rows = self._curl_search()
        if not rows:
            logger.error("SearchDocket returned no rows")
            return []
        out: list[dict] = []
        seen: set[int] = set()
        for r in rows:
            tid = r.get("TransactionId")
            fname = r.get("FileName") or ""
            if not tid or tid in seen or not fname:
                continue
            seen.add(tid)
            docket = (r.get("DocketNumber") or "").strip() or None
            pdf_url = (f"{DOC_URL}?id={tid}"
                       f"&fname={urllib.parse.quote(fname)}")
            out.append({
                "transaction_id": tid,
                "pdf_url": pdf_url,
                "slug": self._slug(tid, docket),
                "docket": docket,
                "petitioner": (r.get("PetitionerName") or "").strip() or None,
                "tax_name": (r.get("TaxName") or "").strip() or None,
                "sub_tax_name": (r.get("SubTaxName") or "").strip() or None,
            })
            if sample and len(out) >= 25:
                break
        logger.info(f"Discovered {len(out)} PA Board of Finance and Revenue decisions")
        return out

    @classmethod
    def _decision_date(cls, text: str) -> str | None:
        m = ANDNOW_RE.search(text)
        candidates = [m] if m else []
        if not candidates:
            # Fall back to the latest "Month D, YYYY" in the document.
            best = None
            for mo, d, y in DATE_RE.findall(text):
                try:
                    iso = f"{int(y):04d}-{MON.index(mo) + 1:02d}-{int(d):02d}"
                except ValueError:
                    continue
                if 2000 <= int(y) <= 2035 and (best is None or iso > best):
                    best = iso
            return best
        mo, d, y = m.group(1), m.group(2), m.group(3)
        try:
            if 2000 <= int(y) <= 2035:
                return f"{int(y):04d}-{MON.index(mo.capitalize()) + 1:02d}-{int(d):02d}"
        except ValueError:
            pass
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_get(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/PA-BFR", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._decision_date(text)
        return doc

    def test_api(self) -> bool:
        logger.info("Testing PA Board of Finance and Revenue search + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('petitioner')} [{raw.get('date')}]")
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
        petitioner = raw.get("petitioner") or ""
        docket = raw.get("docket") or ""
        if petitioner and docket:
            title = (f"In re {petitioner} (Pa. Bd. of Finance and Revenue, "
                     f"BF&R Docket No. {docket})")
        elif petitioner:
            title = f"In re {petitioner} (Pa. Bd. of Finance and Revenue)"
        else:
            title = f"Pennsylvania Board of Finance and Revenue — Decision {docket}".strip()
        return {
            "_id": f"US/PA-BFR/{raw['slug']}",
            "_source": "US/PA-BFR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket": docket or None,
            "court": "Pennsylvania Board of Finance and Revenue",
            "case_name": (f"In re {petitioner}" if petitioner else None),
            "petitioner": petitioner or None,
            "tax_type": raw.get("tax_name") or None,
            "tax_subtype": raw.get("sub_tax_name") or None,
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-PA",
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

    parser = argparse.ArgumentParser(description="US/PA-BFR bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PABFRScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
