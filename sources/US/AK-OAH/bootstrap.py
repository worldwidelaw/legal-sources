#!/usr/bin/env python3
"""
US/AK-OAH -- Alaska Office of Administrative Hearings (OAH) — Decisions

Fetches the full text of every published final decision of the Alaska
Office of Administrative Hearings (OAH). OAH is Alaska's centralized,
independent administrative tribunal AND, by statute, the state's tax
court of general jurisdiction (AS 44.64): a single hearing officer /
administrative law judge hears a contested case between a person and a
State agency — state tax appeals (corporate income tax, oil & gas
production tax, fisheries taxes), permanent-fund-dividend, public
assistance, Medicaid, professional licensing, and many other agency
matters — and issues a final decision that resolves that specific case
= case_law. AS 44.64.070 requires OAH to make its final decisions
available online in an electronic database. These are official Alaska
state-government works in the public domain (government edicts,
17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  Every individual decision is served as a born-digital PDF at a
  deterministic, sequential URL:

      https://aws.state.ak.us/OAH/Decision/Display?rec={N}

  where {N} is a sequential integer record id. The PDF body opens
  "BEFORE THE ALASKA / STATE OF ALASKA OFFICE OF ADMINISTRATIVE
  HEARINGS ..." and carries an OAH case number of the form
  "OAH No. YY-NNNN-XXX", whose trailing suffix encodes the case
  category (TAX, PFD, MED, DHS, ACU, etc.).

VANTAGE / ACCESS:
  aws.state.ak.us (158.145.75.84) TCP-times-out on :443 from every
  build vantage tried (datacenter + residential + WebFetch egress) —
  the host silently drops foreign connections. The Internet Archive,
  however, crawled the whole Decision/Display tree from a reachable
  vantage: ~5,650 decision PDFs are preserved with HTTP 200. We
  therefore discover + fetch through the Wayback Machine, which:
    * enumerates every rec id via the CDX API (statuscode:200, one row
      per URL), and
    * serves the original PDF bytes verbatim via /web/{ts}id_/{url}.
  Fully public open data, no JavaScript / CAPTCHA / auth.

Strategy:
  1. Query the CDX API for aws.state.ak.us/OAH/Decision/Display* and
     collapse to one capture per URL; parse the rec id from each.
  2. Fetch each capture's PDF bytes from the Wayback id_ endpoint.
  3. Extract full text via the shared common.pdf_extract helper; keep
     only pages carrying the OAH decision markers + enough text.
  4. Parse the OAH case number, category, matter and decision date from
     the text; normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all CDX recs)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AK-OAH")

BASE_URL = "https://aws.state.ak.us/OAH"
DISPLAY_TEMPLATE = "https://aws.state.ak.us/OAH/Decision/Display?rec={rec}"

# Wayback endpoints — the live host geo/drop-blocks foreign IPs, so we go
# through the Internet Archive which crawled the Display tree.
CDX_API = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"
DISPLAY_PREFIX = "aws.state.ak.us/OAH/Decision/Display"
REC_RE = re.compile(r"[?&]rec=(\d+)", re.I)

# Markers that confirm a page is an actual OAH decision (not a gap /
# error / listing page).
OAH_MARKERS = (
    "OFFICE OF ADMINISTRATIVE HEARINGS",
    "OAH No.",
    "OAH NO.",
)

# OAH case number, e.g. "OAH No. 15-0873-TAX" / "OAH No. 09-1234-PFD".
CASE_NO_RE = re.compile(r"OAH\s+No\.?\s*([0-9]{2}-[0-9]{3,5}-[A-Z]{2,5})", re.I)

DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

# Case-number suffix -> readable category / subject matter.
CATEGORY_NAMES = {
    "TAX": "Tax",
    "PFD": "Permanent Fund Dividend",
    "MED": "Medicaid",
    "DHS": "Health & Social Services",
    "DPA": "Public Assistance",
    "PA": "Public Assistance",
    "INS": "Insurance",
    "OIL": "Oil & Gas",
    "CBP": "Child Care Benefits",
    "PFE": "Permanent Fund Dividend (Eligibility)",
    "LIC": "Professional Licensing",
    "REV": "Revenue",
}

# "In the Matter of ..." caption line -> matter title.
MATTER_RE = re.compile(
    r"In\s+the\s+Matter\s+of[:\s]+(.{3,180}?)(?:[.\n]|OAH|Agency|Case)",
    re.I | re.S,
)


class AKOAHScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua,
                     "-H", "Accept: application/pdf,*/*", url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _cdx(self, retries: int = 4) -> list[dict]:
        """Enumerate all archived Display?rec= captures via the CDX API."""
        url = (
            f"{CDX_API}?url={DISPLAY_PREFIX}*&output=json"
            "&fl=original,timestamp&filter=statuscode:200&collapse=urlkey"
        )
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            raw = self._curl_bytes(url)
            if raw:
                try:
                    rows = json.loads(raw.decode("utf-8", "replace"))
                except Exception as e:
                    logger.warning(f"CDX parse error (attempt {attempt + 1}): {e}")
                    time.sleep(3 * (attempt + 1))
                    continue
                out = []
                seen: set[int] = set()
                for r in rows[1:]:  # first row is the header
                    if len(r) < 2:
                        continue
                    m = REC_RE.search(r[0])
                    if not m:
                        continue
                    rec = int(m.group(1))
                    if rec in seen:
                        continue
                    seen.add(rec)
                    out.append({"rec": rec, "timestamp": r[1], "original": r[0]})
                out.sort(key=lambda d: d["rec"])
                return out
            time.sleep(3 * (attempt + 1))
        return []

    # ------------------------------------------------------------- parsing
    @staticmethod
    def _clean(text: str) -> str | None:
        """Normalize extracted PDF text; require OAH decision markers."""
        if not text:
            return None
        text = text.replace("\r", "\n")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()
        if not text:
            return None
        upper = text.upper()
        if not any(m.upper() in upper for m in OAH_MARKERS):
            return None
        return text

    @staticmethod
    def _case_number(text: str) -> str | None:
        m = CASE_NO_RE.search(text)
        return m.group(1).upper() if m else None

    @staticmethod
    def _category(case_number: str | None) -> str | None:
        if not case_number:
            return None
        suffix = case_number.rsplit("-", 1)[-1].upper()
        return CATEGORY_NAMES.get(suffix, suffix)

    @staticmethod
    def _norm_date(m) -> str | None:
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @classmethod
    def _decision_date(cls, text: str) -> str | None:
        # Prefer an explicit sign-off date ("Dated: Month D, YYYY").
        signoff = re.search(
            r"(?:Dated|Date[d]?\s+this|Entered|Issued)[:\s].{0,40}?" + DATE_RE.pattern,
            text, re.I | re.S,
        )
        if signoff:
            iso = cls._norm_date(signoff)
            if iso:
                return iso
        for m in DATE_RE.finditer(text):
            iso = cls._norm_date(m)
            if iso:
                return iso
        return None

    @staticmethod
    def _matter(text: str) -> str | None:
        m = MATTER_RE.search(text)
        if not m:
            return None
        matter = re.sub(r"\s+", " ", m.group(1)).strip(" ,.;:")
        return matter[:200] or None

    # ---------------------------------------------------------- build raw
    def _build_raw(self, doc: dict) -> dict | None:
        rec = doc["rec"]
        wb_url = WB_RAW.format(ts=doc["timestamp"], url=doc["original"])
        content = self._curl_bytes(wb_url)
        if not content or content[:5] != b"%PDF-":
            return None
        text = extract_pdf_markdown(
            "US/AK-OAH", str(rec), pdf_bytes=content,
            table="case_law", force=True,
        )
        text = self._clean(text or "")
        if not text or len(text) < 400:
            return None
        case_number = self._case_number(text)
        return {
            "rec": rec,
            "text": text,
            "case_number": case_number,
            "category": self._category(case_number),
            "date": self._decision_date(text),
            "matter": self._matter(text),
            "url": DISPLAY_TEMPLATE.format(rec=rec),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing AK OAH via Internet Archive (CDX + PDF)...")
        docs = self._cdx()
        if not docs:
            logger.error("API test FAILED: CDX returned no captures")
            return False
        logger.info(f"  CDX discovered {len(docs)} archived decisions")
        for doc in docs[:8]:
            raw = self._build_raw(doc)
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(
                    f"  rec={doc['rec']} OK ({len(raw['text'])} chars) — "
                    f"{raw.get('case_number') or 'no case no'} "
                    f"date={raw.get('date')}")
                logger.info("API test PASSED")
                return True
        logger.error("API test FAILED: no decision text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cn = raw.get("case_number")
        matter = raw.get("matter")
        category = raw.get("category")
        if cn:
            title = f"Alaska OAH No. {cn}"
        else:
            title = f"Alaska OAH Decision (rec {raw['rec']})"
        if matter:
            title = f"{title}: {matter}"
        title = title[:300]
        return {
            "_id": f"US/AK-OAH/{raw['rec']}",
            "_source": "US/AK-OAH",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "rec": raw["rec"],
            "case_number": cn,
            "category": category,
            "matter": matter,
            "issuer": "Alaska Office of Administrative Hearings",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-AK",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        docs = self._cdx()
        logger.info(f"CDX discovered {len(docs)} archived AK-OAH decisions")
        for doc in docs:
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

    parser = argparse.ArgumentParser(description="US/AK-OAH bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AKOAHScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
