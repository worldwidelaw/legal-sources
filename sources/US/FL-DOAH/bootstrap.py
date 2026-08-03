#!/usr/bin/env python3
"""
US/FL-DOAH -- Florida Division of Administrative Hearings (DOAH)
Recommended / Final Orders

Fetches the full text of every published order issued by an
Administrative Law Judge of the Florida Division of Administrative
Hearings (DOAH). DOAH is Florida's central, independent administrative
tribunal (the "central panel"): an ALJ hears a contested case between a
person and a State agency under the Administrative Procedure Act
(Ch. 120, Fla. Stat.) and issues a Recommended Order (later adopted by
the agency as a Final Order), a Summary Final Order, or a Final Order
directly. Each order resolves a specific contested case = case_law.
Orders are official Florida state-government works in the public domain
(government edicts, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The order archive is served as browsable IIS directory listings, one
  directory per year, at
      https://www.doah.state.fl.us/ROS/{YEAR}/
  covering 1975-present. Each listing links the born-digital text-layer
  order PDFs at
      https://www.doah.state.fl.us/ROS/{YEAR}/{casedigits}[suffix].pdf
  where {casedigits} is the 8-digit DOAH case number YYNNNNNN (e.g.
  25000021.pdf == case 25-000021). A duplicate electronically-signed
  copy sometimes exists as {casedigits}_282_<date>_<n>_e.pdf; when a
  plain copy exists the "_e" duplicate is skipped. "Amended" revisions
  ({casedigits}Amended.pdf) are kept as distinct records.

Strategy:
  1. For each year directory /ROS/{YEAR}/, GET the IIS listing and
     collect every *.pdf filename.
  2. Group by 8-digit case number; drop the "_e" e-signature duplicate
     when a plain copy of the same case exists.
  3. Download each PDF (curl, browser UA, ~1 req/s), extract its text
     via common.pdf_extract, and parse the DOAH case number, order
     type, decision date and parties from the order body.

Usage:
  python bootstrap.py bootstrap            # Full pull (1975-present)
  python bootstrap.py bootstrap --sample   # Fetch ~12 recent samples
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
logger = logging.getLogger("legal-data-hunter.US.FL-DOAH")

BASE_URL = "https://www.doah.state.fl.us"
DIR_TEMPLATE = BASE_URL + "/ROS/{year}/"
FIRST_YEAR = 1975

# links in the IIS directory listing: <A HREF="/ROS/2025/25000021.pdf">...
PDF_HREF_RE = re.compile(r'href="(/ROS/\d{4}/[^"]+?\.pdf)"', re.I)
# 8 leading digits of a filename = DOAH case number YYNNNNNN
CASE_DIGITS_RE = re.compile(r"(\d{8})")
# an electronically-signed duplicate copy
ESIG_RE = re.compile(r"_\d+_\d{6,8}_\d+_e\.pdf$", re.I)

# "Case No. 26-1139PL" (optional letter suffix encodes the case type)
CASE_NO_RE = re.compile(r"Case\s+No\.?\s*(\d{2}-\d{3,4}[A-Z]{0,5})", re.I)
# "DONE AND ENTERED this 1st day of July, 2026"
DATE_ENTERED_RE = re.compile(
    r"(?:DONE\s+AND\s+(?:ENTERED|ORDERED)|ENTERED)[^.]*?"
    r"(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+([A-Za-z]+),?\s+(\d{4})",
    re.I,
)
# generic "Month D, YYYY" fallback
DATE_MDY_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})", re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

# order-type headings, most-specific first
ORDER_TYPES = [
    "SUMMARY FINAL ORDER",
    "FINAL ORDER AND ORDER CLOSING FILE",
    "RECOMMENDED ORDER OF DISMISSAL",
    "RECOMMENDED ORDER",
    "FINAL ORDER",
    "ORDER CLOSING FILE AND RELINQUISHING JURISDICTION",
    "ORDER CLOSING FILE",
    "ORDER OF DISMISSAL",
    "ORDER DENYING",
    "ORDER GRANTING",
    "ORDER",
]

AGENCY_HINTS = ("DEPARTMENT", "BOARD", "COMMISSION", "AGENCY", "DIVISION",
                "OFFICE", "AUTHORITY", "DISTRICT", "COUNTY", "CITY OF",
                "SCHOOL")


class FLDOAHScraper(BaseScraper):

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
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_text(self, url: str) -> str | None:
        b = self._curl_bytes(url)
        return b.decode("latin-1", "replace") if b else None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _case_digits(filename: str) -> str | None:
        m = CASE_DIGITS_RE.match(filename)
        return m.group(1) if m else None

    @staticmethod
    def _case_number_from_digits(digits: str) -> str:
        # digits = YYNNNNNN ; year prefix 75-99 -> 19xx else 20xx
        yy = int(digits[:2])
        return f"{digits[:2]}-{digits[2:]}"

    # --------------------------------------------------------- discovery
    def _list_year(self, year: int) -> list[dict]:
        html = self._curl_text(DIR_TEMPLATE.format(year=year))
        if not html:
            return []
        hrefs = []
        seen = set()
        for path in PDF_HREF_RE.findall(html):
            fn = path.rsplit("/", 1)[-1]
            if fn.lower() in seen:
                continue
            seen.add(fn.lower())
            hrefs.append((fn, path))

        # group by 8-digit case number to drop "_e" duplicates when a
        # plain copy exists
        by_case: dict[str, list[tuple[str, str]]] = {}
        loose: list[tuple[str, str]] = []
        for fn, path in hrefs:
            d = self._case_digits(fn)
            if d:
                by_case.setdefault(d, []).append((fn, path))
            else:
                loose.append((fn, path))

        docs: list[dict] = []
        for digits, files in by_case.items():
            plain_exists = any(not ESIG_RE.search(fn) for fn, _ in files)
            for fn, path in files:
                if ESIG_RE.search(fn) and plain_exists:
                    continue  # skip e-signature duplicate of the plain copy
                docs.append(self._mk_doc(fn, path, year, digits))
        for fn, path in loose:
            docs.append(self._mk_doc(fn, path, year, None))
        # deterministic order within a year
        docs.sort(key=lambda r: r["filename"].lower())
        return docs

    def _mk_doc(self, fn: str, path: str, year: int, digits: str | None) -> dict:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "-", fn)[:80]
        return {
            "filename": fn,
            "safe_slug": f"{year}-{safe}",
            "doc_url": BASE_URL + path,
            "listing_year": str(year),
            "case_digits": digits,
            "case_number_guess": (
                self._case_number_from_digits(digits) if digits else None
            ),
        }

    def discover_documents(self, sample: bool = False) -> list[dict]:
        this_year = datetime.now(timezone.utc).year
        years = list(range(this_year, FIRST_YEAR - 1, -1))
        out: list[dict] = []
        for year in years:
            docs = self._list_year(year)
            logger.info(f"  /ROS/{year}/: {len(docs)} order PDFs "
                        f"(total {len(out) + len(docs)})")
            out.extend(docs)
            if sample and len(out) >= 20:
                break
        logger.info(f"Discovered {len(out)} DOAH order documents")
        return out

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/FL-DOAH", doc["safe_slug"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        text = text.strip()
        doc = dict(doc)
        doc["text"] = text
        doc["case_number"] = self._case_number(text, doc.get("case_number_guess"))
        doc["order_type"] = self._order_type(text)
        doc["date"] = self._doc_date(text, doc.get("listing_year"))
        pet, res, agency = self._parties(text)
        doc["petitioner"] = pet
        doc["respondent"] = res
        doc["agency"] = agency
        return doc

    @staticmethod
    def _case_number(text: str, guess: str | None) -> str | None:
        m = CASE_NO_RE.search(text)
        if m:
            return m.group(1).upper()
        return guess

    @staticmethod
    def _order_type(text: str) -> str | None:
        head = text[:4000].upper()
        for ot in ORDER_TYPES:
            if re.search(r"(?m)^\s*" + re.escape(ot) + r"\s*$", head):
                return ot.title()
        for ot in ORDER_TYPES:
            if ot in head:
                return ot.title()
        return None

    @staticmethod
    def _doc_date(text: str, year: str | None) -> str | None:
        m = DATE_ENTERED_RE.search(text)
        if m:
            mo = MONTHS.get(m.group(2).lower())
            d = int(m.group(1))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1970 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        # last "Month D, YYYY" in the doc (orders are dated near the end)
        best = None
        for m in DATE_MDY_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1970 <= y <= 2035:
                best = f"{y:04d}-{mo:02d}-{d:02d}"
        if best:
            return best
        if year and re.fullmatch(r"\d{4}", year.strip()):
            return f"{year.strip()}-01-01"
        return None

    _NAME = r"[A-Z0-9][A-Za-z0-9 .,&'/()\-]{2,90}?"

    @classmethod
    def _parties(cls, text: str):
        """Extract petitioner, respondent and the agency party from the
        DOAH caption. Captions read:
            <PETITIONER>, Petitioner(s), vs. Case No. ... <RESPONDENT>,
            Respondent(s)."""
        # flatten the caption table punctuation
        t = re.sub(r"[)]+", " ", text[:2500])
        t = re.sub(r"[ \t]+", " ", t)
        pet = res = None
        pm = re.search(rf"({cls._NAME}),\s*Petitioner", t)
        if pm:
            pet = re.sub(r"\s+", " ", pm.group(1)).strip(" ,.")
        rm = re.search(rf"({cls._NAME}),\s*Respondent", t)
        if rm:
            cand = re.sub(r"\s+", " ", rm.group(1)).strip(" ,.")
            # strip a leading "Case No. ..." that can precede the respondent
            cand = re.sub(r"^.*?Case\s+No\.?\s*\S+\s*", "", cand, flags=re.I).strip()
            res = cand or None
        agency = None
        for party in (pet, res):
            if party and any(h in party.upper() for h in AGENCY_HINTS):
                agency = party
                break
        return pet, res, agency

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing FL DOAH directory discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} / {raw.get('order_type')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cn = raw.get("case_number")
        ot = raw.get("order_type") or "Order"
        pet, res = raw.get("petitioner"), raw.get("respondent")
        title = f"DOAH {cn} {ot}" if cn else f"DOAH {ot}"
        if pet and res:
            title = f"{title}: {pet} vs. {res}"
        title = re.sub(r"\s+", " ", title).strip()[:300]
        return {
            "_id": f"US/FL-DOAH/{raw['safe_slug']}",
            "_source": "US/FL-DOAH",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "filename": raw["filename"],
            "case_number": cn,
            "order_type": ot,
            "petitioner": pet,
            "respondent": res,
            "agency": raw.get("agency"),
            "issuer": "Florida Division of Administrative Hearings",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-FL",
        }

    # ------------------------------------------------------------- fetch
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

    parser = argparse.ArgumentParser(description="US/FL-DOAH bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLDOAHScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
