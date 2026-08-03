#!/usr/bin/env python3
"""
US/CA-OAH-DDS -- California Office of Administrative Hearings
(General Jurisdiction Division — DDS / Lanterman Act Decisions)

Fetches the full text of every published DDS ("Department of Developmental
Services") Decision of the California Office of Administrative Hearings
(OAH). OAH is California's central, independent tribunal; through its
General Jurisdiction Division it hears "fair hearing" appeals brought by a
consumer (or their family) against a regional center under the Lanterman
Developmental Disabilities Services Act, when the regional center denies,
reduces, or terminates a service or support. An Administrative Law Judge
issues a Decision that resolves that specific contested case = case_law.
These Decisions are official California state-government works in the
public domain (government edicts).

Access (no CAPTCHA, no auth):
  The decision library is a Sitecore "search list" page paginated
  server-side by a plain ?page=N query parameter:

      https://www.dgs.ca.gov/OAH/Case-Types/General-Jurisdiction/Resources/DDS-Decisions?page={N}

  Each page renders ~25 result rows, each linking to a born-digital
  text-layer PDF in the DDS-Decisions media folder:

      /-/media/Divisions/OAH/General-Jurisdiction/DDS-Decisions/<name>.pdf

  where <name> begins with the OAH case number (a 10-digit YYYYMMNNNN
  string) followed by the "084" DDS agency code and optional suffixes
  (Acc = accessibility-remediated, Adopted, Revised, or a
  consolidated "<case1>-<case2>084"). ~102 pages are indexed
  (page 1..102, oldest→newest), so ~2,550 decisions.

  Each PDF opens
  "BEFORE THE OFFICE OF ADMINISTRATIVE HEARINGS STATE OF CALIFORNIA",
  carries "OAH No. <caseno>" and the decision/issue date in the body,
  and is extracted via common.pdf_extract (no OCR needed).

Strategy:
  1. Walk the ?page=N HTML pages until a page yields no DDS-Decisions
     PDF links (retrying a zero-link page a few times for transient
     empties).
  2. For each row collect the PDF url and the OAH case number from the
     filename.
  3. Download each PDF (curl, browser UA, ~1 req/s), extract text via
     common.pdf_extract, prefer the decision date parsed from the body,
     and normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (~2,550 decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
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
logger = logging.getLogger("legal-data-hunter.US.CA-OAH-DDS")

BASE_URL = "https://www.dgs.ca.gov"
LISTING_TEMPLATE = (
    BASE_URL + "/OAH/Case-Types/General-Jurisdiction/Resources/DDS-Decisions?page={page}"
)
LISTING_PAGE = (
    BASE_URL + "/OAH/Case-Types/General-Jurisdiction/Resources/DDS-Decisions"
)

# Every DDS decision PDF href on a listing page. The path is emitted
# without a leading slash (e.g. -/media/Divisions/OAH/...); we prepend
# BASE_URL + "/". Filenames vary: <caseno>084.pdf, <caseno>084Acc.pdf,
# <caseno>Adopted084.pdf, <caseno>084-Revised.pdf,
# <case1>-<case2>084.pdf.
PDF_HREF_RE = re.compile(
    r'href="/?(-/media/Divisions/OAH/General-Jurisdiction/DDS-Decisions/'
    r'(?P<file>[^"?]+?\.pdf))"',
    re.I,
)
# OAH DDS case numbers are the leading 10-digit YYYYMMNNNN token(s).
CASE_NO_RE = re.compile(r"\b(\d{10})\b")
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
_NAME = r"[A-Za-z][A-Za-z0-9 .,&'/\-]{2,80}?"


class CAOAHDDSScraper(BaseScraper):

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
        return b.decode("utf-8", "replace") if b else None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _case_numbers(filename: str) -> list[str]:
        nums = CASE_NO_RE.findall(filename)
        seen, out = set(), []
        for n in nums:
            # skip the trailing 084 agency code (not 10 digits, so already
            # excluded by \d{10}); keep distinct 10-digit case numbers.
            if n not in seen:
                seen.add(n)
                out.append(n)
        return out

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield decision metadata page-by-page (lazy) so sample mode stops
        after ~1 listing page instead of crawling the whole index first."""
        seen: set[str] = set()
        total = 0
        page = 1
        empty_streak = 0
        while True:
            url = LISTING_TEMPLATE.format(page=page)
            rows = []
            for _ in range(3):
                html = self._curl_text(url)
                rows = list(PDF_HREF_RE.finditer(html)) if html else []
                if rows:
                    break
                time.sleep(1.5)
            if not rows:
                empty_streak += 1
                logger.info(f"  page {page}: no DDS PDF links "
                            f"(empty streak {empty_streak})")
                # Two consecutive genuinely-empty pages = end of listing.
                if empty_streak >= 2:
                    break
                page += 1
                continue
            empty_streak = 0
            new_on_page = 0
            for m in rows:
                rel = m.group(1)
                filename = m.group("file")
                doc_url = f"{BASE_URL}/{rel}"
                key = filename.lower()
                if key in seen:
                    continue
                seen.add(key)
                cnums = self._case_numbers(filename)
                total += 1
                new_on_page += 1
                yield {
                    "file": filename,
                    "doc_url": doc_url,
                    "case_numbers": cnums,
                    "case_number": cnums[0] if cnums else None,
                    "page": page,
                }
            logger.info(f"  page {page}: {new_on_page} decisions (total {total})")
            if sample and total >= 16:
                break
            page += 1
            if page > 200:  # hard safety cap (~5,000 docs)
                logger.warning("Reached page safety cap (200)")
                break
        logger.info(f"Discovered {total} CA OAH DDS (Lanterman Act) decisions")

    # ------------------------------------------------------- build record
    @staticmethod
    def _doc_id(filename: str) -> str:
        # stable id from the media filename (unique within the folder)
        stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
        return re.sub(r"[^A-Za-z0-9_-]", "_", stem)

    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        doc_id = self._doc_id(doc["file"])
        text = pdf_extract.extract_pdf_markdown(
            "US/CA-OAH-DDS", doc_id, pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["doc_id"] = doc_id
        doc["text"] = text.strip()
        doc["date"] = self._decision_date(text)
        doc["parties"] = self._parties(text)
        # prefer the OAH No. printed in the body if the filename had none
        if not doc.get("case_number"):
            bm = re.search(r"OAH\s+No\.?\s*(\d{10})", text, re.I)
            if bm:
                doc["case_number"] = bm.group(1)
                doc["case_numbers"] = [bm.group(1)]
        return doc

    @staticmethod
    def _decision_date(text: str) -> str | None:
        # The last real "Month D, YYYY" is typically the decision/issue
        # (signature) date; the first is usually the hearing date.
        best = None
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d, y = int(m.group(2)), int(m.group(3))
            if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
                best = f"{y:04d}-{mo:02d}-{d:02d}"
        return best

    @classmethod
    def _parties(cls, text: str) -> str | None:
        # DDS captions read "In the Matter of: CLAIMANT vs. <REGIONAL
        # CENTER>, Service Agency." near the top.
        t = re.sub(r"[ \t]+", " ", text[:1500])
        m = re.search(
            r"In the Matter of:?\s*\n?\s*(?P<a>%s)\s*\n?\s*"
            r"(?:vs?\.?|v\.)\s*\n?\s*(?P<b>%s)[.,]" % (_NAME, _NAME),
            t, re.I,
        )
        if not m:
            return None
        a = re.sub(r"\s+", " ", m.group("a")).strip(" ,.")
        b = re.sub(r"\s+", " ", m.group("b")).strip(" ,.")
        if not a or not b:
            return None
        return f"{a} v. {b}"[:250]

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CA OAH DDS listing + PDF extraction...")
        try:
            docs = list(self.discover_documents(sample=True))
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number') or raw.get('doc_id')} "
                            f"[{raw.get('date')}]")
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
        parties = raw.get("parties")
        if cn:
            title = f"CA OAH DDS (Lanterman Act) Decision, OAH No. {cn}"
        else:
            title = "CA OAH DDS (Lanterman Act) Decision"
        if parties:
            title = f"{title}: {parties}"
        title = title[:300]
        return {
            "_id": f"US/CA-OAH-DDS/{raw['doc_id']}",
            "_source": "US/CA-OAH-DDS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "case_number": cn,
            "case_numbers": raw.get("case_numbers") or None,
            "parties": parties,
            "issuer": "California Office of Administrative Hearings, General Jurisdiction Division (DDS / Lanterman Act)",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CA",
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

    parser = argparse.ArgumentParser(description="US/CA-OAH-DDS bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAOAHDDSScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
