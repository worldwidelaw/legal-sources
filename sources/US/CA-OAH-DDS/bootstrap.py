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
  The decision library moved to the same Sitecore "MediaSearch" widget
  the Special Education library uses. The listing page itself renders
  only the first 10 rows; every page is served by the API endpoint:

      https://www.dgs.ca.gov/api/sitecore/MediaSearch/GetSearchResults
          ?page={N}
          &folderPath=/sitecore/media library/Divisions/OAH/General Jurisdiction/DDS Decisions
          &sortBy=date_desc

  Each page returns an HTML fragment of ~10 `result-item` rows linking
  to a born-digital text-layer PDF by opaque GUID:

      /-/media/<32-hex-guid>.pdf

  The human filename (link text) begins with the OAH case number (a
  10-digit YYYYMMNNNN string) followed by the "084" DDS agency code and
  optional suffixes (Acc = accessibility-remediated, Adopted, Revised,
  or a consolidated "<case1>-<case2>084"). ~256 pages are indexed
  (newest→oldest), so ~2,550 decisions.

  Each PDF opens "BEFORE THE OFFICE OF ADMINISTRATIVE HEARINGS STATE OF
  CALIFORNIA" (or "BEFORE THE DEPARTMENT OF DEVELOPMENTAL SERVICES" for
  proposed decisions), carries "OAH No. <caseno>" and the decision date
  in the body, and is extracted via common.pdf_extract (no OCR needed).

Strategy:
  1. Walk the MediaSearch API pages until a page returns no rows
     (retrying a zero-row page a few times for transient empties).
  2. For each row collect the GUID PDF url, the OAH case number from the
     link filename, and the listing "Document Date".
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
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-OAH-DDS")

BASE_URL = "https://www.dgs.ca.gov"
FOLDER_PATH = "/sitecore/media library/Divisions/OAH/General Jurisdiction/DDS Decisions"
SEARCH_TEMPLATE = (
    BASE_URL + "/api/sitecore/MediaSearch/GetSearchResults"
    "?page={page}&folderPath={folder}&sortBy=date_desc"
)
LISTING_PAGE = (
    BASE_URL + "/OAH/Case-Types/General-Jurisdiction/Resources/DDS-Decisions"
)

# One MediaSearch result row: title, GUID media link, human filename,
# document date. Filenames vary: <caseno>084.pdf, <caseno>084Acc.pdf,
# <caseno>Adopted084.pdf, <caseno>084-Revised.pdf,
# <case1>-<case2>084.pdf.
RESULT_RE = re.compile(
    r'<div class="result-item">.*?'
    r'<div class="result-title">\s*(?P<title>.*?)\s*</div>.*?'
    r'href="(?P<url>/-/media/[a-f0-9]+\.pdf)"[^>]*>\s*(?P<file>.*?)\s*</a>'
    r'(?:.*?<strong>Document Date:</strong>\s*(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4}))?',
    re.I | re.S,
)
GUID_RE = re.compile(r"/-/media/([a-f0-9]+)\.pdf", re.I)
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
    def _curl_bytes(self, url: str, xhr: bool = False) -> bytes | None:
        headers = ["-H", "Accept: */*"]
        if xhr:
            headers += ["-H", "X-Requested-With: XMLHttpRequest",
                        "-H", f"Referer: {LISTING_PAGE}"]
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     *headers, url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_text(self, url: str, xhr: bool = False) -> str | None:
        b = self._curl_bytes(url, xhr=xhr)
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

    @staticmethod
    def _listing_date(s: str | None) -> str | None:
        if not s:
            return None
        m = DATE_RE.search(s)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d, y = int(m.group(2)), int(m.group(3))
        if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield decision metadata page-by-page (lazy) so sample mode stops
        after ~1 listing page instead of crawling the whole index first."""
        folder = quote(FOLDER_PATH, safe="")
        seen: set[str] = set()
        total = 0
        page = 1
        while True:
            url = SEARCH_TEMPLATE.format(page=page, folder=folder)
            # Retry a zero-row page a few times: the API occasionally returns
            # an empty/partial fragment transiently, and a false "0 rows"
            # would silently drop 10 real decisions or end the crawl early.
            rows = []
            for _ in range(3):
                html = self._curl_text(url, xhr=True)
                rows = list(RESULT_RE.finditer(html)) if html else []
                if rows:
                    break
                time.sleep(1.5)
            if not rows:
                logger.info(f"  page {page}: no results after retries — stopping")
                break
            new_on_page = 0
            for m in rows:
                url_pdf = m.group("url")
                gm = GUID_RE.search(url_pdf)
                if not gm:
                    continue
                guid = gm.group(1)
                if guid in seen:
                    continue
                seen.add(guid)
                filename = re.sub(r"\s+", " ", m.group("file") or "").strip()
                cnums = self._case_numbers(filename)
                total += 1
                new_on_page += 1
                yield {
                    "guid": guid,
                    "file": filename,
                    "doc_url": BASE_URL + url_pdf,
                    "case_numbers": cnums,
                    "case_number": cnums[0] if cnums else None,
                    "listing_date": self._listing_date(m.group("date")),
                    "page": page,
                }
            logger.info(f"  page {page}: {new_on_page} decisions (total {total})")
            if sample and total >= 16:
                break
            page += 1
            if page > 600:  # hard safety cap (~6,000 docs)
                logger.warning("Reached page safety cap (600)")
                break
        logger.info(f"Discovered {total} CA OAH DDS (Lanterman Act) decisions")

    # ------------------------------------------------------- build record
    @staticmethod
    def _doc_id(doc: dict) -> str:
        # stable id from the media filename (unique within the folder, and
        # readable); the opaque GUID is the fallback when the MediaSearch
        # row carries no link text.
        filename = doc.get("file") or ""
        stem = re.sub(r"\.pdf$", "", filename, flags=re.I).strip()
        if not stem:
            return doc["guid"]
        return re.sub(r"[^A-Za-z0-9_-]", "_", stem)

    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        doc_id = self._doc_id(doc)
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
        doc["date"] = self._decision_date(text) or doc.get("listing_date")
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
        # DDS captions read "In the Matter of: CLAIMANT and <REGIONAL
        # CENTER>, Service Agency." near the top ("vs."/"v." on older
        # decisions).
        t = re.sub(r"[ \t]+", " ", text[:1500])
        m = re.search(
            r"In the Matter of:?\s*\n?\s*(?P<a>%s)\s*\n?\s*"
            r"(?:vs?\.|v\.|and)\s*\n?\s*(?P<b>%s)[.,]" % (_NAME, _NAME),
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
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
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
