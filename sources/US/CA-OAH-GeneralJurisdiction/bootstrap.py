#!/usr/bin/env python3
"""
US/CA-OAH-GeneralJurisdiction -- California Office of Administrative
Hearings (General Jurisdiction Division — agency adjudication Decisions)

Fetches the full text of every published General Jurisdiction Decision
of the California Office of Administrative Hearings (OAH). OAH is
California's central, independent administrative tribunal; its General
Jurisdiction Division adjudicates contested cases for ~1,500 state and
local agencies under the Administrative Procedure Act (Gov. Code
Sec. 11500 et seq.) -- professional-license discipline, agency
enforcement, public-benefit and other administrative disputes -- and an
Administrative Law Judge issues a Decision that resolves that specific
contested case = case_law. These Decisions are official California
state-government works in the public domain (government edicts).
DISTINCT from the sibling US/CA-OAH-SpecialEd (special-education
due-process decisions).

Access (no CAPTCHA, no auth):
  The decision library is a Sitecore media folder browsed at
      https://www.dgs.ca.gov/OAH/Case-Types/General-Jurisdiction/Services/Decisions
  The listing is paginated client-side by a Sitecore MediaSearch API
  that returns a server-rendered HTML fragment (10 results/page):

      https://www.dgs.ca.gov/api/sitecore/MediaSearch/GetSearchResults
          ?page={N}
          &folderPath=/sitecore/media library/Divisions/OAH/General Jurisdiction/GJ Decisions
          &sortBy=date_desc

  (Note: the on-page pagination links use the /sitecore/shell/... prefix,
  which IIS 401-blocks for external clients; the equivalent /api/sitecore/
  prefix is public and returns the same fragment.)

  Each result row exposes:
    - a result-title  (e.g. "2025050862 Decision Accessibility Modified")
    - a document link  /-/media/{guid}.pdf  (born-digital, text-layer PDF)
    - a Document Date  (e.g. "May 08, 2025")

  ~5,344 media items are indexed. The folder ALSO contains non-decision
  noise -- multilingual "Notice of Collection" privacy forms and
  "Quarterly Data Report" statistics -- which carry NO OAH case number;
  and every decision is published in both a plain and an
  "Accessibility Modified"/"084" remediated copy sharing one case number.
  So discovery KEEPS only rows bearing a numeric OAH case number and
  DEDUPES by case number (one Decision per case). PDFs open
  "BEFORE THE OFFICE OF ADMINISTRATIVE HEARINGS STATE OF CALIFORNIA",
  carry the OAH case number(s) and the decision date in the body, and
  are extracted via common.pdf_extract (no OCR needed).

Strategy:
  1. Walk the MediaSearch API pages until a page returns no results.
  2. Keep only rows with a numeric OAH case number; dedupe by case number.
  3. Download each PDF (curl, browser UA, ~1 req/s), extract text via
     common.pdf_extract, prefer the decision date parsed from the body,
     and normalize into the case_law schema.

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
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-OAH-GeneralJurisdiction")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

BASE_URL = "https://www.dgs.ca.gov"
FOLDER_PATH = "/sitecore/media library/Divisions/OAH/General Jurisdiction/GJ Decisions"
SEARCH_TEMPLATE = (
    BASE_URL + "/api/sitecore/MediaSearch/GetSearchResults"
    "?page={page}&folderPath={folder}&sortBy=date_desc"
)
LISTING_PAGE = (
    BASE_URL + "/OAH/Case-Types/General-Jurisdiction/Services/Decisions"
)

# One search-result row: title, media link (guid pdf), filename, doc date.
RESULT_RE = re.compile(
    r'<div class="result-item">.*?'
    r'<div class="result-title">\s*(?P<title>.*?)\s*</div>.*?'
    r'href="(?P<url>/-/media/[a-f0-9]+\.pdf)"[^>]*>\s*(?P<file>.*?)\s*</a>'
    r'(?:.*?<strong>Document Date:</strong>\s*(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4}))?',
    re.I | re.S,
)
GUID_RE = re.compile(r"/-/media/([a-f0-9]+)\.pdf", re.I)
# CA OAH case numbers are numeric (e.g. 2025050862); older ones 6-10 digits.
CASE_NO_RE = re.compile(r"\b(\d{6,12})\b")
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


class CAOAHGeneralJurisdictionScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = USER_AGENT

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
    def _case_numbers(title: str, filename: str) -> list[str]:
        nums = CASE_NO_RE.findall(title) or CASE_NO_RE.findall(filename)
        seen, out = set(), []
        for n in nums:
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
    def iter_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Stream decision descriptors page-by-page (yields as it crawls, so
        sample runs stop after a few pages instead of enumerating the whole
        ~5,344-item folder first)."""
        folder = quote(FOLDER_PATH, safe="")
        seen_guid: set[str] = set()
        seen_case: set[str] = set()
        found = 0
        page = 0
        while True:
            url = SEARCH_TEMPLATE.format(page=page, folder=folder)
            # Retry a zero-row page a few times: the API occasionally returns an
            # empty/partial fragment transiently, and a false "0 rows" would
            # silently drop 10 real decisions or end the crawl early.
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
                if guid in seen_guid:
                    continue
                seen_guid.add(guid)
                title = re.sub(r"\s+", " ", m.group("title") or "").strip()
                filename = re.sub(r"\s+", " ", m.group("file") or "").strip()
                cnums = self._case_numbers(title, filename)
                # Keep ONLY real decisions: they carry a numeric OAH case number.
                # This drops "Notice of Collection" privacy forms and the
                # "Quarterly Data Report" statistics that share the folder.
                if not cnums:
                    continue
                # One Decision per case: skip the Accessibility-Modified/"084"
                # remediated duplicate copy that shares the same case number.
                cn = cnums[0]
                if cn in seen_case:
                    continue
                seen_case.add(cn)
                found += 1
                new_on_page += 1
                yield {
                    "guid": guid,
                    "doc_url": BASE_URL + url_pdf,
                    "raw_title": title,
                    "filename": filename,
                    "case_numbers": cnums,
                    "case_number": cn,
                    "listing_date": self._listing_date(m.group("date")),
                    "page": page,
                }
            logger.info(f"  page {page}: {new_on_page} new decisions (total {found})")
            if sample and found >= 16:
                break
            page += 1
            if page > 700:  # hard safety cap (~7,000 rows)
                logger.warning("Reached page safety cap (700)")
                break
        logger.info(f"Discovered {found} CA OAH General Jurisdiction decisions")

    def discover_documents(self, sample: bool = False) -> list[dict]:
        return list(self.iter_documents(sample=sample))

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
            "US/CA-OAH-GeneralJurisdiction", doc["guid"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._decision_date(text) or doc.get("listing_date")
        doc["parties"] = self._parties(text)
        return doc

    @staticmethod
    def _decision_date(text: str) -> str | None:
        # first real "Month D, YYYY" in the body is the decision/issue date
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d, y = int(m.group(2)), int(m.group(3))
            if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @classmethod
    def _parties(cls, text: str) -> str | None:
        # GJ captions read "In the Matter of ... <PARTY>" or "<PARTY>, v.
        # <PARTY>." near the top; try the "v." form first, else the
        # accusation/matter caption.
        t = re.sub(r"[ \t]+", " ", text[:2000])
        m = re.search(rf"({_NAME}),?\s*\n?\s*v\.?\s*\n?\s*({_NAME})[.,]", t, re.I)
        if m:
            a = re.sub(r"\s+", " ", m.group(1)).strip(" ,.")
            b = re.sub(r"\s+", " ", m.group(2)).strip(" ,.")
            if a and b:
                return f"{a} v. {b}"[:250]
        m = re.search(r"In the Matter of[^\n]{3,180}", t, re.I)
        if m:
            return re.sub(r"\s+", " ", m.group(0)).strip(" ,.")[:250]
        return None

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CA OAH General Jurisdiction MediaSearch + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number') or raw.get('guid')} "
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
            title = f"CA OAH General Jurisdiction Decision {cn}"
        else:
            title = "CA OAH General Jurisdiction Decision"
        if parties:
            title = f"{title}: {parties}"
        title = title[:300]
        return {
            "_id": f"US/CA-OAH-GeneralJurisdiction/{raw['guid']}",
            "_source": "US/CA-OAH-GeneralJurisdiction",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "guid": raw["guid"],
            "case_number": cn,
            "case_numbers": raw.get("case_numbers") or None,
            "parties": parties,
            "issuer": "California Office of Administrative Hearings, General Jurisdiction Division",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.iter_documents(sample=sample):
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

    parser = argparse.ArgumentParser(description="US/CA-OAH-GeneralJurisdiction bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAOAHGeneralJurisdictionScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
