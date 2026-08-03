#!/usr/bin/env python3
"""
US/LA-EAB -- Louisiana Ethics Adjudicatory Board Decisions

Fetches the full text of the adjudicatory decisions of the Louisiana Ethics
Adjudicatory Board (EAB). The EAB is a panel of administrative law judges of
the Louisiana Division of Administrative Law (DAL) that hears and decides the
ethics charges instituted by the Louisiana Board of Ethics under the Code of
Governmental Ethics (La. R.S. 42:1101 et seq.) and the Campaign Finance
Disclosure Act. Each "Decision and Order" resolves a specific contested case
against a named respondent (public official/employee/candidate), making formal
findings of fact and conclusions of law and imposing penalties = case_law
(government edict). These are official Louisiana state-government works in the
public domain (17 U.S.C. s 105 rationale; edicts of government).

Source: the DAL publishes each EAB decision as a born-digital PDF on its
website, https://www.adminlaw.la.gov/. The "Ethics Decisions" page is a
Solr-backed search result list, paginated with ``?_page=N`` (10 results per
page), where each result links directly to the decision PDF hosted on the
DAL's S3 bucket (redball-la-solr.s3.us-east-2.amazonaws.com/<key>.pdf). The
result title carries the docket number and respondent name, e.g.
``2025-20439-BOE-A - Brian J. Henly Decision.pdf``. The PDFs carry an embedded
text layer (born-digital), so the full text is extracted with the shared
``common.pdf_extract`` helper. No CAPTCHA, no auth.

  - Enumerate:  GET /ethics-decisions/?_page=N
                -> <a class="search-result-document" href="{s3 pdf url}">
                   {docket} - {respondent} Decision.pdf</a>
                paginate N=1.. until a page yields no (new) results.
  - Per item:   download the S3 PDF, extract text.
                docket + respondent parsed from the result title;
                decision date parsed from the body ("Rendered and signed
                on <Month DD, YYYY> ...").

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.LA-EAB")

BASE = "https://www.adminlaw.la.gov"
LIST_URL = BASE + "/ethics-decisions/?_page={n}"

# Result rows: <a class="search-result-document" href="<s3 pdf>" ...>title.pdf</a>
ROW_RE = re.compile(
    r'<a\s+class="search-result-document"\s+href="(https://[^"]+?\.pdf)"[^>]*>'
    r'\s*(.*?)\s*</a>',
    re.I | re.S,
)
DOCKET_RE = re.compile(r"(\d{4}-\d+-[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)")
RENDERED_RE = re.compile(
    r"Rendered\s+and\s+signed\s+on\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", re.I)
LONG_DATE_RE = re.compile(r"([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})")
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
MIN_TEXT_CHARS = 200
MAX_PAGES = 400

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


class LAEABScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.5

    # ---------------------------------------------------------------- http
    def _get(self, url: str, binary: bool = False):
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                accept = "application/pdf,*/*" if binary else "text/html,*/*"
                req = Request(url, headers={"User-Agent": UA, "Accept": accept})
                with urlopen(req, timeout=90) as resp:
                    data = resp.read()
                return data if binary else data.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"GET failed ({url[:70]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @classmethod
    def _clean_text(cls, text: str) -> str:
        if not text:
            return ""
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = [WS_RE.sub(" ", ln).strip() for ln in text.split("\n")]
        text = "\n".join(lines)
        return NL_RE.sub("\n\n", text).strip()

    @staticmethod
    def _title_text(raw_html: str) -> str:
        t = TAG_RE.sub("", raw_html)
        t = re.sub(r"\s+", " ", t).strip()
        if t.lower().endswith(".pdf"):
            t = t[:-4]
        return t

    @classmethod
    def _parse_date(cls, text: str) -> str | None:
        m = RENDERED_RE.search(text)
        cand = m.group(1) if m else None
        if not cand:
            return None
        dm = LONG_DATE_RE.search(cand)
        if not dm:
            return None
        mon = MONTHS.get(dm.group(1).lower())
        day, yr = int(dm.group(2)), int(dm.group(3))
        if mon and 1 <= day <= 31 and 1990 <= yr <= 2100:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
        return None

    @staticmethod
    def _s3_key(url: str) -> str:
        key = url.rstrip("/").split("/")[-1]
        if key.lower().endswith(".pdf"):
            key = key[:-4]
        return key

    # --------------------------------------------------------- discovery
    def discover(self, sample: bool = False) -> Generator[tuple[str, str], None, None]:
        """Yield (pdf_url, title) for each EAB decision, paginating the list."""
        seen: set[str] = set()
        empty_streak = 0
        for n in range(1, MAX_PAGES + 1):
            html = self._get(LIST_URL.format(n=n))
            if html is None:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            rows = ROW_RE.findall(html)
            new = 0
            for url, raw_title in rows:
                key = self._s3_key(url)
                if key in seen:
                    continue
                seen.add(key)
                new += 1
                yield url, self._title_text(raw_title)
                if sample and len(seen) >= 14:
                    logger.info(f"Discovered {len(seen)} EAB decisions (sample)")
                    return
            if new == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
        logger.info(f"Discovered {len(seen)} Louisiana EAB decisions")

    # ------------------------------------------------------- build record
    def _build_raw(self, url: str, title: str) -> dict | None:
        pdf = self._get(url, binary=True)
        if not pdf or pdf[:5] != b"%PDF-":
            logger.warning(f"Non-PDF or empty payload for {url[:70]}")
            return None
        key = self._s3_key(url)
        text = self._clean_text(
            extract_pdf_markdown("US/LA-EAB", key, pdf_bytes=pdf,
                                 table="case_law", force=True) or "")
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text for {key} ({len(text)} chars)")
            return None

        dm = DOCKET_RE.search(title)
        docket = dm.group(1) if dm else None
        respondent = title
        if docket:
            respondent = title.split(docket, 1)[-1]
        respondent = re.sub(r"\bDecision\b.*$", "", respondent, flags=re.I)
        respondent = re.sub(r"^\s*(?:In\s+Re:?|In\s+the\s+Matter\s+of)\s*",
                            "", respondent, flags=re.I)
        respondent = respondent.strip().strip(",").strip("-–—:").strip()
        # collapse leftover stray separators/commas at the ends
        respondent = re.sub(r"^[\s,\-–—:]+|[\s,\-–—:]+$", "", respondent) or None

        date = self._parse_date(text)

        parts = []
        if docket:
            parts.append(f"EAB Docket {docket}")
        if respondent:
            parts.append(respondent)
        heading = " — ".join(parts) if parts else (title or "EAB Decision")
        full_title = f"Louisiana Ethics Adjudicatory Board Decision — {heading}"

        return {
            "key": key,
            "docket": docket,
            "respondent": respondent,
            "title": full_title[:300],
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Louisiana EAB (adminlaw.la.gov) source...")
        try:
            items = list(self.discover(sample=True))
            if not items:
                logger.error("  No decisions discovered")
                return False
            logger.info(f"  Discovered {len(items)} decisions (sample)")
            raw = None
            for url, title in items:
                raw = self._build_raw(url, title)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('docket')} [{raw.get('date')}]")
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
            "_id": f"US/LA-EAB/{raw['key']}",
            "_source": "US/LA-EAB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["key"],
            "docket_number": raw.get("docket"),
            "respondent": raw.get("respondent"),
            "court": "Louisiana Ethics Adjudicatory Board",
            "issuer": "Louisiana Division of Administrative Law",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-LA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for url, title in self.discover(sample=sample):
            raw = self._build_raw(url, title)
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

    parser = argparse.ArgumentParser(description="US/LA-EAB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LAEABScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
