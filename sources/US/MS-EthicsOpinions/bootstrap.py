#!/usr/bin/env python3
"""
US/MS-EthicsOpinions -- Mississippi Ethics Commission -- Ethics Advisory Opinions.

Fetches the full text of the Ethics Advisory Opinions of the Mississippi Ethics
Commission construing the Ethics in Government Law (Miss. Code Ann. Title 25,
Chapter 4) and Section 109 of the Mississippi Constitution of 1890. An ethics
advisory opinion is the Commission's written interpretation of those provisions,
issued to a public servant on request and published (with the requester's
identity edited out) = doctrine.

Access (no JavaScript-rendered content required, no CAPTCHA, no auth):
  The Commission's opinion search at

      https://www.ms.gov/msec/ethics/opinion

  is a server-side DataTable backed by a public JSON endpoint

      https://www.ms.gov/msec/ethics/api/opinion/list?draw=1&start=0&length=2000

  which returns every opinion as {id, documentId, number, summary,
  subjectTitleList}. Each opinion's detail page

      https://www.ms.gov/msec/ethics/opinion/details/{id}

  links to the born-digital opinion PDF at

      https://www.ms.gov/msec/ethics/Opinion/Document/{file}.pdf

  Full text comes from that PDF (clean text layer).

Strategy:
  1. GET the JSON list endpoint once -> all opinions with metadata.
  2. For each opinion, GET its detail page, extract the Opinion/Document/*.pdf
     href (the PDF filename is derived irregularly from the number, so we read
     the actual href rather than guessing), download the PDF, and extract full
     text via the shared common.pdf_extract backend chain.
  3. The issue date is parsed from the "Month DD, YYYY" line in the PDF body,
     falling back to Jan 1 of the two-digit year in the opinion number.
  All records are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MS-EthicsOpinions")

BASE_URL = "https://www.ms.gov/msec/ethics"
LIST_URL = f"{BASE_URL}/api/opinion/list"
DETAILS_URL = f"{BASE_URL}/opinion/details/{{id}}"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

# The born-digital PDF href on each detail page.
DOC_HREF_RE = re.compile(r'href="([^"]*Opinion/Document/[^"]+\.pdf)"', re.I)

# Opinion number: "26-011-E", "25-036-ER", "06-001-E" -> YY-NNN-SUFFIX.
NUM_RE = re.compile(r"^(\d{2})-(\d{1,4})-([A-Z]+)$")

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _year_from_number(number: str) -> Optional[int]:
    m = NUM_RE.match(number.strip())
    if not m:
        return None
    yy = int(m.group(1))
    return 1900 + yy if yy >= 50 else 2000 + yy


def _iso_from_body(text: str) -> Optional[str]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS.index(m.group(1)) + 1
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class MSEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": UA,
            "Accept": "text/html,application/json,*/*",
            "Referer": f"{BASE_URL}/opinion",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _collect_index(self) -> list[dict]:
        """Return every opinion's metadata from the JSON list endpoint."""
        r = self._get(f"{LIST_URL}?draw=1&start=0&length=5000")
        if r is None or r.status_code != 200:
            logger.error("Could not fetch opinion list endpoint")
            return []
        try:
            payload = r.json()
        except Exception as e:
            logger.error(f"List endpoint did not return JSON: {e}")
            return []
        rows = payload.get("data") or []
        out = []
        for row in rows:
            number = _clean(str(row.get("number") or ""))
            if not number or row.get("id") is None:
                continue
            out.append({
                "id": row["id"],
                "document_id": row.get("documentId"),
                "number": number,
                "summary": _clean(str(row.get("summary") or "")),
                "subjects": _clean(
                    str(row.get("subjectTitleList") or "").replace("||", "; ")
                ),
            })
        logger.info(f"Index collected: {len(out)} opinions "
                    f"(recordsTotal={payload.get('recordsTotal')})")
        return out

    def _pdf_url_for(self, opinion_id) -> Optional[str]:
        r = self._get(DETAILS_URL.format(id=opinion_id))
        if r is None or r.status_code != 200:
            return None
        m = DOC_HREF_RE.search(r.text)
        if not m:
            return None
        return urljoin("https://www.ms.gov/", m.group(1))

    def _fetch_one(self, row: dict) -> Optional[dict]:
        pdf_url = self._pdf_url_for(row["id"])
        if not pdf_url:
            logger.warning(f"  {row['number']}: no PDF href on detail page — skipped")
            return None
        r = self._get(pdf_url)
        if r is None or r.status_code != 200 or not r.content:
            logger.warning(f"  {row['number']}: PDF download failed — skipped")
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row['number']}: download is not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row['number']}: thin text ({len(text)} chars) — skipped")
            return None
        year = _year_from_number(row["number"])
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_from_body(text) or (f"{year:04d}-01-01" if year else None)
        out["pdf_url"] = r.url
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Mississippi Ethics Commission advisory opinions...")
        index = self._collect_index()
        if len(index) < 100:
            logger.error(f"API test FAILED: index too small ({len(index)})")
            return False
        ok = 0
        for row in index[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        return {
            "_id": f"US/MS-EthicsOpinions/{number}",
            "_source": "US/MS-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "document_type": "Ethics Advisory Opinion",
            "issuer": "Mississippi Ethics Commission",
            "title": f"Ethics Advisory Opinion No. {number}",
            "summary": raw.get("summary") or None,
            "subjects": raw.get("subjects") or None,
            "text": raw["text"],
            "url": raw.get("pdf_url"),
            "date": raw.get("date"),
            "jurisdiction": "US-MS",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MS-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MSEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
