#!/usr/bin/env python3
"""
US/IN-EthicsOpinions -- Indiana State Ethics Commission -- Formal Advisory
Opinions.

Fetches the full text of the formal advisory opinions of the Indiana State
Ethics Commission (administered by the Office of Inspector General) construing
the Code of Ethics for state officers, employees and special state appointees
(Ind. Code 4-2-6 and 42 IAC 1). A formal advisory opinion is the Commission's
authoritative written interpretation of the ethics code, issued on request at a
monthly public meeting and published = doctrine.

Access (no JavaScript, no CAPTCHA, no auth):
  in.gov/ig is a state CMS. The opinions index

      https://www.in.gov/ig/opinions/

  links one listing page per year

      https://www.in.gov/ig/opinions/advisory-opinions-{YYYY}/    (1988-present)

  whose rows are anchors to the born-digital opinion PDFs

      https://www.in.gov/ig/files/opinions/{YYYY}/s{YY}-I-{N}_{tags}.pdf

  e.g. s06-I-25_PERF-Misc.FDS.pdf = Advisory Opinion No. 06-I-25. Full text comes
  from that PDF (clean text layer; OCR fallback for the oldest scans).

Strategy:
  1. GET the opinions index -> collect the advisory-opinions-{YYYY} year slugs.
  2. For each year page (follow the trailing-slash redirect), collect every
     /ig/files/opinions/{YYYY}/*.pdf href, dedup.
  3. Download each PDF, extract full text via the shared common.pdf_extract
     backend chain. The opinion number is parsed from the filename ("s06-I-25"
     -> "06-I-25"); the issue date is parsed from the "Month DD, YYYY" line in
     the body, falling back to Jan 1 of the year.
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
logger = logging.getLogger("legal-data-hunter.US.IN-EthicsOpinions")

BASE_URL = "https://www.in.gov"
INDEX_URL = f"{BASE_URL}/ig/opinions/"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

YEAR_SLUG_RE = re.compile(r"/ig/opinions/advisory-opinions-(\d{4})", re.I)
PDF_HREF_RE = re.compile(r'href="([^"]*/ig/files/opinions/\d{4}/[^"]+\.pdf)"', re.I)
# Filename opinion number. Two schemes across the years:
#   old (pre-2020): s06-I-25_... -> 06-I-25   (YY - letter - seq)
#   new (2020+):    2020-FAO-001-... -> 2020-FAO-001
FNAME_NUM_RE = re.compile(
    r"/(?:s(\d{2}-[A-Za-z]-\d{1,4})|(\d{4}-FAO-\d{1,4}))", re.I
)

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _year_from_yy(yy: int) -> int:
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


class INEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

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
    def _year_slugs(self) -> list[str]:
        r = self._get(INDEX_URL)
        if r is None or r.status_code != 200:
            logger.error("Could not fetch opinions index")
            return []
        years = sorted(set(YEAR_SLUG_RE.findall(r.text)), reverse=True)
        logger.info(f"Discovered {len(years)} year pages ({years[-1]}-{years[0]})"
                    if years else "No year pages found")
        return years

    def _year_pdfs(self, year: str) -> list[dict]:
        url = f"{BASE_URL}/ig/opinions/advisory-opinions-{year}/"
        r = self._get(url)
        if r is None or r.status_code != 200:
            return []
        rows: list[dict] = []
        seen: set[str] = set()
        for href in PDF_HREF_RE.findall(r.text):
            pdf_url = urljoin(BASE_URL, href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            m = FNAME_NUM_RE.search(href)
            number = ((m.group(1) or m.group(2)).upper() if m else None)
            rows.append({
                "number": number,
                "year": int(year),
                "pdf_url": pdf_url,
                "year_url": url,
            })
        return rows

    def _collect_index(self) -> list[dict]:
        out: list[dict] = []
        seen_num: set[str] = set()
        for year in self._year_slugs():
            rows = self._year_pdfs(year)
            for row in rows:
                key = row["number"] or row["pdf_url"]
                if key in seen_num:
                    continue
                seen_num.add(key)
                out.append(row)
            logger.info(f"{year}: {len(rows)} opinions ({len(out)} distinct so far)")
        logger.info(f"Index collected: {len(out)} distinct opinions")
        return out

    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["pdf_url"])
        if r is None or r.status_code != 200 or not r.content:
            logger.warning(f"  {row.get('number')}: PDF download failed — skipped")
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row.get('number')}: download is not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row.get('number')}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_from_body(text) or f"{row['year']:04d}-01-01"
        out["pdf_final_url"] = r.url
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec.get('number')} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Indiana State Ethics Commission advisory opinions...")
        rows = self._year_pdfs("2006")
        if len(rows) < 5:
            logger.error(f"API test FAILED: year page too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec.get('number')} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        label = f"No. {number}" if number else Path(raw["pdf_url"]).stem
        return {
            "_id": f"US/IN-EthicsOpinions/{number or Path(raw['pdf_url']).stem}",
            "_source": "US/IN-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "document_type": "Formal Advisory Opinion",
            "issuer": "Indiana State Ethics Commission",
            "title": f"Indiana State Ethics Commission Advisory Opinion {label}",
            "text": raw["text"],
            "url": raw.get("pdf_final_url") or raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-IN",
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

    parser = argparse.ArgumentParser(description="US/IN-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = INEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
