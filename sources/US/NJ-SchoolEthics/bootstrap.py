#!/usr/bin/env python3
"""
US/NJ-SchoolEthics -- New Jersey School Ethics Commission (SEC) --
Public Advisory Opinions.

Fetches the full text of the public advisory opinions of the New Jersey School
Ethics Commission (SEC) interpreting the School Ethics Act (N.J.S.A. 18A:12-21
et seq.) -- the conflict-of-interest and prohibited-acts code for local board
of education members and school administrators. Each public advisory opinion is
the Commission's authoritative written interpretation, issued on request and
released for public use = doctrine (pd-us).

NOTE: this is the New Jersey *School* Ethics Commission (Dept. of Education),
distinct from the NJ *State* Ethics Commission (whose opinion database is a
browser-bound JS app) and from US/NJ-PERC / US/NJ-OAL.

Access (no CAPTCHA, no auth, no JavaScript engine needed):
  The advisory-opinions index

      https://www.nj.gov/education/legal/ethics/advisory/

  is a single server-rendered page listing every opinion as a link to a
  born-digital PDF. Two path shapes appear:
    - by year:     /education/legal/ethics/advisory/{YYYY}/{file}.pdf  (recent)
    - by category: /education/legal/ethics/advisory/cat{1-7}/{file}.pdf (older)
  Filenames are irregular (A13-20.pdf, a2804pub.pdf, a0198opn.pdf, ...), so the
  href is read directly from each anchor and the opinion number is taken from
  the anchor text ("A12-26"), falling back to the filename.

Full text:
  Each PDF is born-digital (clean text layer; OCR fallback for any scan) and
  extracted via the shared common.pdf_extract backend. The issue date is parsed
  from the "Month DD, YYYY" line near the top of the opinion.

All records are advisory opinions = doctrine.

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
from requests.utils import requote_uri
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NJ-SchoolEthics")

BASE_URL = "https://www.nj.gov"
INDEX_URL = f"{BASE_URL}/education/legal/ethics/advisory/"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

ADVISORY_PDF_RE = re.compile(r"/ethics/advisory/[^\"'<>]+\.pdf", re.I)
# Opinion number: A{num}-{yy}, e.g. A12-26, A28-04, A01-98
NUM_RE = re.compile(r"\bA?\s*(\d{1,2})\s*-\s*(\d{2,4})\b", re.I)
FNAME_NUM_RE = re.compile(r"a?(\d{1,2})-?(\d{2})", re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
DATE_RE = re.compile(
    r"(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})", re.I
)


def _norm_num(text: str) -> Optional[str]:
    m = NUM_RE.search(text or "")
    if not m:
        return None
    n = int(m.group(1))
    yy = m.group(2)
    return f"A{n:02d}-{yy}"


def _iso_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS[m.group(1).lower()]
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class NJSchoolEthicsScraper(BaseScraper):

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
    def _collect_index(self) -> list[dict]:
        r = self._get(INDEX_URL)
        if r is None or r.status_code != 200:
            logger.error("Could not fetch advisory-opinions index")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: list[dict] = []
        seen_num: set[str] = set()
        seen_url: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower() or "/ethics/advisory/" not in href.lower():
                continue
            pdf_url = requote_uri(urljoin(BASE_URL, href.strip()))
            if pdf_url in seen_url:
                continue
            seen_url.add(pdf_url)
            anchor = a.get_text(" ", strip=True)
            number = _norm_num(anchor) or _norm_num(Path(href).stem)
            key = number or pdf_url
            if key in seen_num:
                continue
            seen_num.add(key)
            rows.append({"number": number, "pdf_url": pdf_url, "anchor": anchor})
        logger.info(f"Index: {len(rows)} advisory-opinion PDFs")
        return rows

    # ------------------------------------------------------------- fetch1
    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["pdf_url"])
        if r is None or r.status_code != 200 or not r.content:
            logger.warning(f"  {row.get('number')}: PDF download failed — skipped")
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row.get('number')}: not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row.get('number')}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_date(text)
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
        logger.info("Testing NJ School Ethics Commission advisory opinions...")
        rows = self._collect_index()
        if len(rows) < 50:
            logger.error(f"API test FAILED: index too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:5]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec.get('number')} OK ({len(rec['text'])} chars)")
                ok += 1
        if ok >= 3:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        label = f"No. {number}" if number else Path(raw["pdf_url"]).stem
        return {
            "_id": f"US/NJ-SchoolEthics/{number or Path(raw['pdf_url']).stem}",
            "_source": "US/NJ-SchoolEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "document_type": "Public Advisory Opinion",
            "issuer": "New Jersey School Ethics Commission",
            "title": f"NJ School Ethics Commission Advisory Opinion {label}",
            "text": raw["text"],
            "url": raw.get("pdf_final_url") or raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NJ",
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

    parser = argparse.ArgumentParser(description="US/NJ-SchoolEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJSchoolEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
