#!/usr/bin/env python3
"""
US/MN-CFBOpinions -- Minnesota Campaign Finance & Public Disclosure Board (CFB)
Advisory Opinions.

Fetches the full text of the formal advisory opinions issued by the Minnesota
Campaign Finance and Public Disclosure Board (and its predecessors, the State
Ethics Commission / Ethical Practices Board) construing Minn. Stat. ch. 10A
(Campaign Finance & Public Disclosure) and the related lobbying, gift-ban,
conflict-of-interest and economic-interest statutes. An advisory opinion is the
Board's written interpretation of those statutes, requested by an official,
candidate, committee, lobbyist or principal = doctrine. Minnesota state agency
public record (government-edict work).

Access (no JavaScript, no CAPTCHA, no auth):
  A single "advisory opinions" listing page

      https://cfb.mn.gov/citizen-resources/the-board/board-decisions/advisory-opinions/

  is a table whose rows carry the opinion number, program (Campaign Finance /
  Lobbying / Gift Ban / Economic Interest / ...), a subject summary, the date
  issued and the requestor, plus a direct href to the opinion PDF at

      https://cfb.mn.gov/pdf/advisory_opinions/AO{N}.pdf

  Modern opinions are born-digital (clean text layer); the oldest (1974–1990s)
  are scanned images and fall back to OCR. Extraction is done by the shared
  common.pdf_extract._extract backend chain (opendataloader → pdfplumber →
  pypdf → OCR).

Strategy:
  Parse the index table once, newest-first, then for each row download the PDF
  and extract full text. All opinions are doctrine. The issue date comes from
  the index "Date issued" column.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
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
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MN-CFBOpinions")

BASE_URL = "https://cfb.mn.gov"
INDEX_URL = (
    "https://cfb.mn.gov/citizen-resources/the-board/board-decisions/"
    "advisory-opinions/"
)

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

PDF_HREF_RE = re.compile(r"advisory_opinions/AO(\d+)\.pdf", re.I)
DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})")


def _iso_from_mdy(text: str) -> str | None:
    """Parse an 'MM/DD/YYYY' date string to ISO 8601."""
    m = DATE_RE.search(text or "")
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class MNCFBOpinionsScraper(BaseScraper):

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
    def _parse_index(self) -> list[dict]:
        """Parse the advisory-opinions listing table into row dicts.

        Returns a list sorted newest-first (descending AO number).
        """
        r = self._get(INDEX_URL)
        if r is None or r.status_code != 200:
            logger.error(f"Index fetch failed (status={getattr(r,'status_code',None)})")
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse index")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: dict[int, dict] = {}
        for a in soup.find_all("a", href=PDF_HREF_RE):
            m = PDF_HREF_RE.search(a.get("href", ""))
            if not m:
                continue
            n = int(m.group(1))
            if n in rows:
                continue
            href = a.get("href")
            if href.startswith("/"):
                href = BASE_URL + href
            tr = a.find_parent("tr")
            program = subject = date_iso = None
            if tr:
                tds = tr.find_all("td")
                if len(tds) >= 4:
                    program = tds[1].get_text(" ", strip=True) or None
                    subject = tds[2].get_text(" ", strip=True) or None
                    date_iso = _iso_from_mdy(tds[3].get_text(" ", strip=True))
            rows[n] = {
                "n": n,
                "opinion_number": f"AO{n}",
                "url": href,
                "program": program,
                "subject": subject,
                "date": date_iso,
            }
        ordered = [rows[k] for k in sorted(rows, reverse=True)]
        logger.info(f"Index parsed: {len(ordered)} advisory opinions")
        return ordered

    def _fetch_one(self, row: dict) -> dict | None:
        """Download the opinion PDF and attach extracted full text."""
        r = self._get(row["url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        if not r.content[:5].startswith(b"%PDF"):
            return None
        text = _pdf_extract_bytes(r.content) or ""
        text = text.strip()
        if len(text) < 200:
            logger.warning(f"  {row['opinion_number']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._parse_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['opinion_number']} OK ({len(rec['text'])} chars)")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing MN CFB advisory opinions...")
        idx = self._parse_index()
        if len(idx) < 50:
            logger.error(f"API test FAILED: index too small ({len(idx)})")
            return False
        ok = 0
        for row in idx[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  {rec['opinion_number']} OK ({len(rec['text'])} chars, date={rec['date']})"
                )
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        program = raw.get("program")
        caption = f"Advisory Opinion {raw['n']}"
        if program:
            caption += f" — {program}"
        return {
            "_id": f"US/MN-CFBOpinions/{num}",
            "_source": "US/MN-CFBOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Minnesota Campaign Finance and Public Disclosure Board",
            "title": caption,
            "program": program,
            "subject": raw.get("subject"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-MN",
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

    parser = argparse.ArgumentParser(description="US/MN-CFBOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MNCFBOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
