#!/usr/bin/env python3
"""
US/MA-EthicsOpinions -- Massachusetts State Ethics Commission — Conflict of
Interest (EC-COI) Advisory Opinions.

Fetches the full text of the formal advisory opinions ("EC-COI" opinions) issued
by the Massachusetts State Ethics Commission construing the conflict-of-interest
law (G.L. c. 268A) and the financial-disclosure law (G.L. c. 268B). An EC-COI
opinion is the Commission's written interpretation of those statutes, requested by
a public official or employee = doctrine. Public-domain Commonwealth of
Massachusetts government work.

Access (no JavaScript, no CAPTCHA, no auth):
  Each opinion has a stable born-digital HTML page at

      https://www.mass.gov/opinion/ec-coi-{YY}-{N}

  where YY is the two-digit year (79 = 1979 ... 12 = 2012) and N is the sequential
  opinion number within that year. There is no single machine-readable index page,
  so the corpus is enumerated by probing {YY}-{N} sequentially per year and
  stopping after a run of consecutive misses (404s). Missing slugs return HTTP 404;
  live ones return HTTP 200 with the opinion body in <div class="ma__rich-text">
  blocks inside <main id="main-content">.

  NOTE: www.mass.gov sits behind Akamai, which 403s browser-like (Mozilla) User-
  Agents but serves a plain "python-requests" UA fine — so we send that UA.

Strategy:
  For each year 1979..present, probe ec-coi-{YY}-{N} for N = 1, 2, 3, ... until
  MAX_MISS consecutive 404/empty pages, extract the rich-text body, and normalize.
  All opinions are doctrine. The issue date is parsed from the "Date: MM/DD/YYYY"
  line in the page header, falling back to the opinion-number year.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions 1979–present)
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
from typing import Generator

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MA-EthicsOpinions")

BASE_URL = "https://www.mass.gov"

# Years the Commission issued numbered EC-COI opinions (1979 through the early
# 2010s; it later shifted to topic "advisories"). We probe a generous upper bound.
FIRST_YEAR = 1979
LAST_YEAR = 2015
# Stop probing a year after this many consecutive missing opinion numbers.
MAX_MISS = 6
# Hard safety cap on opinion numbers probed per year.
MAX_N = 90

DATE_RE = re.compile(r"Date:\s*(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})")


def _iso_from_header(page_text: str) -> str | None:
    """Parse the 'Date: MM/DD/YYYY' line the mass.gov opinion header carries."""
    m = DATE_RE.search(page_text)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _year_from_yy(yy: int) -> int:
    """Two-digit opinion year -> full year (79->1979, 12->2012)."""
    return 1900 + yy if yy >= 79 else 2000 + yy


class MAEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        # Akamai on mass.gov 403s Mozilla UAs but passes a plain requests UA.
        self.session.headers.update({"User-Agent": "python-requests/2.31"})

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=45, allow_redirects=True)
                return r
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- extract
    @staticmethod
    def _extract_body(page_html: str) -> str:
        """Join the ma__rich-text blocks that hold the opinion body."""
        if BeautifulSoup is None:
            # Fallback: crude tag strip of the <main> region.
            m = re.search(r"<main[^>]*id=\"main-content\"[^>]*>(.*?)</main>",
                          page_html, re.S | re.I)
            frag = m.group(1) if m else page_html
            frag = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", frag, flags=re.S | re.I)
            frag = re.sub(r"<[^>]+>", " ", frag)
            return re.sub(r"\s+", " ", frag).strip()
        soup = BeautifulSoup(page_html, "html.parser")
        parts = []
        for d in soup.select("div.ma__rich-text"):
            t = d.get_text(" ", strip=True)
            if len(t) > 60:
                parts.append(t)
        return "\n\n".join(parts).strip()

    def _fetch_one(self, yy: int, n: int) -> dict | None:
        """Return a raw record for ec-coi-{yy}-{n} or None if it doesn't exist."""
        slug = f"ec-coi-{yy:02d}-{n}"
        url = f"{BASE_URL}/opinion/{slug}"
        r = self._get(url)
        if r is None or r.status_code != 200:
            return None
        # Guard against soft pages that lack the opinion marker.
        if slug.upper() not in r.text.upper():
            return None
        body = self._extract_body(r.text)
        if not body or len(body) < 400:
            return None
        number = f"{_year_from_yy(yy)}-{n}"  # e.g. 1992-18
        date = _iso_from_header(BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
                                if BeautifulSoup else r.text)
        return {
            "number": number,
            "slug": slug,
            "url": url,
            "text": body,
            "date": date,
        }

    # ---------------------------------------------------------- discovery
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        # Newest first so the sample lands on clean, long modern opinions.
        for year in range(LAST_YEAR, FIRST_YEAR - 1, -1):
            yy = year % 100
            miss = 0
            n = 1
            while miss < MAX_MISS and n <= MAX_N:
                rec = self._fetch_one(yy, n)
                if rec:
                    miss = 0
                    yield rec
                    emitted += 1
                    logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars)")
                    if sample and emitted >= 12:
                        return
                else:
                    miss += 1
                n += 1

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing MA State Ethics Commission EC-COI opinions...")
        ok = 0
        for yy, n in [(92, 18), (79, 1), (10, 1), (0, 1)]:
            rec = self._fetch_one(yy, n)
            if rec and len(rec["text"]) > 400:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        year = number.split("-")[0]
        date = raw.get("date") or f"{year}-01-01"
        return {
            "_id": f"US/MA-EthicsOpinions/{number}",
            "_source": "US/MA-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "issuer": "Massachusetts State Ethics Commission",
            "title": f"EC-COI-{raw['slug'].split('ec-coi-')[1].upper()}",
            "text": raw["text"],
            "url": raw["url"],
            "date": date,
            "jurisdiction": "US-MA",
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

    parser = argparse.ArgumentParser(description="US/MA-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MAEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
