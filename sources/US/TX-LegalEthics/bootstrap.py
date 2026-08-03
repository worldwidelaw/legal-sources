#!/usr/bin/env python3
"""
US/TX-LegalEthics -- Texas Committee on Professional Ethics — Ethics Opinions

Fetches the full text of the ethics opinions issued by the Professional Ethics
Committee for the State Bar of Texas (a nine-member committee appointed by the
Supreme Court of Texas). Each opinion expresses the Committee's view on the
propriety of professional conduct under the Texas Disciplinary Rules of
Professional Conduct, answering a specific inquiry from a member of the bar =
doctrine (the Committee's official written interpretation of the attorney-
conduct rules).

The opinions run in one continuous, numbered series from Opinion 1 (1966) to the
present (Opinion 710 as of 2026-07), published as born-digital HTML pages by the
Texas Center for Legal Ethics (the official repository). Distinct from the Texas
Ethics Commission (which advises public officials, not lawyers) and from Texas
Attorney General opinions.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  Each opinion is a static page under a predictable, sequential URL:

      https://www.legalethicstexas.com/resources/opinions/opinion-{N}/

  The body sits in <div class="resourcesDetail"> and is composed of a
  "Question Presented" block plus tabbed panels (Statement of Facts, Discussion,
  Conclusion) whose full content is present in the HTML — extracted directly,
  no OCR, no PDF. The current highest opinion number is discovered from the
  index page (/resources/opinions/, sorted newest-first); the scraper then walks
  opinion-1 .. opinion-{max} and skips any 404 (withdrawn) numbers.

Strategy:
  Read the ceiling off the listing page, iterate opinion-1 .. opinion-{ceiling}
  (with a small look-ahead buffer), GET each page and slice out
  div.resourcesDetail (minus the search widget / tab buttons / social links).

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
from typing import Generator

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-LegalEthics")

BASE = "https://www.legalethicstexas.com"
LISTING_URL = BASE + "/resources/opinions/"
OPINION_URL = BASE + "/resources/opinions/opinion-{n}/"
# Fallback ceiling if the listing page can't be parsed (as published 2026-07).
DEFAULT_MAX = 710

OPINION_LINK_RE = re.compile(r"opinion-(\d+)")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


class TXLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._max = None
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> tuple[int, str | None]:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200 and r.text:
                    return 200, r.text
                if r.status_code == 404:
                    return 404, None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return 0, None

    # ------------------------------------------------------------- ceiling
    def _ceiling(self) -> int:
        """Highest opinion number, read from the newest-first index page."""
        if self._max:
            return self._max
        _, html = self._get(LISTING_URL)
        if html:
            nums = [int(n) for n in OPINION_LINK_RE.findall(html)]
            if nums:
                self._max = max(nums)
                logger.info(f"  live ceiling: opinion-{self._max}")
                return self._max
        self._max = DEFAULT_MAX
        logger.info(f"  fallback ceiling: opinion-{self._max}")
        return self._max

    # -------------------------------------------------------- extraction
    @staticmethod
    def _parse_date(time_text: str, bluebook: str, body: str) -> str | None:
        # 1) <time> text, e.g. "February 2025" or "March 12, 1999".
        if time_text:
            t = time_text.strip().lower().replace(",", " ")
            parts = t.split()
            mon = next((MONTHS[p] for p in parts if p in MONTHS), None)
            yr = next((int(p) for p in parts if p.isdigit() and len(p) == 4), None)
            day = next((int(p) for p in parts if p.isdigit() and 1 <= len(p) <= 2), None)
            if yr:
                return f"{yr:04d}-{(mon or 1):02d}-{(day or 1):02d}"
        # 2) Bluebook citation "... Op. N (YYYY)".
        for src in (bluebook, body):
            if src:
                m = re.search(r"\((\d{4})\)", src)
                if m:
                    return f"{int(m.group(1)):04d}-01-01"
        return None

    def _extract(self, html: str, num: int) -> dict | None:
        soup = BeautifulSoup(html, "html.parser")
        detail = soup.find(class_="resourcesDetail")
        if detail is None:
            return None

        time_el = detail.find("time")
        time_text = time_el.get_text(strip=True) if time_el else ""

        # Bluebook citation is usually the last labelled block; grab it before we
        # strip so we can use it for a date fallback.
        bluebook = ""
        for h in detail.find_all(["h2", "h3", "h4"]):
            if "bluebook" in h.get_text(strip=True).lower():
                sib = h.find_next(["p", "div", "span"])
                if sib:
                    bluebook = sib.get_text(" ", strip=True)
                break

        # Strip site chrome: search widget, tab buttons, copy/share links.
        for sel in ("script", "style", "form", "button"):
            for e in detail.find_all(sel):
                e.decompose()
        for cls in ("resourcesSearch", "copyToClipboard", "links", "col btns"):
            for e in detail.find_all(class_=cls):
                e.decompose()
        for e in detail.find_all(attrs={"role": "tablist"}):
            e.decompose()

        text = detail.get_text("\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        # Drop the leading "Opinion N" / date lines already captured as metadata,
        # but keep them in text too — harmless. Require real body content.
        if len(text) < 120:
            return None

        date = self._parse_date(time_text, bluebook, text)
        return {
            "opinion_number": num,
            "title": f"Texas Ethics Opinion {num}",
            "text": text,
            "date": date,
            "url": OPINION_URL.format(n=num),
        }

    def _fetch_one(self, num: int) -> dict | None:
        status, html = self._get(OPINION_URL.format(n=num))
        if status != 200 or not html:
            return None
        return self._extract(html, num)

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Texas Committee on Professional Ethics opinions...")
        ceiling = self._ceiling()
        if ceiling < 1:
            logger.error("API test FAILED: could not determine ceiling")
            return False
        ok = 0
        for num in (1, 100, 500, ceiling):
            rec = self._fetch_one(num)
            if rec and len(rec["text"]) > 150:
                logger.info(f"  Opinion {num} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  Opinion {num} — no text")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: insufficient full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/TX-LegalEthics/opinion-{num}",
            "_source": "US/TX-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": str(num),
            "issuer": "Texas Committee on Professional Ethics (State Bar of Texas)",
            "title": raw.get("title") or f"Texas Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-TX",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        ceiling = self._ceiling()
        # Walk a small buffer past the known ceiling in case the index lags.
        upper = ceiling + 5
        emitted = 0
        misses = 0
        for num in range(1, upper + 1):
            rec = self._fetch_one(num)
            if not rec:
                misses += 1
                # Past the real ceiling: stop after a run of consecutive 404s.
                if num > ceiling and misses >= 5:
                    break
                continue
            misses = 0
            yield rec
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

    parser = argparse.ArgumentParser(description="US/TX-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TXLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
