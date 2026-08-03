#!/usr/bin/env python3
"""
US/TN-LegalEthics -- Board of Professional Responsibility of the Supreme Court
of Tennessee — Formal Ethics Opinions

Fetches the full text of the Formal Ethics Opinions issued by the Board of
Professional Responsibility (BPR) of the Supreme Court of Tennessee. Each
opinion is the Board's written interpretation of the Tennessee Rules of
Professional Conduct in response to an inquiry from an attorney, issued as
guidance to LAWYERS statewide = doctrine (advisory). One continuous series
numbered "{YY|YYYY}-F-{N}" running from 80-F-1 (1980) to the present
(2023-F-170 and later), ~171 unique opinions.

The Board of Professional Responsibility is an arm of the Supreme Court of
Tennessee (created under Tenn. Sup. Ct. R. 9) with authority over the
licensure and discipline of Tennessee attorneys, so the 17 U.S.C. § 105
government-edicts rationale applies directly.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. The opinions are listed on the public page
       https://www.tbpr.org/for-legal-professionals/formal-ethics-opinions
     Each row links to a detail page /ethic_opinions/{slug} where {slug} is the
     opinion number (e.g. 80-f-1, 2002-f-91a) optionally followed by a topic
     slug (e.g. 2023-f-169-ethical-obligations-of-departing-attorneys...).
     Several opinions are linked twice (a bare-number slug and a topic slug);
     they are de-duplicated on the canonical opinion number.
  2. Each detail page renders the opinion body in clean HTML inside <main>,
     starting at an <h2> that carries the printed opinion number and topic
     title, followed by the opinion text — extracted directly with
     BeautifulSoup, NO PDF/OCR needed.

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
logger = logging.getLogger("legal-data-hunter.US.TN-LegalEthics")

BASE = "https://www.tbpr.org"
INDEX_PATH = "/for-legal-professionals/formal-ethics-opinions"

DETAIL_RE = re.compile(r"^/ethic_opinions/([^/\"?#]+)/?$", re.I)

# Printed opinion number at the head of the <h2>, e.g.
#   "80-F-1 - Structured Settlements"
#   "2002-F-91(a) - Vacated*"
#   "2023-F-169 Ethical obligations of departing attorneys..."
#   "Formal Ethics Opinion 2025-F-171"  (newest opinions carry a label prefix)
# A revision/supplement suffix, when present, is ALWAYS parenthesised in the
# printed heading (e.g. "91(a)"); a bare trailing letter belongs to the next
# title word, so it must NOT be treated as a suffix.
NUM_RE = re.compile(
    r"^\s*(?:Formal\s+Ethics\s+Opinion\s+)?"
    r"((?:19|20)?\d{2})\s*-\s*F\s*-\s*(\d{1,3})(\([a-zA-Z]\))?",
    re.I,
)

MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(?:(\d{1,2})(?:st|nd|rd|th)?,?\s+)?((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class TNLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept": "text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[tuple[str, str]]:
        """Return [(slug, detail_url)] from the single index page."""
        r = self._get(BASE + INDEX_PATH)
        if not r:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            m = DETAIL_RE.match(a["href"])
            if not m:
                continue
            slug = m.group(1)
            if slug in out:
                continue
            out[slug] = BASE + a["href"].split("#")[0]
        result = list(out.items())
        logger.info(f"  discovered {len(result)} opinion detail links")
        return result

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _extract(self, html: str) -> tuple[str, str]:
        """Return (h2_title_line, body_text). Body starts at the first <h2>
        inside <main> (which carries the number + topic), so the surrounding
        breadcrumb / nav chrome is dropped."""
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find("main") or soup
        h2 = main.find("h2")
        if h2 is None:
            return "", ""
        h2_text = self._clean(h2.get_text(" ", strip=True))
        # Drop chrome, then take everything from the <h2> onward within <main>.
        for tag in main.find_all(["script", "style", "nav", "form",
                                  "footer", "header"]):
            tag.decompose()
        full = self._clean(main.get_text("\n", strip=True))
        # Trim to the opinion: cut everything before the h2's number/title.
        idx = full.find(h2_text)
        body = full[idx:] if idx != -1 else full
        return h2_text, body

    @staticmethod
    def _canon_number(h2_text: str) -> tuple[str, int] | None:
        m = NUM_RE.match(h2_text)
        if not m:
            return None
        y = int(m.group(1))
        if y < 100:
            y = 1900 + y if y >= 30 else 2000 + y
        seq = int(m.group(2))
        suf = (m.group(3) or "").strip("()").lower()
        num = f"{y}-F-{seq}" + (f"({suf})" if suf else "")
        return num, y

    def _fetch_one(self, slug: str, url: str) -> dict | None:
        r = self._get(url)
        if not r or not r.text:
            return None
        h2_text, body = self._extract(r.text)
        if not h2_text:
            logger.warning(f"  {slug}: no <h2> content block")
            return None
        parsed = self._canon_number(h2_text)
        if not parsed:
            logger.warning(f"  {slug}: no opinion number in '{h2_text[:40]}'")
            return None
        num, year = parsed
        if len(body) < 150:
            logger.warning(f"  {num}: insufficient text ({len(body)} chars)")
            return None

        # Title: the h2 line with the leading number token removed.
        title = NUM_RE.sub("", h2_text).lstrip(" -–—\xa0").strip()
        if not title:
            title = f"Tennessee Formal Ethics Opinion {num}"

        # Date: prefer an explicit in-range date in the body, else YYYY-01-01.
        date = f"{year}-01-01"
        for mm in MONTH_DATE_RE.finditer(body):
            mon = mm.group(1).lower()
            day = int(mm.group(2)) if mm.group(2) else 1
            yr = int(mm.group(3))
            if abs(yr - year) <= 1 and 1 <= day <= 31:
                date = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                break

        return {
            "opinion_number": num,
            "title": title,
            "text": body,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Tennessee BPR formal ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for slug, url in ops[:4]:
            rec = self._fetch_one(slug, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({url})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/TN-LegalEthics/{num}",
            "_source": "US/TN-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("Board of Professional Responsibility of the "
                       "Supreme Court of Tennessee"),
            "title": raw.get("title") or f"Tennessee Formal Ethics Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-TN",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        best: dict[str, dict] = {}
        # First pass: fetch all, keep the fullest record per canonical number.
        for slug, url in self._list_opinions():
            rec = self._fetch_one(slug, url)
            if not rec:
                continue
            num = rec["opinion_number"]
            if num not in best or len(rec["text"]) > len(best[num]["text"]):
                if num not in best:
                    # emit as soon as first seen so sample mode terminates fast
                    best[num] = rec
                    yield rec
                    emitted += 1
                    if sample and emitted >= 12:
                        return
                else:
                    best[num] = rec

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

    parser = argparse.ArgumentParser(description="US/TN-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TNLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
