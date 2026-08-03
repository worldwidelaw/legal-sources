#!/usr/bin/env python3
"""
US/SC-LegalEthics -- South Carolina Bar — Ethics Advisory Opinions

Fetches the full text of the Ethics Advisory Opinions issued by the South
Carolina Bar's Ethics Advisory Committee. Each opinion is the Committee's
written response, upon a member's request, on the ethical propriety of the
inquirer's contemplated conduct under the South Carolina Rules of Professional
Conduct = doctrine (advisory; the Committee has no disciplinary authority —
lawyer discipline is administered by the S.C. Supreme Court through its
Commission on Lawyer Conduct). One numbered series "YY-NN" (with occasional
letter-suffixed sub-opinions, e.g. 98-32c/98-32d) running from 1989 to present,
~500 opinions.

Distinct from US/SC-JudicialEthics (the S.C. Supreme Court's Advisory
Committee on Standards of Judicial Conduct — judges) and US/SC-Courts. This is
the attorney professional-conduct advisory-opinion series that in other states
we build as US/{ST}-LegalEthics (lawyers).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. The opinions are listed on the S.C. Bar's public page
       https://www.scbar.org/for-lawyers/quicklinks/legal-resources/ethics-advisory-opinions/
     paginated via ?page=1..N (10 per page, ~51 pages). Each opinion links to a
     detail page /…/ethics-advisory-opinion-{YY-NN}/ (the newest omit the word
     "advisory": /…/ethics-opinion-{YY-NN}/).
  2. Each detail page renders the opinion body in clean HTML inside the main
     <article class="col-md-8 …"> column — extracted directly, NO PDF/OCR
     needed. Body carries the opinion number, cited RPC rules, Facts, Question
     and Conclusion.

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
logger = logging.getLogger("legal-data-hunter.US.SC-LegalEthics")

BASE = "https://www.scbar.org"
INDEX_PATH = "/for-lawyers/quicklinks/legal-resources/ethics-advisory-opinions/"
INDEX_URL = BASE + INDEX_PATH
MAX_PAGES = 80  # generous ceiling (~51 real pages); loop stops on empty pages

# Detail-page slug, e.g. ethics-advisory-opinion-24-01 or ethics-opinion-26-01,
# with an optional single-letter sub-opinion suffix (98-32c, 98-32d).
SLUG_RE = re.compile(
    r"ethics-advisory-opinions/(ethics-(?:advisory-)?opinion-(\d{2})-(\d{1,3})([a-z]?))/",
    re.I,
)
MONTH_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b",
    re.I,
)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class SCLegalEthicsScraper(BaseScraper):

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
    @staticmethod
    def _norm_num(yy: str, seq: str, suffix: str) -> str:
        """Normalise 'YY-NN[letter]' to canonical 'YYYY-NN[letter]'.
        SC EAOs run 1989 (89-01) to present (26-01); YY>=50 => 19YY."""
        y = int(yy)
        year = 1900 + y if y >= 50 else 2000 + y
        return f"{year}-{int(seq):02d}{suffix.lower()}"

    def _list_opinions(self) -> list[tuple[str, str, int]]:
        """Return [(opinion_number 'YYYY-NN[letter]', detail_url, year)],
        de-duplicated on opinion_number, ordered oldest-first.

        Walks the paginated index ?page=1..N, stopping after two consecutive
        pages that surface no new opinion slugs."""
        out: dict[str, tuple[str, str, int]] = {}
        empty_streak = 0
        for page in range(1, MAX_PAGES + 1):
            r = self._get(f"{INDEX_URL}?page={page}")
            if not r:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue
            new_here = 0
            for m in SLUG_RE.finditer(r.text):
                slug, yy, seq, suffix = m.group(1), m.group(2), m.group(3), m.group(4)
                num = self._norm_num(yy, seq, suffix)
                if num in out:
                    continue
                url = BASE + INDEX_PATH + slug + "/"
                out[num] = (num, url, int(num[:4]))
                new_here += 1
            if new_here == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
        result = sorted(out.values(), key=lambda x: x[0])
        logger.info(f"  discovered {len(result)} unique ethics advisory opinions")
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

    def _extract_body(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        art = soup.find("article", class_=re.compile(r"\bcol-md-8\b"))
        if art is None:
            return ""
        # Drop the "Download PDF Version" link and any nav/share widgets.
        for a in art.find_all("a", href=True):
            if "download pdf" in a.get_text(" ", strip=True).lower():
                a.decompose()
        for tag in art.find_all(["script", "style", "nav", "form"]):
            tag.decompose()
        text = art.get_text("\n", strip=True)
        return self._clean(text)

    def _fetch_one(self, num: str, url: str, year: int) -> dict | None:
        r = self._get(url)
        if not r or not r.text:
            return None
        text = self._extract_body(r.text)
        # strip a leading bare "Download PDF Version" residue if present
        text = re.sub(r"^\s*Download PDF Version\s*", "", text).strip()
        if len(text) < 150:
            logger.warning(f"  {num}: insufficient text ({len(text)} chars)")
            return None

        # Date: SC EAOs are dated by year only (encoded in the number). Use an
        # explicit in-range Month DD, YYYY date if the body carries one; else
        # fall back to YYYY-01-01.
        date = f"{year}-01-01"
        for mm in MONTH_DATE_RE.finditer(text):
            mon, day, yr = mm.group(1).lower(), int(mm.group(2)), int(mm.group(3))
            if abs(yr - year) <= 1 and 1 <= day <= 31:
                date = f"{yr:04d}-{_MONTHS[mon]:02d}-{day:02d}"
                break

        return {
            "opinion_number": num,
            "title": f"South Carolina Bar Ethics Advisory Opinion {num}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing South Carolina Bar ethics advisory opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for num, url, year in ops[:2] + ops[-1:]:
            rec = self._fetch_one(num, url, year)
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
            "_id": f"US/SC-LegalEthics/{num}",
            "_source": "US/SC-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "South Carolina Bar — Ethics Advisory Committee",
            "title": raw.get("title") or f"SC Bar Ethics Advisory Opinion {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-SC",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for num, url, year in self._list_opinions():
            rec = self._fetch_one(num, url, year)
            if not rec:
                logger.warning(f"  no text for {num}, skipping")
                continue
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

    parser = argparse.ArgumentParser(description="US/SC-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
