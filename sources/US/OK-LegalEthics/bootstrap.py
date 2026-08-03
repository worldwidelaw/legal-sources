#!/usr/bin/env python3
"""
US/OK-LegalEthics -- Oklahoma Bar Association — Legal Ethics Advisory Opinions.

Fetches the full text of the advisory legal-ethics opinions issued by the
Oklahoma Bar Association's Legal Ethics Committee / Legal Ethics Advisory Panel,
interpreting the Oklahoma Rules of Professional Conduct (and, for the older
opinions, the predecessor Canons/Code) in response to a stated question, to
advise LAWYERS = doctrine (advisory). One continuous globally-numbered series,
"Opinion No. N", running No. 1 (1931) -> No. 330+ (present); the finalized
opinions are published in the Oklahoma Bar Journal and cited "___ OK LEG ETH ___".

The Oklahoma Bar Association is the state's INTEGRATED (mandatory / unified) bar,
created by and operating under the Supreme Court of Oklahoma, so its ethics
opinions are the work of a government-authorized body -> pd-us (17 U.S.C. § 105
government-edicts rationale, consistent with the other state-bar legal-ethics
sources).

Distinct from US/OK-Courts, US/OK-Legislation and US/OK-TaxDecisions.

Access (no JavaScript execution, no CAPTCHA, no auth):
  1. The corpus is a paginated WordPress (Beaver Builder) archive at
       https://www.okbar.org/ethics/page/{p}/    (p = 1 .. ~34, then 404)
     Each page links the opinion detail pages as
       /ethics/ethics-opinion-no-{N}/            (some carry a "-2" WP slug
                                                   collision suffix)
  2. Each detail page is clean born-HTML; the opinion body lives in the
     Beaver-Builder post-content module
       div.fl-module-fl-post-content .fl-module-content
     -> extracted with BeautifulSoup, NO PDF, NO OCR.

  The opinion NUMBER is taken from the slug (ethics-opinion-no-{N}); the issue
  date from the "Adopted / Issued <Month DD, YYYY>" line in the body.

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
logger = logging.getLogger("legal-data-hunter.US.OK-LegalEthics")

BASE = "https://www.okbar.org"
LIST_URL = BASE + "/ethics/page/{}/"
MAX_PAGES = 60  # safety cap; real archive ends ~page 34 (404 thereafter)

SLUG_RE = re.compile(r"/ethics/(ethics-opinion-no-(\d+)[a-z0-9-]*)/", re.I)
CONTENT_SEL = "div.fl-module-fl-post-content .fl-module-content"

MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
# "Adopted January 22, 1932", "Issued July 3, 2013", "Amended May 1, 2004".
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),\s*(\d{4})", re.I)


class OKLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.0 Safari/605.1.15"
            ),
            "Accept": "text/html,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                logger.warning(f"GET {url[:90]} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> list[dict]:
        """Walk the paginated archive; return [{number, slugs[]}] deduped.

        A number may map to more than one slug (WP "-2" collision suffix); we
        keep every distinct slug and, at fetch time, pick the one that yields
        the longest text.
        """
        out: dict[int, dict] = {}
        empty_streak = 0
        for p in range(1, MAX_PAGES + 1):
            r = self._get(LIST_URL.format(p))
            if r is None:
                break
            found = 0
            for m in SLUG_RE.finditer(r.text):
                slug, num = m.group(1), int(m.group(2))
                rec = out.setdefault(num, {"number": num, "slugs": []})
                if slug not in rec["slugs"]:
                    rec["slugs"].append(slug)
                    found += 1
            if found == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
        result = sorted(out.values(), key=lambda x: x["number"])
        logger.info(f"  discovered {len(result)} published ethics opinions")
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

    def _page_text(self, slug: str) -> str | None:
        r = self._get(f"{BASE}/ethics/{slug}/")
        if not r:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        el = soup.select_one(CONTENT_SEL)
        if not el:
            return None
        # drop the standing "Legal Ethics Committee Advisory Opinions" header
        text = self._clean(el.get_text("\n", strip=True))
        return text or None

    @staticmethod
    def _issue_date(text: str) -> str | None:
        m = DATE_RE.search(text[:2000])
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        day = int(m.group(2))
        yr = int(m.group(3))
        if mon and 1 <= day <= 31 and 1900 <= yr <= 2100:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
        return None

    def _fetch_one(self, op: dict) -> dict | None:
        best = ""
        best_slug = op["slugs"][0]
        for slug in op["slugs"]:
            text = self._page_text(slug)
            if text and len(text) > len(best):
                best, best_slug = text, slug
        if len(best) < 150:
            logger.warning(f"  No. {op['number']}: insufficient text "
                           f"({len(best)}), skipping")
            return None
        return {
            "number": op["number"],
            "text": best,
            "date": self._issue_date(best),
            "url": f"{BASE}/ethics/{best_slug}/",
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Oklahoma Bar Association ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  No. {rec['number']} OK ({len(rec['text'])} "
                            f"chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text (No. {op['number']})")
        if ok >= 2:
            logger.info(f"API test PASSED ({len(ops)} opinions available)")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["number"]
        return {
            "_id": f"US/OK-LegalEthics/{num}",
            "_source": "US/OK-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"Opinion No. {num}",
            "issuer": "Oklahoma Bar Association — Legal Ethics Committee",
            "title": f"Oklahoma Bar Association Legal Ethics Opinion No. {num}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-OK",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for op in self._list_opinions():
            rec = self._fetch_one(op)
            if not rec:
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

    parser = argparse.ArgumentParser(description="US/OK-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OKLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
