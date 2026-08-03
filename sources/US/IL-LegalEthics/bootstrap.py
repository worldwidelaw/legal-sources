#!/usr/bin/env python3
"""
US/IL-LegalEthics -- Illinois State Bar Association (ISBA) — Professional
Conduct Advisory Opinions

Fetches the full text of the Advisory Opinions on Professional Conduct issued by
the Illinois State Bar Association's Standing Committee on Professional Conduct.
Each opinion expresses the ISBA's interpretation of the Illinois Rules of
Professional Conduct (IRPC) in response to a stated hypothetical fact situation
and advises lawyers whether the described conduct is proper = doctrine (the
Committee's official written interpretation of the attorney-conduct rules).

The corpus is one online archive dating back to the early sequential-numbered
opinions (Opinion No. 6xx/7xx, late 1970s-1983) and continuing in the modern
"{YY}-{NN}" series (Opinion No. 84-1 to the present). Distinct from
US/IL-AGOpinions (Illinois Attorney General opinions) and other Illinois state
sources; this is the state *bar*'s attorney-ethics advisory opinion series.

Access (no JavaScript execution needed, no CAPTCHA, no auth, browser UA):
  1. The "Ethics Opinions by Year" index enumerates EVERY opinion as a direct
     link to /ethics/opinions/{id}:
       https://www.isba.org/ethics/years
  2. Each opinion page is a born-digital HTML page. The <article> body carries
     labelled "Opinion Number" / "Opinion Date" fields, the opinion title (h1),
     and the Digest / Facts / Question / Opinion sections plus references. The
     trailing "See Related Opinions" navigation is trimmed off.

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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IL-LegalEthics")

BASE = "https://www.isba.org"
LISTING_URL = BASE + "/ethics/years"

OPINION_HREF_RE = re.compile(r'href="(/ethics/opinions/\d+)"')
OPINION_NO_RE = re.compile(r"Opinion\s+No\.?\s*([0-9]{1,4}(?:-[0-9]+)?)", re.I)
RELATED_MARKER_RE = re.compile(r"See\s+Related\s+Opinions", re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class ILLegalEthicsScraper(BaseScraper):

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
                "Version/16.0 Safari/605.1.15"
            ),
            "Accept": "text/html,application/xhtml+xml",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
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
    def _list_opinions(self) -> list[str]:
        """Return the list of absolute opinion-page URLs, de-duplicated,
        preserving the index order (newest first)."""
        r = self._get(LISTING_URL)
        if not r:
            logger.error("could not fetch the ethics-opinions index page")
            return []
        seen: set[str] = set()
        out: list[str] = []
        for m in OPINION_HREF_RE.finditer(r.text):
            path = m.group(1)
            if path in seen:
                continue
            seen.add(path)
            out.append(urljoin(BASE, path))
        logger.info(f"  index yields {len(out)} unique opinions")
        return out

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _parse_date(raw: str) -> str | None:
        """Parse an ISBA date line like 'March 2024' / 'November 1980'."""
        if not raw:
            return None
        low = raw.lower()
        ym = re.search(r"([a-z]+)\s+(\d{4})", low)
        if ym and ym.group(1) in MONTHS:
            return f"{ym.group(2)}-{MONTHS[ym.group(1)]:02d}-01"
        y = re.search(r"\b(19|20)\d{2}\b", raw)
        if y:
            return f"{y.group(0)}-01-01"
        return None

    def _fetch_one(self, url: str) -> dict | None:
        r = self._get(url)
        if not r or not r.text:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        art = soup.select_one("article") or soup.select_one("#content") or soup
        for t in art(["script", "style", "nav", "aside"]):
            t.decompose()

        h1 = art.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else None

        full = art.get_text("\n", strip=True)

        # Opinion number & date sit in labelled fields near the top.
        number = None
        mnum = OPINION_NO_RE.search(full)
        if mnum:
            number = mnum.group(1)

        date = None
        mdate = re.search(r"Opinion\s+Date:\s*\n([^\n]+)", full, re.I)
        if mdate:
            date = self._parse_date(mdate.group(1))

        # Referenced IRPC rules from the trailing "By IRPC rule:" nav.
        rules: list[str] = []
        mrule = re.search(r"By\s+IRPC\s+rule:\s*\n(.*)$", full, re.I | re.S)
        if mrule:
            for line in mrule.group(1).split("\n"):
                line = line.strip()
                if re.fullmatch(r"[0-9]+(?:\.[0-9A-Za-z()]+)*", line):
                    rules.append(line)

        # Trim trailing "See Related Opinions" navigation from the body.
        body = full
        mrel = RELATED_MARKER_RE.search(body)
        if mrel:
            body = body[:mrel.start()]
        text = self._clean(body)

        if len(text) < 150 or not number:
            return None

        return {
            "opinion_number": number,
            "title": title or f"ISBA Professional Conduct Advisory Opinion No. {number}",
            "text": text,
            "date": date,
            "rules": rules,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing ISBA Professional Conduct Advisory Opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for url in ops[:2] + ops[-1:]:
            rec = self._fetch_one(url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion No. {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({url})")
        if ok >= 1:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/IL-LegalEthics/{num}",
            "_source": "US/IL-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("Illinois State Bar Association — Standing Committee "
                       "on Professional Conduct"),
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "rules": raw.get("rules") or [],
            "jurisdiction": "US-IL",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen: set[str] = set()
        for url in self._list_opinions():
            rec = self._fetch_one(url)
            if not rec:
                logger.warning(f"  no text for {url}, skipping")
                continue
            if rec["opinion_number"] in seen:
                continue
            seen.add(rec["opinion_number"])
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

    parser = argparse.ArgumentParser(description="US/IL-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
