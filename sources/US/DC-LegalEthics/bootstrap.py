#!/usr/bin/env python3
"""
US/DC-LegalEthics -- District of Columbia Bar, Legal Ethics Committee — Ethics
Opinions.

Fetches the full text of the ethics opinions issued by the D.C. Bar Legal
Ethics Committee. Each opinion interprets the District of Columbia Rules of
Professional Conduct as applied to a lawyer's contemplated conduct to advise
LAWYERS = doctrine. The D.C. Bar cites its opinions by number ("D.C. Legal
Ethics Opinion 388"). Opinions 210-present are published as clean, born-digital
HTML pages (one page per opinion); ~180 opinions, roughly 1990-present.

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  1. A single public index page lists every opinion 210-present as an anchor to
     its own detail page:
       https://www.dcbar.org/for-lawyers/legal-ethics/ethics-opinions-210-present
     Each anchor -> /For-Lawyers/Legal-Ethics/Ethics-Opinions-210-Present/
     Ethics-Opinion-{N}  (a "-(Revised)" variant exists for a few numbers).
  2. Each detail page carries the full opinion text in an
     <article class="c-news-detail"> container (h1 = "Ethics Opinion N",
     h2 = title, then <p>/<ul>/<ol> body). Extracted directly from the HTML,
     NO OCR, NO PDF.

Opinions 2-209 (interpreting the pre-1991 D.C. Code of Professional
Responsibility) are published only as a single consolidated 1991-edition PDF
and are intentionally NOT covered here — this source is the current, per-opinion
HTML series (210-present).

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
logger = logging.getLogger("legal-data-hunter.US.DC-LegalEthics")

BASE = "https://www.dcbar.org"
INDEX_URL = ("https://www.dcbar.org/for-lawyers/legal-ethics/"
             "ethics-opinions-210-present")

# The per-opinion detail URLs end in ".../Ethics-Opinion-{N}" where {N} is the
# opinion number, optionally with a "-(Revised)" / "-Revised" suffix.
SLUG_RE = re.compile(r"/ethics-opinion-([0-9]+(?:-?\(?revised\)?)?)\s*$",
                     re.IGNORECASE)
NUM_RE = re.compile(r"(\d+)")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


class DCLegalEthicsScraper(BaseScraper):

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
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90, allow_redirects=True)
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
    def _list_opinions(self) -> list[dict]:
        """Return [{slug, num, url}], de-duplicated on the opinion slug."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the D.C. Bar ethics opinions index")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].split("#")[0]
            m = SLUG_RE.search(href)
            if not m:
                continue
            raw = m.group(1)
            nm = NUM_RE.search(raw)
            if not nm:
                continue
            base_num = nm.group(1)
            revised = "revised" in raw.lower()
            num = f"{base_num} (Revised)" if revised else base_num
            slug = f"{base_num}-revised" if revised else base_num
            url = urljoin(BASE, a["href"].split("#")[0])
            if slug in out:
                continue
            out[slug] = {"slug": slug, "num": num, "base_num": base_num,
                         "url": url}
        # Sort numerically by base number, revised after the base.
        result = sorted(out.values(),
                        key=lambda x: (int(x["base_num"]), x["slug"]))
        logger.info(f"  discovered {len(result)} unique ethics opinions")
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

    @staticmethod
    def _parse_date(text: str) -> str | None:
        """The newer opinions carry a 'Published: <Month> <Year>' (or
        'Published: <Year>') line; decode to an ISO date (first of month).
        Older opinions carry no explicit publication date -> None."""
        m = re.search(r"Published:?\s*([A-Za-z]+)?\s*(\d{4})", text)
        if not m:
            return None
        year = int(m.group(2))
        mon = m.group(1)
        mm = MONTHS.get(mon.lower()) if mon else None
        if mm:
            return f"{year:04d}-{mm:02d}-01"
        return f"{year:04d}-01-01"

    def _fetch_one(self, op: dict) -> dict | None:
        r = self._get(op["url"])
        if not r or not r.text:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        art = soup.find("article", class_="c-news-detail")
        if art is None:
            logger.warning(f"  {op['num']}: no article container")
            return None
        # Opinion title from the <h2> heading (h1 is just "Ethics Opinion N").
        h2 = art.find("h2")
        heading = h2.get_text(" ", strip=True) if h2 else ""
        text = self._clean(art.get_text("\n", strip=True))
        if len(text) < 200:
            logger.warning(f"  {op['num']}: insufficient text ({len(text)} chars)"
                           f" - skipping")
            return None
        title = f"D.C. Legal Ethics Opinion {op['num']}"
        if heading:
            title += f" — {heading}"
        return {
            "opinion_number": op["num"],
            "slug": op["slug"],
            "title": title,
            "text": text,
            "date": self._parse_date(text),
            "url": op["url"],
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing D.C. Bar ethics opinions index...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for op in ops[:2] + ops[-1:]:
            rec = self._fetch_one(op)
            if rec and len(rec["text"]) > 500:
                logger.info(f"  Opinion {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text ({op['url']})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        slug = raw["slug"]
        return {
            "_id": f"US/DC-LegalEthics/{slug}",
            "_source": "US/DC-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": raw["opinion_number"],
            "issuer": "District of Columbia Bar — Legal Ethics Committee",
            "title": raw.get("title") or f"D.C. Legal Ethics Opinion {slug}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-DC",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        # Iterate newest opinion first. The framework's sample mode keeps the
        # first N records off fetch_all(), and the newer opinions are the ones
        # that carry an explicit "Published:" date, so a newest-first order
        # yields a representative, mostly-dated sample. Ordering is irrelevant
        # for the full pull (every opinion is processed either way).
        ops = list(reversed(self._list_opinions()))
        emitted = 0
        for op in ops:
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

    parser = argparse.ArgumentParser(description="US/DC-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DCLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
