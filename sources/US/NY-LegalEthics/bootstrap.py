#!/usr/bin/env python3
"""
US/NY-LegalEthics -- New York State Bar Association (NYSBA) — Committee on
Professional Ethics — Ethics Opinions

Fetches the full text of the Ethics Opinions issued by the New York State Bar
Association's Committee on Professional Ethics. Each opinion is advisory and
expresses the Committee's interpretation of the New York Rules of Professional
Conduct (and, for older opinions, the Code of Professional Responsibility) in
response to an attorney's inquiry about their own proposed conduct = doctrine
(the committee's official written interpretation of the attorney-conduct rules).

The corpus is retrospective to 1957 and runs as one continuous numbered series
(Opinion #9 to Opinion #1295+ as of July 2026, ~1,286 opinions), published free
to the public on nysba.org. This is the state *bar*'s attorney-ethics series and
is distinct from US/NY-EthicsOpinions (COELIG/JCOPE, public officials) and from
New York Attorney General opinions.

Access (no JavaScript execution, no CAPTCHA, no auth; browser UA):
  1. Discovery — the "Ethics Opinions" category index paginates every opinion as
     a post:
       https://nysba.org/category/ethics-opinions/page/{p}/
     Each opinion post uses one of two permalink schemes:
       /opinion-{N}/            (older opinions)
       /ethics-opinion-{N}[-slug]/   (newer opinions)
     The scraper walks the pages, reads each <article>'s title link, and takes
     the exact href (the number is parsed from the href, never constructed — a
     bare /ethics-opinion-1/ wrongly prefix-redirects to /opinion-100/).
  2. Full text — each opinion page is a born-digital HTML page; the
     .single-post-content body carries the Topic / Digest / Code / Question /
     Opinion text. No OCR. The published date is read from the
     <meta property="article:published_time"> tag (backfilled to the real issue
     date for older opinions).

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
logger = logging.getLogger("legal-data-hunter.US.NY-LegalEthics")

BASE = "https://nysba.org"
CATEGORY_URL = BASE + "/category/ethics-opinions/page/{p}/"

# Two permalink schemes; number is the first digit run after "opinion-".
OPINION_HREF_RE = re.compile(
    r'href="(https://nysba\.org/(?:ethics-)?opinion-(\d+)[a-z0-9-]*/)"', re.I
)
BOILERPLATE_LINES = {
    "news center",
    "view and download as pdf",
}


class NYLegalEthicsScraper(BaseScraper):

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
    def _list_opinions(self, max_pages: int = 200) -> list[tuple[str, str]]:
        """Return [(number, url), ...] for every opinion, newest-first,
        de-duplicated on number. Walks category pages until two consecutive
        pages yield no opinion links."""
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        empty_streak = 0
        for p in range(1, max_pages + 1):
            r = self._get(CATEGORY_URL.format(p=p))
            found = 0
            if r:
                page_seen: set[str] = set()
                for m in OPINION_HREF_RE.finditer(r.text):
                    url, num = m.group(1), m.group(2)
                    key = f"{url}|{num}"
                    if key in page_seen:
                        continue
                    page_seen.add(key)
                    found += 1
                    if num in seen:
                        continue
                    seen.add(num)
                    out.append((num, url))
            if found == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
        logger.info(f"  index yields {len(out)} unique opinions")
        return out

    # -------------------------------------------------------- extraction
    @classmethod
    def _clean(cls, text: str) -> str:
        lines = []
        for ln in text.split("\n"):
            s = ln.strip()
            if s.lower() in BOILERPLATE_LINES:
                continue
            lines.append(ln)
        text = "\n".join(lines)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _fetch_one(self, number: str, url: str) -> dict | None:
        r = self._get(url)
        if not r or not r.text:
            return None
        soup = BeautifulSoup(r.text, "html.parser")

        date = None
        meta = soup.select_one('meta[property="article:published_time"]')
        if meta and meta.get("content"):
            date = meta["content"][:10]

        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else None

        body = (soup.select_one(".single-post-content")
                or soup.select_one("article")
                or soup.find("main"))
        if body is None:
            return None
        for t in body(["script", "style", "nav", "aside", "form"]):
            t.decompose()
        text = self._clean(body.get_text("\n", strip=True))

        # Drop a leading duplicated title line if present.
        if title:
            text = re.sub(
                r"^(?:" + re.escape(title) + r"\n)+", "", text
            ).strip()

        if len(text) < 150:
            return None

        return {
            "opinion_number": number,
            "title": title or f"NYSBA Ethics Opinion {number}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NYSBA Committee on Professional Ethics opinions...")
        ops = self._list_opinions(max_pages=3)
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        logger.info(f"  page-1..3 sample: {[n for n, _ in ops[:5]]}")
        ok = 0
        # Probe a recent one and an old-scheme one.
        probes = [ops[0]]
        old = ("706", BASE + "/opinion-706/")
        probes.append(old)
        for number, url in probes:
            rec = self._fetch_one(number, url)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion {number} OK ({len(rec['text'])} chars) "
                            f"date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  Opinion {number} — no text ({url})")
        if ok >= 1:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/NY-LegalEthics/{num}",
            "_source": "US/NY-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": ("New York State Bar Association — Committee on "
                       "Professional Ethics"),
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NY",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for number, url in self._list_opinions():
            rec = self._fetch_one(number, url)
            if not rec:
                logger.warning(f"  no text for opinion {number} ({url}), skipping")
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

    parser = argparse.ArgumentParser(description="US/NY-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NYLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
