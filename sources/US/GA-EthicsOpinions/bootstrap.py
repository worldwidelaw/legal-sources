#!/usr/bin/env python3
"""
US/GA-EthicsOpinions -- Georgia Government Transparency and Campaign Finance
Commission (formerly State Ethics Commission) -- Advisory Opinions.

Fetches the full text of the formal advisory opinions issued by the Georgia
Government Transparency and Campaign Finance Commission construing the Georgia
Government Transparency and Campaign Finance Act (O.C.G.A. Title 21, ch. 5) and
the Commission's rules. An advisory opinion is the Commission's written
interpretation of those statutes, requested by a candidate, public officer,
committee, lobbyist or agency = doctrine. Georgia state agency public record
(government-edict work), spanning 1987-present.

Access (no JavaScript, no CAPTCHA, no auth):
  The site is WordPress. The "Advisory Opinions" category (id 23) enumerates the
  full corpus through the public WP REST API:

      https://ethics.ga.gov/wp-json/wp/v2/posts?categories=23&per_page=100

  Each post's `content.rendered` carries the FULL born-digital text of the
  opinion inline (the lead "PDF Copy of the Advisory Opinion" link is stripped).
  The post `title.rendered` carries the opinion number ("Advisory Opinion:
  2023-01"). All opinions are doctrine.

Strategy:
  Pull every post in category 23 (one page, ~99 posts), strip the HTML from
  content.rendered, parse the opinion number from the title, dedup by number
  (some posts are duplicated re-publishes), parse the issue date from the body
  (fallback to Jan 1 of the opinion year).

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
import html
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
logger = logging.getLogger("legal-data-hunter.US.GA-EthicsOpinions")

BASE_URL = "https://ethics.ga.gov"
API_URL = "https://ethics.ga.gov/wp-json/wp/v2/posts"
CATEGORY_ID = 23  # "Advisory Opinions"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# Opinion number: {YYYY}-{N} in the title, e.g. "Advisory Opinion: 2023-01",
# "ADVISORY OPINION: 2011-1".
NUM_RE = re.compile(r"((?:19|20)\d{2})-(\d{1,2})")

MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|"
    "November|December"
)
DATE_RE = re.compile(rf"({MONTHS})\s+(\d{{1,2}}),\s+((?:19|20)\d{{2}})")
MONTH_NUM = {
    m: i + 1
    for i, m in enumerate(
        "January February March April May June July August September "
        "October November December".split()
    )
}


def _iso_from_body(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if m:
        mo = MONTH_NUM[m.group(1)]
        d = int(m.group(2))
        y = int(m.group(3))
        if 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def _strip_html(fragment: str) -> str:
    """Strip HTML tags and the lead 'PDF Copy' link, return clean text."""
    if BeautifulSoup is not None:
        soup = BeautifulSoup(fragment, "html.parser")
        for junk in soup.select("script, style"):
            junk.decompose()
        text = soup.get_text("\n", strip=True)
    else:
        frag = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", fragment,
                      flags=re.S | re.I)
        frag = re.sub(r"<[^>]+>", "\n", frag)
        text = html.unescape(frag)
    # Drop the boilerplate PDF-copy link line at the top of every post.
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines
             if ln and not re.fullmatch(
                 r"(pdf copy of the advisory opinion|pdf copy|download( pdf)?)",
                 ln, re.I)]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


class GAEthicsOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str, params: dict = None):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, params=params, timeout=60,
                                        allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _collect(self) -> list[dict]:
        """Pull all posts in the Advisory Opinions category, dedup by number."""
        found: dict[str, dict] = {}
        page = 1
        while True:
            r = self._get(API_URL, params={
                "categories": CATEGORY_ID,
                "per_page": 100,
                "page": page,
                "_fields": "id,date,link,title,content",
            })
            if r is None or r.status_code != 200:
                break
            try:
                posts = r.json()
            except Exception:
                break
            if not posts:
                break
            for p in posts:
                title = html.unescape(p.get("title", {}).get("rendered", "")).strip()
                m = NUM_RE.search(title)
                if not m:
                    continue
                year, seq = int(m.group(1)), int(m.group(2))
                number = f"{year}-{seq:02d}"
                body = _strip_html(p.get("content", {}).get("rendered", ""))
                if not body or len(body) < 300:
                    continue
                # Keep the longest body if the number appears twice (duplicated
                # re-publish posts exist).
                prev = found.get(number)
                if prev and len(prev["text"]) >= len(body):
                    continue
                found[number] = {
                    "number": number,
                    "year": year,
                    "seq": seq,
                    "title": title,
                    "text": body,
                    "url": p.get("link") or BASE_URL,
                    "date": _iso_from_body(body) or f"{year:04d}-01-01",
                }
            total_pages = r.headers.get("X-WP-TotalPages")
            if total_pages and page >= int(total_pages):
                break
            page += 1
        ordered = sorted(
            found.values(), key=lambda x: (x["year"], x["seq"]), reverse=True
        )
        logger.info(f"Collected {len(ordered)} advisory opinions")
        return ordered

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect():
            yield row
            emitted += 1
            logger.info(f"  AO {row['number']} OK ({len(row['text'])} chars)")
            if sample and emitted >= 12:
                return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing GA ethics advisory opinions API...")
        idx = self._collect()
        if len(idx) < 30:
            logger.error(f"API test FAILED: index too small ({len(idx)})")
            return False
        ok = sum(1 for r in idx[:4] if len(r["text"]) > 300)
        if ok >= 2:
            logger.info(
                f"API test PASSED ({len(idx)} opinions; "
                f"newest {idx[0]['number']} {len(idx[0]['text'])} chars, "
                f"date={idx[0]['date']})"
            )
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        return {
            "_id": f"US/GA-EthicsOpinions/AO-{number}",
            "_source": "US/GA-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"Advisory Opinion {number}",
            "issuer": "Georgia Government Transparency and Campaign Finance Commission",
            "title": f"Georgia Advisory Opinion {number}",
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-GA",
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

    parser = argparse.ArgumentParser(description="US/GA-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GAEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
