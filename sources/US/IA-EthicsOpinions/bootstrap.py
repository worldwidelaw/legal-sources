#!/usr/bin/env python3
"""
US/IA-EthicsOpinions -- Iowa Ethics and Campaign Disclosure Board (IECDB)
Advisory Opinions.

Fetches the full text of the formal advisory opinions issued by the Iowa Ethics
and Campaign Disclosure Board construing Iowa Code ch. 68A (Campaign Finance) and
ch. 68B (Government Ethics and Lobbying) and the Board's rules. An advisory
opinion is the Board's written interpretation of those statutes, requested by an
official, candidate, committee, lobbyist or agency = doctrine. Iowa state agency
public record (government-edict work).

Access (no JavaScript, no CAPTCHA, no auth):
  A paginated listing at

      https://ethics.iowa.gov/rulings/advisory-opinions?page={p}

  (p = 0, 1, 2, ...) links each opinion's HTML detail page at

      https://ethics.iowa.gov/rulings/advisory-opinions/iecdb-ao-{YYYY}-{NN}

  The listing anchor text carries the opinion number and subject
  ("AO 2008-12: Campaign Sign Placement"); the detail page holds the full
  born-digital text in <div class="node__content">, with the issue date as the
  first "Month DD, YYYY" in the body. All opinions are doctrine.

Strategy:
  Walk the listing pages until one yields no new opinion links, collect the
  (number, subject, url) triples, then fetch each detail page newest-first and
  extract the body text + date.

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

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IA-EthicsOpinions")

BASE_URL = "https://ethics.iowa.gov"
LISTING_URL = "https://ethics.iowa.gov/rulings/advisory-opinions"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

SLUG_RE = re.compile(r"/rulings/advisory-opinions/iecdb-ao-(\d{4})-(\d+)", re.I)
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

MAX_PAGES = 40


def _iso_from_body(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if m:
        mo = MONTH_NUM[m.group(1)]
        d = int(m.group(2))
        y = int(m.group(3))
        if 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class IAEthicsOpinionsScraper(BaseScraper):

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
    def _collect_index(self) -> list[dict]:
        """Walk listing pages and collect (number, subject, url) triples."""
        found: dict[str, dict] = {}
        for p in range(MAX_PAGES):
            r = self._get(f"{LISTING_URL}?page={p}")
            if r is None or r.status_code != 200:
                break
            if BeautifulSoup is None:
                logger.error("BeautifulSoup unavailable — cannot parse index")
                break
            soup = BeautifulSoup(r.text, "html.parser")
            new = 0
            for a in soup.find_all("a", href=SLUG_RE):
                m = SLUG_RE.search(a.get("href", ""))
                if not m:
                    continue
                year, num = m.group(1), int(m.group(2))
                number = f"{year}-{num:02d}"
                if number in found:
                    continue
                href = a.get("href")
                if href.startswith("/"):
                    href = BASE_URL + href
                anchor = a.get_text(" ", strip=True)
                subject = None
                if ":" in anchor:
                    subject = anchor.split(":", 1)[1].strip() or None
                found[number] = {
                    "number": number,
                    "year": int(year),
                    "num": num,
                    "subject": subject,
                    "url": href,
                }
                new += 1
            logger.info(f"  listing page {p}: {new} new (total {len(found)})")
            if new == 0:
                break
        # Newest first (by year then number).
        ordered = sorted(
            found.values(), key=lambda r: (r["year"], r["num"]), reverse=True
        )
        logger.info(f"Index collected: {len(ordered)} advisory opinions")
        return ordered

    @staticmethod
    def _extract_body(page_html: str) -> str:
        if BeautifulSoup is None:
            frag = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", page_html,
                          flags=re.S | re.I)
            frag = re.sub(r"<[^>]+>", " ", frag)
            return re.sub(r"\s+", " ", frag).strip()
        soup = BeautifulSoup(page_html, "html.parser")
        node = soup.select_one("div.node__content") or soup.select_one("main")
        if not node:
            return ""
        # Drop obvious non-content chrome.
        for junk in node.select("nav, .field--name-field-topics + *, script, style"):
            junk.decompose()
        return node.get_text("\n", strip=True)

    def _fetch_one(self, row: dict) -> dict | None:
        r = self._get(row["url"])
        if r is None or r.status_code != 200:
            return None
        body = self._extract_body(r.text)
        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        if not body or len(body) < 300:
            return None
        date = _iso_from_body(body)
        out = dict(row)
        out["text"] = body
        out["date"] = date
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  AO {rec['number']} OK ({len(rec['text'])} chars)")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing IA IECDB advisory opinions...")
        idx = self._collect_index()
        if len(idx) < 30:
            logger.error(f"API test FAILED: index too small ({len(idx)})")
            return False
        ok = 0
        for row in idx[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 300:
                logger.info(
                    f"  AO {rec['number']} OK ({len(rec['text'])} chars, date={rec['date']})"
                )
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw["number"]
        subject = raw.get("subject")
        caption = f"IECDB AO {number}"
        if subject:
            caption += f": {subject}"
        return {
            "_id": f"US/IA-EthicsOpinions/AO-{number}",
            "_source": "US/IA-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": f"AO {number}",
            "issuer": "Iowa Ethics and Campaign Disclosure Board",
            "title": caption,
            "subject": subject,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-IA",
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

    parser = argparse.ArgumentParser(description="US/IA-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IAEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
