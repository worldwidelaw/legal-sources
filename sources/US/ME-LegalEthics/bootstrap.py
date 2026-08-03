#!/usr/bin/env python3
"""
US/ME-LegalEthics -- Maine Board of Overseers of the Bar — Ethics Opinions

Fetches the full text of the advisory Ethics Opinions issued by the
Professional Ethics Commission of the Maine Board of Overseers of the Bar.
Each opinion is the Commission's formal written interpretation and
application of the Maine Rules of Professional Conduct (formerly the Maine
Bar Rules / Code of Professional Responsibility), rendered on request to the
Court, Board, Grievance Commission, Bar Counsel and members of the Maine bar
to advise LAWYERS whether described conduct is proper = doctrine.

The corpus is one continuous numbered series ("Opinion #N", 1979-present,
~228 opinions). Many older opinions also carry an "Enduring Ethics Opinion"
update note. Distinct from US/ME-AGOpinions (Attorney General) and any Maine
executive ethics commission.

Access (no JavaScript, CAPTCHA or auth):
  1. The public index lists every opinion as an
     opinion.html?id={id} link with anchor text "#N. {Title}":
       https://www.mebaroverseers.org/attorney_services/ethics_opinions.html
  2. Each opinion page is born-digital HTML; the opinion body is
     div#maincontent2, carrying "Opinion #N. {Title}", "Date Issued:
     {Month DD, YYYY}", the Question and the Commission's full Opinion.

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
logger = logging.getLogger("legal-data-hunter.US.ME-LegalEthics")

BASE = "https://www.mebaroverseers.org"
INDEX_URL = BASE + "/attorney_services/ethics_opinions.html"
ID_RE = re.compile(r"opinion\.html\?id=(\d+)", re.I)
NUM_RE = re.compile(r"Opinion\s*#\s*(\d+)\.?\s*(.*)", re.I)
DATE_RE = re.compile(
    r"Date\s+Issued:\s*(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{1,2}),\s+((?:19|20)\d\d)", re.I)
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class MELegalEthicsScraper(BaseScraper):

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
            "Accept": "text/html,application/xhtml+xml",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> requests.Response | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=90)
                if r.status_code == 200:
                    if not r.encoding or r.encoding.lower() == "iso-8859-1":
                        r.encoding = r.apparent_encoding or "utf-8"
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
        """Return [(opinion_id, index_title)] de-duplicated on id, in order."""
        r = self._get(INDEX_URL)
        if not r:
            logger.error("could not fetch the Maine ethics opinions index")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            m = ID_RE.search(a["href"])
            if not m:
                continue
            oid = m.group(1)
            if oid in seen:
                continue
            seen.add(oid)
            out.append((oid, a.get_text(" ", strip=True)))
        logger.info(f"  index yields {len(out)} unique opinions")
        return out

    # -------------------------------------------------------- extraction
    @staticmethod
    def _clean(text: str) -> str:
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _fetch_one(self, oid: str, index_title: str) -> dict | None:
        url = f"{BASE}/attorney_services/opinion.html?id={oid}"
        r = self._get(url)
        if not r:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        main = soup.find("div", id="maincontent2")
        if not main:
            logger.warning(f"  id={oid}: no maincontent2 div")
            return None
        text = self._clean(main.get_text("\n", strip=True))
        if len(text) < 150:
            return None

        # Opinion number + title from the "Opinion #N. Title" header.
        num = None
        title = None
        mh = NUM_RE.search(text)
        if mh:
            num = mh.group(1)
            title = (mh.group(2) or "").strip(" .") or None
        if not num:
            mi = re.search(r"#\s*(\d+)", index_title)
            num = mi.group(1) if mi else oid
        if not title:
            title = re.sub(r"^#\s*\d+\.?\s*", "", index_title).strip() or None

        # Date from "Date Issued: Month DD, YYYY".
        date = None
        md = DATE_RE.search(text)
        if md:
            date = f"{int(md.group(3)):04d}-{_MONTHS[md.group(1).lower()]:02d}-{int(md.group(2)):02d}"

        return {
            "opinion_number": num,
            "opinion_id": oid,
            "title": title or f"Maine Ethics Opinion #{num}",
            "text": text,
            "date": date,
            "url": url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Maine Board of Overseers ethics opinions...")
        ops = self._list_opinions()
        if not ops:
            logger.error("API test FAILED: no opinions listed")
            return False
        ok = 0
        for oid, itl in ops[:2] + ops[-1:]:
            rec = self._fetch_one(oid, itl)
            if rec and len(rec["text"]) > 300:
                logger.info(f"  Opinion #{rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
            else:
                logger.warning(f"  no text (id={oid})")
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        return {
            "_id": f"US/ME-LegalEthics/{num}",
            "_source": "US/ME-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "Maine Board of Overseers of the Bar — Professional Ethics Commission",
            "title": raw.get("title"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-ME",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_num: set[str] = set()
        for oid, itl in self._list_opinions():
            rec = self._fetch_one(oid, itl)
            if not rec:
                logger.warning(f"  no text for id={oid}, skipping")
                continue
            if rec["opinion_number"] in seen_num:
                continue
            seen_num.add(rec["opinion_number"])
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

    parser = argparse.ArgumentParser(description="US/ME-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MELegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
