#!/usr/bin/env python3
"""
US/NC-LegalEthics -- North Carolina State Bar — Legal (Attorney) Ethics Opinions

Fetches the full text of the ethics opinions adopted by the Council of the North
Carolina State Bar. The State Bar is the agency that licenses and regulates
lawyers in North Carolina; its Ethics Committee issues formal opinions
interpreting the North Carolina Rules of Professional Conduct and advising
lawyers on their professional-responsibility obligations. Each opinion answers a
specific inquiry and states the Committee's conclusion = doctrine (the State
Bar's official written interpretation of the attorney-conduct rules).

Three historical series are published under one index:
  - CPR  (opinions under the pre-1985 Code of Professional Responsibility)
  - RPC  (opinions under the 1985 Rules of Professional Conduct)
  - Formal Ethics Opinions ("{YYYY} Formal Ethics Opinion N", Revised Rules 1997+)

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  The listing is a Lit web component backed by a public JSON search endpoint:

      POST https://www.ncbar.gov/myethicsopinions/search/
      Content-Type: application/json
      {"IndexGuid": "<guid>", "Term": [], "Status": ["adopted"],
       "StartDate": "", "EndDate": "", "Categories": [],
       "PageNumber": <0-based>, "SortBy": "date-asc"}

  Response: {"success": true, "data": {"results": [{headline, dateString,
  statusValue, url}, ...], "totalCount", "totalPages", "currentPage"}}.
  20 results per page. Each result "url" points to a born-digital HTML opinion
  page whose body sits in <div class="ethicsContent"> (full text: Inquiry /
  Opinion / analysis + footnotes) — extracted directly from the HTML, no OCR.

  The IndexGuid is read live from the listing page's
  <ethics-opinions-index-page-sidebar-lit indexGuid="..."> attribute so the
  scraper keeps working if the Bar re-publishes the index.

Strategy:
  Read the IndexGuid off the listing page, page through the search API for all
  adopted opinions, then GET each opinion page and slice out div.ethicsContent.

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
logger = logging.getLogger("legal-data-hunter.US.NC-LegalEthics")

BASE = "https://www.ncbar.gov"
LISTING_URL = BASE + "/for-lawyers/ethics-and-governing-rules/ethics-opinions/"
SEARCH_URL = BASE + "/myethicsopinions/search/"
# Fallback GUID (as published 2026-07); overridden by the live value when reachable.
DEFAULT_INDEX_GUID = "a7949cfc-45ae-459a-97cf-e9f1d3cfab64"

GUID_RE = re.compile(r'indexGuid="([0-9a-fA-F-]{36})"')


class NCLegalEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._guid = None
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept": "application/json, text/html",
        })

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.get(url, timeout=60)
                if r.status_code == 200 and r.text:
                    return r.text
                logger.warning(f"GET {url} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _post_search(self, page: int) -> dict | None:
        payload = {
            "IndexGuid": self._index_guid(),
            "Term": [],
            "Status": ["adopted"],
            "StartDate": "",
            "EndDate": "",
            "Categories": [],
            "PageNumber": page,
            "SortBy": "date-asc",
        }
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = self._session.post(
                    SEARCH_URL, json=payload, timeout=60,
                    headers={"Content-Type": "application/json",
                             "Accept": "application/json"},
                )
                if r.status_code == 200:
                    data = r.json()
                    if data.get("success"):
                        return data.get("data")
                logger.warning(f"POST search page={page} -> HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"POST search page={page} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- config
    def _index_guid(self) -> str:
        if self._guid:
            return self._guid
        html = self._get(LISTING_URL)
        if html:
            m = GUID_RE.search(html)
            if m:
                self._guid = m.group(1)
                logger.info(f"  using live IndexGuid {self._guid}")
                return self._guid
        self._guid = DEFAULT_INDEX_GUID
        logger.info(f"  using fallback IndexGuid {self._guid}")
        return self._guid

    # ---------------------------------------------------------- discovery
    def _list_opinions(self) -> Generator[dict, None, None]:
        """Yield every adopted-opinion index row (headline, dateString, url)."""
        first = self._post_search(0)
        if not first:
            logger.error("could not fetch first search page")
            return
        total_pages = int(first.get("totalPages") or 1)
        total = int(first.get("totalCount") or 0)
        logger.info(f"  {total} adopted opinions across {total_pages} pages")
        seen: set[str] = set()
        for row in first.get("results", []):
            if row.get("url") and row["url"] not in seen:
                seen.add(row["url"])
                yield row
        for page in range(1, total_pages):
            data = self._post_search(page)
            if not data:
                logger.warning(f"  page {page} failed, skipping")
                continue
            for row in data.get("results", []):
                if row.get("url") and row["url"] not in seen:
                    seen.add(row["url"])
                    yield row

    # -------------------------------------------------------- extraction
    @staticmethod
    def _opinion_number(headline: str) -> str:
        """'2024 Formal Ethics Opinion 1: ...' -> '2024 Formal Ethics Opinion 1';
        'CPR 2' -> 'CPR 2'."""
        return (headline.split(":", 1)[0]).strip()

    @staticmethod
    def _parse_date(date_string: str) -> str | None:
        if not date_string:
            return None
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(date_string.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        node = soup.find(class_="ethicsContent")
        if node is None:
            # Fallback: the opinion wrapper minus site chrome.
            node = soup.find(class_="ethicsOpinion")
        if node is None:
            node = soup.find("main")
        if node is None:
            return ""
        # Drop share/print widgets if present.
        for junk in node.select("script, style, nav, .socialShare, .printOpinion"):
            junk.decompose()
        text = node.get_text("\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _fetch_one(self, row: dict) -> dict | None:
        url = row.get("url")
        if not url:
            return None
        full_url = url if url.startswith("http") else BASE + url
        html = self._get(full_url)
        if not html:
            return None
        text = self._extract_text(html)
        if len(text) < 120:
            return None
        headline = (row.get("headline") or "").strip()
        return {
            "opinion_number": self._opinion_number(headline),
            "headline": headline,
            "text": text,
            "date": self._parse_date(row.get("dateString", "")),
            "categories": row.get("categoryStrings") or [],
            "url": full_url,
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NC State Bar Legal Ethics Opinions...")
        rows = list(self._islice(self._list_opinions(), 5))
        if not rows:
            logger.error("API test FAILED: no opinions listed")
            return False
        logger.info(f"  listed {len(rows)} opinion rows (first page sample)")
        ok = 0
        for row in rows:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['opinion_number']} OK "
                            f"({len(rec['text'])} chars) date={rec['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    @staticmethod
    def _islice(it, n):
        out = []
        for x in it:
            out.append(x)
            if len(out) >= n:
                break
        return out

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw["opinion_number"]
        slug = re.sub(r"[^A-Za-z0-9]+", "-", num).strip("-")
        return {
            "_id": f"US/NC-LegalEthics/{slug}",
            "_source": "US/NC-LegalEthics",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "issuer": "North Carolina State Bar (Ethics Committee)",
            "title": raw.get("headline") or num,
            "text": raw["text"],
            "categories": raw.get("categories") or [],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NC",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._list_opinions():
            rec = self._fetch_one(row)
            if not rec:
                logger.warning(f"  no text for {row.get('url')}, skipping")
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

    parser = argparse.ArgumentParser(description="US/NC-LegalEthics bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCLegalEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
