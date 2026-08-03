#!/usr/bin/env python3
"""
US/WA-PERC -- Washington State Public Employment Relations Commission (PERC)

Fetches the full text of every published decision of the Washington State
Public Employment Relations Commission (PERC), the state's quasi-judicial
agency that adjudicates public-sector labor-relations disputes -- unfair
labor practice complaints, representation/unit-determination cases, interest
arbitration, and related matters under Washington's public-employee
collective-bargaining statutes (RCW 41.56, 41.58, 41.59, 41.76, 41.80, the
PSRA, etc.). Each decision resolves a specific contested case = case_law.
These are official Washington state-government works in the public domain
(government edicts).

The corpus is served by Lexum's "Decisia" platform at
https://decisions.perc.wa.gov/ across five collections:
  - decisions                              (Commission + Examiner decisions)
  - advisory-opinions
  - interest-arbritations                  (sic -- their spelling)
  - law-enforcement-disciplinary-arbitration
  - marine-employees-commission

Access (no CAPTCHA, no auth):
  Each collection browses by year:
      /waperc/{collection}/en/{YYYY}/nav_date.do?page={N}&iframe=true
  paging (25 items/page) yields item links of the form
      /waperc/{collection}/en/item/{id}/index.do
  The clean, un-wrapped decision page is at
      /waperc/{collection}/en/item/{id}/index.do?iframe=true
  which renders a metadata table (Collection, Date, Case Number, Decision
  Maker, Case Type, Statute) followed by the full decision text inside
  <div id="document-content"> ... <div class="documentcontent"> ... </div>.
  (Without ?iframe=true the same page is wrapped in the perc.wa.gov
  WordPress theme; the iframe view is the raw Decisia content.)

Strategy:
  1. For each collection, read /{collection}/en/nav_date.do?iframe=true to
     discover the list of available years.
  2. For each year, page through nav_date.do collecting item URLs.
  3. For each item, fetch ?iframe=true, parse the metadata table and the
     documentcontent body (clean HTML -> plain text), normalize to case_law.

Usage:
  python bootstrap.py bootstrap            # Full pull (~10,000 decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-PERC")

BASE_URL = "https://decisions.perc.wa.gov"
COLLECTIONS = [
    "decisions",
    "advisory-opinions",
    "interest-arbritations",
    "law-enforcement-disciplinary-arbitration",
    "marine-employees-commission",
]

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"[ \t\r\f\v]+")
NL_RE = re.compile(r"\n{3,}")
ITEM_RE = re.compile(r"/waperc/([^/]+)/en/item/(\d+)/index\.do")
YEAR_RE = re.compile(r"/waperc/[^/]+/en/(\d{4})/nav_date\.do")
# metadata cell: <td class="label">Date</td><td class="metadata"> VALUE </td|</tr>
META_RE = re.compile(
    r'<td class="label">\s*([^<]+?)\s*</td>\s*'
    r'<td class="metadata">\s*(.*?)\s*(?:</td>|</tr>)',
    re.S | re.I,
)
DOCCONTENT_RE = re.compile(
    r'<div[^>]*id="document-content"[^>]*>(.*?)(?:<div id="document-footer"|</body>|\Z)',
    re.S | re.I,
)
US_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


class WAPERCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
    def _get(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua,
                     "-H", "Accept: text/html,*/*", url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @classmethod
    def _clean_text(cls, fragment: str) -> str:
        """Convert an HTML fragment to readable plain text."""
        # preserve paragraph / line breaks
        frag = re.sub(r"(?i)</p\s*>", "\n\n", fragment)
        frag = re.sub(r"(?i)<br\s*/?>", "\n", frag)
        frag = re.sub(r"(?i)</div\s*>", "\n", frag)
        frag = re.sub(r"(?i)</tr\s*>", "\n", frag)
        txt = TAG_RE.sub(" ", frag)
        txt = _html.unescape(txt)
        # collapse spaces, tidy blank lines
        lines = [WS_RE.sub(" ", ln).strip() for ln in txt.split("\n")]
        txt = "\n".join(lines)
        txt = NL_RE.sub("\n\n", txt)
        return txt.strip()

    @classmethod
    def _inline(cls, fragment: str) -> str:
        return WS_RE.sub(" ", _html.unescape(TAG_RE.sub(" ", fragment))).strip()

    @staticmethod
    def _iso_date(us: str | None) -> str | None:
        if not us:
            return None
        m = US_DATE_RE.search(us)
        if not m:
            return None
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31 and 1900 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    # --------------------------------------------------------- discovery
    def _years_for(self, collection: str) -> list[str]:
        html = self._get(
            f"{BASE_URL}/waperc/{collection}/en/nav_date.do?iframe=true"
        )
        if not html:
            return []
        years = sorted({m for m in YEAR_RE.findall(html)})
        return years

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total = 0
        for collection in COLLECTIONS:
            years = self._years_for(collection)
            if not years:
                # collection may have no year sub-browse; fall back to the
                # bare (recent) date listing
                years = [""]
            for year in years:
                page = 1
                while True:
                    if year:
                        url = (f"{BASE_URL}/waperc/{collection}/en/{year}/"
                               f"nav_date.do?page={page}&iframe=true")
                    else:
                        url = (f"{BASE_URL}/waperc/{collection}/en/"
                               f"nav_date.do?page={page}&iframe=true")
                    html = self._get(url)
                    if not html:
                        break
                    found = 0
                    for m in ITEM_RE.finditer(html):
                        coll, item_id = m.group(1), m.group(2)
                        if item_id in seen:
                            continue
                        seen.add(item_id)
                        total += 1
                        found += 1
                        yield {"collection": coll, "item_id": item_id,
                               "url": f"{BASE_URL}/waperc/{coll}/en/item/"
                                      f"{item_id}/index.do"}
                        if sample and total >= 16:
                            logger.info(f"Sample cap reached ({total})")
                            return
                    if found == 0:
                        break
                    page += 1
                    if page > 200:  # safety
                        logger.warning(f"Page cap for {collection}/{year}")
                        break
                logger.info(f"  {collection}/{year or 'recent'}: cumulative {total}")
        logger.info(f"Discovered {total} WA PERC decisions")

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        html = self._get(doc["url"] + "?iframe=true")
        if not html:
            logger.warning(f"Fetch failed: {doc['url']}")
            return None

        meta = {}
        for m in META_RE.finditer(html):
            label = self._inline(m.group(1)).rstrip(":")
            value = self._inline(m.group(2))
            if label:
                meta[label.lower()] = value

        body_m = DOCCONTENT_RE.search(html)
        text = ""
        if body_m:
            text = self._clean_text(body_m.group(1))
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['url']} "
                           f"({len(text) if text else 0} chars)")
            return None

        # title from the <title> tag or the metadata heading
        title = None
        tm = re.search(r"<title>\s*(.*?)\s*</title>", html, re.S | re.I)
        if tm:
            title = self._inline(tm.group(1))
            title = re.sub(r"\s*-\s*Washington State Public Employment.*$", "",
                           title, flags=re.I).strip()
        if not title:
            title = f"WA PERC decision {doc['item_id']}"

        raw = dict(doc)
        raw["title"] = title
        raw["text"] = text.strip()
        raw["case_number"] = meta.get("case number")
        raw["decision_maker"] = meta.get("decision maker")
        raw["case_type"] = meta.get("case type")
        raw["statute"] = meta.get("statute")
        raw["collection_name"] = meta.get("collection")
        raw["date"] = self._iso_date(meta.get("date"))
        return raw

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WA PERC Decisia enumeration + text extraction...")
        try:
            docs = list(self.discover_documents(sample=True))
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} items (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} [{raw.get('date')}]")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/WA-PERC/{raw['collection']}-{raw['item_id']}",
            "_source": "US/WA-PERC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "item_id": raw["item_id"],
            "collection": raw.get("collection_name") or raw["collection"],
            "case_number": raw.get("case_number"),
            "decision_maker": raw.get("decision_maker"),
            "case_type": raw.get("case_type"),
            "statute": raw.get("statute"),
            "issuer": "Washington State Public Employment Relations Commission (PERC)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
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

    parser = argparse.ArgumentParser(description="US/WA-PERC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WAPERCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
