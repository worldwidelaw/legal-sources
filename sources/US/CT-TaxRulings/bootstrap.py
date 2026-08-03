#!/usr/bin/env python3
"""
US/CT-TaxRulings -- Connecticut Department of Revenue Services Rulings

Fetches the full text of the Rulings issued by the Connecticut Department
of Revenue Services (DRS). A DRS Ruling is the Department's written
interpretation of how Connecticut tax law applies to a described set of
facts, published openly by the agency. These are official state-government
interpretive guidance, not adjudications of a contested case, so the
corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  The DRS "Rulings" library on the ct.gov portal is organized into one
  server-rendered index page per year:

      https://portal.ct.gov/DRS/Publications/Rulings/{YEAR}/{YEAR}-Rulings

  Each year index links that year's individual ruling pages
  (.../rulings/{YEAR}/ruling-{NN}-{slug}). Each ruling page renders the
  full ruling text as HTML inside a <div class="content"> block (an <h1>
  caption followed by FACTS / ISSUE / DISCUSSION / RULING paragraphs), so
  the full text is read directly from the page — no PDF, no API key.

Strategy:
  1. Walk each year index (1990-present), collecting every ruling-page URL.
  2. Fetch each ruling page and extract the <div class="content"> body
     (cut at </main>), strip tags, decode entities. A short/"Oops" page
     (retired URL) is skipped.
  3. Normalize into the standard doctrine schema (ruling number + title
     from the <h1>; date from the body or the ruling number's year).

Usage:
  python bootstrap.py bootstrap            # Full pull (all years)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _htmllib
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-TaxRulings")

BASE_URL = "https://portal.ct.gov"
INDEX_PATH = "/DRS/Publications/Rulings/{year}/{year}-Rulings"
FIRST_YEAR = 1990
LAST_YEAR = 2026  # over-estimate is harmless; empty years yield no rows
MIN_TEXT_CHARS = 200

RULING_LINK_RE = re.compile(
    r'href="(https://portal\.ct\.gov/drs/publications/rulings/(\d{4})/ruling-[^"#]+)"',
    re.I,
)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", re.S | re.I)
NUM_RE = re.compile(r"Ruling\s*(?:No\.?\s*)?#?\s*([0-9]{2,4}\s*-\s*[0-9]{1,3})", re.I)
_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
BODY_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\b", re.I
)
OOPS_RE = re.compile(r"no longer here|page (?:no longer exists|has moved)", re.I)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class CTTaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    @staticmethod
    def _slug(url: str) -> str:
        seg = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", seg).strip("-")[:90]

    @staticmethod
    def _norm_number(raw: str | None) -> str | None:
        if not raw:
            return None
        return re.sub(r"\s+", "", raw)

    def _extract_content(self, html: str) -> str:
        start = html.find('<div class="content">')
        if start < 0:
            return ""
        end = html.find("</main>", start)
        seg = html[start:end] if end > start else html[start:]
        seg = SCRIPT_STYLE_RE.sub(" ", seg)
        txt = TAG_RE.sub(" ", seg)
        txt = _htmllib.unescape(txt)
        return clean_text(txt)

    @staticmethod
    def _extract_h1(html: str) -> str:
        m = H1_RE.search(html)
        if not m:
            return ""
        return clean_text(_htmllib.unescape(TAG_RE.sub(" ", m.group(1))))

    def _body_date(self, text: str, year: int | None) -> str | None:
        m = BODY_DATE_RE.search(text)
        if m:
            mon = _MONTHS[m.group(1).lower()]
            day = int(m.group(2))
            yr = int(m.group(3))
            if 1980 <= yr <= 2100 and 1 <= day <= 31:
                return f"{yr}-{mon}-{day:02d}"
        if year:
            return f"{year}-01-01"
        return None

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield ruling-page descriptors discovered across the year indexes."""
        seen: set[str] = set()
        total = 0
        for year in range(LAST_YEAR, FIRST_YEAR - 1, -1):
            url = BASE_URL + INDEX_PATH.format(year=year)
            html = self._get(url)
            if not html:
                continue
            found = 0
            for m in RULING_LINK_RE.finditer(html):
                page_url, yr = m.group(1), int(m.group(2))
                if page_url in seen:
                    continue
                seen.add(page_url)
                total += 1
                found += 1
                yield {
                    "url": page_url,
                    "slug": self._slug(page_url),
                    "year": yr,
                }
            if found:
                logger.info(f"[{year}] {found} rulings (running total {total})")

    def _build_raw(self, doc: dict) -> dict | None:
        html = self._get(doc["url"])
        if not html or OOPS_RE.search(html[:4000]):
            logger.debug(f"Retired/empty ruling page: {doc['url']}")
            return None
        text = self._extract_content(html)
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {doc['slug']}")
            return None
        h1 = self._extract_h1(html)
        nm = NUM_RE.search(h1) or NUM_RE.search(text[:200])
        doc = dict(doc)
        doc["text"] = text
        doc["title_h1"] = h1
        doc["number"] = self._norm_number(nm.group(1)) if nm else None
        doc["date"] = self._body_date(text, doc.get("year"))
        return doc

    def test_api(self) -> bool:
        logger.info("Testing CT DRS Rulings library...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No rulings discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ rulings (partial crawl)")
            raw = None
            for d in docs:
                raw = self._build_raw(d)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number')}: {raw.get('title_h1')[:60]}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw.get("number")
        h1 = (raw.get("title_h1") or "").strip()
        if h1:
            title = h1
        elif number:
            title = f"CT DRS Ruling {number}"
        else:
            title = "CT DRS Ruling"
        title = title[:300]
        return {
            "_id": f"US/CT-TaxRulings/{raw['slug']}",
            "_source": "US/CT-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number,
            "issuer": "Connecticut Department of Revenue Services",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CT",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents():
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 30:
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

    parser = argparse.ArgumentParser(description="US/CT-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTTaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
