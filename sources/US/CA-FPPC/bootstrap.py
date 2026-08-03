#!/usr/bin/env python3
"""
US/CA-FPPC -- California Fair Political Practices Commission --
Advice Letters and Commission Opinions.

Fetches the full text of the two published classes of the FPPC's written
interpretations of the Political Reform Act (Cal. Gov. Code sec. 81000 et seq.),
both of which are the Commission's authoritative construction of the Act =
doctrine:

  - Advice Letter    -- the FPPC's written response to a specific request for
    advice under Gov. Code sec. 83114. Formal ("A-") advice letters provide
    immunity; informal ("I-") advice letters provide guidance. ~13,000+ letters
    spanning the 1970s to present.
  - Commission Opinion -- a formal opinion adopted by the Commission under
    Gov. Code sec. 83114(a) interpreting a provision of the Act. ~150 opinions.

Access (no JavaScript, no CAPTCHA, no auth):
  The FPPC site (Optimizely/EPiServer) exposes a faceted search that renders
  server-side. Each publication type is enumerated by paginating

      https://www.fppc.ca.gov/search/?FacetPublicationType={TYPE}
             &FacetPageType=Document&page={N}&sortBy=relevance

  where {TYPE} is "Advice+Letters" or "Commission+Opinions". Each result card

      <a href="{pdf}"><h3><span class="heading__main">{Title}</span></h3></a>

  links a born-digital PDF (clean text layer, no OCR for the modern corpus)
  under /siteassets/documents/... and carries a title of the form
  "{Requester} - {File No} - {Month DD, YYYY}".

Strategy:
  For each publication type, walk the search pages until they run dry, collect
  every result card (pdf url + title), dedup by pdf path, download each PDF and
  extract full text via the shared common.pdf_extract backend chain. The file
  number and date are parsed from the card title (falling back to the PDF body).
  All records are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all documents)
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
from urllib.parse import unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-FPPC")

BASE_URL = "https://www.fppc.ca.gov"
SEARCH_URL = (
    "https://www.fppc.ca.gov/search/?FacetPublicationType={ftype}"
    "&FacetPageType=Document&page={page}&sortBy=relevance"
)

# The two publication types, with the label used in the normalized record.
PUB_TYPES = [
    ("Advice+Letters", "Advice Letter"),
    ("Commission+Opinions", "Commission Opinion"),
]

# Stop paginating after this many consecutive empty result pages.
EMPTY_PAGE_LIMIT = 3
# Hard ceiling on pages per facet (the site caps deep pagination well below this).
MAX_PAGES = 2000

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

PDF_HREF_RE = re.compile(r"/siteassets/[^\"']+\.pdf", re.I)
# FPPC file numbers: A-88-060 (formal advice), I-99-123 (informal), plus the
# opinion numbers "No. 75-173" / "75-173".
FILENO_RE = re.compile(r"\b([AIO]-\d{2,4}-\d{1,4}|\d{2,4}-\d{1,4})\b")

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
MONTH_IDX = {m: i + 1 for i, m in enumerate(MONTHS)}
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


def _clean(text: str) -> str:
    text = (text or "").replace("\xa0", " ").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", text).strip()


def _iso_from_text(text: str, prefer_last: bool = False) -> str | None:
    matches = DATE_RE.findall(text or "")
    if not matches:
        return None
    mo_name, d, y = matches[-1 if prefer_last else 0]
    mo = MONTH_IDX[mo_name]
    d = int(d)
    if 1 <= d <= 31:
        return f"{int(y):04d}-{mo:02d}-{d:02d}"
    return None


def _slug_from_href(href: str) -> str:
    stem = unquote(href).rsplit("/", 1)[-1]
    stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
    return stem[:180] or "document"


class CAFPPCScraper(BaseScraper):

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
    def _parse_page(self, ftype: str, page: int) -> list[dict]:
        """Parse one search-results page into result-card dicts."""
        r = self._get(SEARCH_URL.format(ftype=ftype, page=page))
        if r is None or r.status_code != 200:
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse search page")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: list[dict] = []
        for a in soup.find_all("a", href=PDF_HREF_RE):
            href = a.get("href", "")
            # Title lives in the nested span.heading__main (fall back to link text).
            span = a.find("span", class_="heading__main")
            title = _clean(span.get_text(" ", strip=True) if span
                           else a.get_text(" ", strip=True))
            if not title:
                continue
            if href.startswith("/"):
                href = BASE_URL + href
            rows.append({"url": href, "title": title})
        return rows

    def _iter_index(self, seen: set) -> Generator[dict, None, None]:
        """Lazily walk both facets' search pages, yielding new card dicts.

        Discovery is interleaved with fetching (the caller downloads each PDF as
        it is discovered), so sample mode can stop after a handful of records
        instead of building the full ~13k-doc index up front.
        """
        collected = 0
        for ftype, label in PUB_TYPES:
            empty = 0
            for page in range(1, MAX_PAGES + 1):
                rows = self._parse_page(ftype, page)
                if not rows:
                    empty += 1
                    if empty >= EMPTY_PAGE_LIMIT:
                        break
                    continue
                empty = 0
                for row in rows:
                    key = unquote(row["url"]).lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    fm = FILENO_RE.search(row["title"])
                    collected += 1
                    yield {
                        "url": row["url"],
                        "title": row["title"],
                        "slug": _slug_from_href(row["url"]),
                        "fileno": fm.group(1) if fm else None,
                        "doc_type": label,
                        "date": _iso_from_text(row["title"]),
                    }
                if page % 25 == 0:
                    logger.info(f"{label}: page {page}, {collected} discovered")

    def _fetch_one(self, row: dict) -> dict | None:
        r = self._get(row["url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row['slug']}: not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row['slug']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        # Prefer the title date; else the first date in the letter body.
        out["date"] = row.get("date") or _iso_from_text(text)
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen: set = set()
        for row in self._iter_index(seen):
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['slug']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']}, {rec['doc_type']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing California FPPC advice letters + opinions...")
        rows = self._parse_page("Advice+Letters", 1)
        if len(rows) < 5:
            logger.error(f"API test FAILED: page 1 too small ({len(rows)})")
            return False
        ok = 0
        for row in rows[:4]:
            key = unquote(row["url"]).lower()
            fm = FILENO_RE.search(row["title"])
            enriched = {
                "url": row["url"], "title": row["title"],
                "slug": _slug_from_href(row["url"]),
                "fileno": fm.group(1) if fm else None,
                "doc_type": "Advice Letter",
                "date": _iso_from_text(row["title"]),
            }
            rec = self._fetch_one(enriched)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['slug']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/CA-FPPC/{raw['slug']}",
            "_source": "US/CA-FPPC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "file_number": raw.get("fileno"),
            "document_type": raw.get("doc_type"),
            "issuer": "California Fair Political Practices Commission",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date"),
            "jurisdiction": "US-CA",
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

    parser = argparse.ArgumentParser(description="US/CA-FPPC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAFPPCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
