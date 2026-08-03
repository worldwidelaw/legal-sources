#!/usr/bin/env python3
"""
US/PA-PLRB -- Pennsylvania Labor Relations Board (PLRB) Final & Proposed Orders

Fetches the full text of every published order of the Pennsylvania Labor
Relations Board (PLRB), the state agency (within the Department of Labor &
Industry) that adjudicates public- and private-sector labor-relations disputes
under the Pennsylvania Public Employe Relations Act (PERA / Act 195), the
Police and Firemen Collective Bargaining Act (Act 111), and the Pennsylvania
Labor Relations Act (PLRA).  The Board decides unfair-labor-practice charges,
representation / certification petitions, and unit-determination cases; a
hearing examiner issues a Proposed Decision and Order, which the Board then
adopts or modifies in a Final Order.  Each order resolves a specific contested
case = case_law, and they are official Pennsylvania state-government works in
the public domain (government edicts).

BUILD RECIPE (builds + validates LOCALLY, no CAPTCHA / JS / auth):
The Commonwealth migrated PLRB to a modern AEM site (www.pa.gov).  Orders are
published on per-year pages under two sections:

  Final Orders     /agencies/dli/.../pennsylvania-labor-relations-board/
                       plrb-final-orders/{YEAR}-...
  Proposed Orders  /agencies/dli/.../pennsylvania-labor-relations-board/
                       plrb-proposed-orders/{YEAR}-...

The per-year URL SLUGS ARE INCONSISTENT (e.g. '2024-final-orders' vs
'2023-plrb-final-orders'; proposed orders 2020-2022 even carry a typo
'{YEAR}-plrb-proposed-urders').  The site's landing pages render their
year-navigation via JavaScript and AEM `.model.json` is blocked, so we
enumerate the year pages from the public XML sitemap instead:

  https://www.pa.gov/en.sitemap.xml   (filter to .../plrb-final-orders/{YEAR}-*
                                        and .../plrb-proposed-orders/{YEAR}-*)

Each year page server-renders <a href> links to the born-digital decision PDFs
under /content/dam/copapwp-pagov/.../plrb/{final|proposed}-orders/{YEAR}/
documents/{slug}.pdf .  Full text is extracted with common.pdf_extract; the
case number and decision date are parsed from the order body (the caption also
appears in the PDF).  No auth, no CAPTCHA.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.PA-PLRB")

HOST = "https://www.pa.gov"
SITEMAP = "https://www.pa.gov/en.sitemap.xml"
PLRB_BASE = ("/agencies/dli/programs-services/labor-management-relations/"
             "pennsylvania-labor-relations-board")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# Year pages for the two order sections (slug after the section is variable).
YEAR_PAGE_RE = re.compile(
    re.escape(PLRB_BASE) +
    r"/plrb-(?:final|proposed)-orders/(\d{4})-[a-z0-9-]+", re.IGNORECASE)
# Decision PDFs under the DAM (relative or absolute).
PDF_HREF_RE = re.compile(
    r'href="((?:https://www\.pa\.gov)?/content/dam/[^"]*?plrb/[^"]*?\.pdf[^"]*)"',
    re.IGNORECASE)
CASE_RE = re.compile(
    r"Case\s+Nos?\.?\s*[:\s]*([A-Z]{1,4}-[A-Z]-\d{1,3}-\d{1,4}-[A-Z]"
    r"(?:\s*,?\s*[A-Z]{1,4}-[A-Z]-\d{1,3}-\d{1,4}-[A-Z])*)", re.IGNORECASE)
LONGDATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},\s+(?:19|20)\d{2}\b")

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


class PLRBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.5
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120))
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _iso_from_longdate(s: str) -> str | None:
        m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", s.strip())
        if not m:
            return None
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        try:
            d, y = int(m.group(2)), int(m.group(3))
        except ValueError:
            return None
        if 1 <= d <= 31 and 1970 <= y <= 2100:
            return f"{y:04d}-{mon:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug_from_url(pdf_url: str) -> str:
        """Stable id from the PDF path: {section}-{year}-{filename}."""
        path = re.sub(r"^https?://[^/]+", "", pdf_url).split("?", 1)[0]
        m = re.search(r"/(final|proposed)-orders/(\d{4})/documents/(.+?)\.pdf$",
                      path, re.IGNORECASE)
        if m:
            base = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        else:
            base = re.sub(r"(?i)\.pdf$", "", path.rsplit("/", 1)[-1])
        slug = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-").lower()
        return slug or re.sub(r"[^A-Za-z0-9]+", "-", path).strip("-").lower()

    # --------------------------------------------------------- discovery
    def _year_pages(self) -> list[str]:
        xml = self._get_text(SITEMAP)
        if not xml:
            return []
        pages: list[str] = []
        seen: set[str] = set()
        for m in YEAR_PAGE_RE.finditer(xml):
            path = m.group(0)
            if path not in seen:
                seen.add(path)
                pages.append(path)
        pages.sort(reverse=True)  # newest years first
        logger.info(f"Sitemap: {len(pages)} PLRB order year-pages")
        return pages

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        seen_pdfs: set[str] = set()
        found = 0
        for page_path in self._year_pages():
            section = "proposed" if "proposed-orders" in page_path else "final"
            yr_m = re.search(r"/(\d{4})-", page_path)
            year = yr_m.group(1) if yr_m else None
            html = self._get_text(HOST + page_path)
            if not html:
                continue
            n_page = 0
            for href in PDF_HREF_RE.findall(html):
                pdf_url = href if href.startswith("http") else HOST + href
                if pdf_url in seen_pdfs:
                    continue
                seen_pdfs.add(pdf_url)
                yield {"pdf_url": pdf_url, "section": section, "year": year,
                       "source_page": HOST + page_path}
                found += 1
                n_page += 1
                if sample and found >= 20:
                    logger.info(f"Sample: stopped after {found} PDF pointers")
                    return
            if n_page:
                logger.info(f"{page_path.rsplit('/',1)[-1]}: {n_page} orders")
        logger.info(f"Discovered {len(seen_pdfs)} PLRB order PDFs")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        pdf_url = entry["pdf_url"]
        source_id = self._slug_from_url(pdf_url)
        if source_id in self._existing:
            return None
        # DAM paths may contain encoded spaces already; re-quote safely.
        fetch_url = quote(pdf_url, safe=":/?&=%")
        pdf_bytes = self._get_bytes(fetch_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/PA-PLRB", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {pdf_url.rsplit('/',1)[-1]} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()

        case_number = None
        m = CASE_RE.search(text)
        if m:
            case_number = re.sub(r"\s+", " ", m.group(1)).strip(" .,")[:80]

        date = None
        spans = [mm.group(0) for mm in LONGDATE_RE.finditer(text)]
        if spans:
            date = self._iso_from_longdate(spans[-1])  # signature/issue date

        fname = pdf_url.rsplit("/", 1)[-1]
        fname = re.sub(r"(?i)\.pdf.*$", "", fname)
        pretty = re.sub(r"[-_]+", " ", fname).strip().title()
        section_label = ("Proposed Decision and Order"
                         if entry["section"] == "proposed" else "Final Order")
        if case_number:
            title = f"PLRB {section_label} — {pretty} ({case_number})"
        else:
            title = f"PLRB {section_label} — {pretty}"

        return {
            "record_id": source_id,
            "case_number": case_number,
            "section": entry["section"],
            "year": entry.get("year"),
            "title": _html.unescape(title)[:500],
            "text": text,
            "date": date,
            "url": pdf_url,
            "source_page": entry.get("source_page"),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Pennsylvania PLRB order pages (via sitemap)...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No order pointers discovered")
                return False
            logger.info(f"  Discovered {len(entries)} pointers (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} [{raw.get('date')}]")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/PA-PLRB/{raw['record_id']}",
            "_source": "US/PA-PLRB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "case_number": raw.get("case_number") or None,
            "issuer": "Pennsylvania Labor Relations Board (PLRB)",
            "order_type": ("Proposed Decision and Order"
                           if raw.get("section") == "proposed"
                           else "Final Order"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-PA",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/PA-PLRB", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
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

    parser = argparse.ArgumentParser(description="US/PA-PLRB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PLRBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
