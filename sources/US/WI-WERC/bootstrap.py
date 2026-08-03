#!/usr/bin/env python3
"""
US/WI-WERC -- Wisconsin Employment Relations Commission (WERC) Decisions

Fetches the full text of the labor-relations decisions of the Wisconsin
Employment Relations Commission (WERC), the state's quasi-judicial agency
that administers Wisconsin's public- and private-sector labor-relations
statutes (the Municipal Employment Relations Act, the State Employment
Labor Relations Act, and the Wisconsin Employment Peace Act). The
Commission decides representation and election petitions, unit
clarifications, prohibited-practice (unfair-labor-practice) complaints,
declaratory rulings, and related contested cases. Each decision resolves a
specific case = case_law and is an official Wisconsin state-government work
in the public domain (government edicts).

BUILD RECIPE (no auth, no CAPTCHA): the Commission's decisions are indexed
by date-range on a set of HTML index pages linked from
https://werc.wi.gov/labor-relations-decisions-2/ :

  https://werc.wi.gov/DOAroot/decisions_july-89_dec-98.htm
  https://werc.wi.gov/DOAroot/decisions_1999.htm
  ... decisions_pdf_2014_on.htm   (July 1989 - present)

Each index page is an HTML table, one row per decision, with columns:
  Date Issued | Decision Docket Identification | Decision Type |
  Decision Author | PDF Filename

The "PDF Filename" cell (e.g. ``41400-A.pdf``) names a born-digital decision
PDF served at ``https://werc.wi.gov/decisions/{filename}``. The scraper reads
each index table, downloads each PDF, and extracts full text with the shared
``common.pdf_extract`` extractor (older scans go through OCR). Date, docket
caption, decision type and author come from the index row.

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
logger = logging.getLogger("legal-data-hunter.US.WI-WERC")

HOST = "https://werc.wi.gov"
INDEX_LIST_PAGE = HOST + "/labor-relations-decisions-2/"
PDF_DIR = "/decisions/"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# Links to the per-date-range HTML index pages.
INDEX_PAGE_RE = re.compile(
    r'href="(https://werc\.wi\.gov/(?:DOAroot/[^"]+\.htm|decisions/[^"]+\.htm))"',
    re.IGNORECASE)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
PDF_NAME_RE = re.compile(r"^([0-9A-Za-z][\w.-]*\.pdf)$", re.IGNORECASE)
PDF_HREF_RE = re.compile(r'href="([^"]*?/decisions/[^"?]+?\.pdf)"', re.IGNORECASE)
# Date issued: MM/DD/YYYY (modern pages) or MM-DD-YY (older pages).
DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b")


class WERCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "latin-1"
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
    def _clean(cell: str) -> str:
        txt = _html.unescape(TAG_RE.sub(" ", cell)).replace("\xa0", " ")
        return re.sub(r"\s+", " ", txt).strip()

    @classmethod
    def _iso_date(cls, s: str) -> str | None:
        m = DATE_RE.search(s or "")
        if not m:
            return None
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if yy < 100:
            yy += 1900 if yy >= 50 else 2000
        if 1 <= mm <= 12 and 1 <= dd <= 31 and 1970 <= yy <= 2100:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None

    @staticmethod
    def _slug(filename: str) -> str:
        core = re.sub(r"(?i)\.pdf$", "", filename)
        return re.sub(r"[^A-Za-z0-9]+", "-", core).strip("-")

    # --------------------------------------------------------- discovery
    def _index_pages(self) -> list[str]:
        html = self._get_text(INDEX_LIST_PAGE)
        pages: list[str] = []
        if html:
            for u in INDEX_PAGE_RE.findall(html):
                if u not in pages:
                    pages.append(u)
        return pages

    def _rows_from_page(self, html: str) -> Generator[dict, None, None]:
        rows = ROW_RE.findall(html)
        emitted = 0
        for raw_row in rows:
            # PDF filename: either a hyperlink (older pages) or a plain-text
            # "NNNNN-A.pdf" cell (modern pages).
            fname = None
            href_m = PDF_HREF_RE.search(raw_row)
            if href_m:
                fname = href_m.group(1).rsplit("/", 1)[-1]
            cells = [self._clean(c) for c in CELL_RE.findall(raw_row)]
            if not fname:
                for c in cells:
                    if PDF_NAME_RE.match(c):
                        fname = c
                        break
            if not fname:
                continue
            stem = re.sub(r"(?i)\.pdf$", "", fname)
            date = None
            for c in cells:
                iso = self._iso_date(c)
                if iso:
                    date = iso
                    break
            # Caption = longest cell that isn't the filename/stem or a bare date.
            others = [c for c in cells
                      if c and c not in (fname, stem)
                      and not DATE_RE.fullmatch(c)]
            caption = max(others, key=len) if others else ""
            emitted += 1
            yield {"filename": fname, "date": date, "caption": caption}
        if emitted == 0:
            # Fallback: any direct /decisions/*.pdf hrefs on the page.
            for href in PDF_HREF_RE.findall(html):
                fname = href.rsplit("/", 1)[-1]
                yield {"filename": fname, "date": None, "caption": ""}

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        found = 0
        for page_url in self._index_pages():
            html = self._get_text(page_url)
            if not html:
                continue
            n_page = 0
            for rec in self._rows_from_page(html):
                key = rec["filename"].lower()
                if key in seen:
                    continue
                seen.add(key)
                rec["pdf_url"] = HOST + PDF_DIR + quote(rec["filename"])
                rec["source_page"] = page_url
                yield rec
                n_page += 1
                found += 1
                if sample and found >= 24:
                    logger.info(f"Sample: stopped after {found} pointers")
                    return
            if n_page:
                logger.info(f"{page_url.rsplit('/',1)[-1]}: {n_page} decisions")
        logger.info(f"Discovered {len(seen)} WERC decision pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        source_id = self._slug(entry["filename"])
        if source_id in self._existing:
            return None
        pdf_bytes = self._get_bytes(entry["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/WI-WERC", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {entry['filename']} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        caption = entry.get("caption") or ""
        title = caption or f"WERC Decision {source_id}"
        return {
            "record_id": source_id,
            "filename": entry["filename"],
            "title": title[:500],
            "text": text,
            "date": entry.get("date"),
            "url": entry["pdf_url"],
            "source_page": entry.get("source_page"),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Wisconsin WERC decision index...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No decision pointers discovered")
                return False
            logger.info(f"  Discovered {len(entries)} pointers (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} [{raw['date']}]")
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
            "_id": f"US/WI-WERC/{raw['record_id']}",
            "_source": "US/WI-WERC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "Wisconsin Employment Relations Commission (WERC)",
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WI",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/WI-WERC", "case_law")
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

    parser = argparse.ArgumentParser(description="US/WI-WERC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WERCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
