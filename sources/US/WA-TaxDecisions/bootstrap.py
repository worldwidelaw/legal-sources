#!/usr/bin/env python3
"""
US/WA-TaxDecisions -- Washington Department of Revenue
   Washington Tax Decisions (WTD)

Fetches the full text of the Washington State Department of Revenue's
appeals-division Determinations, published as Washington Tax Decisions
(WTD) under RCW 82.32.410. These are the Department's published
quasi-adjudicative rulings on taxpayer appeals — formal interpretive
guidance applying Washington's excise tax law (B&O tax, retail sales
tax, use tax, etc.) to contested facts.

Each decision carries a citation of the form "<vol> WTD <page>" (e.g.
"45 WTD 080"). The official index at /washington-tax-decisions is a set
of server-rendered HTML tables, one block per volume, each row being:

    [Title (Det. No. + citation + year), Date, Document (PDF link),
     Description (one-line summary)]

linking a born-digital text-layer PDF at a deterministic path
/sites/default/files/<yyyy-mm>/<vol>WTD<page>.pdf. No JavaScript, no
CAPTCHA, no auth. Public domain Washington state government works.

This source is distinct from US/WA-BTA (the independent Board of Tax
Appeals) — WTDs are the DOR's OWN appeals-division determinations.

Strategy:
  1. Fetch the index page; parse every table row into
     (title, citation, date, pdf_url, description).
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Normalize into the standard doctrine schema.

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
import subprocess
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WA-TaxDecisions")

BASE_URL = "https://dor.wa.gov"
INDEX_PATH = "/washington-tax-decisions"

ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
HREF_PDF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")
# ISO datetime in the <time datetime="..."> element of the date cell.
TIME_ATTR_RE = re.compile(r'datetime="(\d{4})-(\d{2})-(\d{2})')
# Fallback: MM/DD/YYYY rendered text.
MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
# Citation "<vol> WTD <page>" anywhere in the title cell.
CITE_RE = re.compile(r"(\d{1,3})\s*WTD\s*(\d{1,4})", re.I)


class WATaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _clean(s: str | None) -> str:
        if not s:
            return ""
        s = TAG_RE.sub(" ", s)
        s = (s.replace("&amp;", "&").replace("&#039;", "'")
              .replace("&#39;", "'").replace("&#8217;", "'")
              .replace("&#8211;", "-").replace("&#8212;", "-")
              .replace("&quot;", '"').replace("&nbsp;", " "))
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _date_from_cell(raw_cell: str) -> str | None:
        m = TIME_ATTR_RE.search(raw_cell)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1980 <= y <= 2035 and 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        m = MDY_RE.search(raw_cell)
        if m:
            mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1980 <= y <= 2035 and 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug(pdf_url: str) -> str:
        name = pdf_url.rstrip("/").split("/")[-1]
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return name[:180]

    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        url = urllib.parse.urljoin(BASE_URL, INDEX_PATH)
        html = self._curl_bytes(url)
        if not html:
            logger.error(f"Failed to fetch index {url}")
            return out
        html = html.decode("utf-8", "replace")
        for rm in ROW_RE.finditer(html):
            row = rm.group(1)
            hm = HREF_PDF_RE.search(row)
            if not hm:
                continue
            pdf_url = urllib.parse.urljoin(url, hm.group(1))
            if pdf_url in seen:
                continue
            # Split the row into individual cells (keep raw for date attr).
            raw_cells = CELL_RE.findall(row)
            cells = [self._clean(c) for c in raw_cells]
            # Title cell = first cell that contains a WTD citation;
            # else the first non-empty cell.
            title = None
            citation = None
            for c in cells:
                cm = CITE_RE.search(c)
                if cm and title is None:
                    title = c
                    citation = f"{int(cm.group(1))} WTD {cm.group(2)}"
                    break
            if title is None:
                non_empty = [c for c in cells if c]
                title = non_empty[0] if non_empty else None
            # Date: scan raw cells for a datetime attr / MM/DD/YYYY.
            date = None
            for rc in raw_cells:
                date = self._date_from_cell(rc)
                if date:
                    break
            # Description: longest cell that is not the title and has no PDF link.
            description = ""
            for c in cells:
                if c and c != title and not CITE_RE.search(c) and len(c) > len(description):
                    if not self._date_from_cell(c):
                        description = c
            seen.add(pdf_url)
            out.append({
                "title": title or citation or self._slug(pdf_url),
                "citation": citation,
                "date": date,
                "pdf_url": pdf_url,
                "description": description,
                "slug": self._slug(pdf_url),
            })
            if sample and len(out) >= 16:
                break
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Washington Tax Decisions")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/WA-TaxDecisions", doc["slug"], pdf_bytes=pdf_bytes,
            table="legislation", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing WA DOR Washington Tax Decisions index + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        citation = raw.get("citation")
        title = (raw.get("title") or "").strip()
        if not title:
            title = citation or "Washington Tax Decision"
        title = title[:300]
        return {
            "_id": f"US/WA-TaxDecisions/{raw['slug']}",
            "_source": "US/WA-TaxDecisions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "citation": citation,
            "issuer": "Washington State Department of Revenue",
            "title": title,
            "description": raw.get("description") or None,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WA",
        }

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

    parser = argparse.ArgumentParser(description="US/WA-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WATaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
