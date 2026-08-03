#!/usr/bin/env python3
"""
US/MD-TaxCourt -- Maryland Tax Court (Memoranda of Grounds for Decision)

Fetches the full text of every decision of the Maryland Tax Court — the
highest administrative tribunal in Maryland for tax appeals, hearing
disputes over income tax, property-tax assessments, inheritance/estate
tax, and other state and local taxes (taxpayer v. Comptroller of
Maryland / State Department of Assessments and Taxation / a local
Supervisor of Assessments or Register of Wills). Each "Memorandum of
Grounds for Decision" resolves a specific tax controversy, so the corpus
is case_law.

The court publishes its decisions on one server-rendered HTML page
(taxcourt.maryland.gov/Decisions.shtml). Each list item links the case's
PDF under /PDF/Decisions/ followed by a "Decided MM/DD/YYYY" date. The
PDFs are born-digital text-layer documents — no JavaScript, no CAPTCHA,
no auth.

Strategy:
  1. GET the Decisions page and parse each <li>: PDF href, case-name
     anchor text and the "Decided MM/DD/YYYY" date.
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
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
logger = logging.getLogger("legal-data-hunter.US.MD-TaxCourt")

BASE_URL = "https://taxcourt.maryland.gov"
INDEX_PAGE = "/Decisions.shtml"
TAG_RE = re.compile(r"<[^>]+>")
# Docket prefix in the filename, e.g. 25-IR-OO-0471, 24-RP-MO-0146-0697.
DOCKET_RE = re.compile(r"^(\d{2}-[A-Z]{2}-[A-Z]{2}-\d+(?:-\d+)?)", re.I)
# <a href="PDF/Decisions/...pdf">Case Name</a> ... Decided MM/DD/YYYY
ROW_RE = re.compile(
    r'<a\s+href="((?:/)?PDF/Decisions/[^"]+\.pdf)"[^>]*>(.*?)</a>'
    r'(.{0,200}?Decided\s+(\d{1,2}/\d{1,2}/\d{4}))?',
    re.I | re.S,
)


class MDTaxCourtScraper(BaseScraper):

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
    def _clean(html: str | None) -> str:
        if not html:
            return ""
        txt = TAG_RE.sub(" ", html)
        txt = (txt.replace("&amp;", "&").replace("&nbsp;", " ")
                  .replace("&#39;", "'").replace("&quot;", '"'))
        # Drop the recurring "(archive)" annotation(s).
        txt = re.sub(r"\(archive\)", " ", txt, flags=re.I)
        txt = re.sub(r"\s+", " ", txt).strip(" ,.")
        return txt

    @staticmethod
    def _norm_date(raw: str | None) -> str | None:
        if not raw:
            return None
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw.strip())
        if m:
            mo, d, y = (int(x) for x in m.groups())
            if 1 <= mo <= 12 and 1 <= d <= 31 and 1980 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    @staticmethod
    def _docket(slug: str) -> str | None:
        m = DOCKET_RE.match(slug)
        return m.group(1).upper() if m else None

    def discover_documents(self, sample: bool = False) -> list[dict]:
        html = self._curl_bytes(BASE_URL + INDEX_PAGE)
        if not html:
            logger.error("Failed to fetch Decisions page")
            return []
        html = html.decode("utf-8", "replace")
        out = []
        seen = set()
        for m in ROW_RE.finditer(html):
            href = m.group(1).strip()
            pdf_url = urllib.parse.urljoin(BASE_URL + INDEX_PAGE, href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            case_name = self._clean(m.group(2))
            date = self._norm_date(m.group(4))
            out.append({
                "case_name": case_name,
                "date": date,
                "pdf_url": pdf_url,
                "slug": self._slug(pdf_url),
            })
        # Newest first by decision date (undated last).
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Maryland Tax Court decisions")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/MD-TaxCourt", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing Maryland Tax Court Decisions page + PDF extraction...")
        try:
            docs = self.discover_documents()
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        case_name = (raw.get("case_name") or "").strip()
        title = case_name or "Maryland Tax Court — Memorandum of Grounds for Decision"
        title = title[:300]
        return {
            "_id": f"US/MD-TaxCourt/{raw['slug']}",
            "_source": "US/MD-TaxCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": self._docket(raw["slug"]),
            "court": "Maryland Tax Court",
            "case_name": case_name or None,
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MD",
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

    parser = argparse.ArgumentParser(description="US/MD-TaxCourt bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MDTaxCourtScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    # Use BaseScraper.bootstrap so the full run streams to data/records.jsonl
    # (and writes sample/ in sample mode), instead of only emitting samples.
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
