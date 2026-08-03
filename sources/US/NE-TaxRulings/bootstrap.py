#!/usr/bin/env python3
"""
US/NE-TaxRulings -- Nebraska Department of Revenue
(Revenue Rulings issued by the Tax Commissioner + General Information Letters)

Fetches the full text of the interpretive written guidance the Nebraska
Department of Revenue (NDR) publishes:

  * Revenue Rulings (NN-NN-N)  -- formal interpretations issued by the Tax
    Commissioner; a taxpayer that follows a current ruling has a "safe
    harbor" from tax, penalty and interest on the issue addressed.
  * General Information Letters (GIL NN-NN-N) -- less formal written
    statements of the Department's position on a topic.

Both are official state-government interpretive guidance, not adjudications
of a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  Two server-rendered listing pages, each a Drupal table
  (<table class="table tablefield">) with the columns
  Number | Tax Type | Title/Topic | Date | Document:

      /about/legal-information/revenue-rulings-issued-tax-commissioner
      /about/legal-information/general-information-letters-gils

  The Document cell links to a born-digital PDF on the same host
  (/sites/default/files/doc/legal/rulings/<file>.pdf). Full text lives only
  in the PDF, so PDF extraction is mandatory.

Strategy:
  1. Fetch each listing page; parse every table row into
     (number, tax_type, title, date, pdf_url). Identify the date cell by an
     MM/DD/YYYY pattern; the number is the first cell; the document cell is
     the one holding the PDF anchor.
  2. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
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
import html as _htmllib
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NE-TaxRulings")

BASE_URL = "https://revenue.nebraska.gov"
INDEX_PAGES = [
    ("Revenue Ruling",
     "/about/legal-information/revenue-rulings-issued-tax-commissioner"),
    ("General Information Letter",
     "/about/legal-information/general-information-letters-gils"),
]

MIN_TEXT_CHARS = 200

TAG_RE = re.compile(r"<[^>]+>")
# A content table on the listing page (skips nav menus).
TABLE_RE = re.compile(
    r'<table[^>]*class="[^"]*tablefield[^"]*"[^>]*>(.*?)</table>', re.S | re.I
)
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
PDF_ANCHOR_RE = re.compile(r'<a\s+[^>]*href="([^"]+?\.pdf)"', re.S | re.I)
# MM/DD/YYYY date in the "Date Issued" / "Date of Letter" cell.
DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(fragment: str) -> str:
    return re.sub(r"\s+", " ", _htmllib.unescape(TAG_RE.sub(" ", fragment))).strip()


def _parse_date(cell: str) -> str | None:
    m = DATE_RE.search(cell)
    if not m:
        return None
    mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1960 <= yyyy <= 2100 and 1 <= mm <= 12 and 1 <= dd <= 31:
        return f"{yyyy}-{mm:02d}-{dd:02d}"
    return None


class NETaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

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

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- parsing -------------------------------------------------------

    @staticmethod
    def _slug(url: str) -> str:
        base = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
        base = urllib.parse.unquote(base).rsplit(".", 1)[0]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:90]

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield document descriptors from both NDR listing pages."""
        seen: set[str] = set()
        total = 0
        for doc_type, path in INDEX_PAGES:
            url = f"{BASE_URL}{path}"
            html = self._get(url)
            if not html:
                logger.error(f"Failed to fetch listing: {url}")
                continue
            page_count = 0
            for table in TABLE_RE.findall(html):
                for row in TR_RE.findall(table):
                    anchor_m = PDF_ANCHOR_RE.search(row)
                    if not anchor_m:
                        continue  # header row or no document
                    pdf_href = _htmllib.unescape(anchor_m.group(1)).strip()
                    pdf_url = urllib.parse.urljoin(BASE_URL,
                                                   pdf_href.replace(" ", "%20"))
                    if pdf_url in seen:
                        continue
                    seen.add(pdf_url)

                    cells = [_strip_tags(c) for c in TD_RE.findall(row)]
                    number = cells[0].strip() if cells else ""
                    # Date = the cell that parses as MM/DD/YYYY.
                    date = None
                    date_idx = None
                    for i, c in enumerate(cells):
                        d = _parse_date(c)
                        if d:
                            date, date_idx = d, i
                            break
                    tax_type = cells[1].strip() if len(cells) > 1 else None
                    # Title = the longest remaining cell that is not the
                    # number, tax-type, date or the "PDF" document label.
                    title = ""
                    for i, c in enumerate(cells):
                        if i in (0, date_idx):
                            continue
                        if c.strip().upper() in ("PDF", "DOCUMENT", ""):
                            continue
                        if i == 1 and len(c) < 40:
                            continue  # tax-type column
                        if len(c) > len(title):
                            title = c.strip()

                    page_count += 1
                    total += 1
                    yield {
                        "pdf_url": pdf_url,
                        "number": number or None,
                        "doc_type": doc_type,
                        "tax_type": tax_type or None,
                        "title": title or None,
                        "date": date,
                        "slug": self._slug(pdf_url),
                    }
            logger.info(f"  {doc_type}: discovered {page_count} documents")
        logger.info(f"Discovered {total} NDR documents with PDFs")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NE-TaxRulings",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Nebraska DOR Revenue Rulings & GILs...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number')}")
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
        number = (raw.get("number") or "").strip()
        doc_type = raw.get("doc_type") or "Revenue Ruling"
        subject = (raw.get("title") or "").strip()
        if number and subject:
            title = f"Nebraska DOR {doc_type} {number}: {subject}"
        elif number:
            title = f"Nebraska DOR {doc_type} {number}"
        elif subject:
            title = f"Nebraska DOR {doc_type}: {subject}"
        else:
            title = f"Nebraska DOR {doc_type}"
        title = title[:300]
        return {
            "_id": f"US/NE-TaxRulings/{raw['slug']}",
            "_source": "US/NE-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number or None,
            "ruling_type": doc_type,
            "tax_type": raw.get("tax_type") or None,
            "issuer": "Nebraska Department of Revenue",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NE",
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

    parser = argparse.ArgumentParser(description="US/NE-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NETaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
