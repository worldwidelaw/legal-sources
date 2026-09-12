#!/usr/bin/env python3
"""
JO/CBJ-Regulations — Central Bank of Jordan (regulations, instructions, circulars)

Fetches the Central Bank of Jordan's English-language legal corpus: banking
laws, regulatory instructions, circulars to banks, supervisory guidelines, and
payment-systems legislation.

Strategy:
  1. For each English document-library category page
     (https://www.cbj.gov.jo/EN/List/<Category>), parse the listing rows. Each
     row pairs a document title (<label>) with a download link pointing at a
     PDF under /ebv4.0/root_storage/en/.
  2. Download each PDF and extract full text (opendataloader-pdf → pdfplumber →
     pypdf via common.pdf_extract).
  3. Skip records that yield no usable text (scanned image-only PDFs).

Usage:
  python bootstrap.py bootstrap           # Full pull
  python bootstrap.py bootstrap --sample  # Sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JO.CBJ-Regulations")

BASE_URL = "https://www.cbj.gov.jo"
SOURCE_ID = "JO/CBJ-Regulations"

# English document-library category path -> (label, _type, doctrine table)
CATEGORIES = {
    "Laws": ("Laws", "legislation"),
    "Payment_Systems_Legislations": ("Payment Systems Legislation", "legislation"),
    "Instructions_": ("Regulations & Instructions", "doctrine"),
    "Circulars": ("Circulars", "doctrine"),
    "Guidelines_and_Frameworks": ("Guidelines & Frameworks", "doctrine"),
}

# Each listing row: a <label>Title</label> followed (within the row) by an
# anchor with class DownloadLink whose href is the PDF.
_ROW_RE = re.compile(
    r"<label>(?P<title>[^<]+)</label>.*?DownloadLink[^>]*href='(?P<pdf>[^']+\.pdf[^']*)'",
    re.DOTALL | re.IGNORECASE,
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open legal data research)",
    "Accept": "text/html, application/pdf, */*",
}


def _clean_title(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


# Circular/law titles frequently embed a date like "dated 10/2/2026" or
# "No. 44 of 2015". Try to pull an ISO date (or fall back to a year) from the title.
_DATE_RE = re.compile(r"dated\s+(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(?:of|/)\s*(\d{4})\b|\b(\d{4})\b")


def _parse_date(title: str) -> Optional[str]:
    m = _DATE_RE.search(title)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            pass
    m = _YEAR_RE.search(title)
    if m:
        year = m.group(1) or m.group(2)
        if year and 1900 <= int(year) <= datetime.now().year + 1:
            return f"{year}-01-01"
    return None


class CBJRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        try:
            return self.session.get(url, timeout=timeout)
        except Exception as e:
            logger.warning("GET failed %s: %s", url, e)
            return None

    def _list_items(self, category: str) -> Generator[tuple[str, str], None, None]:
        """Yield (title, absolute_pdf_url) for a category listing page."""
        resp = self._get(f"{BASE_URL}/EN/List/{category}")
        if resp is None or resp.status_code != 200:
            logger.warning("Category page failed (%s): %s",
                           getattr(resp, "status_code", "?"), category)
            return
        for m in _ROW_RE.finditer(resp.text):
            title = _clean_title(m.group("title"))
            pdf = m.group("pdf")
            if pdf.startswith("/"):
                pdf = BASE_URL + pdf
            if title and pdf:
                yield title, pdf

    def _build_record(self, title: str, pdf_url: str, label: str,
                      doc_type: str, seen_pdfs: Optional[set] = None) -> Optional[dict]:
        norm_pdf = pdf_url.split("?")[0]
        if seen_pdfs is not None:
            if norm_pdf in seen_pdfs:
                return None
            seen_pdfs.add(norm_pdf)

        doc_id = "cbj-" + hashlib.sha1(norm_pdf.encode()).hexdigest()[:16]
        table = "legislation" if doc_type == "legislation" else "doctrine"

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_url=pdf_url,
            table=table,
        )
        if not text or len(text) < 200:
            logger.warning("No usable text (scanned?): %s", title[:60])
            return None

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": _parse_date(title),
            "url": pdf_url,
            "document_type": label.lower(),
            "category": label,
            "language": "en",
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        seen_pdfs: set = set()
        for category, (label, doc_type) in CATEGORIES.items():
            logger.info("Fetching category: %s", label)
            for title, pdf_url in self._list_items(category):
                record = self._build_record(title, pdf_url, label, doc_type, seen_pdfs)
                if record:
                    yield record
                time.sleep(1)

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        # The CBJ listing pages have no reliable per-item date filter; re-scan
        # all categories. Idempotency is handled downstream via dedup by PDF.
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JO/CBJ-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBJRegulationsScraper()

    if args.command == "test-api":
        for category, (label, _t) in CATEGORIES.items():
            items = list(scraper._list_items(category))
            logger.info("%s: %d items", label, len(items))
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars (%s)",
                count,
                record["title"][:55],
                len(record.get("text", "")),
                record.get("category"),
            )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
