#!/usr/bin/env python3
"""
NI/SIBOIF-Normativa — Superintendencia de Bancos y de Otras Instituciones Financieras

Fetches regulatory documents (leyes, normas, resoluciones, circulares, reglamentos)
from the SIBOIF document repository. HTML listing is paginated; each document is a PDF.

Strategy:
  1. Scrape paginated HTML table at /consultas/documentos?page=N
  2. Parse rows for title, code, type, date, category, departments, PDF URL
  3. Download PDFs and extract text with pdfplumber (fallback PyMuPDF)
  4. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NI.SIBOIF-Normativa")

BASE_URL = "https://www.superintendencia.gob.ni"
SOURCE_ID = "NI/SIBOIF-Normativa"
LISTING_URL = f"{BASE_URL}/consultas/documentos"
MAX_PAGES = 30  # Safety limit


def _extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    """Extract text from PDF using pdfplumber."""
    import pdfplumber
    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n\n".join(text_parts)


def _extract_text_pymupdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using PyMuPDF (fitz)."""
    import fitz
    text_parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page in doc:
        text = page.get_text()
        if text:
            text_parts.append(text)
    doc.close()
    return "\n\n".join(text_parts)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes, trying pdfplumber first then PyMuPDF."""
    text = ""
    try:
        text = _extract_text_pdfplumber(pdf_bytes)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}, trying PyMuPDF")
    if not text.strip():
        try:
            text = _extract_text_pymupdf(pdf_bytes)
        except Exception as e:
            logger.warning(f"Both PDF extractors failed: {e}")
    return text.strip()


def _clean_cell(html: str) -> str:
    """Strip HTML tags and clean whitespace from a table cell."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    return " ".join(text.split()).strip()


def _extract_link(html: str) -> Optional[str]:
    """Extract first href from an HTML fragment."""
    m = re.search(r'href="([^"]+)"', html)
    if m:
        url = unescape(m.group(1))
        if url.startswith("/"):
            return BASE_URL + url
        return url
    return None


def _parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY date to ISO 8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _make_id(pdf_url: str, title: str) -> str:
    """Generate a stable document ID from URL or title."""
    import hashlib
    key = pdf_url or title
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def parse_listing_page(html: str) -> list[dict]:
    """Parse a documents listing page and return list of document metadata dicts."""
    docs = []
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 4:
            continue
        title = _clean_cell(cells[0])
        if not title:
            continue
        pdf_url = _extract_link(row)
        if not pdf_url or not pdf_url.lower().endswith(".pdf"):
            continue
        doc_code = _clean_cell(cells[1]) if len(cells) > 1 else ""
        doc_type = _clean_cell(cells[2]) if len(cells) > 2 else ""
        date_str = _clean_cell(cells[3]) if len(cells) > 3 else ""
        category = _clean_cell(cells[4]) if len(cells) > 4 else ""
        departments = _clean_cell(cells[5]) if len(cells) > 5 else ""
        docs.append({
            "title": title,
            "doc_code": doc_code,
            "doc_type": doc_type,
            "date": _parse_date(date_str),
            "date_raw": date_str,
            "category": category,
            "departments": departments,
            "pdf_url": pdf_url,
        })
    return docs


class SIBOIFNormativaScraper(BaseScraper):
    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            max_retries=3,
            timeout=60,
        )

    def test_api(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.http.get(LISTING_URL, params={"page": "0"})
            if resp.status_code == 200 and "sites/default/files/documentos" in resp.text:
                logger.info("API test passed — documents listing accessible")
                return True
            logger.error(f"API test failed — status {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

    def _fetch_all_metadata(self) -> list[dict]:
        """Fetch all document metadata from paginated listing."""
        all_docs = []
        seen_urls = set()
        for page in range(MAX_PAGES):
            logger.info(f"Fetching listing page {page}...")
            try:
                resp = self.http.get(LISTING_URL, params={"page": str(page)})
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch page {page}: {e}")
                break
            docs = parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page}, stopping pagination")
                break
            new_count = 0
            for doc in docs:
                if doc["pdf_url"] not in seen_urls:
                    seen_urls.add(doc["pdf_url"])
                    all_docs.append(doc)
                    new_count += 1
            logger.info(f"Page {page}: {len(docs)} docs ({new_count} new)")
            time.sleep(1)
        logger.info(f"Total unique documents found: {len(all_docs)}")
        return all_docs

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text content."""
        try:
            resp = self.http.get(pdf_url, timeout=90)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None
            text = extract_text_from_pdf(resp.content)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text extracted from {pdf_url}")
                return None
            return text
        except Exception as e:
            logger.warning(f"Failed to download/extract {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        return {
            "_id": _make_id(raw["pdf_url"], raw["title"]),
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "doc_code": raw.get("doc_code", ""),
            "doc_type": raw.get("doc_type", ""),
            "category": raw.get("category", ""),
            "departments": raw.get("departments", ""),
            "language": "es",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all normalized documents with full text."""
        metadata_list = self._fetch_all_metadata()
        for i, meta in enumerate(metadata_list):
            logger.info(f"[{i+1}/{len(metadata_list)}] Downloading: {meta['title'][:80]}")
            text = self._download_and_extract(meta["pdf_url"])
            if not text:
                logger.warning(f"Skipping (no text): {meta['title'][:80]}")
                continue
            meta["text"] = text
            yield self.normalize(meta)
            time.sleep(1)

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Yield documents updated since a given date."""
        yield from self.fetch_all()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="NI/SIBOIF-Normativa bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = SIBOIFNormativaScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else None

        for record in scraper.fetch_all():
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                f"  #{count} | {record['title'][:60]} | "
                f"text={text_len} chars | date={record.get('date', 'N/A')}"
            )
            if args.sample or count <= 15:
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            if limit and count >= limit:
                logger.info(f"Sample limit reached ({limit} records)")
                break

        logger.info(f"Done. {count} records fetched.")
        print(json.dumps({"_source": SOURCE_ID, "records": count}))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
