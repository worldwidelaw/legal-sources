#!/usr/bin/env python3
"""
MZ/BM-Regulations — Banco de Moçambique Avisos & Regulamentos

Fetches regulatory documents (avisos, circulars, decrees, instructions) from
the Banco de Moçambique website. Full text extracted from PDF downloads.

Strategy:
  1. Scrape paginated HTML listing at /pt/o-banco/normativos/
  2. Extract PDF links, titles, dates from each page
  3. Download PDFs and extract text with PyMuPDF
  4. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import logging
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional
from urllib.parse import urljoin, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MZ.BM-Regulations")

BASE_URL = "https://www.bancomoc.mz"
SOURCE_ID = "MZ/BM-Regulations"
LISTING_PATH = "/pt/o-banco/normativos/"
DOCS_PER_PAGE = 5


def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using PyMuPDF."""
    try:
        import fitz
    except ImportError:
        try:
            import pdfplumber
            import io
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p.strip())
                return text if text.strip() else None
        except ImportError:
            logger.error("No PDF library available (need PyMuPDF or pdfplumber)")
            return None

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                pages.append(text)
        doc.close()
        full_text = "\n\n".join(pages)
        return full_text if full_text.strip() else None
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return None


def _infer_doc_type(title: str) -> str:
    """Infer document type from title."""
    title_upper = title.upper()
    if title_upper.startswith("AVISO"):
        return "aviso"
    elif title_upper.startswith("CIRCULAR"):
        return "circular"
    elif "DECRETO" in title_upper:
        return "decreto"
    elif "DESPACHO" in title_upper:
        return "despacho"
    elif title_upper.startswith("LEI ") or "LEI N" in title_upper:
        return "lei"
    elif "REGULAMENTO" in title_upper:
        return "regulamento"
    elif "INSTRUÇÃO" in title_upper or "INSTRUCAO" in title_upper:
        return "instrucao"
    elif "DIPLOMA" in title_upper:
        return "diploma"
    return "outro"


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date from format like '27-02-2026' or '2-04-2026'."""
    date_str = date_str.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class BMScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.7",
            },
            timeout=120,
        )

    def _scrape_listing_page(self, page: int) -> list[dict]:
        """Scrape one page of the normativos listing."""
        url = f"{BASE_URL}{LISTING_PATH}?query=&page={page}"
        logger.info("Fetching listing page %d: %s", page, url)

        resp = self.http.get(url, timeout=60)
        if resp.status_code != 200:
            logger.warning("Failed to fetch page %d: HTTP %d", page, resp.status_code)
            return []

        html = resp.text
        docs = []

        # Pattern: <a class="download__link" href="/media/..." download="...">
        #            <p class="download__date">DATE</p>
        #            <p class="download__title">TITLE</p>
        #            <p class="download__size">SIZE</p>
        pattern = (
            r'<a\s+class="download__link"\s+href="([^"]+)"[^>]*>\s*'
            r'<p\s+class="download__date">([^<]+)</p>\s*'
            r'<p\s+class="download__title">([^<]+)</p>'
        )

        for href, date_str, title_html in re.findall(pattern, html, re.DOTALL):
            pdf_url = unescape(href)
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url
            title = unescape(title_html).strip()
            date = _parse_date(date_str)
            doc_type = _infer_doc_type(title)

            docs.append({
                "pdf_url": pdf_url,
                "title": title,
                "date": date,
                "date_raw": date_str.strip(),
                "doc_type": doc_type,
            })

        logger.info("Found %d documents on page %d", len(docs), page)
        return docs

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        logger.info("Downloading PDF: %s", pdf_url)
        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed: HTTP %d for %s", resp.status_code, pdf_url)
                return None

            content = resp.content
            if len(content) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(content), pdf_url)
                return None

            text = _extract_text_from_pdf(content)
            if text:
                logger.info("Extracted %d chars from %s", len(text), pdf_url)
            else:
                logger.warning("No text extracted from %s (scanned PDF?)", pdf_url)
            return text

        except Exception as e:
            logger.warning("Error downloading %s: %s", pdf_url, e)
            return None

    def _make_id(self, doc: dict) -> str:
        """Generate a stable document ID."""
        # Use the PDF filename as ID base
        filename = doc["pdf_url"].rstrip("/").split("/")[-1]
        # Remove .pdf extension
        if filename.endswith(".pdf"):
            filename = filename[:-4]
        return filename

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": raw["_fetched_at"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "doc_type": raw.get("doc_type", "outro"),
            "language": "pt",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        max_pages = 63 if not sample else 8
        fetched_at = datetime.now(timezone.utc).isoformat()
        doc_count = 0
        sample_limit = 15 if sample else 999999

        for page in range(1, max_pages + 1):
            if doc_count >= sample_limit:
                break

            docs = self._scrape_listing_page(page)
            if not docs:
                logger.info("No docs on page %d, stopping pagination", page)
                break

            for doc in docs:
                if doc_count >= sample_limit:
                    break

                time.sleep(2)  # Rate limit for PDF downloads
                text = self._download_and_extract(doc["pdf_url"])

                if not text:
                    logger.warning("Skipping %s — no extractable text", doc["title"][:60])
                    continue

                doc_id = self._make_id(doc)
                record = {
                    "_id": doc_id,
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": fetched_at,
                    "title": doc["title"],
                    "text": text,
                    "date": doc.get("date"),
                    "pdf_url": doc["pdf_url"],
                    "doc_type": doc.get("doc_type", "outro"),
                }

                # Yield RAW record; the framework calls normalize() once.
                # (Do NOT normalize here — double-normalize drops pdf_url and
                #  raises KeyError on the framework's second pass. See #996.)
                yield record
                doc_count += 1
                logger.info("Yielded doc %d: %s (%d chars)", doc_count, doc["title"][:50], len(text))

            time.sleep(1)  # Rate limit between pages

        logger.info("Total documents fetched: %d", doc_count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def test_api(self) -> dict:
        """Quick connectivity test."""
        docs = self._scrape_listing_page(1)
        if not docs:
            return {"status": "error", "message": "No documents found on page 1"}

        first = docs[0]
        text = self._download_and_extract(first["pdf_url"])
        return {
            "status": "ok",
            "first_doc": first["title"],
            "text_length": len(text) if text else 0,
            "has_text": text is not None and len(text) > 0,
        }


def main():
    scraper = BMScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        result = scraper.test_api()
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in scraper.fetch_all(sample=sample):
            record = scraper.normalize(raw)
            count += 1
            if sample:
                out = sample_dir / f"{count:03d}.json"
                out.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"  [{count}] {record['title'][:60]} ({len(record.get('text',''))} chars)")

        print(f"\nTotal: {count} records")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
