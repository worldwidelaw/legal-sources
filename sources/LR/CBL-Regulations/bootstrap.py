#!/usr/bin/env python3
"""
LR/CBL-Regulations -- Central Bank of Liberia Regulations and Directives

Fetches banking regulations, directives, and guidelines from the Central Bank
of Liberia (cbl.org.lr). Documents are published as PDFs.

Document types:
  - Regulations (~45): Banking rules and prudential standards
  - Directives (~20): Binding instructions to financial institutions
  - Guidelines (~19): Supervisory guidance and best practices

Strategy:
  - Scrape HTML listing pages (Drupal views with pagination)
  - Download each PDF and extract text with pdfplumber
  - ~84 documents total, all English language

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LR.CBL-Regulations")

BASE_URL = "https://www.cbl.org.lr"
DOC_TYPE_URLS = {
    "regulation": f"{BASE_URL}/publications/document-type/regulations",
    "directive": f"{BASE_URL}/publications/document-type/directives",
    "guideline": f"{BASE_URL}/publications/document-type/guidelines",
}
MAX_PAGES_PER_TYPE = 5


class _ListingParser(HTMLParser):
    """Parse CBL publication listing pages to extract document links and metadata.

    HTML structure:
      <li class="row-item-list">
        <div class="views-field views-field-field-content-post-date">
          <div class="field-content">
            <time datetime="2024-10-28T12:00:00Z">Monday, October 28, 2024</time>
          </div>
        </div>
        <div><span><a href="/sites/default/files/documents/file.pdf " target="_blank">Title</a></span></div>
      </li>
    """

    def __init__(self):
        super().__init__()
        self.docs: List[Dict[str, str]] = []
        self._in_title_link = False
        self._current_doc: Dict[str, str] = {}
        self._last_datetime = ""

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)

        # Capture date from <time datetime="...">
        if tag == "time" and "datetime" in attrs_d:
            self._last_datetime = attrs_d["datetime"][:10]  # "2024-10-28"

        if tag == "a":
            href = attrs_d.get("href", "").strip()
            if href.lower().endswith((".pdf", ".docx", ".doc")):
                if not href.startswith("http"):
                    href = BASE_URL + href
                self._current_doc["pdf_url"] = href
                self._current_doc["date_raw"] = self._last_datetime
                self._in_title_link = True
                self._current_doc.setdefault("title", "")

    def handle_endtag(self, tag):
        if tag == "a" and self._in_title_link:
            self._in_title_link = False
            if self._current_doc.get("pdf_url"):
                self.docs.append(self._current_doc)
                self._current_doc = {}

    def handle_data(self, data):
        if self._in_title_link:
            self._current_doc["title"] = self._current_doc.get("title", "") + data.strip()


def _parse_date(raw: str) -> str:
    """Try to parse various date formats into ISO 8601."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


class CBLRegulationsScraper(BaseScraper):
    """Scraper for LR/CBL-Regulations -- Central Bank of Liberia."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _scrape_listing_page(self, url: str) -> List[Dict[str, str]]:
        resp = self._request(url)
        if resp is None:
            return []
        parser = _ListingParser()
        parser.feed(resp.text)
        return parser.docs

    def _get_all_docs_for_type(self, doc_type: str, base_url: str) -> List[Dict[str, str]]:
        all_docs = []
        for page in range(MAX_PAGES_PER_TYPE):
            url = base_url if page == 0 else f"{base_url}?page={page}"
            docs = self._scrape_listing_page(url)
            if not docs:
                break
            for d in docs:
                d["doc_type"] = doc_type
            all_docs.extend(docs)
            logger.info(f"{doc_type} page {page}: {len(docs)} docs")
        return all_docs

    def _extract_pdf_text(self, content: bytes) -> str:
        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(pages)
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = raw.get("doc_id", "")
        return {
            "_id": doc_id,
            "_source": "LR/CBL-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "doc_type": raw.get("doc_type", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0
        seen_urls = set()

        for doc_type, base_url in DOC_TYPE_URLS.items():
            docs = self._get_all_docs_for_type(doc_type, base_url)

            for doc in docs:
                if max_records and count >= max_records:
                    return

                pdf_url = doc.get("pdf_url", "")
                if not pdf_url or pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                # Skip non-PDF files (e.g. .docx)
                if not pdf_url.lower().endswith(".pdf"):
                    logger.info(f"Skipping non-PDF: {pdf_url}")
                    continue

                resp = self._request(pdf_url, timeout=120)
                if resp is None:
                    logger.warning(f"Failed to download: {pdf_url}")
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text ({len(text)} chars): {pdf_url}")
                    continue

                title = doc.get("title", "").strip()
                if not title:
                    title = pdf_url.split("/")[-1].replace("%20", " ").replace(".pdf", "")

                date = doc.get("date_raw", "").strip()[:10]  # Already ISO from <time datetime>

                # Create stable ID from filename
                filename = pdf_url.split("/")[-1]
                doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", filename.replace(".pdf", "").replace("%20", "_"))

                raw = {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": date,
                    "doc_type": doc.get("doc_type", ""),
                    "url": pdf_url,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = CBLRegulationsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
