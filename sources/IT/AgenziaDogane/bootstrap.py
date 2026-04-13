#!/usr/bin/env python3
"""
IT/AgenziaDogane -- Italian Customs Agency (ADM) Data Fetcher

Fetches doctrine documents (circolari, risoluzioni, determinazioni) from ADM.

Strategy:
  - Scrapes multiple listing pages on adm.gov.it
  - Extracts PDF links from listing pages and year-based archives
  - Downloads PDFs and extracts full text using pdfplumber
  - Covers: circolari (customs), determinazioni direttoriali, risoluzioni (2001-2020)

License: Italian Open Data (public domain)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import subprocess
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.AgenziaDogane")

BASE_URL = "https://www.adm.gov.it"

# All pages containing doctrine documents
LISTING_PAGES = [
    ("/portale/circolari-dogane", "circolare"),
    ("/portale/determinazioni-direttoriali", "determinazione"),
]

# Risoluzioni year archive pages
RISOLUZIONI_YEARS = {
    2001: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2001",
    2002: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2002",
    2003: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2003",
    2004: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2004",
    2005: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2005",
    2006: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2006",
    2007: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2007",
    2008: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2008",
    2009: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2009",
    2010: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2010",
    2012: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2012",
    2013: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2013",
    2014: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2014",
    2015: "/portale/dogane/operatore/atti-amministrativi-generali/risoluzioni/risoluzioni-2015",
    2017: "/portale/ris-anno-2017",
    2019: "/portale/anno-20193",
    2020: "/portale/risoluzioni-2020",
}

# Archive circolari year pages (from /portale/archivio-circolari-e-note)
ARCHIVE_PAGES = [
    "/portale/-/3218699",   # 2016
    "/portale/-/3218724",
    "/portale/-/3218733",
    "/portale/-/3218742",
    "/portale/-/3218759",
    "/portale/-/3218768",
    "/portale/-/3218777",
    "/portale/-/3218786",
    "/portale/-/3218795",
    "/portale/-/3218804",
    "/portale/-/3218821",
    "/portale/-/3218846",
]


class ADMScraper(BaseScraper):
    """
    Scraper for IT/AgenziaDogane -- Italian Customs Agency.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._seen_urls = set()

    def _curl_get(self, url: str, timeout: int = 60) -> Optional[bytes]:
        """Download URL using curl."""
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", str(timeout), url],
                capture_output=True,
                timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
            return None
        except Exception as e:
            logger.warning(f"curl failed for {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IT/AgenziaDogane",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def _extract_pdfs_from_page(self, html: str) -> List[Tuple[str, str]]:
        """Extract PDF URLs and their context text from an HTML page."""
        results = []

        # Find PDF links with surrounding text
        pdf_pattern = re.compile(
            r'<a[^>]*href="([^"]*\.pdf[^"]*)"[^>]*>([^<]*)</a>',
            re.I
        )
        for match in pdf_pattern.finditer(html):
            url = match.group(1)
            link_text = match.group(2).strip()

            if url.startswith("/"):
                url = BASE_URL + url

            if url in self._seen_urls:
                continue
            self._seen_urls.add(url)

            # Try to get surrounding context for title
            pos = match.start()
            context = html[max(0, pos - 500):pos + 500]

            # Extract a title from context
            title = link_text or self._title_from_context(context, url)
            results.append((url, title))

        return results

    def _title_from_context(self, context: str, url: str) -> str:
        """Extract a meaningful title from HTML context or URL."""
        # Try to find nearby heading or strong text
        heading = re.search(r'<(?:h[1-6]|strong|b)[^>]*>([^<]+)', context)
        if heading:
            return heading.group(1).strip()

        # Extract from filename
        filename = unquote(url.split("/")[-1].split("?")[0])
        filename = re.sub(r'\.pdf$', '', filename, flags=re.I)
        filename = filename.replace("+", " ").replace("%20", " ")
        return filename

    def _parse_date_from_text(self, text: str) -> Optional[str]:
        """Try to extract a date from text content."""
        # Pattern: DD/MM/YYYY or DD-MM-YYYY
        match = re.search(r'(\d{2})[/-](\d{2})[/-](\d{4})', text)
        if match:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

        # Pattern: DD.MM.YYYY
        match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text)
        if match:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

        return None

    def _classify_doc(self, title: str, url: str) -> str:
        """Classify document type from title/url."""
        lower = (title + " " + url).lower()
        if "circolar" in lower:
            return "circolare"
        elif "risolu" in lower:
            return "risoluzione"
        elif "determin" in lower:
            return "determinazione"
        elif "decreto" in lower:
            return "decreto"
        elif "nota" in lower or "note" in lower:
            return "nota"
        return "documento"

    def _make_id(self, url: str) -> str:
        """Create stable ID from URL."""
        # Use hash of the document path (excluding timestamp params)
        clean_url = re.sub(r'\?t=\d+$', '', url)
        return hashlib.md5(clean_url.encode()).hexdigest()[:16]

    def normalize(self, raw: Dict) -> Dict:
        """Normalize raw data into standard schema."""
        title = raw.get("title", "Unknown")
        text = raw.get("text", "")
        url = raw.get("url", "")

        # Try to extract date from title or text
        date = self._parse_date_from_text(title) or self._parse_date_from_text(text[:500] if text else "")

        doc_type = self._classify_doc(title, url)

        return {
            "_id": self._make_id(url),
            "_source": "IT/AgenziaDogane",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_type": doc_type,
        }

    def _fetch_pdfs_from_pages(self, pages: List[Tuple[str, str]], sample_limit: int) -> Generator[Dict, None, None]:
        """Fetch PDFs from a list of pages and yield normalized records."""
        total = 0
        for page_path, doc_type in pages:
            if total >= sample_limit:
                break

            url = BASE_URL + page_path if page_path.startswith("/") else page_path
            logger.info(f"Fetching page: {page_path}")
            data = self._curl_get(url)
            if not data:
                logger.warning(f"Failed to fetch {page_path}")
                continue

            html = data.decode("utf-8", errors="ignore")
            pdf_items = self._extract_pdfs_from_page(html)
            logger.info(f"Found {len(pdf_items)} PDFs on {page_path}")

            for pdf_url, title in pdf_items:
                if total >= sample_limit:
                    break

                logger.info(f"Downloading: {title[:60]}...")
                pdf_bytes = self._curl_get(pdf_url, timeout=120)
                time.sleep(1.5)

                if not pdf_bytes or len(pdf_bytes) < 500:
                    logger.warning(f"PDF download failed: {pdf_url}")
                    continue

                text = self._extract_pdf_text(pdf_bytes) or ""
                if not text:
                    logger.warning(f"No text extracted from {title[:60]}")
                    continue

                raw = {
                    "title": title,
                    "text": text,
                    "url": pdf_url,
                    "doc_type": doc_type,
                }
                record = self.normalize(raw)
                yield record
                total += 1
                logger.info(f"[{total}] {title[:60]} ({len(text)} chars)")

            time.sleep(2)

    def fetch_all(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch all ADM doctrine documents."""
        sample_limit = 15 if sample else 10000

        # Build full page list
        all_pages = list(LISTING_PAGES)

        if not sample:
            # Add risoluzioni year pages
            for year, path in sorted(RISOLUZIONI_YEARS.items()):
                all_pages.append((path, "risoluzione"))
            # Add archive pages
            for path in ARCHIVE_PAGES:
                all_pages.append((path, "circolare"))

        yield from self._fetch_pdfs_from_pages(all_pages, sample_limit)

    def fetch_updates(self, since: str) -> Generator[Dict, None, None]:
        """Fetch recent documents (re-fetches main listing pages)."""
        yield from self._fetch_pdfs_from_pages(LISTING_PAGES, 10000)

    def test_api(self) -> bool:
        """Test connectivity."""
        logger.info("Testing ADM website connectivity...")
        data = self._curl_get(BASE_URL + "/portale/circolari-dogane")
        if not data:
            logger.error("Failed to fetch circolari page")
            return False

        html = data.decode("utf-8", errors="ignore")
        pdfs = self._extract_pdfs_from_page(html)
        logger.info(f"Found {len(pdfs)} PDFs on circolari page")

        if pdfs:
            pdf_url, title = pdfs[0]
            pdf_bytes = self._curl_get(pdf_url, timeout=30)
            if pdf_bytes:
                text = self._extract_pdf_text(pdf_bytes)
                if text:
                    logger.info(f"PDF text extraction OK ({len(text)} chars)")
                    return True

        logger.error("PDF download/extraction failed")
        return False


def main():
    scraper = ADMScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=sample):
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
