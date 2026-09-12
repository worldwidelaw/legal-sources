#!/usr/bin/env python3
"""
BB/CBB-Regulations -- Central Bank of Barbados Regulatory Guidelines

Fetches regulatory guidelines, prudential standards, exchange control
circulars, and key legislation PDFs from the Central Bank of Barbados.

Strategy:
  - Scrape regulatory pages for PDF links at cdn.centralbank.org.bb
  - Download PDFs and extract full text with pdfplumber
  - ~84 documents covering capital adequacy, AML/CFT, governance, etc.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import io
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Set

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BB.CBB-Regulations")

BASE_URL = "https://www.centralbank.org.bb"
CDN_PREFIX = "https://cdn.centralbank.org.bb/documents/"

REGULATORY_PAGES = [
    "/financial-stability-and-financial-regulation/regulatory-guidelines",
    "/financial-stability-and-financial-regulation/legislation-and-guidelines",
    "/financial-stability-and-financial-regulation/pillar-i-guideline",
    "/financial-stability-and-financial-regulation/pillar-ii-guideline",
    "/foreign-exchange/exchange-control-circulars",
    "/news/guidelines",
    "/news/paymentsguidelines",
    "/legislation/legislation-1",
    "/legislation/regulatory-legislation",
]

# Date pattern in CDN URLs: YYYY-MM-DD-HH-MM-SS-filename.pdf
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})-\d{2}-\d{2}-\d{2}-(.*?)\.pdf$", re.I)


def extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        pdf = pdfplumber.open(io.BytesIO(content))
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        pdf.close()
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def filename_to_title(filename: str) -> str:
    """Convert a CDN filename to a human-readable title."""
    # Remove date prefix
    m = DATE_RE.search(filename)
    if m:
        name = m.group(4)
    else:
        name = filename.replace(".pdf", "").replace(".PDF", "")
    # Replace hyphens with spaces, clean up
    name = name.replace("---", " — ").replace("--", " — ").replace("-", " ")
    # Capitalize first letter of each word for short names
    if len(name) < 100:
        name = name.title()
    return name.strip()


def extract_date(url: str) -> Optional[str]:
    """Extract publication date from CDN URL."""
    m = DATE_RE.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


class CBBRegulationsScraper(BaseScraper):
    """
    Scraper for BB/CBB-Regulations — Central Bank of Barbados.
    Country: BB
    URL: https://www.centralbank.org.bb/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)",
        })
        import urllib3
        urllib3.disable_warnings()

    def _collect_pdf_urls(self) -> Set[str]:
        """Scrape all regulatory pages for unique PDF URLs."""
        pdfs = set()
        for page_path in REGULATORY_PAGES:
            url = f"{BASE_URL}{page_path}"
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code == 200:
                    found = re.findall(
                        r'href="(https://cdn\.centralbank\.org\.bb/documents/[^"]+\.pdf)"',
                        r.text, re.I
                    )
                    pdfs.update(found)
                    logger.info(f"Page {page_path}: {len(found)} PDFs")
            except Exception as e:
                logger.warning(f"Failed to scrape {page_path}: {e}")
            time.sleep(0.5)
        logger.info(f"Total unique PDFs: {len(pdfs)}")
        return pdfs

    def _download_and_extract(self, pdf_url: str) -> Optional[dict]:
        """Download a PDF and extract its text."""
        try:
            r = self.session.get(pdf_url, timeout=60)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

        text = extract_pdf_text(r.content)
        if len(text) < 100:
            logger.warning(f"Insufficient text from {pdf_url}: {len(text)} chars")
            return None

        filename = pdf_url.split("/")[-1]
        title = filename_to_title(filename)
        date = extract_date(pdf_url)

        return {
            "url": pdf_url,
            "title": title,
            "text": text,
            "date": date,
            "filename": filename,
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF data into standard schema."""
        return {
            "_id": raw["url"],
            "_source": "BB/CBB-Regulations",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "filename": raw.get("filename", ""),
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents with full text."""
        pdf_urls = self._collect_pdf_urls()
        yielded = 0

        for pdf_url in sorted(pdf_urls):
            result = self._download_and_extract(pdf_url)
            if result:
                yield result
                yielded += 1
                if yielded % 10 == 0:
                    logger.info(f"Processed {yielded}/{len(pdf_urls)} PDFs")
            time.sleep(1.0)

        logger.info(f"fetch_all complete: {yielded} documents")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield documents newer than `since` date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        pdf_urls = self._collect_pdf_urls()
        yielded = 0

        for pdf_url in sorted(pdf_urls):
            date = extract_date(pdf_url)
            if date and date >= since:
                result = self._download_and_extract(pdf_url)
                if result:
                    yield result
                    yielded += 1
                time.sleep(1.0)

        logger.info(f"fetch_updates complete: {yielded} documents since {since}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="BB/CBB-Regulations — Central Bank of Barbados"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = CBBRegulationsScraper()

    if args.command == "test":
        logger.info("Testing CBB connectivity...")
        try:
            pdfs = scraper._collect_pdf_urls()
            logger.info(f"Found {len(pdfs)} unique PDFs")

            if pdfs:
                test_url = sorted(pdfs)[0]
                logger.info(f"Testing PDF download: {test_url}")
                result = scraper._download_and_extract(test_url)
                if result:
                    logger.info(f"Title: {result['title']}")
                    logger.info(f"Text: {len(result['text'])} chars")
                    logger.info(f"Preview: {result['text'][:200]}")
                    logger.info("Connectivity test passed!")
                else:
                    logger.warning("PDF extraction failed")
                    sys.exit(1)
            else:
                logger.warning("No PDFs found")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
