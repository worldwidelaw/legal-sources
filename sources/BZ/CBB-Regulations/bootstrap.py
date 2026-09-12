#!/usr/bin/env python3
"""
BZ/CBB-Regulations -- Central Bank of Belize Laws & Regulations

Fetches banking legislation, amendments, regulations, practice directions,
and AML/CFT guidelines PDFs from the Central Bank of Belize website.

Strategy:
  - Scrape 11 legislative category pages for PDF links
  - Download PDFs and extract full text with pdfplumber
  - ~119 documents covering banking law, AML/CFT, exchange control, etc.

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
from typing import Generator, Optional, Tuple
from urllib.parse import urljoin, unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BZ.CBB-Regulations")

BASE_URL = "https://www.centralbank.org.bz"

CATEGORY_PAGES = [
    ("central-bank-act", "Central Bank of Belize Act"),
    ("domestic-banks-financial-institutions-act", "Domestic Banks & Financial Institutions Act"),
    ("international-banking-act", "International Banking Act"),
    ("credit-unions-act", "Credit Unions Act"),
    ("money-laundering-terrorism-(prevention)-act", "Money Laundering & Terrorism (Prevention) Act"),
    ("exchange-control-act", "Exchange Control Act"),
    ("treasury-bill-act", "Treasury Bill Act and Rules"),
    ("national-payment-system-act", "National Payment System Act"),
    ("moneylenders-act", "Moneylenders Act"),
    ("deposit-insurance-act", "Deposit Insurance Act"),
    ("credit-reporting-act", "Credit Reporting Act"),
]

USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


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


def extract_date(title: str, filename: str) -> Optional[str]:
    """Try to extract a year or date from title/filename."""
    # Match patterns like "Act No. 15 of 2025", "2020", "December 2023"
    m = re.search(r'\b(20\d{2}|19\d{2})\b', title)
    if m:
        return f"{m.group(1)}-01-01"
    m = re.search(r'\b(20\d{2}|19\d{2})\b', filename)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def url_to_title(url: str, link_text: str = "") -> str:
    """Derive a document title from link text or URL filename."""
    if link_text and len(link_text.strip()) > 3:
        return link_text.strip()
    filename = unquote(url.split("/")[-1].split("?")[0])
    name = re.sub(r"\.pdf$", "", filename, flags=re.I)
    name = name.replace("-", " ").replace("_", " ")
    return name.strip().title()


class CBBRegulationsScraper(BaseScraper):
    """
    Scraper for BZ/CBB-Regulations — Central Bank of Belize.
    Country: BZ
    URL: https://www.centralbank.org.bz/about-the-bank/laws-and-regulations

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _collect_pdf_links(self) -> list[Tuple[str, str, str]]:
        """Scrape all category pages for unique PDF URLs with titles.
        Returns list of (url, title, category_slug) tuples."""
        seen_urls = set()
        results = []

        for slug, cat_name in CATEGORY_PAGES:
            url = f"{BASE_URL}/about-the-bank/laws-and-regulations/{slug}"
            try:
                r = SESSION.get(url, timeout=30)
                r.raise_for_status()
                html = r.text
                links = re.findall(
                    r'<a[^>]+href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>',
                    html, re.I | re.DOTALL
                )
                count = 0
                for href, text in links:
                    full_url = urljoin(url, href)
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        clean_text = re.sub(r"<[^>]+>", "", text).strip()
                        results.append((full_url, clean_text, slug))
                        count += 1
                logger.info(f"Category '{slug}': {count} PDFs")
            except Exception as e:
                logger.warning(f"Failed to scrape {slug}: {e}")
            time.sleep(1.0)

        logger.info(f"Total unique PDFs collected: {len(results)}")
        return results

    def _download_and_extract(self, pdf_url: str, title: str, category: str) -> Optional[dict]:
        """Download a PDF and extract its text."""
        try:
            r = SESSION.get(pdf_url, timeout=90)
            r.raise_for_status()
            content = r.content
        except Exception as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

        if len(content) < 500:
            logger.warning(f"PDF too small ({len(content)} bytes): {pdf_url}")
            return None

        text = extract_pdf_text(content)
        if len(text) < 50:
            logger.warning(f"Insufficient text from {pdf_url}: {len(text)} chars")
            return None

        filename = unquote(pdf_url.split("/")[-1].split("?")[0])
        date = extract_date(title, filename)

        return {
            "url": pdf_url,
            "title": title,
            "text": text,
            "date": date,
            "category": category,
            "filename": filename,
            "pdf_size": len(content),
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF data into standard schema."""
        return {
            "_id": raw["url"],
            "_source": "BZ/CBB-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category", ""),
            "filename": raw.get("filename", ""),
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents with full text."""
        pdf_links = self._collect_pdf_links()
        yielded = 0

        for pdf_url, title, category in pdf_links:
            result = self._download_and_extract(
                pdf_url, url_to_title(pdf_url, title), category
            )
            if result:
                yield result
                yielded += 1
                if yielded % 10 == 0:
                    logger.info(f"Processed {yielded}/{len(pdf_links)} PDFs")
            time.sleep(1.5)

        logger.info(f"fetch_all complete: {yielded} documents")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield all documents (no date filtering possible for this source)."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="BZ/CBB-Regulations — Central Bank of Belize"
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
            pdf_links = scraper._collect_pdf_links()
            logger.info(f"Found {len(pdf_links)} unique PDFs")

            if pdf_links:
                test_url, test_title, test_cat = pdf_links[0]
                logger.info(f"Testing PDF download: {test_url}")
                result = scraper._download_and_extract(test_url, test_title, test_cat)
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
