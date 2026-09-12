#!/usr/bin/env python3
"""
BW/BOB-Regulations -- Bank of Botswana Regulations & Directives

Fetches banking legislation, regulations, circulars, directives, and
prudential guidelines PDFs from the Bank of Botswana website.

Strategy:
  - Scrape 4 regulatory section pages for PDF links
  - Download PDFs and extract full text with pdfplumber
  - ~32 documents covering banking law, capital adequacy, AML/CFT, etc.

Note: Uses subprocess+curl for HTTPS because the system Python 3.9
      has LibreSSL 2.8.3 which cannot negotiate TLS with this server.

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
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Tuple
from urllib.parse import urljoin, unquote

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BW.BOB-Regulations")

BASE_URL = "https://www.bankofbotswana.bw"

REGULATORY_PAGES = [
    "/content/legislation",
    "/content/regulations",
    "/content/circulars-and-directives",
    "/content/policies-and-guidelines",
    "/content/legislation-and-laws",
]

USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"


def curl_get(url: str, timeout: int = 60, binary: bool = False):
    """Fetch a URL using curl subprocess (bypasses Python SSL issues)."""
    cmd = [
        "curl", "-sS", "-L",
        "--max-time", str(timeout),
        "-H", f"User-Agent: {USER_AGENT}",
        url
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=timeout + 10)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed ({result.returncode}): {result.stderr.decode()[:200]}")
    if binary:
        return result.stdout
    return result.stdout.decode("utf-8", errors="replace")


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


def url_to_title(url: str, link_text: str = "") -> str:
    """Derive a document title from link text or URL filename."""
    if link_text and len(link_text.strip()) > 3:
        return link_text.strip()
    filename = unquote(url.split("/")[-1])
    name = re.sub(r"\.pdf$", "", filename, flags=re.I)
    name = name.replace("-", " ").replace("_", " ").replace("%20", " ")
    return name.strip().title()


class BOBRegulationsScraper(BaseScraper):
    """
    Scraper for BW/BOB-Regulations — Bank of Botswana.
    Country: BW
    URL: https://www.bankofbotswana.bw/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _collect_pdf_links(self) -> list[Tuple[str, str]]:
        """Scrape all regulatory pages for unique PDF URLs with titles."""
        seen_urls = set()
        results = []

        for page_path in REGULATORY_PAGES:
            url = f"{BASE_URL}{page_path}"
            try:
                html = curl_get(url, timeout=30)
                # Find all <a> tags linking to PDFs
                links = re.findall(
                    r'<a[^>]+href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
                    html, re.I | re.DOTALL
                )
                for href, text in links:
                    full_url = urljoin(url, href)
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        clean_text = re.sub(r"<[^>]+>", "", text).strip()
                        results.append((full_url, clean_text))
                logger.info(f"Page {page_path}: found {len(links)} PDF links")
            except Exception as e:
                logger.warning(f"Failed to scrape {page_path}: {e}")
            time.sleep(1.0)

        logger.info(f"Total unique PDFs collected: {len(results)}")
        return results

    def _download_and_extract(self, pdf_url: str, title: str) -> Optional[dict]:
        """Download a PDF and extract its text."""
        try:
            content = curl_get(pdf_url, timeout=90, binary=True)
        except Exception as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

        if len(content) < 1000:
            logger.warning(f"PDF too small ({len(content)} bytes): {pdf_url}")
            return None

        text = extract_pdf_text(content)
        if len(text) < 100:
            logger.warning(f"Insufficient text from {pdf_url}: {len(text)} chars")
            return None

        return {
            "url": pdf_url,
            "title": title,
            "text": text,
            "date": None,
            "filename": unquote(pdf_url.split("/")[-1]),
            "pdf_size": len(content),
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF data into standard schema."""
        return {
            "_id": raw["url"],
            "_source": "BW/BOB-Regulations",
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
        pdf_links = self._collect_pdf_links()
        yielded = 0

        for pdf_url, title in pdf_links:
            result = self._download_and_extract(pdf_url, url_to_title(pdf_url, title))
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
        description="BW/BOB-Regulations — Bank of Botswana"
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

    scraper = BOBRegulationsScraper()

    if args.command == "test":
        logger.info("Testing BOB connectivity...")
        try:
            pdf_links = scraper._collect_pdf_links()
            logger.info(f"Found {len(pdf_links)} unique PDFs")

            if pdf_links:
                test_url, test_title = pdf_links[0]
                logger.info(f"Testing PDF download: {test_url}")
                result = scraper._download_and_extract(test_url, test_title)
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
