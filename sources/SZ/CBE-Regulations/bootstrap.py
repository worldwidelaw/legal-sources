#!/usr/bin/env python3
"""
SZ/CBE-Regulations -- Central Bank of Eswatini — Financial Regulations & Circulars

Fetches banking laws, regulations, guidelines, directives and payment system
rules from centralbank.org.sz (WordPress site with PDF uploads).

Strategy:
  1. Scrape multiple pages on centralbank.org.sz for PDF links
  2. Download each PDF and extract full text via pdfplumber
  3. Categorize documents by source page (Acts, Orders, Regulations, NPS, etc.)

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SZ.CBE-Regulations")

BASE_URL = "https://www.centralbank.org.sz"
DELAY = 2.0

# Pages to scrape: (path, category)
SOURCE_PAGES = [
    ("legislation", "Legislation"),
    ("financial-regulation", "Financial Regulation"),
    ("national-payment-systems", "National Payment Systems"),
]


def _fetch_page(path: str) -> Optional[str]:
    """Fetch HTML content of a CBE page."""
    import requests
    url = f"{BASE_URL}/{path}/"
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                return r.text
            logger.warning("Page fetch attempt %d for %s: HTTP %d", attempt + 1, path, r.status_code)
        except Exception as e:
            logger.warning("Page fetch attempt %d for %s: %s", attempt + 1, path, e)
        if attempt < 2:
            time.sleep(3)
    return None


def _extract_pdf_links(html: str) -> List[Tuple[str, str]]:
    """Extract (title, url) pairs for PDF links from HTML."""
    results = []
    seen = set()

    # Find all PDF URLs in the page
    pdf_urls = re.findall(
        r'(https?://www\.centralbank\.org\.sz/wp-content/uploads/[^\s"\'<>]+\.pdf)',
        html,
    )

    for url in pdf_urls:
        if url in seen:
            continue
        seen.add(url)

        # Try to find the anchor text for this PDF
        pattern = re.compile(
            r'<a[^>]*href=["\']' + re.escape(url) + r'["\'][^>]*>(.*?)</a>',
            re.DOTALL | re.IGNORECASE,
        )
        m = pattern.search(html)
        if m:
            title = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            title = re.sub(r"&nbsp;", " ", title)
            title = re.sub(r"&amp;", "&", title)
        else:
            title = ""

        if not title or len(title) < 3:
            title = _title_from_url(url)

        results.append((title, url))

    return results


def _title_from_url(url: str) -> str:
    """Extract readable title from PDF URL."""
    m = re.search(r"/([^/]+)$", url)
    if m:
        name = unquote(m.group(1))
        name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"-optimized$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[_-]+", " ", name)
        return name.strip()
    return "Untitled"


def _extract_year(text: str) -> Optional[str]:
    """Extract a year from text or filename."""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        year = int(m.group(1))
        if 1990 <= year <= 2030:
            return f"{year}-01-01"
    return None


def _make_id(url: str) -> str:
    """Create a stable document ID from URL."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", url).strip("_").lower()
    if len(slug) > 80:
        slug = slug[:80]
    return f"SZ_CBE_{abs(hash(slug)) % 10**10}"


def _download_pdf(url: str) -> Optional[bytes]:
    """Download a PDF file."""
    import requests
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and len(r.content) > 200:
                return r.content
            logger.warning("PDF download attempt %d: HTTP %d, %d bytes",
                           attempt + 1, r.status_code, len(r.content))
        except Exception as e:
            logger.warning("PDF download attempt %d: %s", attempt + 1, e)
        if attempt < 2:
            time.sleep(3)
    return None


class CBERegulationsScraper(BaseScraper):
    """Scraper for SZ/CBE-Regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _collect_documents(self) -> List[Dict[str, Any]]:
        """Collect all PDF document references from CBE pages."""
        docs = []
        seen_urls = set()

        for path, category in SOURCE_PAGES:
            logger.info("Fetching page: %s (%s)", path, category)
            html = _fetch_page(path)
            if html is None:
                logger.warning("Could not fetch page: %s", path)
                continue

            pdf_links = _extract_pdf_links(html)
            logger.info("Found %d PDFs on %s", len(pdf_links), path)

            for title, url in pdf_links:
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                docs.append({
                    "title": title,
                    "url": url,
                    "category": category,
                    "page": path,
                })

        logger.info("Total unique PDF documents collected: %d", len(docs))
        return docs

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "SZ/CBE-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        doc_list = self._collect_documents()
        if not doc_list:
            logger.error("No documents found")
            return

        count = 0
        for doc in doc_list:
            if max_records and count >= max_records:
                return

            title = doc["title"]
            url = doc["url"]
            doc_id = _make_id(url)
            logger.info("Downloading PDF [%d/%d]: %s", count + 1, len(doc_list), title[:60])

            pdf_bytes = _download_pdf(url)
            if pdf_bytes is None:
                logger.warning("Failed to download: %s", url)
                continue
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF: %s", url)
                continue

            try:
                text = extract_pdf_markdown(
                    source="SZ/CBE-Regulations",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                )
            except Exception as e:
                logger.warning("PDF extraction failed for %s: %s", url, e)
                continue

            if not text or len(text) < 50:
                logger.warning("Insufficient text (%d chars): %s",
                               len(text or ""), title[:50])
                continue

            date = _extract_year(title) or _extract_year(url)
            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "category": doc["category"],
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing access to centralbank.org.sz...")
        html = _fetch_page("legislation")
        if html is None:
            logger.error("Cannot reach centralbank.org.sz")
            return False

        pdfs = _extract_pdf_links(html)
        logger.info("Legislation page: %d PDF links found", len(pdfs))

        if pdfs:
            test_title, test_url = pdfs[0]
            logger.info("Test download: %s", test_title[:60])
            pdf_bytes = _download_pdf(test_url)
            if pdf_bytes and len(pdf_bytes) > 200:
                logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            else:
                logger.warning("PDF download failed")
                return False

        return True


def main():
    parser = argparse.ArgumentParser(description="SZ/CBE-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBERegulationsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
