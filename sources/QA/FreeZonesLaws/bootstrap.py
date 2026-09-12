#!/usr/bin/env python3
"""
QA/FreeZonesLaws -- Qatar Free Zones Authority — Laws & Regulations

Fetches laws and regulations from qfz.gov.qa. Static WordPress site with
PDF documents on the laws and regulations pages.

Strategy:
  1. Parse /resource/laws/ and /authority/regulations/ for PDF links
  2. Download each PDF and extract full text via pdfplumber
  3. Normalize into standard schema

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
from urllib.parse import unquote, urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.QA.FreeZonesLaws")

BASE_URL = "https://qfz.gov.qa"
DELAY = 2.0

INDEX_PAGES = [
    f"{BASE_URL}/resource/laws/",
    f"{BASE_URL}/authority/regulations/",
]

# Exclude non-regulatory documents (FAQs, forms, templates)
EXCLUDE_PATTERNS = [
    r"faq",
    r"declaration.form",
    r"ubo.*form",
    r"template",
]


def _fetch_page(url: str) -> Optional[str]:
    """Fetch an HTML page."""
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            })
            if r.status_code == 200:
                return r.text
            logger.warning("Page fetch attempt %d for %s: HTTP %d", attempt + 1, url, r.status_code)
        except Exception as e:
            logger.warning("Page fetch attempt %d for %s: %s", attempt + 1, url, e)
        if attempt < 2:
            time.sleep(3)
    return None


def _extract_pdf_links(html: str, base_url: str) -> List[Tuple[str, str]]:
    """Extract (title, url) pairs for PDF links from HTML."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".pdf"):
            continue

        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)

        title = a.get_text(strip=True)
        if not title or len(title) < 3:
            title = _title_from_url(url)

        title = re.sub(r"\s+", " ", title).strip()
        if title.lower().endswith(".pdf"):
            title = title[:-4].strip()

        results.append((title, url))

    return results


def _title_from_url(url: str) -> str:
    """Extract readable title from PDF URL."""
    m = re.search(r"/([^/]+)$", url)
    if m:
        name = unquote(m.group(1))
        name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[_-]+", " ", name)
        return name.strip()
    return ""


def _extract_year(text: str) -> Optional[str]:
    """Extract a year from text."""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        year = int(m.group(1))
        if 1990 <= year <= 2030:
            return f"{year}-01-01"
    return None


def _make_id(url: str) -> str:
    """Create a stable document ID from URL."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", unquote(url)).strip("_").lower()
    if len(slug) > 80:
        slug = slug[:80]
    return f"QA_QFZ_{abs(hash(slug)) % 10**10}"


def _download_pdf(url: str) -> Optional[bytes]:
    """Download a PDF file."""
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            r = requests.get(url, timeout=60, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            })
            if r.status_code == 200 and len(r.content) > 200:
                return r.content
            logger.warning("PDF download attempt %d: HTTP %d, %d bytes from %s",
                           attempt + 1, r.status_code, len(r.content), url)
        except Exception as e:
            logger.warning("PDF download attempt %d: %s", attempt + 1, e)
        if attempt < 2:
            time.sleep(3)
    return None


class FreeZonesLawsScraper(BaseScraper):
    """Scraper for QA/FreeZonesLaws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _collect_pdf_refs(self) -> List[Tuple[str, str]]:
        """Collect all PDF references from index pages."""
        all_refs = []
        seen_urls = set()

        for page_url in INDEX_PAGES:
            logger.info("Fetching index page: %s", page_url)
            html = _fetch_page(page_url)
            if html is None:
                logger.warning("Could not fetch %s", page_url)
                continue

            refs = _extract_pdf_links(html, page_url)
            for title, url in refs:
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_refs.append((title, url))

            logger.info("Found %d PDF links on %s", len(refs), page_url)

        # Filter out non-regulatory documents
        filtered = []
        for title, url in all_refs:
            combined = (title + " " + unquote(url)).lower()
            if any(re.search(pat, combined) for pat in EXCLUDE_PATTERNS):
                logger.debug("Excluded: %s", title[:60])
                continue
            filtered.append((title, url))

        logger.info("Total unique PDF references: %d (after filtering %d excluded)",
                     len(filtered), len(all_refs) - len(filtered))
        return filtered

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "QA/FreeZonesLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        pdf_refs = self._collect_pdf_refs()
        if not pdf_refs:
            logger.error("No PDF references found")
            return

        count = 0
        for title, url in pdf_refs:
            if max_records and count >= max_records:
                return

            doc_id = _make_id(url)
            logger.info("Downloading PDF [%d/%d]: %s", count + 1, len(pdf_refs), title[:60])

            pdf_bytes = _download_pdf(url)
            if pdf_bytes is None:
                logger.warning("Failed to download: %s", url)
                continue

            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a valid PDF: %s", url)
                continue

            try:
                # force=True: the rows already in Neon hold this corpus in
                # character-reversed visual order (issue #1560), and the helper
                # skips any document it finds stored with non-empty text — so
                # without it the re-crawl meant to replace them emits nothing.
                # table= was defaulting to "case_law", which pointed the
                # idempotency preload at the wrong table for a statute corpus.
                text = extract_pdf_markdown(
                    source="QA/FreeZonesLaws",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                    table="legislation",
                    force=True,
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
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        logger.info("Testing access to qfz.gov.qa...")
        html = _fetch_page(f"{BASE_URL}/resource/laws/")
        if html is None:
            logger.error("Cannot reach qfz.gov.qa")
            return False

        refs = _extract_pdf_links(html, f"{BASE_URL}/resource/laws/")
        logger.info("Laws page: %d PDF links", len(refs))

        if refs:
            test_title, test_url = refs[0]
            logger.info("Test download: %s", test_title[:60])
            pdf_bytes = _download_pdf(test_url)
            if pdf_bytes and len(pdf_bytes) > 200:
                logger.info("PDF download OK: %d bytes", len(pdf_bytes))
            else:
                logger.warning("PDF download failed")

        return len(refs) > 0


def main():
    parser = argparse.ArgumentParser(description="QA/FreeZonesLaws data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FreeZonesLawsScraper()

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
