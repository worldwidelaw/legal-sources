#!/usr/bin/env python3
"""
JO/ConstitutionalCourt -- Jordan Constitutional Court Decisions Fetcher

Fetches rulings and interpretative decisions from cco.gov.jo.

Strategy:
  - Paginate through two listing pages: Rulings and Interpretative Decisions
  - Extract PDF URLs from the HTML listings
  - Download PDFs and extract full text using pdfplumber

Endpoints:
  - Rulings: https://cco.gov.jo/EN/List/Rullings_of_the_Court?page={N}
  - Decisions: https://cco.gov.jo/EN/List/Decisions_of_the_Court?page={N}
  - PDFs: https://cco.gov.jo/ebv4.0/root_storage/en/eb_list_page/{filename}.pdf

Data:
  - ~55 rulings + ~18 interpretative decisions (2012-present)
  - English translations available
  - Rate limit: 2 seconds between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JO.ConstitutionalCourt")

BASE_URL = "https://cco.gov.jo"

# Two categories of decisions
LISTING_PAGES = [
    {
        "path": "/EN/List/Rullings_of_the_Court",
        "type": "ruling",
        "max_pages": 10,
    },
    {
        "path": "/EN/List/Decisions_of_the_Court",
        "type": "interpretative_decision",
        "max_pages": 5,
    },
]


class JordanConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for JO/ConstitutionalCourt -- Jordan Constitutional Court.
    Country: JO
    URL: https://cco.gov.jo/EN/

    Data types: case_law
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            },
            timeout=120,
        )

    def _get_listings_page(self, path: str, page: int, decision_type: str) -> List[Dict]:
        """
        Fetch a page of decisions from a listing.
        Returns list of dicts with title, pdf_url, decision_type.
        """
        items = []

        try:
            self.rate_limiter.wait()
            url = f"{path}?page={page}" if page > 1 else path
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Extract PDF links from the listing
            # Pattern: href="/ebv4.0/root_storage/en/eb_list_page/filename.pdf"
            pdf_pattern = re.compile(
                r'href="(/ebv4\.0/root_storage/en/eb_list_page/[^"]+\.pdf)"',
                re.IGNORECASE
            )

            # Also extract titles - they appear near the PDF links
            # The structure has title text before or near the PDF link
            # Try to find title + PDF pairs
            # Pattern: title text followed by PDF link in nearby HTML
            item_pattern = re.compile(
                r'<(?:h[2-4]|a|strong|div)[^>]*>\s*'
                r'((?:Judgment|Ruling|Decision|Interpretative)[^<]*?(?:No\.?\s*\(?[\d]+\)?\s*(?:of\s*)?\d{4})[^<]*)'
                r'</(?:h[2-4]|a|strong|div)>',
                re.IGNORECASE | re.DOTALL
            )

            # First collect all PDF URLs
            pdf_urls = pdf_pattern.findall(content)

            # Try to find titles for each PDF
            titles = item_pattern.findall(content)

            # Clean titles
            titles = [html.unescape(t.strip()) for t in titles]

            # If we have matching counts, pair them
            if pdf_urls:
                for i, pdf_url in enumerate(pdf_urls):
                    title = titles[i] if i < len(titles) else ""

                    # Extract number and year from title or filename
                    num_year_match = re.search(r'(\d+)[-_\s]*(?:of\s*)?(\d{4})', title or pdf_url)
                    number = num_year_match.group(1) if num_year_match else ""
                    year = num_year_match.group(2) if num_year_match else ""

                    # If no title, derive from filename
                    if not title:
                        filename = unquote(pdf_url.split('/')[-1].replace('.pdf', ''))
                        filename = filename.replace('_', ' ').replace('-', ' ').strip()
                        title = filename.title()

                    items.append({
                        "title": title,
                        "pdf_url": pdf_url,
                        "decision_type": decision_type,
                        "number": number,
                        "year": year,
                        "decision_number": f"{number}/{year}" if number and year else "",
                    })

            logger.info(f"{path} page {page}: Found {len(items)} items")
            return items

        except Exception as e:
            logger.warning(f"Failed to get {path} page {page}: {e}")
            return []

    def _download_and_extract_pdf(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="JO/ConstitutionalCourt",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from both listing categories."""
        seen = set()

        for listing in LISTING_PAGES:
            path = listing["path"]
            decision_type = listing["type"]
            max_pages = listing["max_pages"]

            for page in range(1, max_pages + 1):
                items = self._get_listings_page(path, page, decision_type)

                if not items:
                    break

                for item in items:
                    pdf_url = item["pdf_url"]
                    if pdf_url in seen:
                        continue
                    seen.add(pdf_url)

                    full_text = self._download_and_extract_pdf(pdf_url)

                    if not full_text:
                        logger.warning(f"No text for {item['title']}, skipping")
                        continue

                    item["full_text"] = full_text
                    yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all decisions (small corpus, re-fetch all)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        decision_type = raw.get("decision_type", "ruling")
        number = raw.get("number", "")
        year = raw.get("year", "")
        decision_number = raw.get("decision_number", "")

        # Build document ID
        if decision_number:
            doc_id = f"JO-CC-{decision_type}-{number}-{year}"
        else:
            # Fallback: use PDF filename
            pdf_url = raw.get("pdf_url", "")
            filename = pdf_url.split('/')[-1].replace('.pdf', '')
            doc_id = f"JO-CC-{filename}"

        # Build full URL
        pdf_url = raw.get("pdf_url", "")
        full_url = f"{BASE_URL}{pdf_url}" if pdf_url.startswith('/') else pdf_url

        # Try to extract a date from the text
        text = raw.get("full_text", "")
        date_str = ""
        if year:
            date_str = f"{year}-01-01"  # Approximate to year

        return {
            "_id": doc_id,
            "_source": "JO/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_str,
            "url": full_url,
            "decision_number": decision_number,
            "decision_type": decision_type,
            "court": "Constitutional Court of Jordan",
            "year": year,
            "jurisdiction": "JO",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Jordan Constitutional Court endpoints...")
        print(f"pdfplumber available: {HAS_PDFPLUMBER}")

        for listing in LISTING_PAGES:
            path = listing["path"]
            dtype = listing["type"]
            print(f"\n1. Testing {dtype} listing ({path})...")
            try:
                items = self._get_listings_page(path, 1, dtype)
                print(f"   Found {len(items)} items")
                if items:
                    print(f"   Sample: {items[0]['title'][:60]}...")
                    print(f"   PDF: {items[0]['pdf_url']}")
            except Exception as e:
                print(f"   ERROR: {e}")

            if items and HAS_PDFPLUMBER:
                print(f"\n2. Testing PDF extraction for {dtype}...")
                try:
                    text = self._download_and_extract_pdf(items[0]["pdf_url"])
                    print(f"   Extracted {len(text)} characters")
                    if text:
                        print(f"   Sample: {text[:200]}...")
                except Exception as e:
                    print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = JordanConstitutionalCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
