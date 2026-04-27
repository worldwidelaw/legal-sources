#!/usr/bin/env python3
"""
AE/ADGM-Legislation -- ADGM Legal Framework Fetcher

Fetches legislation from ADGM by scraping PDF links from two pages:
  1. Courts legislation and procedures page (57 PDFs)
  2. Abu Dhabi founding legislation page (3 PDFs)

All PDFs are hosted on assets.adgm.com with no authentication.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator
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
logger = logging.getLogger("legal-data-hunter.AE.ADGM-Legislation")

# Pages containing PDF links
LEGISLATION_PAGES = [
    {
        "url": "https://www.adgm.com/adgm-courts/legislation-and-procedures",
        "category": "courts",
    },
    {
        "url": "https://www.adgm.com/legal-framework/abu-dhabi-legislation",
        "category": "abu_dhabi",
    },
]


def title_from_url(pdf_url: str) -> str:
    """Extract a readable title from the PDF URL filename."""
    # Get the filename part (before the hash UUID)
    parts = pdf_url.split("/assets/")[-1]
    filename = parts.split("/")[0]
    # Decode URL encoding and replace + with spaces
    title = unquote(filename.replace("+", " "))
    # Remove .pdf extension
    title = re.sub(r"\.pdf$", "", title, flags=re.IGNORECASE)
    # Clean up extra whitespace
    title = re.sub(r"\s+", " ", title).strip()
    return title


def id_from_url(pdf_url: str) -> str:
    """Generate a stable ID from the PDF URL."""
    title = title_from_url(pdf_url)
    # Slugify: lowercase, replace non-alphanum with hyphens
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    # Truncate to reasonable length
    if len(slug) > 80:
        slug = slug[:80].rsplit("-", 1)[0]
    return f"ADGM-LEG-{slug}"


class ADGMLegislationScraper(BaseScraper):
    """
    Scraper for AE/ADGM-Legislation -- ADGM Legal Framework.
    Country: AE
    URL: https://www.adgm.com/legal-framework

    Data types: legislation
    Auth: none (public PDF downloads from assets.adgm.com)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- Helpers ------------------------------------------------------------

    def _extract_pdf_links(self, page_url: str, category: str) -> list[dict]:
        """Fetch a page and extract all assets.adgm.com PDF links."""
        self.rate_limiter.wait()
        resp = self.client.get(page_url)
        resp.raise_for_status()
        html = resp.text

        # Find all PDF URLs
        pattern = re.compile(
            r'href="(https://assets\.adgm\.com/download/assets/[^"]+)"'
        )
        raw_urls = pattern.findall(html)

        # Decode HTML entities and deduplicate while preserving order
        seen = set()
        results = []
        for raw_url in raw_urls:
            url = unescape(raw_url)
            if url in seen:
                continue
            seen.add(url)

            title = title_from_url(url)
            doc_id = id_from_url(url)

            results.append({
                "pdf_url": url,
                "title": title,
                "doc_id": doc_id,
                "category": category,
                "source_page": page_url,
            })

        return results

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> str:
        """Extract text from legislation PDF."""
        if not pdf_url:
            return ""
        return extract_pdf_markdown(
            source="AE/ADGM-Legislation",
            source_id=source_id,
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents from ADGM pages."""
        for page_info in LEGISLATION_PAGES:
            logger.info(f"Fetching PDFs from {page_info['url']}...")
            try:
                docs = self._extract_pdf_links(
                    page_info["url"], page_info["category"]
                )
                logger.info(f"  Found {len(docs)} PDFs")
                for doc in docs:
                    yield doc
            except Exception as e:
                logger.error(f"Error fetching {page_info['url']}: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (no date filtering possible for PDFs)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF info into standard schema."""
        pdf_url = raw.get("pdf_url", "")
        source_id = raw.get("doc_id", "")

        # Extract full text from PDF
        full_text = self._extract_pdf_text(pdf_url, source_id)

        # Try to extract year from title
        date_str = ""
        year_match = re.search(r"\b(20\d{2})\b", raw.get("title", ""))
        if year_match:
            date_str = f"{year_match.group(1)}-01-01"

        # Try to extract more specific date from title
        date_match = re.search(r"(\d{2})(\d{2})(20\d{2})", raw.get("title", ""))
        if date_match:
            day, month, year = date_match.groups()
            if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                date_str = f"{year}-{month}-{day}"

        return {
            "_id": source_id,
            "_source": "AE/ADGM-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": full_text,
            "date": date_str,
            "url": pdf_url,
            "category": raw.get("category", ""),
            "source_page": raw.get("source_page", ""),
            "country": "AE",
            "language": "en",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing ADGM Legislation pages...")

        total = 0
        for page_info in LEGISLATION_PAGES:
            docs = self._extract_pdf_links(
                page_info["url"], page_info["category"]
            )
            print(f"  {page_info['category']}: {len(docs)} PDFs from {page_info['url']}")
            total += len(docs)
            if docs:
                print(f"    First: {docs[0]['title'][:80]}")

        print(f"\n  Total PDFs: {total}")

        # Test PDF extraction
        if total > 0:
            all_docs = []
            for page_info in LEGISLATION_PAGES:
                all_docs.extend(
                    self._extract_pdf_links(page_info["url"], page_info["category"])
                )
            doc = all_docs[0]
            print(f"\n  Testing PDF extraction: {doc['title'][:60]}...")
            text = self._extract_pdf_text(doc["pdf_url"], doc["doc_id"])
            if text:
                print(f"  PDF extraction: SUCCESS ({len(text)} chars)")
                print(f"  Sample: {text[:200]}...")
            else:
                print("  PDF extraction: FAILED")

        print("\nTest completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = ADGMLegislationScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
