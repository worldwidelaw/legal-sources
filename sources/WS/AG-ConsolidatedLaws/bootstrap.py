#!/usr/bin/env python3
"""
WS/AG-ConsolidatedLaws -- Samoa Attorney General Consolidated Laws 2023

Fetches all consolidated legislation of Samoa from the Office of the
Attorney General website. ~267 Acts and Ordinances available as PDFs.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
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
logger = logging.getLogger("legal-data-hunter.WS.AG-ConsolidatedLaws")

SOURCE_ID = "WS/AG-ConsolidatedLaws"
INDEX_URL = "https://www.ag.gov.ws/consolidation-of-laws-of-samoa/"


def title_from_filename(filename: str) -> str:
    """Convert PDF filename to human-readable title."""
    # Remove .pdf extension
    name = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    # Replace hyphens with spaces
    name = name.replace("-", " ")
    # Fix common patterns
    name = re.sub(r"\s+", " ", name).strip()
    return name


def id_from_filename(filename: str) -> str:
    """Generate a stable ID from the PDF filename."""
    # Remove .pdf and lowercase
    slug = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    slug = slug.lower().strip()
    # Truncate to reasonable length
    if len(slug) > 100:
        slug = slug[:100].rsplit("-", 1)[0]
    return slug


def extract_year(title: str) -> str:
    """Extract the year from a title like 'Crimes Act 2013'."""
    # Look for 4-digit year (1900-2029)
    match = re.search(r"\b(19\d{2}|20[0-2]\d)\b", title)
    if match:
        return f"{match.group(1)}-01-01"
    return ""


class AGConsolidatedLawsScraper(BaseScraper):
    """
    Scraper for WS/AG-ConsolidatedLaws -- Samoa consolidated legislation.
    Country: WS
    URL: https://www.ag.gov.ws/consolidation-of-laws-of-samoa/

    Data types: legislation
    Auth: none (public PDF downloads)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _discover_pdfs(self) -> list[dict]:
        """Fetch the index page and extract all PDF links."""
        self.rate_limiter.wait()
        resp = self.client.get(INDEX_URL)
        resp.raise_for_status()
        html = resp.text

        # Find all PDF URLs from wp-content/uploads
        pattern = re.compile(
            r'href="(https?://www\.ag\.gov\.ws/wp-content/uploads/[^"]+\.pdf)"',
            re.IGNORECASE,
        )
        raw_urls = pattern.findall(html)

        # Deduplicate while preserving order
        seen = set()
        results = []
        for url in raw_urls:
            if url in seen:
                continue
            seen.add(url)

            # Extract filename from URL
            filename = unquote(url.split("/")[-1])
            title = title_from_filename(filename)
            doc_id = id_from_filename(filename)

            results.append({
                "pdf_url": url,
                "title": title,
                "doc_id": doc_id,
                "filename": filename,
            })

        logger.info(f"Discovered {len(results)} PDF documents")
        return results

    def _extract_text(self, pdf_url: str, source_id: str) -> str:
        """Extract text from a legislation PDF."""
        if not pdf_url:
            return ""
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all consolidated legislation documents."""
        docs = self._discover_pdfs()
        logger.info(f"Starting fetch of {len(docs)} documents")
        for doc in docs:
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (static collection, no date filtering)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF metadata into standard schema."""
        pdf_url = raw.get("pdf_url", "")
        source_id = raw.get("doc_id", "")
        title = raw.get("title", "")

        # Extract full text from PDF
        full_text = self._extract_text(pdf_url, source_id)

        # Extract year from title
        date_str = extract_year(title)

        return {
            "_id": source_id,
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": pdf_url,
            "country": "WS",
            "language": "en",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Samoa AG Consolidated Laws page...")
        docs = self._discover_pdfs()
        print(f"  Found {len(docs)} PDF documents")
        if docs:
            print(f"  First: {docs[0]['title']}")
            print(f"  Last:  {docs[-1]['title']}")
            # Test downloading first PDF
            print(f"\n  Testing PDF download: {docs[0]['pdf_url'][:80]}...")
            text = self._extract_text(docs[0]["pdf_url"], docs[0]["doc_id"])
            if text:
                print(f"  SUCCESS: extracted {len(text)} chars")
                print(f"  Preview: {text[:200]}...")
            else:
                print("  FAILED: no text extracted")


def main():
    scraper = AGConsolidatedLawsScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
