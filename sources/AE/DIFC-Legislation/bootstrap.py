#!/usr/bin/env python3
"""
AE/DIFC-Legislation -- DIFC Legal Database Fetcher

Fetches legislation from DIFC by scraping PDF links from the legal database page:
  https://www.difc.com/business/laws-and-regulations/legal-database

Categories: DIFC Laws, Regulations, Amendment Laws.
All PDFs hosted on assets.difc.com or edge.sitecorecloud.io (no auth).

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
from typing import Generator, Optional
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
logger = logging.getLogger("legal-data-hunter.AE.DIFC-Legislation")

LEGAL_DB_URL = "https://www.difc.com/business/laws-and-regulations/legal-database"

# PDF URL patterns for DIFC documents
PDF_PATTERNS = [
    re.compile(r'href="(https://assets\.difc\.com/[^"]+\.pdf)"', re.IGNORECASE),
    re.compile(r'href="(https://edge\.sitecorecloud\.io/[^"]+\.pdf)"', re.IGNORECASE),
]


def title_from_url(pdf_url: str) -> str:
    """Extract a readable title from the PDF URL filename."""
    # Get the last path segment (the filename)
    filename = pdf_url.rstrip("/").split("/")[-1]
    # Decode URL encoding
    filename = unquote(filename)
    # Remove .pdf extension
    title = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    # Replace underscores and hyphens with spaces
    title = title.replace("_", " ").replace("-", " ")
    # Clean up extra whitespace
    title = re.sub(r"\s+", " ", title).strip()
    # Title-case
    title = title.title()
    return title


def id_from_url(pdf_url: str) -> str:
    """Generate a stable ID from the PDF URL."""
    # Use hash of URL for stability
    url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
    title = title_from_url(pdf_url)
    # Slugify: lowercase, replace non-alphanum with hyphens
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    # Truncate to reasonable length
    if len(slug) > 70:
        slug = slug[:70].rsplit("-", 1)[0]
    return f"DIFC-LEG-{slug}"


def classify_document(title: str, pdf_url: str) -> str:
    """Classify document as difc_law, regulation, or amendment_law."""
    title_lower = title.lower()
    # Get just the filename from the URL for classification
    filename = pdf_url.rstrip("/").split("/")[-1].lower()
    if "amendment" in title_lower or "amendment" in filename:
        return "amendment_law"
    if "regulation" in title_lower or "regulation" in filename:
        return "regulation"
    return "difc_law"


class DIFCLegislationScraper(BaseScraper):
    """
    Scraper for AE/DIFC-Legislation -- DIFC Legal Database.
    Country: AE
    URL: https://www.difc.com/business/laws-and-regulations/legal-database

    Data types: legislation
    Auth: none (public PDF downloads)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=90,
        )

    # -- Helpers ------------------------------------------------------------

    def _extract_pdf_links(self) -> list[dict]:
        """Fetch legal database page and extract all PDF links."""
        self.rate_limiter.wait()
        resp = self.client.get(LEGAL_DB_URL)
        resp.raise_for_status()
        html = resp.text

        # Find all PDF URLs matching our patterns
        seen = set()
        results = []
        for pattern in PDF_PATTERNS:
            raw_urls = pattern.findall(html)
            for raw_url in raw_urls:
                url = unescape(raw_url)
                if url in seen:
                    continue
                seen.add(url)

                title = title_from_url(url)
                doc_id = id_from_url(url)
                category = classify_document(title, url)

                results.append({
                    "pdf_url": url,
                    "title": title,
                    "doc_id": doc_id,
                    "category": category,
                })

        logger.info(f"Found {len(results)} PDFs on legal database page")
        return results

    def _find_title_from_html(self, html: str, pdf_url: str) -> Optional[str]:
        """Try to find the link text associated with a PDF URL in the HTML."""
        # Look for patterns like <a href="URL">TITLE</a>
        escaped_url = re.escape(pdf_url)
        pattern = re.compile(
            rf'<a[^>]*href="{escaped_url}"[^>]*>([^<]+)</a>',
            re.IGNORECASE,
        )
        match = pattern.search(html)
        if match:
            return unescape(match.group(1)).strip()
        return None

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> str:
        """Extract text from legislation PDF."""
        if not pdf_url:
            return ""
        return extract_pdf_markdown(
            source="AE/DIFC-Legislation",
            source_id=source_id,
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents from DIFC legal database."""
        logger.info(f"Fetching PDFs from {LEGAL_DB_URL}...")

        # Also fetch the raw HTML to try to get better titles
        self.rate_limiter.wait()
        resp = self.client.get(LEGAL_DB_URL)
        resp.raise_for_status()
        html = resp.text

        docs = self._extract_pdf_links()
        for doc in docs:
            # Try to find a better title from the link text
            better_title = self._find_title_from_html(html, doc["pdf_url"])
            if better_title and len(better_title) > 5:
                doc["title"] = better_title
                doc["category"] = classify_document(better_title, doc["pdf_url"])
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (no date filtering for static PDFs)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF info into standard schema."""
        pdf_url = raw.get("pdf_url", "")
        source_id = raw.get("doc_id", "")

        # Extract full text from PDF
        full_text = self._extract_pdf_text(pdf_url, source_id)

        # Try to extract year from title
        date_str = ""
        year_match = re.search(r"\b((?:19|20)\d{2})\b", raw.get("title", ""))
        if year_match:
            date_str = f"{year_match.group(1)}-01-01"

        return {
            "_id": source_id,
            "_source": "AE/DIFC-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": full_text,
            "date": date_str,
            "url": pdf_url,
            "category": raw.get("category", ""),
            "country": "AE",
            "jurisdiction": "DIFC",
            "language": "en",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing DIFC Legal Database...")

        docs = self._extract_pdf_links()
        by_cat = {}
        for d in docs:
            by_cat.setdefault(d["category"], []).append(d)

        for cat, items in sorted(by_cat.items()):
            print(f"  {cat}: {len(items)} PDFs")
            if items:
                print(f"    First: {items[0]['title'][:80]}")

        print(f"\n  Total PDFs: {len(docs)}")

        # Test PDF extraction on first doc
        if docs:
            doc = docs[0]
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
    scraper = DIFCLegislationScraper()

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
