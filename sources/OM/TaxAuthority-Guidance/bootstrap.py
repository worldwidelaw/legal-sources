#!/usr/bin/env python3
"""
OM/TaxAuthority-Guidance -- Oman Tax Authority Circulars and Guidance

Fetches tax laws, regulations, guidelines, circulars, and chairman decisions
from the Oman Tax Authority portal (tms.taxoman.gov.om).

Strategy:
  - Scrape category pages from the Liferay-based portal
  - Extract PDF download links from HTML
  - Download PDFs and extract text via common.pdf_extract
  - Categories: Income Tax, VAT, Excise Tax, Personal Income Tax,
    Double Tax Agreements (laws, regulations, guidelines for each)

Data Coverage:
  - ~100+ documents: Royal Decrees, Executive Regulations, Chairman
    Decisions, Guidelines, Tax Treaties
  - Income Tax, VAT (introduced 2021), Excise Tax, PIT (effective 2028)
  - Arabic and English documents

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.OM.TaxAuthority-Guidance")

BASE_URL = "https://tms.taxoman.gov.om"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Category pages to scrape — each has PDF document links
CATEGORIES = [
    {
        "url": "/portal/web/taxportal/income-tax-law-regulations",
        "name": "Income Tax Law & Regulations",
        "data_type": "legislation",
        "prefix": "IT-LAW",
    },
    {
        "url": "/portal/web/taxportal/income-tax-guidelines",
        "name": "Income Tax Guidelines",
        "data_type": "doctrine",
        "prefix": "IT-GUIDE",
    },
    {
        "url": "/portal/web/taxportal/vat-law-regulations",
        "name": "VAT Law & Regulations",
        "data_type": "legislation",
        "prefix": "VAT-LAW",
    },
    {
        "url": "/portal/web/taxportal/vat-guidelines",
        "name": "VAT Guidelines",
        "data_type": "doctrine",
        "prefix": "VAT-GUIDE",
    },
    {
        "url": "/portal/web/taxportal/excise-tax-law-regulations",
        "name": "Excise Tax Law & Regulations",
        "data_type": "legislation",
        "prefix": "EX-LAW",
    },
    {
        "url": "/portal/web/taxportal/excise-tax-guidelines",
        "name": "Excise Tax Guidelines",
        "data_type": "doctrine",
        "prefix": "EX-GUIDE",
    },
    {
        "url": "/portal/web/taxportal/personal-income-tax-law-and-regulation",
        "name": "Personal Income Tax Law & Regulations",
        "data_type": "legislation",
        "prefix": "PIT-LAW",
    },
    {
        "url": "/portal/web/taxportal/pit-guidelines",
        "name": "PIT Guidelines",
        "data_type": "doctrine",
        "prefix": "PIT-GUIDE",
    },
    {
        "url": "/portal/double-tax-agreements",
        "name": "Double Tax Agreements",
        "data_type": "legislation",
        "prefix": "DTA",
    },
]

# Regex to find PDF links in Liferay HTML
# Matches /portal/documents/20126/... paths ending in .pdf (with optional UUID suffix)
PDF_LINK_RE = re.compile(
    r'<a[^>]+href="(/portal/documents/20126/[^"]+\.(?:pdf|PDF)[^"]*)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Also match links where the document path includes .pdf in the middle (Liferay UUID style)
PDF_LINK_ALT_RE = re.compile(
    r'<a[^>]+href="(/portal/documents/20126/[^"]+\.(?:pdf|PDF)/[^"]*)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Fallback: match any href to /portal/documents/ that looks like a downloadable file
DOC_LINK_RE = re.compile(
    r'href="(/portal/documents/20126/[^"]+)"',
    re.IGNORECASE,
)


def _clean_title(raw_html: str) -> str:
    """Strip HTML tags and clean up a title string."""
    text = re.sub(r'<[^>]+>', '', raw_html)
    text = html_module.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _extract_filename(url: str) -> str:
    """Extract a readable filename from a Liferay document URL."""
    # Remove query params and UUID suffix
    path = url.split("?")[0]
    # Get the filename part — may be URL-encoded
    parts = path.split("/")
    # Find the .pdf part
    for i, part in enumerate(parts):
        if part.lower().endswith(".pdf"):
            return unquote(part)
    # If the PDF is in the middle (UUID style), find it
    for part in parts:
        if ".pdf" in part.lower():
            return unquote(part.split(".pdf")[0] + ".pdf")
    return unquote(parts[-1]) if parts else "unknown.pdf"


class OmanTaxScraper(BaseScraper):
    """
    Scraper for OM/TaxAuthority-Guidance.
    Country: OM
    URL: https://tms.taxoman.gov.om/portal/

    Data types: doctrine, legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "en,ar;q=0.9",
        })

    def _fetch_page(self, url_path: str, timeout: int = 30) -> str:
        """Fetch an HTML page from the portal."""
        url = BASE_URL + url_path
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _parse_pdf_links(self, html_content: str) -> List[Dict[str, str]]:
        """Extract PDF document links and titles from a category page."""
        docs = []
        seen_urls = set()

        # Strategy 1: Look for <a> tags with PDF hrefs and link text
        for pattern in [PDF_LINK_RE, PDF_LINK_ALT_RE]:
            for match in pattern.finditer(html_content):
                url_path = match.group(1)
                title_html = match.group(2)
                title = _clean_title(title_html)

                if url_path in seen_urls:
                    continue
                seen_urls.add(url_path)

                # Skip non-PDF files (xlsx, etc.)
                filename = _extract_filename(url_path)
                if not filename.lower().endswith(".pdf"):
                    continue

                if not title:
                    title = filename.replace(".pdf", "").replace("+", " ")

                docs.append({
                    "url_path": url_path,
                    "title": title,
                    "filename": filename,
                })

        # Strategy 2: If no links found with text, use bare href matching
        if not docs:
            for match in DOC_LINK_RE.finditer(html_content):
                url_path = match.group(1)
                if url_path in seen_urls:
                    continue
                seen_urls.add(url_path)

                filename = _extract_filename(url_path)
                # Only include PDF-like documents
                lower = url_path.lower()
                if ".pdf" not in lower and ".xlsx" not in lower:
                    continue
                if ".xlsx" in lower:
                    continue

                title = filename.replace(".pdf", "").replace("+", " ").replace("%20", " ")
                title = unquote(title)

                docs.append({
                    "url_path": url_path,
                    "title": title,
                    "filename": filename,
                })

        return docs

    def _download_pdf_text(self, url_path: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        url = BASE_URL + url_path
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60, headers={
                "Accept": "application/pdf,*/*",
            })
            if resp.status_code != 200:
                logger.warning(f"PDF download failed {url}: HTTP {resp.status_code}")
                return ""
            if not resp.content or len(resp.content) < 100:
                logger.warning(f"Empty PDF content from {url}")
                return ""
            # Check for PDF magic bytes
            if resp.content[:4] != b"%PDF":
                # Some Liferay responses redirect to login or error pages
                logger.warning(f"Not a PDF (wrong magic bytes) from {url}")
                return ""
        except Exception as e:
            logger.warning(f"PDF download error {url}: {e}")
            return ""

        text = extract_pdf_markdown(
            source="OM/TaxAuthority-Guidance",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="doctrine",
        )
        return text or ""

    def _crawl_category(self, category: Dict[str, str]) -> Generator[Dict[str, Any], None, None]:
        """Crawl a single category page and yield documents with full text."""
        cat_name = category["name"]
        prefix = category["prefix"]
        data_type = category["data_type"]

        logger.info(f"Crawling category: {cat_name}")

        html = self._fetch_page(category["url"])
        if not html:
            logger.error(f"Could not access category: {cat_name}")
            return

        docs = self._parse_pdf_links(html)
        logger.info(f"{cat_name}: found {len(docs)} PDF documents")

        for i, doc in enumerate(docs):
            doc_id = f"{prefix}-{i+1:03d}"
            url_path = doc["url_path"]
            title = doc["title"]

            logger.info(f"  [{i+1}/{len(docs)}] {title[:80]}")

            text = self._download_pdf_text(url_path, doc_id)
            if not text or len(text) < 20:
                logger.warning(f"  Insufficient text for {doc_id}: {len(text)} chars")
                continue

            yield {
                "doc_id": doc_id,
                "title": title,
                "full_text": text,
                "pdf_url": BASE_URL + url_path,
                "filename": doc["filename"],
                "category": cat_name,
                "data_type": data_type,
            }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all categories."""
        logger.info("Starting full Oman Tax Authority crawl...")
        for category in CATEGORIES:
            yield from self._crawl_category(category)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-crawl all categories (documents are static, so same as fetch_all)."""
        logger.info(f"Fetching updates since {since.isoformat()}...")
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        doc_id = raw.get("doc_id", "")
        data_type = raw.get("data_type", "doctrine")

        # Map data_type to _type
        if data_type == "legislation":
            record_type = "legislation"
        else:
            record_type = "doctrine"

        return {
            "_id": doc_id,
            "_source": "OM/TaxAuthority-Guidance",
            "_type": record_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": "",
            "url": raw.get("pdf_url", ""),
            "category": raw.get("category", ""),
            "filename": raw.get("filename", ""),
            "language": "ar/en",
        }

    def test_api(self):
        """Quick connectivity and listing test."""
        print("Testing Oman Tax Authority portal...")
        total_docs = 0

        for category in CATEGORIES:
            cat_name = category["name"]
            print(f"\n--- {cat_name} ---")

            html = self._fetch_page(category["url"])
            if not html:
                print("  ERROR: Could not fetch page")
                continue

            docs = self._parse_pdf_links(html)
            print(f"  Documents found: {len(docs)}")
            total_docs += len(docs)

            if docs:
                d = docs[0]
                print(f"  First: {d['title'][:80]}")
                print(f"  URL: {d['url_path'][:100]}")

                # Test PDF download for first doc
                print("  Testing PDF extraction...")
                text = self._download_pdf_text(d["url_path"], "test-0")
                if text:
                    print(f"    Text: {len(text)} chars")
                    preview = text[:150].replace("\n", " ")
                    print(f"    Preview: {preview}...")
                else:
                    print("    WARNING: No text extracted")

        print(f"\nTotal documents across all categories: {total_docs}")
        print("Test complete!")


def main():
    scraper = OmanTaxScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
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
