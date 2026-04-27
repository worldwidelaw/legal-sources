#!/usr/bin/env python3
"""
Legal Data Hunter - Vanuatu Revenue Tax Guidance Scraper

Fetches tax and customs legislation, regulations, and guidance PDFs from the
Vanuatu Department of Customs and Inland Revenue (DCIR) website.

Strategy:
  - Crawl index pages on customsinlandrevenue.gov.vu for PDF links
  - Download each PDF and extract full text via common/pdf_extract
  - Covers: VAT, business licensing, tax administration, import/export
    duties, excise tax, customs regulations, and related guidance

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
"""

import re
import sys
import json
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

from bs4 import BeautifulSoup

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
logger = logging.getLogger("VU/Revenue-TaxGuidance")

MAX_PDF_SIZE = 30 * 1024 * 1024  # 30MB

BASE_URL = "https://customsinlandrevenue.gov.vu"

# Pages to crawl for PDF links
INDEX_PAGES = [
    "/taxes-and-licensing/legislations.html",
    "/customs/legislations/customs-act.html",
    "/customs/legislations/excise-tax.html",
    "/customs/legislations/import-duties.html",
    "/customs/legislations/export-duties.html",
    "/customs/legislations/regulations.html",
    "/customs/legislations/others.html",
]

# Skip non-content PDFs
SKIP_PATTERNS = [
    "Client_Service_Charter",
    "General_documents",
]


class VURevenueTaxGuidanceScraper(BaseScraper):
    """
    Scraper for Vanuatu DCIR tax and customs guidance.

    Crawls index pages for PDF links, downloads each PDF,
    and extracts full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "text/html",
            },
            timeout=30,
        )

    def _collect_pdf_links(self) -> list[dict]:
        """Crawl all index pages and collect unique PDF links with titles."""
        seen_urls = set()
        pdfs = []

        for page_path in INDEX_PAGES:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(page_path)
                if resp.status_code != 200:
                    logger.warning(f"Page returned {resp.status_code}: {page_path}")
                    continue
            except Exception as e:
                logger.warning(f"Failed to fetch {page_path}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if ".pdf" not in href.lower():
                    continue

                # Skip non-content PDFs
                if any(pat in href for pat in SKIP_PATTERNS):
                    continue

                # Resolve relative URLs
                full_url = urljoin(BASE_URL + page_path, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                title = a.get_text(strip=True)
                if not title:
                    # Extract title from filename
                    title = unquote(href.split("/")[-1].replace(".pdf", "").replace("_", " "))

                # Determine category from page path
                category = "tax"
                if "customs" in page_path:
                    category = "customs"
                if "import" in page_path:
                    category = "import_duties"
                elif "export" in page_path:
                    category = "export_duties"
                elif "excise" in page_path:
                    category = "excise"
                elif "regulations" in page_path:
                    category = "regulations"
                elif "others" in page_path:
                    category = "related_legislation"

                pdfs.append({
                    "url": full_url,
                    "title": title,
                    "category": category,
                    "source_page": page_path,
                })

        logger.info(f"Collected {len(pdfs)} unique PDF links from {len(INDEX_PAGES)} pages")
        return pdfs

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF from the DCIR website."""
        self.rate_limiter.wait()
        try:
            import requests
            resp = requests.get(url, headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
            }, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"PDF download returned {resp.status_code}: {url}")
                return None
            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
                logger.warning(f"Not a PDF ({content_type}): {url}")
                return None
            if len(resp.content) > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"PDF download failed: {url}: {e}")
            return None

    def _make_doc_id(self, url: str) -> str:
        """Generate a stable document ID from the URL."""
        # Use the path portion after /images/
        path = url.split("customsinlandrevenue.gov.vu")[-1]
        # Clean up for readability
        path = unquote(path).replace("/images/", "").replace("/", "_").replace(" ", "_")
        if path.lower().endswith(".pdf"):
            path = path[:-4]
        # Truncate if too long
        if len(path) > 120:
            path = path[:100] + "_" + hashlib.md5(path.encode()).hexdigest()[:8]
        return path

    def _extract_year(self, title: str) -> Optional[str]:
        """Try to extract a year from the document title."""
        years = re.findall(r"\b(19\d{2}|20[0-2]\d)\b", title)
        if years:
            return years[-1]
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all DCIR guidance documents with full text."""
        pdfs = self._collect_pdf_links()
        count = 0
        skipped = 0

        for info in pdfs:
            pdf_bytes = self._download_pdf(info["url"])
            if not pdf_bytes:
                skipped += 1
                continue

            doc_id = self._make_doc_id(info["url"])
            text = extract_pdf_markdown(
                source="VU/Revenue-TaxGuidance",
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="doctrine",
            ) or ""

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {info['title']}: {len(text)} chars")
                skipped += 1
                continue

            count += 1
            yield {
                "doc_id": doc_id,
                "title": info["title"],
                "text": text,
                "url": info["url"],
                "category": info["category"],
                "source_page": info["source_page"],
                "year": self._extract_year(info["title"]),
            }

            if count % 10 == 0:
                logger.info(f"  {count} documents fetched ({skipped} skipped)")

        logger.info(f"Total: {count} documents with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all documents (no incremental updates available)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        year = raw.get("year")
        date_iso = f"{year}-01-01" if year else None

        return {
            "_id": f"VU/Revenue-TaxGuidance/{raw.get('doc_id', '')}",
            "_source": "VU/Revenue-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw.get("doc_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_iso,
            "category": raw.get("category", ""),
            "url": raw.get("url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = VURevenueTaxGuidanceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
