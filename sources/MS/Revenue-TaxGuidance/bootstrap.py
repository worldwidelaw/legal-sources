#!/usr/bin/env python3
"""
Legal Data Hunter - Montserrat Tax Guidance Scraper

Fetches tax-related legislation, SROs, and budget documents from Montserrat's
government website (gov.ms).

Strategy:
  - Crawl revised acts page for tax/customs/revenue/finance-related PDFs
  - Crawl SRO pages for tax-related statutory instruments
  - Crawl budget publications
  - Download PDFs and extract full text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
"""

import re
import sys
import json
import hashlib
import logging
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
logger = logging.getLogger("MS/Revenue-TaxGuidance")

MAX_PDF_SIZE = 30 * 1024 * 1024  # 30MB

BASE_URL = "https://www.gov.ms"

# Pages to crawl
INDEX_PAGES = [
    "/government/legal-department/attorney-generals-chambers/acts-revised/",
    "/government/legal-department/attorney-generals-chambers/sros/",
    "/government/legal-department/attorney-generals-chambers/sros-2018/",
    "/government/legal-department/attorney-generals-chambers/sros-2015-2016/",
    "/government/legal-department/attorney-generals-chambers/sros-2013-2014/",
    "/government/ministries/ministry-of-finance-economic-management/",
    "/government/ministries/ministry-of-finance-economic-management/finance-publications-budgets",
]

# Keywords to filter tax-related documents
TAX_KEYWORDS = [
    "tax", "duty", "duties", "customs", "revenue", "stamp", "income",
    "finance", "fiscal", "budget", "tariff", "excise", "consumption",
    "embarkation", "hotel", "property tax", "compliance", "exchange",
    "telecommunications service",
]


def is_tax_related(title: str, href: str) -> bool:
    """Check if a document is tax/revenue-related."""
    combined = (title + " " + href).lower()
    return any(kw in combined for kw in TAX_KEYWORDS)


class MSRevenueTaxGuidanceScraper(BaseScraper):
    """
    Scraper for Montserrat tax guidance documents.

    Crawls government pages for tax-related PDFs (acts, SROs, budgets)
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
        """Crawl index pages and collect tax-related PDF links."""
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

                # Resolve relative URLs
                full_url = urljoin(BASE_URL + page_path, href)
                # Remove fragment
                full_url = full_url.split("#")[0]

                if full_url in seen_urls:
                    continue

                title = a.get_text(strip=True)
                if not title:
                    title = unquote(href.split("/")[-1].replace(".pdf", "").replace("-", " "))

                # For acts page, only include tax-related; for SRO/budget pages, include all
                if "acts-revised" in page_path:
                    if not is_tax_related(title, href):
                        continue
                elif "sros" in page_path:
                    if not is_tax_related(title, href):
                        continue
                # Budget/finance pages: include all PDFs

                seen_urls.add(full_url)

                # Determine category
                category = "legislation"
                if "sro" in page_path.lower():
                    category = "statutory_instrument"
                elif "budget" in page_path.lower() or "finance" in page_path.lower():
                    category = "budget"

                pdfs.append({
                    "url": full_url,
                    "title": title,
                    "category": category,
                    "source_page": page_path,
                })

        logger.info(f"Collected {len(pdfs)} tax-related PDF links from {len(INDEX_PAGES)} pages")
        return pdfs

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF."""
        self.rate_limiter.wait()
        try:
            import requests
            resp = requests.get(url, headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
            }, timeout=60, verify=False)
            if resp.status_code != 200:
                logger.warning(f"PDF download returned {resp.status_code}: {url}")
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
        path = url.split("gov.ms")[-1]
        path = unquote(path).replace("/wp-content/uploads/", "").replace("/", "_").replace(" ", "_")
        if path.lower().endswith(".pdf"):
            path = path[:-4]
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
        """Yield all tax guidance documents with full text."""
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
                source="MS/Revenue-TaxGuidance",
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
            "_id": f"MS/Revenue-TaxGuidance/{raw.get('doc_id', '')}",
            "_source": "MS/Revenue-TaxGuidance",
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
    scraper = MSRevenueTaxGuidanceScraper()

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
