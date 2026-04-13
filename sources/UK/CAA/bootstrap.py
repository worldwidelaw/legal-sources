#!/usr/bin/env python3
"""
Legal Data Hunter - UK Civil Aviation Authority (CAA) Scraper

Fetches regulatory publications from the CAA using:
  - GET /data-and-publications/publications/publication-series/ (series index)
  - GET /data-and-publications/publications/publication-series/{slug}/ (document listing)
  - GET /data-and-publications/publications/documents/content/{slug}/ (document metadata)
  - GET /publication/download/{id} (PDF full text)

Coverage: ~5,000+ publications across 47 series (CAPs, ORS, ADs, Safety Notices, etc.)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
"""

import io
import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

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
logger = logging.getLogger("UK/CAA")


class UKCAAScraper(BaseScraper):
    """
    Scraper for: UK Civil Aviation Authority (CAA)
    Country: UK
    URL: https://www.caa.co.uk

    Data types: doctrine
    Auth: none

    Strategy:
    - Crawl publication series pages to discover document URLs
    - Fetch each document page for metadata and PDF download link
    - Download PDF and extract full text with pdfminer/pdfplumber
    """

    BASE_URL = "https://www.caa.co.uk"
    SERIES_INDEX = "/data-and-publications/publications/publication-series/"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=60,
        )

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="UK/CAA",
            source_id="",
            pdf_bytes=pdf_content,
            table="doctrine",
        ) or ""

    def _get_series_urls(self) -> list:
        """Fetch the series index page and extract all series URLs."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SERIES_INDEX)
            if resp.status_code != 200:
                logger.error(f"Series index returned {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch series index: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        series_urls = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/publication-series/" in href and href != self.SERIES_INDEX:
                # Normalize
                if href.startswith("/"):
                    href = f"{self.BASE_URL}{href}"
                elif not href.startswith("http"):
                    href = f"{self.BASE_URL}/{href}"
                # Avoid duplicates and the index page itself
                if href not in series_urls and href != f"{self.BASE_URL}{self.SERIES_INDEX}":
                    series_urls.append(href)

        logger.info(f"Found {len(series_urls)} publication series")
        return series_urls

    def _get_document_urls_from_series(self, series_url: str) -> list:
        """Fetch a series page and extract document content URLs."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(series_url)
            if resp.status_code != 200:
                logger.warning(f"Series page {series_url} returned {resp.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Failed to fetch series page {series_url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        doc_urls = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/documents/content/" in href:
                if href.startswith("/"):
                    full_url = f"{self.BASE_URL}{href}"
                elif not href.startswith("http"):
                    full_url = f"{self.BASE_URL}/{href}"
                else:
                    full_url = href
                if full_url not in doc_urls:
                    doc_urls.append(full_url)

        return doc_urls

    def _fetch_document(self, doc_url: str) -> Optional[dict]:
        """Fetch a document page, extract metadata and PDF text."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(doc_url)
            if resp.status_code != 200:
                logger.warning(f"Document page {doc_url} returned {resp.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

        # Extract slug from URL for ID
        slug = doc_url.rstrip("/").split("/")[-1]

        # Extract metadata from definition lists or structured content
        status = ""
        version_date = ""
        effective_date = ""
        edition = ""
        description = ""
        series = ""
        categories = []

        # Look for metadata in dt/dd pairs
        for dt in soup.find_all("dt"):
            label = dt.get_text(strip=True).lower()
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            value = dd.get_text(strip=True)

            if "status" in label:
                status = value
            elif "version date" in label or "date" == label:
                version_date = value
            elif "effective" in label:
                effective_date = value
            elif "edition" in label or "version" in label:
                edition = value
            elif "series" in label:
                series = value

        # Try alternate metadata extraction from spans/divs with class hints
        if not status:
            status_el = soup.find(string=re.compile(r"Current|Withdrawn|Cancelled", re.I))
            if status_el:
                status = status_el.strip()

        # Extract description - look for a paragraph after the metadata
        desc_section = soup.find("div", class_=re.compile(r"description|summary|body|content", re.I))
        if desc_section:
            description = desc_section.get_text(strip=True)
        else:
            # Try to find description in main content paragraphs
            main = soup.find("main") or soup.find("article") or soup
            for p in main.find_all("p"):
                text = p.get_text(strip=True)
                if len(text) > 50 and "cookie" not in text.lower():
                    description = text
                    break

        # Extract categories
        cat_section = soup.find("div", class_=re.compile(r"categor|tag", re.I))
        if cat_section:
            for li in cat_section.find_all("li"):
                cat = li.get_text(strip=True)
                if cat:
                    categories.append(cat)
        if not categories:
            for link in soup.find_all("a", href=True):
                if "/publication-categories/" in link["href"]:
                    cat = link.get_text(strip=True)
                    if cat and cat not in categories:
                        categories.append(cat)

        # Extract series from breadcrumb or links
        if not series:
            for link in soup.find_all("a", href=True):
                if "/publication-series/" in link["href"]:
                    series = link.get_text(strip=True)
                    break

        # Find PDF download link
        pdf_url = None
        pdf_size = ""
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/publication/download/" in href:
                if href.startswith("/"):
                    pdf_url = f"{self.BASE_URL}{href}"
                else:
                    pdf_url = href
                # Try to get file size from link text
                link_text = link.get_text(strip=True)
                size_match = re.search(r"\(([\d.]+\s*[KMG]B)", link_text, re.I)
                if size_match:
                    pdf_size = size_match.group(1)
                break

        # Download PDF and extract text
        text = ""
        if pdf_url:
            # Skip very large PDFs (>50MB based on header)
            self.rate_limiter.wait()
            try:
                resp_pdf = self.client.get(pdf_url, stream=True)
                if resp_pdf.status_code == 200:
                    content_length = resp_pdf.headers.get("Content-Length", "0")
                    if int(content_length or 0) > 50 * 1024 * 1024:
                        logger.warning(f"Skipping large PDF ({content_length} bytes): {pdf_url}")
                        text = f"[PDF too large: {content_length} bytes]"
                    else:
                        pdf_content = resp_pdf.content
                        text = self._extract_pdf_text(pdf_content)
                else:
                    logger.warning(f"PDF download returned {resp_pdf.status_code}: {pdf_url}")
            except Exception as e:
                logger.warning(f"Failed to download PDF {pdf_url}: {e}")

        # Parse date
        date_str = version_date or effective_date
        date_iso = self._parse_date(date_str)

        return {
            "slug": slug,
            "title": title,
            "text": text,
            "description": description,
            "status": status,
            "edition": edition,
            "version_date": version_date,
            "effective_date": effective_date,
            "date_iso": date_iso,
            "series": series,
            "categories": categories,
            "url": doc_url,
            "pdf_url": pdf_url or "",
            "pdf_size": pdf_size,
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        # Try common formats
        for fmt in [
            "%d %B %Y", "%d %b %Y", "%d-%b-%Y", "%d-%B-%Y",
            "%d/%m/%Y", "%Y-%m-%d", "%B %Y", "%d %B, %Y",
        ]:
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Try extracting just the date part
        match = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str)
        if match:
            try:
                return datetime.strptime(match.group(0), "%d %B %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents by crawling publication series."""
        series_urls = self._get_series_urls()
        all_doc_urls = set()

        # Collect all document URLs from all series
        for i, series_url in enumerate(series_urls):
            doc_urls = self._get_document_urls_from_series(series_url)
            new_count = len([u for u in doc_urls if u not in all_doc_urls])
            all_doc_urls.update(doc_urls)
            series_name = series_url.rstrip("/").split("/")[-1]
            logger.info(f"Series {i+1}/{len(series_urls)} [{series_name}]: {new_count} new docs (total: {len(all_doc_urls)})")

        logger.info(f"Total unique documents to fetch: {len(all_doc_urls)}")

        # Fetch each document
        for i, doc_url in enumerate(sorted(all_doc_urls)):
            doc = self._fetch_document(doc_url)
            if doc and doc.get("text"):
                yield doc
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(all_doc_urls)} documents fetched")
            elif doc:
                logger.debug(f"Skipping document with no text: {doc_url}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since the given date (re-crawls all series)."""
        # CAA doesn't provide date-filtered listings, so we re-crawl
        # and rely on the upsert strategy to handle updates
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        return {
            "_id": f"UK/CAA/{raw['slug']}",
            "_source": "UK/CAA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "date": raw.get("date_iso"),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "status": raw.get("status", ""),
            "edition": raw.get("edition", ""),
            "series": raw.get("series", ""),
            "categories": raw.get("categories", []),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKCAAScraper()

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
