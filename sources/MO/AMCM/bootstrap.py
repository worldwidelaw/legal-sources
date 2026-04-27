#!/usr/bin/env python3
"""
MO/AMCM -- Macau Monetary Authority Circulars & Guidelines

Fetches regulatory documents from the AMCM REST API:
  - Press releases (gap): ~246 items
  - Statistical releases (gee): ~620 items
  - Notices / warnings (notice): ~8 items
  - Other related news (other-related-news): small set

API pattern:
  GET /api/v1.0/cms/news?PageSize=100&Page=N&Filters.CatSlug=SLUG
  Header: Api-Language: en

Each item has inline HTML content and optional PDF attachments on
cdn.amcm.gov.mo. Full text is extracted from the HTML content field.

Also fetches regulatory guidelines pages (banking, insurance, other)
which contain links to PDF circulars on cdn.amcm.gov.mo.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MO.AMCM")

API_BASE = "https://www.amcm.gov.mo"

NEWS_CATEGORIES = [
    ("gap", "Press Releases"),
    ("gee", "Statistical Releases"),
    ("notice", "Notices"),
    ("other-related-news", "Other Related News"),
]

# Regulatory guidelines pages (static HTML with embedded PDF links)
GUIDELINE_PAGES = [
    ("bank/bank-regulatory-guidelines", "Banking Regulatory Guidelines"),
    ("insurance-sector/regulatory-guidelines", "Insurance Regulatory Guidelines"),
    ("other-institution/other-institution-regulatory-guidelines", "Other Financial Institutions Guidelines"),
]


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    if not html:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_iso_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601 date string."""
    if not date_str:
        return None
    # Try ISO format first (2024-01-15T00:00:00)
    m = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
    if m:
        return m.group(1)
    return None


class MOAMCMScraper(BaseScraper):
    """Scraper for MO/AMCM -- Macau Monetary Authority Circulars."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
                "Api-Language": "en",
            },
            timeout=60,
        )

    def _fetch_news_page(self, cat_slug: str, page: int, page_size: int = 100) -> Dict[str, Any]:
        """Fetch a page of news items from a category."""
        url = f"/api/v1.0/cms/news?PageSize={page_size}&Page={page}&Filters.CatSlug={cat_slug}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch news page {cat_slug} p{page}: {e}")
            return {}

    def _fetch_guideline_page(self, page_path: str) -> str:
        """Fetch a regulatory guidelines page content."""
        url = f"/api/v1.0/page?path={page_path}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            content = data.get("data", {}).get("content", "")
            return content
        except Exception as e:
            logger.warning(f"Failed to fetch guideline page {page_path}: {e}")
            return ""

    def _extract_pdf_links(self, html: str) -> List[Dict[str, str]]:
        """Extract PDF links and their descriptions from HTML content."""
        links = []
        # Find all anchor tags with href pointing to PDFs
        for match in re.finditer(r'<a[^>]+href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>', html, re.DOTALL | re.IGNORECASE):
            url = match.group(1)
            label = strip_html(match.group(2)).strip()
            if url and label:
                links.append({"url": url, "label": label})
        return links

    def _download_pdf_text(self, url: str) -> str:
        """Download a PDF and extract text using common pdf_extract."""
        try:
            from common.pdf_extract import extract_pdf_markdown
        except ImportError:
            logger.warning("common.pdf_extract not available, skipping PDF")
            return ""
        try:
            if not url.startswith("http"):
                url = f"https://cdn.amcm.gov.mo{url}"
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            return extract_pdf_markdown(resp.content)
        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {url}: {e}")
            return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        return {
            "_id": f"MO/AMCM/{doc_id}",
            "_source": "MO/AMCM",
            "_type": "doctrine",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": doc_id,
            "category": raw.get("category", ""),
        }

    def _iter_news(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Iterate over all news categories."""
        limit = 15 if sample else None
        count = 0

        for cat_slug, cat_name in NEWS_CATEGORIES:
            if limit and count >= limit:
                break

            logger.info(f"Fetching category: {cat_name} ({cat_slug})")
            page = 1
            page_size = 20 if sample else 50  # API rejects PageSize>50

            while True:
                if limit and count >= limit:
                    break

                result = self._fetch_news_page(cat_slug, page, page_size)
                data = result.get("data") or {}
                items = data.get("data") or []
                total = data.get("total", 0)

                if not items:
                    break

                logger.info(f"  Page {page}: {len(items)} items (total: {total})")

                for item in items:
                    if limit and count >= limit:
                        break

                    item_id = item.get("id", "")
                    slug = item.get("slug", "")
                    title = item.get("title", "").strip()
                    date_str = item.get("date", "")
                    content_html = item.get("content", "")

                    text = strip_html(content_html)

                    # Also check for PDF attachments
                    file_info = item.get("fileInfo") or []
                    pdf_texts = []
                    for fi in file_info:
                        file_url = fi.get("fileUrl", "")
                        if file_url and file_url.lower().endswith(".pdf"):
                            pdf_text = self._download_pdf_text(file_url)
                            if pdf_text:
                                pdf_texts.append(pdf_text)

                    if pdf_texts:
                        text = text + "\n\n---\n\n" + "\n\n---\n\n".join(pdf_texts)

                    if not text or len(text.strip()) < 50:
                        logger.debug(f"  Skipping {item_id}: insufficient text")
                        continue

                    iso_date = parse_iso_date(date_str)
                    url = f"{API_BASE}/en/news/{slug}" if slug else f"{API_BASE}/en/news"

                    yield {
                        "doc_id": f"news-{item_id}",
                        "title": title,
                        "text": text,
                        "date": iso_date,
                        "url": url,
                        "category": cat_name,
                    }
                    count += 1

                page += 1
                if page * page_size > total + page_size:
                    break

        logger.info(f"Fetched {count} news items total")

    def _iter_guidelines(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Iterate over regulatory guidelines pages, extracting PDF text."""
        limit = 5 if sample else None
        count = 0

        for page_path, page_name in GUIDELINE_PAGES:
            if limit and count >= limit:
                break

            logger.info(f"Fetching guidelines: {page_name}")
            html_content = self._fetch_guideline_page(page_path)
            if not html_content:
                continue

            # Extract the page text itself as one document
            page_text = strip_html(html_content)
            if page_text and len(page_text) >= 50:
                yield {
                    "doc_id": f"guidelines-{page_path.replace('/', '-')}",
                    "title": page_name,
                    "text": page_text,
                    "date": None,
                    "url": f"{API_BASE}/en/{page_path}",
                    "category": "Regulatory Guidelines",
                }
                count += 1

            # Extract linked PDFs and fetch their text
            pdf_links = self._extract_pdf_links(html_content)
            for pdf_info in pdf_links:
                if limit and count >= limit:
                    break
                pdf_url = pdf_info["url"]
                pdf_label = pdf_info["label"]
                logger.info(f"  PDF: {pdf_label[:60]}...")

                pdf_text = self._download_pdf_text(pdf_url)
                if not pdf_text or len(pdf_text.strip()) < 50:
                    continue

                # Create a stable doc_id from the PDF filename
                pdf_filename = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "").replace(".PDF", "")
                yield {
                    "doc_id": f"guideline-pdf-{pdf_filename}",
                    "title": pdf_label,
                    "text": pdf_text,
                    "date": None,
                    "url": pdf_url if pdf_url.startswith("http") else f"https://cdn.amcm.gov.mo{pdf_url}",
                    "category": "Regulatory Guidelines",
                }
                count += 1

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_news(sample=sample)
        yield from self._iter_guidelines(sample=sample)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent news items since a given date."""
        for cat_slug, cat_name in NEWS_CATEGORIES:
            page = 1
            while True:
                result = self._fetch_news_page(cat_slug, page, 50)
                data = result.get("data", {})
                items = data.get("data", [])
                if not items:
                    break

                found_old = False
                for item in items:
                    date_str = item.get("date", "")
                    iso_date = parse_iso_date(date_str)
                    if iso_date and iso_date < since:
                        found_old = True
                        break

                    item_id = item.get("id", "")
                    slug = item.get("slug", "")
                    title = item.get("title", "").strip()
                    content_html = item.get("content", "")
                    text = strip_html(content_html)

                    if not text or len(text.strip()) < 50:
                        continue

                    url = f"{API_BASE}/en/news/{slug}" if slug else f"{API_BASE}/en/news"
                    yield {
                        "doc_id": f"news-{item_id}",
                        "title": title,
                        "text": text,
                        "date": iso_date,
                        "url": url,
                        "category": cat_name,
                    }

                if found_old:
                    break
                page += 1


if __name__ == "__main__":
    scraper = MOAMCMScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
