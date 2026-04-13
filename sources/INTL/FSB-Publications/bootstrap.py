#!/usr/bin/env python3
"""
INTL/FSB-Publications -- Financial Stability Board Standards and Reports

Fetches FSB publications (policy documents, standards, consultations, reports)
with full text extracted from PDFs.

Strategy:
  - GET /sitemap.xml to discover all publication URLs (930+ items)
  - For each /YYYY/MM/slug/ page, scrape HTML for metadata + PDF link
  - Download PDF and extract full text via pdfplumber
  - Fall back to inline HTML article text if no PDF available

Data:
  - ~930 publications (2009-present)
  - Types: Policy documents, consultations, reports, press releases, speeches
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent documents
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from html import unescape

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.FSB-Publications")

BASE_URL = "https://www.fsb.org"
SITEMAP_URL = BASE_URL + "/sitemap.xml"


class FSBPublicationsScraper(BaseScraper):
    """Scraper for INTL/FSB-Publications -- FSB Standards and Reports."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def _fetch_sitemap_urls(self) -> List[str]:
        """Fetch the sitemap and extract all year-based publication URLs."""
        self.rate_limiter.wait()
        resp = self.session.get(SITEMAP_URL, timeout=30)
        resp.raise_for_status()

        # Extract all /YYYY/MM/slug/ URLs from sitemap
        urls = re.findall(
            r'<loc>(https://www\.fsb\.org/\d{4}/\d{2}/[^<]+)</loc>',
            resp.text,
        )
        logger.info("Sitemap returned %d publication URLs", len(urls))
        return urls

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/FSB-Publications",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _clean_html(self, html_text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', html_text,
                       flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _parse_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single publication page for metadata + PDF."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                logger.warning("Page 404: %s", url)
                return None
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to fetch page %s: %s", url, e)
            return None

        html = resp.text

        # Extract title from <h1>
        title_m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        title = self._clean_html(title_m.group(1)) if title_m else ""

        # Extract date from URL pattern /YYYY/MM/
        date_m = re.match(r'https://www\.fsb\.org/(\d{4})/(\d{2})/', url)
        date_str = None
        if date_m:
            year, month = date_m.group(1), date_m.group(2)
            # Try to find exact day in the article text
            day_m = re.search(
                r'(\d{1,2})\s+'
                r'(?:January|February|March|April|May|June|July|August|'
                r'September|October|November|December)\s+'
                + re.escape(year),
                html,
            )
            if day_m:
                day = day_m.group(1).zfill(2)
                date_str = f"{year}-{month}-{day}"
            else:
                date_str = f"{year}-{month}-01"

        # Extract PDF links
        pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"', html)
        # Prefer /uploads/ PDFs, filter out duplicates
        pdf_url = None
        for link in pdf_links:
            if link.startswith("/"):
                link = BASE_URL + link
            if "/uploads/" in link:
                pdf_url = link
                break
        if not pdf_url and pdf_links:
            link = pdf_links[0]
            if link.startswith("/"):
                link = BASE_URL + link
            pdf_url = link

        # Extract article content as fallback text
        article_m = re.search(r'<article[^>]*>(.*?)</article>', html,
                               re.DOTALL)
        article_text = ""
        if article_m:
            article_text = self._clean_html(article_m.group(1))

        # Extract slug for ID
        slug_m = re.search(r'/\d{4}/\d{2}/([^/]+)/?$', url)
        slug = slug_m.group(1) if slug_m else url.split("/")[-2]

        return {
            "url": url,
            "title": title,
            "date": date_str,
            "pdf_url": pdf_url,
            "article_text": article_text,
            "slug": slug,
        }

    def _process_publication(self, url: str) -> Optional[Dict[str, Any]]:
        """Process a single publication: scrape page + extract PDF text."""
        page = self._parse_page(url)
        if not page or not page["title"]:
            return None

        text = None

        # Try PDF first
        if page["pdf_url"]:
            text = self._download_pdf_text(page["pdf_url"])

        # Fall back to article text if no PDF or PDF extraction failed
        if not text and page["article_text"] and len(page["article_text"]) > 200:
            text = page["article_text"]

        if not text:
            logger.warning("No text extracted for: %s", url)
            return None

        doc_id = f"FSB-{page['slug']}"

        return {
            "_id": doc_id,
            "title": page["title"],
            "text": text,
            "date": page["date"],
            "url": page["url"],
            "pdf_url": page.get("pdf_url", ""),
        }

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw FSB record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "INTL/FSB-Publications",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all FSB publications."""
        urls = self._fetch_sitemap_urls()
        for i, url in enumerate(urls):
            logger.info("[%d/%d] %s", i + 1, len(urls), url.split("/")[-2][:60])
            doc = self._process_publication(url)
            if doc:
                yield doc

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent publications (last 180 days)."""
        urls = self._fetch_sitemap_urls()
        cutoff = (datetime.now() - timedelta(days=180)).strftime("%Y/%m")
        recent = [u for u in urls if self._url_date(u) >= cutoff]
        logger.info("Found %d recent items (since %s)", len(recent), cutoff)
        for url in recent:
            doc = self._process_publication(url)
            if doc:
                yield doc

    @staticmethod
    def _url_date(url: str) -> str:
        """Extract YYYY/MM from a publication URL for sorting/filtering."""
        m = re.search(r'/(\d{4}/\d{2})/', url)
        return m.group(1) if m else "0000/00"

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            urls = self._fetch_sitemap_urls()
            logger.info("Connection OK: %d publications in sitemap", len(urls))
            return len(urls) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode (15 records)")
            urls = self._fetch_sitemap_urls()
            count = 0
            target = 15

            # Sample evenly across years: pick from recent publications
            for url in urls:
                if count >= target:
                    break
                doc = self._process_publication(url)
                if doc:
                    normalized = self.normalize(doc)
                    fname = re.sub(r'[^\w\-.]', '_',
                                   f"{normalized['_id'][:80]}.json")
                    with open(sample_dir / fname, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                    count += 1
                    logger.info("[%d/%d] %s (%d chars)",
                                count, target,
                                normalized["title"][:60],
                                len(normalized["text"]))

            logger.info("Sample bootstrap complete: %d records saved", count)
            return count
        else:
            count = 0
            for doc in self.fetch_all():
                normalized = self.normalize(doc)
                sample_dir.mkdir(exist_ok=True)
                fname = re.sub(r'[^\w\-.]', '_',
                               f"{normalized['_id'][:80]}.json")
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 50 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="INTL/FSB-Publications Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample only")
    args = parser.parse_args()

    scraper = FSBPublicationsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for doc in scraper.fetch_updates():
            normalized = scraper.normalize(doc)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
