#!/usr/bin/env python3
"""
NP/LawCommission -- Nepal Law Commission Fetcher

Fetches legislation from lawcommission.gov.np:
  - Acts (~240+), Ordinances (~18), Regulations (~18),
    Constitution, Formation Orders
  - PDF full text extracted via pdfplumber
  - ~500+ documents total

Strategy:
  1. Crawl category listing pages to collect document URLs
  2. Visit each document page to extract the embedded PDF URL
  3. Download PDF and extract text with pdfplumber
  4. 10-second delay between requests (robots.txt crawl-delay)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NP.LawCommission")

BASE_URL = "https://lawcommission.gov.np"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NP/LawCommission"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Category IDs and their types
CATEGORIES = [
    {"id": "1757", "name": "Acts", "type": "act", "max_pages": 80},
    {"id": "1809", "name": "Ordinances", "type": "ordinance", "max_pages": 10},
    {"id": "1811", "name": "Regulations", "type": "regulation", "max_pages": 10},
    {"id": "1807", "name": "Constitution", "type": "constitution", "max_pages": 2},
    {"id": "development-committee-formation-order", "name": "Formation Orders",
     "type": "formation_order", "max_pages": 5},
]

CRAWL_DELAY = 10  # robots.txt specifies 10-second crawl delay


class LawCommissionScraper(BaseScraper):
    """Scraper for NP/LawCommission -- Nepal Law Commission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {"fetched_ids": [], "last_category": None, "last_page": 0}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _get_page(self, url: str) -> Optional[str]:
        """Fetch a page with rate limiting. Returns raw HTML."""
        time.sleep(CRAWL_DELAY)
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _extract_listing_items(self, soup: BeautifulSoup) -> list[dict]:
        """Extract document links from a category listing page."""
        items = []
        # Look for card links in the grid
        for card in soup.select('.grid__card'):
            link = card.select_one('a[href*="/content/"]')
            if not link:
                continue
            href = link.get('href', '').strip()
            title_el = card.select_one('h3 a') or card.select_one('.card__details h3') or link
            title = title_el.get_text(strip=True) if title_el else ''

            # Extract date if present
            date_el = card.select_one('.card__date') or card.select_one('span')
            date_text = date_el.get_text(strip=True) if date_el else ''

            # Extract doc ID from URL like /content/13544/customs-act--2082/
            match = re.search(r'/content/(\d+)/', href)
            if match:
                items.append({
                    'doc_id': match.group(1),
                    'title': title,
                    'url': urljoin(BASE_URL, href),
                    'date_text': date_text,
                })
        return items

    def _extract_pdf_url(self, html: str) -> Optional[str]:
        """Extract the PDF URL from a document page (FlipBook viewer)."""
        # Look for JavaScript variable: var pdf = 'https://...'
        match = re.search(r"var\s+pdf\s*=\s*['\"]([^'\"]+\.pdf)['\"]", html)
        if match:
            return match.group(1)

        # Fallback: any PDF URL in page
        match = re.search(r'(https?://[^\s\'"]+\.pdf)', html)
        if match:
            return match.group(1)

        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text content."""
        try:
            time.sleep(CRAWL_DELAY)
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()

            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None

            # Extract text with pdfplumber
            text_parts = []
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

            full_text = '\n\n'.join(text_parts).strip()
            if len(full_text) < 50:
                logger.warning(f"Very little text extracted ({len(full_text)} chars): {pdf_url}")
                return full_text if full_text else None

            return full_text
        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {pdf_url}: {e}")
            return None

    def _fetch_document(self, item: dict, category_type: str) -> Optional[dict]:
        """Fetch a single document: get PDF URL from page, download and extract text."""
        logger.info(f"  Fetching document: {item['doc_id']} - {item['title'][:60]}")

        # Get the document page to find the PDF URL
        html = self._get_page(item['url'])
        if not html:
            logger.warning(f"  Could not fetch document page: {item['url']}")
            return None

        pdf_url = self._extract_pdf_url(html)
        if not pdf_url:
            logger.warning(f"  No PDF found on page: {item['url']}")
            return None

        logger.info(f"  Found PDF: {unquote(pdf_url)[:80]}...")

        # Extract text from PDF
        text = self._extract_text_from_pdf(pdf_url)
        if not text:
            logger.warning(f"  No text extracted from PDF: {pdf_url}")
            return None

        logger.info(f"  Extracted {len(text)} chars of text")

        return {
            'doc_id': item['doc_id'],
            'title': item['title'],
            'text': text,
            'date_text': item.get('date_text', ''),
            'url': item['url'],
            'pdf_url': pdf_url,
            'category': category_type,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            '_id': f"NP-LC-{raw['doc_id']}",
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw['title'],
            'text': raw['text'],
            'date': raw.get('date_text', ''),
            'url': raw['url'],
            'pdf_url': raw.get('pdf_url', ''),
            'category': raw.get('category', ''),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all documents from all categories."""
        sample_limit = 15 if sample else None
        total_yielded = 0
        checkpoint = self._load_checkpoint()
        fetched_ids = set(checkpoint.get('fetched_ids', []))

        for cat in CATEGORIES:
            if sample_limit and total_yielded >= sample_limit:
                break

            logger.info(f"\n=== Category: {cat['name']} ===")

            for page_num in range(1, cat['max_pages'] + 1):
                if sample_limit and total_yielded >= sample_limit:
                    break

                url = f"{BASE_URL}/category/{cat['id']}/?page={page_num}"
                if page_num == 1:
                    url = f"{BASE_URL}/category/{cat['id']}/"

                logger.info(f"Listing page {page_num}: {url}")
                html = self._get_page(url)
                if not html:
                    logger.info(f"  No more pages (page {page_num} returned None)")
                    break

                soup = BeautifulSoup(html, 'html.parser')
                items = self._extract_listing_items(soup)
                if not items:
                    logger.info(f"  No items found on page {page_num}, stopping")
                    break

                logger.info(f"  Found {len(items)} items on page {page_num}")

                for item in items:
                    if sample_limit and total_yielded >= sample_limit:
                        break

                    if item['doc_id'] in fetched_ids:
                        logger.info(f"  Skipping already-fetched: {item['doc_id']}")
                        continue

                    raw = self._fetch_document(item, cat['type'])
                    if raw:
                        record = self.normalize(raw)
                        yield record
                        total_yielded += 1
                        fetched_ids.add(item['doc_id'])

                        # Save checkpoint periodically
                        if total_yielded % 5 == 0:
                            checkpoint['fetched_ids'] = list(fetched_ids)
                            checkpoint['last_category'] = cat['id']
                            checkpoint['last_page'] = page_num
                            self._save_checkpoint(checkpoint)

            # Save checkpoint after each category
            checkpoint['fetched_ids'] = list(fetched_ids)
            self._save_checkpoint(checkpoint)

        logger.info(f"\nTotal documents fetched: {total_yielded}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents (page 1 of Acts category)."""
        logger.info("Fetching updates (recent Acts page 1)")
        url = f"{BASE_URL}/category/1757/"
        html = self._get_page(url)
        if not html:
            return

        soup = BeautifulSoup(html, 'html.parser')
        items = self._extract_listing_items(soup)
        checkpoint = self._load_checkpoint()
        fetched_ids = set(checkpoint.get('fetched_ids', []))

        for item in items:
            if item['doc_id'] in fetched_ids:
                continue
            raw = self._fetch_document(item, 'act')
            if raw:
                yield self.normalize(raw)

    def test(self):
        """Quick connectivity test."""
        logger.info("Testing connectivity to lawcommission.gov.np...")
        try:
            resp = self.session.get(f"{BASE_URL}/category/1757/", timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = self._extract_listing_items(soup)
            logger.info(f"Connection OK. Found {len(items)} items on Acts page 1.")

            if items:
                # Test PDF extraction on first item
                logger.info(f"Testing PDF extraction for: {items[0]['title'][:60]}")
                time.sleep(CRAWL_DELAY)
                doc_html = self._get_page(items[0]['url'])
                if doc_html:
                    pdf_url = self._extract_pdf_url(doc_html)
                    if pdf_url:
                        logger.info(f"PDF URL found: {unquote(pdf_url)[:80]}")
                        logger.info("Test PASSED")
                        return True
                    else:
                        logger.error("No PDF URL found on document page")
                        return False
            return True
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description='NP/LawCommission fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'])
    parser.add_argument('--sample', action='store_true', help='Fetch only 15 sample records')
    args = parser.parse_args()

    scraper = LawCommissionScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            out_file = SAMPLE_DIR / f"{record['_id']}.json"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved [{count}]: {record['title'][:60]} ({len(record.get('text', ''))} chars)")
        logger.info(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")

    elif args.command == 'update':
        count = 0
        for record in scraper.fetch_updates():
            out_file = SAMPLE_DIR / f"{record['_id']}.json"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} new records")


if __name__ == '__main__':
    main()
