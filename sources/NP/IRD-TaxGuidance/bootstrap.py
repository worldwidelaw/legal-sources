#!/usr/bin/env python3
"""
NP/IRD-TaxGuidance -- Nepal Inland Revenue Department Tax Guidance

Fetches tax doctrine from ird.gov.np:
  - Acts (~5), Rules (~18), Directives (~18),
    Procedures (~18), Notices (~60)
  - PDF full text extracted via common/pdf_extract
  - ~100+ documents total

Strategy:
  1. Crawl category listing pages to collect document URLs
  2. Visit each document page to extract the embedded PDF URL
  3. Download PDF and extract text with pdf_extract
  4. 1-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NP.IRD-TaxGuidance")

BASE_URL = "https://ird.gov.np"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NP/IRD-TaxGuidance"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

CATEGORIES = [
    {"slug": "acts", "name": "Acts", "type": "act", "max_pages": 3},
    {"slug": "rules-and-regulation", "name": "Rules & Regulations", "type": "rule", "max_pages": 5},
    {"slug": "directives", "name": "Directives", "type": "directive", "max_pages": 5},
    {"slug": "procedure", "name": "Procedures", "type": "procedure", "max_pages": 5},
    {"slug": "notice", "name": "Notices", "type": "notice", "max_pages": 5},
]

CRAWL_DELAY = 1


class IRDTaxGuidanceScraper(BaseScraper):
    """Scraper for NP/IRD-TaxGuidance -- Nepal Inland Revenue Department."""

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
        """Fetch a page with rate limiting."""
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

    def _extract_listing_items(self, soup: BeautifulSoup) -> list:
        """Extract document links from a category listing page."""
        items = []
        seen_ids = set()

        for card in soup.select('.grid__card'):
            link = card.select_one('a[href*="/content/"]')
            if not link:
                continue
            href = link.get('href', '').strip()
            match = re.search(r'/content/(\d+)/', href)
            if not match:
                continue

            doc_id = match.group(1)
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            title_el = card.select_one('h3 a') or card.select_one('.card__details h3') or link
            title = title_el.get_text(strip=True) if title_el else ''
            date_el = card.select_one('.card__date') or card.select_one('span')
            date_text = date_el.get_text(strip=True) if date_el else ''

            items.append({
                'doc_id': doc_id,
                'title': title,
                'url': urljoin(BASE_URL, href),
                'date_text': date_text,
            })
        return items

    def _extract_pdf_url(self, html: str) -> Optional[str]:
        """Extract the PDF URL from a document page (FlipBook viewer)."""
        match = re.search(r"var\s+pdf\s*=\s*['\"]([^'\"]+\.pdf)['\"]", html)
        if match:
            return match.group(1)
        match = re.search(r'(https?://giwmscdn[^\s\'"]+\.pdf)', html)
        if match:
            return match.group(1)
        match = re.search(r'(https?://[^\s\'"]+\.pdf)', html)
        if match:
            return match.group(1)
        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="NP/IRD-TaxGuidance",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _fetch_document(self, item: dict, category_type: str) -> Optional[dict]:
        """Fetch a single document: get PDF URL from page, download and extract text."""
        logger.info(f"  Fetching document: {item['doc_id']} - {item['title'][:60]}")

        html = self._get_page(item['url'])
        if not html:
            logger.warning(f"  Could not fetch document page: {item['url']}")
            return None

        pdf_url = self._extract_pdf_url(html)
        if not pdf_url:
            logger.warning(f"  No PDF found on page: {item['url']}")
            return None

        logger.info(f"  Found PDF: {unquote(pdf_url)[:80]}...")

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
            '_id': f"NP-IRD-{raw['doc_id']}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
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

                if page_num == 1:
                    url = f"{BASE_URL}/category/{cat['slug']}/"
                else:
                    url = f"{BASE_URL}/category/{cat['slug']}/?page={page_num}"

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
                        yield raw
                        total_yielded += 1
                        fetched_ids.add(item['doc_id'])

                        if total_yielded % 5 == 0:
                            checkpoint['fetched_ids'] = list(fetched_ids)
                            checkpoint['last_category'] = cat['slug']
                            checkpoint['last_page'] = page_num
                            self._save_checkpoint(checkpoint)

            checkpoint['fetched_ids'] = list(fetched_ids)
            self._save_checkpoint(checkpoint)

        logger.info(f"\nTotal documents fetched: {total_yielded}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents (page 1 of notices)."""
        logger.info("Fetching updates (recent Notices page 1)")
        url = f"{BASE_URL}/category/notice/"
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
            raw = self._fetch_document(item, 'notice')
            if raw:
                yield self.normalize(raw)

    def test(self):
        """Quick connectivity test."""
        logger.info("Testing connectivity to ird.gov.np...")
        try:
            resp = self.session.get(f"{BASE_URL}/category/directives/", timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = self._extract_listing_items(soup)
            logger.info(f"Connection OK. Found {len(items)} items on Directives page 1.")

            if items:
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
    parser = argparse.ArgumentParser(description='NP/IRD-TaxGuidance fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'])
    parser.add_argument('--sample', action='store_true', help='Fetch only 15 sample records')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IRDTaxGuidanceScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        scraper.bootstrap(sample_mode=args.sample)

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
