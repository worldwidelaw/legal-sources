#!/usr/bin/env python3
"""
Niedersachsen State Law (VORIS) Fetcher

Official legal information system of Niedersachsen (Lower Saxony), Germany.
https://voris.wolterskluwer-online.de

This fetcher uses browser-based scraping (Playwright) because VORIS is a
Drupal 11 CMS with JavaScript-driven navigation. The content is public
domain (amtliche Werke) under German law (§ 5 UrhG).

Approach:
1. Use search to discover laws by category/filter
2. For each law, fetch the parent document to get TOC
3. Collect all section IDs from TOC
4. Fetch each section and combine for full text
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add common directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    from common.browser_scraper import BrowserScraper
    BROWSER_AVAILABLE = True
except ImportError:
    BROWSER_AVAILABLE = False
    logger.warning("BrowserScraper not available. Install playwright: pip install playwright && playwright install chromium")

# Constants
BASE_URL = "https://voris.wolterskluwer-online.de"
SEARCH_URL = f"{BASE_URL}/search"
NIEDERSACHSEN_FILTER = "a52e918e-8a02-41f8-8b62-1c4b6a92ff6a"


class NiedersachsenFetcher:
    """Fetcher for Niedersachsen state legislation from VORIS"""

    def __init__(self, headless: bool = True):
        if not BROWSER_AVAILABLE:
            raise ImportError("BrowserScraper required. Install: pip install playwright && playwright install chromium")

        self.headless = headless
        self.scraper = None
        self.page = None
        self.seen_docs: Set[str] = set()
        self.rate_limit = 1.5  # seconds between requests

    def __enter__(self):
        self.scraper = BrowserScraper(headless=self.headless, timeout=60000)
        self.scraper.start()
        self.page = self.scraper.new_page()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scraper:
            self.scraper.stop()

    def _navigate(self, url: str, wait_until: str = "networkidle") -> str:
        """Navigate to URL with rate limiting"""
        time.sleep(self.rate_limit)
        self.page.goto(url, wait_until=wait_until)
        time.sleep(1)  # Extra wait for JS
        return self.page.content()

    def _clean_html(self, html: str) -> str:
        """Clean HTML to plain text"""
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'</h[1-6]>', '\n\n', text)
        text = re.sub(r'<h[1-6][^>]*>', '\n', text)
        text = re.sub(r'</p>', '\n', text)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</div>', '\n', text)
        text = re.sub(r'</li>', '\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
        return text.strip()

    def _extract_title(self, html: str) -> str:
        """Extract document title from HTML"""
        match = re.search(r'class="wkde-doctitle"[^>]*>([^<]+)</h1>', html)
        if match:
            return unescape(match.group(1).strip())
        return ""

    def _extract_bibliography(self, html: str) -> Dict[str, str]:
        """Extract bibliography metadata from HTML"""
        result = {}
        bib_match = re.search(r'class="wkde-bibliography"[^>]*>(.*?)</section>', html, re.DOTALL)
        if bib_match:
            pairs = re.findall(r'<dt>([^<]+)</dt><dd>([^<]+)</dd>', bib_match.group(1))
            for dt, dd in pairs:
                key = dt.strip().lower().replace(' ', '_').replace('.', '').replace('-', '_')
                result[key] = unescape(dd.strip())
        return result

    def _extract_body_text(self, html: str) -> str:
        """Extract document body text from HTML"""
        match = re.search(r'class="wkde-document-body"[^>]*>(.*?)</section>', html, re.DOTALL)
        if match:
            return self._clean_html(match.group(1))
        return ""

    def _get_section_ids(self, html: str) -> List[str]:
        """Extract section document IDs from TOC"""
        toc_match = re.search(r'class="[^"]*toc-list[^"]*"[^>]*>(.*?)</ul>', html, re.DOTALL)
        if toc_match:
            ids = re.findall(r'/browse/document/([a-f0-9-]{36})', toc_match.group(1))
            return list(dict.fromkeys(ids))  # Preserve order, remove duplicates
        return []

    def _discover_parent_docs(self, max_pages: int = 5, limit: int = None) -> List[Tuple[str, str]]:
        """
        Discover parent document IDs from search results.
        Returns list of (doc_id, title) tuples for parent/overview documents.
        """
        results = []
        seen_abbrevs = set()
        page_num = 0

        while page_num < max_pages:
            url = f"{SEARCH_URL}?page={page_num}" if page_num > 0 else SEARCH_URL
            logger.info(f"Searching page {page_num + 1}: {url}")

            html = self._navigate(url)

            # Find document links
            doc_matches = re.findall(
                r'/browse/document/([a-f0-9-]{36})[^"]*"[^>]*>.*?class="wkde-doctitle"[^>]*>([^<]+)',
                html, re.DOTALL
            )

            if not doc_matches:
                # Try simpler pattern
                doc_ids = list(set(re.findall(r'/browse/document/([a-f0-9-]{36})', html)))
                for doc_id in doc_ids:
                    if doc_id not in self.seen_docs:
                        results.append((doc_id, ""))
                        self.seen_docs.add(doc_id)
            else:
                for doc_id, title in doc_matches:
                    if doc_id not in self.seen_docs:
                        # Skip individual sections (Abschnitt X)
                        clean_title = unescape(title.strip())
                        if not re.match(r'^Abschnitt \d+', clean_title):
                            results.append((doc_id, clean_title))
                            self.seen_docs.add(doc_id)

            logger.info(f"Found {len(results)} candidate documents so far")

            if limit and len(results) >= limit:
                break

            # Check for next page
            if f'page={page_num + 1}' not in html:
                break

            page_num += 1

        return results[:limit] if limit else results

    def _fetch_full_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a complete document by collecting all its sections.
        Returns normalized document dict or None if fetch failed.
        """
        doc_url = f"{BASE_URL}/browse/document/{doc_id}"
        logger.info(f"Fetching document: {doc_id}")

        try:
            html = self._navigate(doc_url)
        except Exception as e:
            logger.error(f"Failed to navigate to {doc_url}: {e}")
            return None

        # Extract metadata from parent
        title = self._extract_title(html)
        bib = self._extract_bibliography(html)

        # Get normtyp to filter for actual laws
        normtyp = bib.get('normtyp', '')

        # Get section IDs
        section_ids = self._get_section_ids(html)

        if not section_ids:
            # This might be a single-section document
            section_ids = [doc_id]

        logger.info(f"Document '{title[:50]}...' has {len(section_ids)} sections")

        # Collect text from all sections
        full_text = ""
        section_count = 0

        for i, sid in enumerate(section_ids):
            try:
                sec_url = f"{BASE_URL}/browse/document/{sid}"
                sec_html = self._navigate(sec_url)

                sec_title = self._extract_title(sec_html)
                sec_text = self._extract_body_text(sec_html)

                if sec_text:
                    if i > 0 and sec_title:
                        full_text += f"\n\n=== {sec_title} ===\n\n"
                    full_text += sec_text
                    section_count += 1

            except Exception as e:
                logger.warning(f"Failed to fetch section {sid}: {e}")
                continue

        full_text = full_text.strip()

        if not full_text or len(full_text) < 200:
            logger.warning(f"Document {doc_id} has insufficient text ({len(full_text)} chars)")
            return None

        # Build raw document
        return {
            'doc_id': doc_id,
            'title': title or bib.get('titel', f'Dokument {doc_id}'),
            'text': full_text,
            'url': doc_url,
            'normtyp': normtyp,
            'abbreviation': bib.get('redaktionelle_abkürzung', ''),
            'gliederungs_nr': bib.get('gliederungs_nr', ''),
            'normgeber': bib.get('normgeber', 'Niedersachsen'),
            'section_count': section_count,
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch Niedersachsen laws with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        # Discover parent documents
        candidates = self._discover_parent_docs(max_pages=10, limit=limit * 3 if limit else None)
        logger.info(f"Found {len(candidates)} candidate documents")

        count = 0
        for doc_id, _ in candidates:
            if limit and count >= limit:
                break

            doc = self._fetch_full_document(doc_id)
            if doc and len(doc.get('text', '')) >= 500:
                yield doc
                count += 1
                logger.info(f"[{count}/{limit or 'all'}] Yielded: {doc['title'][:60]}... ({len(doc['text']):,} chars)")

        logger.info(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent documents"""
        # For updates, fetch a limited number of recent documents
        yield from self.fetch_all(limit=30)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        doc_id = raw_doc.get('doc_id', '')
        title = raw_doc.get('title', '')

        # Determine document type
        normtyp = raw_doc.get('normtyp', '').lower()
        if 'gesetz' in normtyp:
            doc_type = 'legislation'
        elif 'verordnung' in normtyp:
            doc_type = 'regulation'
        elif 'verwaltungsvorschrift' in normtyp:
            doc_type = 'administrative_regulation'
        else:
            doc_type = 'legislation'

        # Try to extract date from text
        date = None
        text = raw_doc.get('text', '')
        date_match = re.search(r'Vom\s+(\d{1,2})\.\s*(\w+)\s*(\d{4})', text)
        if date_match:
            day = date_match.group(1).zfill(2)
            month_name = date_match.group(2)
            year = date_match.group(3)

            months = {
                'Januar': '01', 'Februar': '02', 'März': '03', 'April': '04',
                'Mai': '05', 'Juni': '06', 'Juli': '07', 'August': '08',
                'September': '09', 'Oktober': '10', 'November': '11', 'Dezember': '12'
            }
            month = months.get(month_name, '01')
            date = f"{year}-{month}-{day}"

        return {
            '_id': f"NI-{doc_id[:12]}",
            '_source': 'DE/Niedersachsen',
            '_type': doc_type,
            '_fetched_at': datetime.now().isoformat(),
            'doc_id': doc_id,
            'title': title,
            'text': text,
            'date': date,
            'url': raw_doc.get('url', ''),
            'normtyp': raw_doc.get('normtyp', ''),
            'abbreviation': raw_doc.get('abbreviation', ''),
            'gliederungs_nr': raw_doc.get('gliederungs_nr', ''),
            'section_count': raw_doc.get('section_count', 0),
            'jurisdiction': 'Niedersachsen',
            'country': 'DE',
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap with BrowserScraper...")

        target_count = 12 if '--sample' in sys.argv else 50
        sample_count = 0

        with NiedersachsenFetcher(headless=True) as fetcher:
            for raw_doc in fetcher.fetch_all(limit=target_count + 10):
                if sample_count >= target_count:
                    break

                normalized = fetcher.normalize(raw_doc)
                text_len = len(normalized.get('text', ''))

                # Require substantial text content
                if text_len < 500:
                    continue

                # Save to sample directory
                doc_id = normalized['_id'].replace('/', '_').replace(' ', '_')
                filename = f"{doc_id}.json"
                filepath = sample_dir / filename

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(normalized, f, indent=2, ensure_ascii=False)

                logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['title'][:60]}... ({text_len:,} chars)")
                sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode
        print("Testing Niedersachsen fetcher...")
        print("Run with: python bootstrap.py bootstrap --sample")

        with NiedersachsenFetcher(headless=True) as fetcher:
            count = 0
            for raw_doc in fetcher.fetch_all(limit=2):
                normalized = fetcher.normalize(raw_doc)
                print(f"\n--- Document {count + 1} ---")
                print(f"ID: {normalized['_id']}")
                print(f"Title: {normalized['title'][:80]}")
                print(f"Normtyp: {normalized['normtyp']}")
                print(f"Date: {normalized['date']}")
                print(f"Sections: {normalized['section_count']}")
                print(f"Text length: {len(normalized.get('text', ''))}")
                print(f"Text preview: {normalized.get('text', '')[:500]}...")
                count += 1


if __name__ == '__main__':
    main()
