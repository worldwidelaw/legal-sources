#!/usr/bin/env python3
"""
German Federal Bar Association (BRAK) Fetcher

Legal news, case law commentary, and professional guidance from the
Bundesrechtsanwaltskammer (BRAK).

Source: https://www.brak.de
Discovery: Paginated HTML listing at /newsroom/news/ (111 pages)
Content: Full text from individual HTML article pages

Content types:
- Rechtsprechung (case law commentary)
- Berufsrecht (professional law)
- Newsletter (Berlin, Brussels)
- Press releases
- Professional guidance

Data is public domain official works under German law (§ 5 UrhG).
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.brak.de"
LISTING_URL = f"{BASE_URL}/newsroom/news/"
TOTAL_PAGES = 111


class BRAKFetcher:
    """Fetcher for German Federal Bar Association (BRAK) publications"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_listing_page(self, page: int) -> List[str]:
        """Fetch article URLs from a listing page"""
        if page == 1:
            url = LISTING_URL
        else:
            params = {
                'tx_wwt3list_recordlist[action]': 'index',
                'tx_wwt3list_recordlist[controller]': 'Recordlist',
                'tx_wwt3list_recordlist[page]': str(page),
            }
            url = LISTING_URL + '?' + urlencode(params)

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch listing page {page}: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')

        article_urls = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if ('/newsroom/news/' in href and
                    href != '/newsroom/news/' and
                    'rss' not in href and
                    '?' not in href and
                    href not in article_urls):
                article_urls.append(href)

        return article_urls

    def _extract_article(self, path: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract full text from an article page"""
        url = f"{BASE_URL}{path}" if path.startswith('/') else path

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract title
        title = None
        h1 = soup.find('h1')
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.get_text(strip=True).split('|')[0].strip()

        if not title:
            return None

        # Extract date from the page
        date_str = None
        # BRAK uses a date display element, often near the article header
        for elem in soup.find_all(class_=re.compile('date|datum', re.I)):
            text = elem.get_text(strip=True)
            m = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
            if m:
                date_str = m.group(1)
                break

        # Fallback: search for DD.MM.YYYY pattern near the top of the page
        if not date_str:
            # Look in the first portion of the page
            header_area = soup.find('article') or soup.find('main') or soup
            header_text = header_area.get_text()[:1000]
            m = re.search(r'(\d{2}\.\d{2}\.\d{4})', header_text)
            if m:
                date_str = m.group(1)

        date_iso = self._normalize_date(date_str)

        # Extract tags/categories (deduplicated)
        tags = []
        seen_tags = set()
        for tag_elem in soup.find_all(class_=re.compile('tag|kategori|thema|schlagwort', re.I)):
            for a in tag_elem.find_all('a'):
                tag_text = a.get_text(strip=True)
                if tag_text and len(tag_text) < 50 and tag_text not in seen_tags:
                    tags.append(tag_text)
                    seen_tags.add(tag_text)

        # Extract main article body text
        text_parts = []

        # Try content containers (TYPO3 patterns)
        content_selectors = [
            'article .ce-bodytext',
            'article',
            '.news-detail',
            '.content-main',
            'main .frame-default',
            'main',
        ]

        content_elem = None
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Verify it has substantial content
                test_text = content_elem.get_text(strip=True)
                if len(test_text) > 200:
                    break
                content_elem = None

        if content_elem:
            # Remove unwanted elements
            for unwanted in content_elem.find_all(['script', 'style', 'nav', 'form', 'iframe', 'noscript']):
                unwanted.decompose()
            for unwanted in content_elem.select('.share, .social, .breadcrumb, .sidebar, .pagination, .news-list, .ce-uploads'):
                unwanted.decompose()

            for elem in content_elem.find_all(['p', 'h2', 'h3', 'h4', 'li', 'blockquote', 'td']):
                text = elem.get_text(strip=True)
                if text and len(text) > 5:
                    text_parts.append(text)

        # Fallback: all paragraphs
        if not text_parts:
            for p in soup.find_all('p'):
                text = p.get_text(strip=True)
                if text and len(text) > 20:
                    text_parts.append(text)

        full_text = '\n\n'.join(text_parts)

        # Strip residual HTML tags
        full_text = re.sub(r'<[^>]+>', '', full_text)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text).strip()

        if len(full_text) < 100:
            logger.warning(f"Insufficient text for {url} ({len(full_text)} chars)")
            return None

        return {
            'title': title,
            'text': full_text,
            'date': date_iso,
            'url': url,
            'tags': tags if tags else None,
            'path': path,
        }

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """Convert DD.MM.YYYY to ISO 8601"""
        if not date_str:
            return None
        m = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})', date_str.strip())
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize extracted article into standard schema"""
        path = raw.get('path', '')
        doc_id = path.strip('/').replace('/', '_')

        return {
            '_id': f"DE_BRAK_{doc_id}",
            '_source': 'DE/BRAK',
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'tags': raw.get('tags'),
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all documents with full text"""
        for page in range(1, TOTAL_PAGES + 1):
            logger.info(f"Listing page {page}/{TOTAL_PAGES}")
            article_paths = self._get_listing_page(page)

            for path in article_paths:
                logger.info(f"  Fetching: {path}")
                article = self._extract_article(path)
                if article:
                    yield self.normalize(article)
                time.sleep(1.5)

            time.sleep(1.0)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents from recent listing pages until we pass the since date"""
        for page in range(1, TOTAL_PAGES + 1):
            logger.info(f"Listing page {page}")
            article_paths = self._get_listing_page(page)
            found_old = False

            for path in article_paths:
                article = self._extract_article(path)
                if article:
                    if article.get('date') and article['date'] < since:
                        found_old = True
                        break
                    yield self.normalize(article)
                time.sleep(1.5)

            if found_old:
                break
            time.sleep(1.0)

    def bootstrap_sample(self, count: int = 15) -> List[Dict[str, Any]]:
        """Fetch a sample of documents for testing"""
        results = []

        # Get articles from a few different pages for diversity
        pages_to_check = [1, 30, 60, 90]
        per_page = max(2, count // len(pages_to_check))

        for page in pages_to_check:
            if len(results) >= count:
                break

            logger.info(f"Listing page {page}")
            article_paths = self._get_listing_page(page)

            for path in article_paths[:per_page]:
                if len(results) >= count:
                    break

                logger.info(f"  Fetching: {path}")
                article = self._extract_article(path)
                if article:
                    results.append(self.normalize(article))
                time.sleep(1.5)

            time.sleep(1.0)

        return results


def main():
    if len(sys.argv) < 2:
        print("Usage: bootstrap.py <command> [options]")
        print("Commands:")
        print("  bootstrap --sample    Fetch sample records")
        print("  bootstrap --full      Fetch all records")
        sys.exit(1)

    command = sys.argv[1]

    if command == 'bootstrap':
        fetcher = BRAKFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        if '--sample' in sys.argv:
            count = 15
            logger.info(f"Fetching {count} sample records...")
            records = fetcher.bootstrap_sample(count)

            for record in records:
                filename = re.sub(r'[^\w\-]', '_', record['_id'])[:100] + '.json'
                filepath = sample_dir / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved {len(records)} sample records to {sample_dir}")

            # Validation summary
            texts = [r.get('text', '') for r in records]
            non_empty = sum(1 for t in texts if len(t) > 100)
            avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
            dates = sum(1 for r in records if r.get('date'))
            print(f"\n=== Validation Summary ===")
            print(f"Records fetched: {len(records)}")
            print(f"Records with substantial text (>100 chars): {non_empty}")
            print(f"Records with dates: {dates}")
            print(f"Average text length: {avg_len:.0f} chars")
            print(f"Tags sample: {[r.get('tags') for r in records[:3]]}")

            if non_empty < len(records) * 0.8:
                print("WARNING: Less than 80% of records have substantial text!")
            else:
                print("OK: Text extraction looks good.")

        elif '--full' in sys.argv:
            logger.info("Fetching all records...")
            count = 0
            for record in fetcher.fetch_all():
                filename = re.sub(r'[^\w\-]', '_', record['_id'])[:100] + '.json'
                filepath = sample_dir / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
            logger.info(f"Saved {count} records to {sample_dir}")


if __name__ == '__main__':
    main()
