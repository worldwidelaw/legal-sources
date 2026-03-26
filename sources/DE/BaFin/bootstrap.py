#!/usr/bin/env python3
"""
German Federal Financial Supervisory Authority (BaFin) Regulatory Documents Fetcher

Official open data from bafin.de
https://www.bafin.de

This fetcher retrieves regulatory circulars (Rundschreiben) and guidance documents from BaFin:
- Search API for document discovery (126+ circulars)
- HTML pages containing full text regulatory guidance

Data is public domain official government works under German law (§ 5 UrhG).
"""

import html
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.bafin.de"
SEARCH_URL = "https://www.bafin.de/SiteGlobals/Forms/Suche/Expertensuche_Formular.html"
SEARCH_PARAMS = {
    "pageNo": 0,
    "documentType_": "News Publication",
    "gts": "dateOfIssue_dt desc",
    "sortOrder": "dateOfIssue_dt desc",
    "language_": "de",
    "cl2Categories_Format": "Rundschreiben"
}


class BaFinFetcher:
    """Fetcher for BaFin regulatory circulars (Rundschreiben)"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _request_with_backoff(self, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with exponential backoff on 429/5xx errors"""
        max_retries = 5
        for attempt in range(max_retries):
            response = self.session.get(url, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                wait = min(2 ** attempt * 5, 120)  # 5, 10, 20, 40, 80 seconds
                retry_after = response.headers.get('Retry-After')
                if retry_after and retry_after.isdigit():
                    wait = max(wait, int(retry_after))
                logger.warning(f"HTTP {response.status_code} on {url}, waiting {wait}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response
        # Final attempt - let it raise
        response.raise_for_status()
        return response

    def _get_search_results(self, page_no: int = 0) -> List[Dict[str, Any]]:
        """Fetch circular entries from search page"""
        params = SEARCH_PARAMS.copy()
        params["pageNo"] = page_no

        logger.info(f"Fetching search results page {page_no}")
        response = self._request_with_backoff(SEARCH_URL, params=params, timeout=60)

        soup = BeautifulSoup(response.content, 'html.parser')
        entries = []

        for result_div in soup.find_all('div', class_='search-result'):
            entry = self._parse_search_result(result_div)
            if entry:
                entries.append(entry)

        logger.info(f"Found {len(entries)} entries on page {page_no}")
        return entries

    def _parse_search_result(self, div) -> Optional[Dict[str, Any]]:
        """Parse a single search result div"""
        try:
            # Find the main link
            h3 = div.find('h3')
            if not h3:
                return None

            link_elem = h3.find('a', href=True)
            if not link_elem:
                return None

            href = link_elem.get('href', '')
            # Remove jsessionid from URL
            href = re.sub(r';jsessionid=[^?]*', '', href)

            # Skip if it's a PDF download link (not HTML content)
            if '/Downloads/' in href and href.endswith('.pdf'):
                return None

            title = link_elem.get_text(strip=True)
            title = re.sub(r'\s+', ' ', title)  # Normalize whitespace

            # Extract date from metadata
            date = None
            date_match = re.search(r'vom\s+(\d{2}\.\d{2}\.\d{4})', str(div))
            if date_match:
                date = date_match.group(1)

            # Extract topic/theme
            topic = None
            theme_span = div.find('span', class_='thema')
            if theme_span:
                topic = theme_span.get_text(strip=True).replace('Thema', '').strip()

            # Extract description from paragraph
            description = None
            p = div.find('p')
            if p:
                description = p.get_text(strip=True)

            # Build full URL
            url = urljoin(BASE_URL, '/' + href.lstrip('/'))

            # Generate doc ID from URL
            doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', href.split('/')[-1].replace('.html', ''))

            return {
                'doc_id': doc_id,
                'url': url,
                'href': href,
                'title': title,
                'date': date,
                'topic': topic,
                'description': description
            }

        except Exception as e:
            logger.warning(f"Error parsing search result: {e}")
            return None

    def _fetch_document_content(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse full document content from HTML page"""
        try:
            logger.info(f"Fetching document: {url}")
            response = self._request_with_backoff(url, timeout=60)

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the main content area
            content_div = soup.find('div', id='content')
            if not content_div:
                content_div = soup.find('main')
            if not content_div:
                content_div = soup.find('article')
            if not content_div:
                content_div = soup.body

            # Extract text content, excluding navigation and footer
            text_parts = []

            # Look for the main article content
            for elem in content_div.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'li', 'td', 'th', 'div']):
                # Skip navigation, menu, sidebar elements
                classes = elem.get('class', [])
                if isinstance(classes, list):
                    classes_str = ' '.join(classes)
                else:
                    classes_str = classes

                if any(skip in classes_str.lower() for skip in ['nav', 'menu', 'footer', 'sidebar', 'skip', 'header']):
                    continue

                # Skip elements inside navigation
                parent_classes = []
                for parent in elem.parents:
                    parent_classes.extend(parent.get('class', []))

                if any(skip in ' '.join(parent_classes).lower() for skip in ['nav', 'menu', 'footer', 'sidebar']):
                    continue

                text = elem.get_text(strip=True)
                if text and len(text) > 10:
                    # Normalize whitespace
                    text = re.sub(r'\s+', ' ', text)
                    if text not in text_parts:  # Avoid duplicates
                        text_parts.append(text)

            # Combine text
            full_text = '\n\n'.join(text_parts)

            # Clean up the text
            full_text = html.unescape(full_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            full_text = re.sub(r' {2,}', ' ', full_text)

            # Extract metadata from the page
            title = soup.find('title')
            title_text = title.get_text(strip=True) if title else ''
            title_text = re.sub(r'\s*-\s*BaFin\s*$', '', title_text)

            # Find date metadata
            date = None
            date_meta = soup.find('meta', {'name': 'date'})
            if date_meta:
                date = date_meta.get('content', '')

            # Extract reference number from content
            reference = None
            ref_match = re.search(r'([A-Z]{2}\s*\d+[-–]\w+[-–]\d+[-/]\d+)', full_text)
            if ref_match:
                reference = ref_match.group(1)

            return {
                'text': full_text,
                'title': title_text,
                'date_meta': date,
                'reference': reference
            }

        except requests.RequestException as e:
            logger.error(f"Error fetching document {url}: {e}")
            return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert date string to ISO 8601"""
        if not date_str:
            return None

        # Format: DD.MM.YYYY
        if re.match(r'\d{2}\.\d{2}\.\d{4}', date_str):
            try:
                dt = datetime.strptime(date_str, '%d.%m.%Y')
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        # Format: YYYY-MM-DD
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str

        return date_str

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch BaFin regulatory circulars with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        page_no = 0
        total_count = 0
        seen_urls = set()

        while True:
            entries = self._get_search_results(page_no)

            if not entries:
                logger.info(f"No more entries found at page {page_no}")
                break

            for entry in entries:
                if limit and total_count >= limit:
                    logger.info(f"Reached limit of {limit} documents")
                    return

                # Skip duplicates
                if entry['url'] in seen_urls:
                    continue
                seen_urls.add(entry['url'])

                # Fetch full content
                content = self._fetch_document_content(entry['url'])

                if content and content.get('text') and len(content.get('text', '')) > 200:
                    # Merge entry metadata with content
                    doc = {
                        'doc_id': entry['doc_id'],
                        'url': entry['url'],
                        'title': entry.get('title') or content.get('title', ''),
                        'date': entry.get('date'),
                        'topic': entry.get('topic'),
                        'description': entry.get('description'),
                        'text': content.get('text', ''),
                        'reference': content.get('reference')
                    }

                    yield doc
                    total_count += 1
                    logger.info(f"[{total_count}] Fetched: {doc['title'][:60]}... ({len(doc['text']):,} chars)")
                else:
                    logger.warning(f"Skipping {entry['url']} - insufficient content")

                # Rate limiting - generous delay to avoid 429s
                time.sleep(3)

            page_no += 1
            time.sleep(2)

        logger.info(f"Fetched {total_count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent documents (first page only for updates)"""
        yield from self.fetch_all(limit=20)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        doc_id = raw_doc.get('doc_id', '')

        # Parse date
        date = self._parse_date(raw_doc.get('date', ''))

        # Build title
        title = raw_doc.get('title', '') or doc_id

        return {
            '_id': f"bafin_{doc_id}",
            '_source': 'DE/BaFin',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': date,
            'url': raw_doc.get('url', ''),
            'topic': raw_doc.get('topic', ''),
            'description': raw_doc.get('description', ''),
            'reference': raw_doc.get('reference', ''),
            'authority': 'BaFin',
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BaFinFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 15 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 5):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(':', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['title'][:50]}... ({text_len:,} chars)")
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
        fetcher = BaFinFetcher()
        print("Testing BaFin fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:100]}")
            print(f"Topic: {normalized['topic']}")
            print(f"Date: {normalized['date']}")
            print(f"URL: {normalized['url']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
