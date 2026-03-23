#!/usr/bin/env python3
"""
EU/OLAF Data Fetcher
European Anti-Fraud Office news and press releases

This fetcher retrieves news articles and press releases from OLAF:
- Press releases about investigations and operations
- News articles about anti-fraud activities
- Cooperation announcements and policy updates

Data flow:
1. Scrape news listing pages with pagination
2. Extract article URLs
3. Fetch each article and extract full text from HTML
4. Normalize to standard schema
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from html import unescape

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://anti-fraud.ec.europa.eu"
NEWS_LISTING_URL = f"{BASE_URL}/media-corner/news_en"


class OLAFFetcher:
    """Fetcher for OLAF news and press releases."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })

    def _make_request(self, url: str, max_retries: int = 3,
                      base_delay: float = 2.0, timeout: int = 30) -> requests.Response:
        """Make HTTP request with retry logic."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                else:
                    raise

    def _extract_article_urls(self, page_num: int = 0) -> List[str]:
        """Extract article URLs from a news listing page.

        Args:
            page_num: Page number (0-indexed)

        Returns:
            List of article URLs
        """
        url = f"{NEWS_LISTING_URL}?page={page_num}"
        logger.info(f"Fetching listing page {page_num}...")

        response = self._make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all news article links
        article_urls = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Match news article URLs
            if '/media-corner/news/' in href and href.endswith('_en'):
                full_url = href if href.startswith('http') else BASE_URL + href
                if full_url not in article_urls:
                    article_urls.append(full_url)

        return article_urls

    def _extract_article_content(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single article.

        Args:
            url: Article URL

        Returns:
            Dict with article metadata and text, or None if failed
        """
        try:
            response = self._make_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract title from <h1> or og:title
            title = None
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text(strip=True)
            if not title:
                og_title = soup.find('meta', property='og:title')
                if og_title:
                    title = og_title.get('content', '')

            if not title:
                logger.warning(f"No title found for {url}")
                return None

            # Extract article body from main content div
            text_parts = []

            # Look for article content in the main structure
            article = soup.find('article')
            if article:
                # Find the content div with class 'ecl' (EC Layout component)
                content_div = article.find('div', class_='ecl')
                if content_div:
                    # Get all paragraphs and list items
                    for elem in content_div.find_all(['p', 'li']):
                        text = elem.get_text(strip=True)
                        if text and len(text) > 10:  # Skip very short fragments
                            text_parts.append(text)

            # If no ecl div found, try alternative selectors
            if not text_parts:
                main_content = soup.find('main', id='main-content')
                if main_content:
                    for p in main_content.find_all('p'):
                        text = p.get_text(strip=True)
                        if text and len(text) > 10:
                            text_parts.append(text)

            # Clean and join text
            text = '\n\n'.join(text_parts)
            text = self._clean_text(text)

            if len(text) < 100:
                logger.warning(f"Article text too short ({len(text)} chars): {url}")
                return None

            # Extract date from description list or meta
            date = None

            # Try to find date in description list
            dt_elems = soup.find_all('dt', class_='ecl-description-list__term')
            for dt in dt_elems:
                if 'Publication date' in dt.get_text():
                    dd = dt.find_next_sibling('dd')
                    if dd:
                        date_text = dd.get_text(strip=True)
                        date = self._parse_date(date_text)
                        break

            # Try URL-based date extraction if not found
            if not date:
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', url)
                if date_match:
                    date = date_match.group(1)

            # Extract news type
            news_type = "news article"
            for dt in dt_elems:
                if 'News type' in dt.get_text():
                    dd = dt.find_next_sibling('dd')
                    if dd:
                        news_type = dd.get_text(strip=True).lower()
                        break

            # Extract ID from URL
            # URL format: /media-corner/news/{slug}-{date}_en
            url_path = url.replace(BASE_URL, '').replace('_en', '')
            article_id = url_path.split('/')[-1] if url_path else url

            return {
                'id': article_id,
                'title': title,
                'text': text,
                'date': date,
                'url': url,
                'news_type': news_type,
            }

        except Exception as e:
            logger.error(f"Failed to extract article {url}: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted text content."""
        # Unescape HTML entities
        text = unescape(text)

        # Remove multiple spaces/newlines
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)

        # Remove common boilerplate
        boilerplate = [
            'Share this page',
            'Skip to main content',
            'Select your language',
        ]
        for bp in boilerplate:
            text = text.replace(bp, '')

        return text.strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO format.

        Args:
            date_str: Date string (e.g., "18 March 2026", "24 October 2023")

        Returns:
            ISO date string (YYYY-MM-DD) or None
        """
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try different formats
        formats = [
            "%d %B %Y",      # 18 March 2026
            "%d %b %Y",      # 18 Mar 2026
            "%B %d, %Y",     # March 18, 2026
            "%Y-%m-%d",      # 2026-03-18
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def fetch_all(self, max_docs: Optional[int] = None,
                  max_pages: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Fetch all available news articles.

        Args:
            max_docs: Maximum number of documents to fetch
            max_pages: Maximum number of listing pages to scrape

        Yields:
            Raw article documents with full text
        """
        page = 0
        total_fetched = 0
        seen_urls = set()
        consecutive_empty = 0

        while True:
            if max_pages is not None and page >= max_pages:
                logger.info(f"Reached max pages limit ({max_pages})")
                break

            article_urls = self._extract_article_urls(page)

            if not article_urls:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.info("No more articles found")
                    break
                page += 1
                continue

            consecutive_empty = 0

            for url in article_urls:
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                if max_docs is not None and total_fetched >= max_docs:
                    return

                article = self._extract_article_content(url)
                if article and article.get('text'):
                    yield article
                    total_fetched += 1
                    logger.info(f"Fetched {total_fetched}: {article['title'][:50]}...")

                # Rate limiting
                time.sleep(2)

            page += 1

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch articles published since a given date.

        Args:
            since: Minimum publication date

        Yields:
            Article documents with full text
        """
        for article in self.fetch_all(max_pages=10):
            if article.get('date'):
                try:
                    article_date = datetime.strptime(article['date'], "%Y-%m-%d")
                    if article_date < since:
                        logger.info(f"Reached articles older than {since.date()}, stopping")
                        return
                except ValueError:
                    pass
            yield article

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema.

        Args:
            raw_doc: Raw article from scraper

        Returns:
            Normalized document dict
        """
        article_id = raw_doc.get('id', '')

        # Determine document subtype
        news_type = raw_doc.get('news_type', 'news article')
        is_press_release = 'press' in news_type.lower()

        return {
            '_id': f"OLAF-{article_id}",
            '_source': 'EU/OLAF',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date'),
            'url': raw_doc.get('url', ''),
            'news_type': news_type,
            'is_press_release': is_press_release,
        }


def main():
    """Main entry point."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        # Bootstrap mode - fetch sample data
        fetcher = OLAFFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 15 if is_sample else 100

        logger.info(f"Fetching {'sample' if is_sample else 'full'} data from OLAF...")
        logger.info(f"Target count: {target_count}")

        sample_count = 0
        text_lengths = []

        for raw_doc in fetcher.fetch_all(max_docs=target_count, max_pages=5 if is_sample else None):
            normalized = fetcher.normalize(raw_doc)

            # Validate: must have substantial text
            text_len = len(normalized.get('text', ''))
            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len} chars)")
                continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace('/', '_').replace(':', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['_id']} - {normalized['title'][:60]}... ({text_len} chars)")
            text_lengths.append(text_len)
            sample_count += 1

            if sample_count >= target_count:
                break

        # Print summary
        if text_lengths:
            avg_len = sum(text_lengths) // len(text_lengths)
            logger.info(f"\nBootstrap complete.")
            logger.info(f"  Documents saved: {sample_count}")
            logger.info(f"  Average text length: {avg_len:,} characters")
            logger.info(f"  Sample directory: {sample_dir}")
        else:
            logger.error("No documents with valid text content found!")
            sys.exit(1)

    else:
        # Test mode - quick verification
        fetcher = OLAFFetcher()

        print("Testing OLAF fetcher...")
        print("=" * 60)

        # Test listing page
        urls = fetcher._extract_article_urls(0)
        print(f"Found {len(urls)} article URLs on page 0")

        if urls:
            # Test single article extraction
            print(f"\nTesting article: {urls[0]}")
            article = fetcher._extract_article_content(urls[0])
            if article:
                print(f"  Title: {article['title'][:60]}...")
                print(f"  Date: {article['date']}")
                print(f"  Text length: {len(article['text'])} chars")
                print(f"  Text preview: {article['text'][:200]}...")
            else:
                print("  Failed to extract article")

        print(f"\n{'=' * 60}")
        print("Test complete.")


if __name__ == '__main__':
    main()
