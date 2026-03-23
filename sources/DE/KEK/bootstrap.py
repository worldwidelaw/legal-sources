#!/usr/bin/env python3
"""
German Media Concentration Commission (KEK) Fetcher

Media concentration decisions from the Kommission zur Ermittlung der
Konzentration im Medienbereich (KEK).

Source: https://www.kek-online.de
Discovery: HTML listing + AJAX loadmore pagination
Content: Full text from individual HTML article pages

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

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.kek-online.de"
LISTING_URL = f"{BASE_URL}/presse/pressemitteilungen/"
AJAX_BASE = "/presse/pressemitteilungen/wwt3listreadmore.json"


class KEKFetcher:
    """Fetcher for German Media Concentration Commission (KEK) decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_all_article_urls(self) -> List[str]:
        """Discover all article URLs from listing + AJAX pagination"""
        all_urls = []

        # Page 1: regular HTML
        logger.info("Fetching listing page 1")
        try:
            resp = self.session.get(LISTING_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')

            for a in soup.find_all('a', href=True):
                href = a['href']
                if ('/presse/pressemitteilungen/' in href and
                        href != '/presse/pressemitteilungen/' and
                        href.count('/') > 3 and
                        href not in all_urls):
                    all_urls.append(href)

            # Find AJAX loadmore URL
            btn_div = soup.find('div', class_='ajaxLoadmore')
            next_url = btn_div.get('data-url') if btn_div else None

        except requests.RequestException as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return all_urls

        # Pages 2+: AJAX JSON
        page = 2
        while next_url:
            logger.info(f"Fetching AJAX page {page}")
            time.sleep(1.0)

            try:
                full_url = f"{BASE_URL}{next_url}" if next_url.startswith('/') else next_url
                resp = self.session.get(full_url, timeout=30,
                                       headers={'X-Requested-With': 'XMLHttpRequest'})
                resp.raise_for_status()
                data = resp.json()

                html_data = data.get('data', '')
                if html_data:
                    soup = BeautifulSoup(html_data, 'html.parser')
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        if ('/presse/pressemitteilungen/' in href and
                                href.count('/') > 3 and
                                href not in all_urls):
                            all_urls.append(href)

                next_url = data.get('nextpage') or None
                page += 1

            except Exception as e:
                logger.error(f"AJAX page {page} failed: {e}")
                break

        logger.info(f"Found {len(all_urls)} article URLs total")
        return all_urls

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
            return None

        # Extract date
        date_str = None
        # Look for date class or pattern
        for elem in soup.find_all(class_=re.compile('date|datum', re.I)):
            text = elem.get_text(strip=True)
            m = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
            if m:
                date_str = m.group(1)
                break

        # Fallback: search in article header area
        if not date_str:
            article = soup.find('article') or soup.find('main')
            if article:
                text = article.get_text()[:500]
                m = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
                if m:
                    date_str = m.group(1)

        date_iso = self._normalize_date(date_str)

        # Extract body text
        text_parts = []
        content_selectors = [
            'article .ce-bodytext',
            '.news-detail',
            'article',
            'main .content',
            'main',
        ]

        content_elem = None
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem and len(content_elem.get_text(strip=True)) > 200:
                break
            content_elem = None

        if content_elem:
            for unwanted in content_elem.find_all(['script', 'style', 'nav', 'form', 'iframe', 'noscript']):
                unwanted.decompose()
            for unwanted in content_elem.select('.share, .social, .breadcrumb, .sidebar, .pagination'):
                unwanted.decompose()

            for elem in content_elem.find_all(['p', 'h2', 'h3', 'h4', 'li', 'blockquote', 'td']):
                text = elem.get_text(strip=True)
                if text and len(text) > 5:
                    text_parts.append(text)

        if not text_parts:
            for p in soup.find_all('p'):
                text = p.get_text(strip=True)
                if text and len(text) > 20:
                    text_parts.append(text)

        full_text = '\n\n'.join(text_parts)
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
            'path': path,
        }

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            return None
        m = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})', date_str.strip())
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        path = raw.get('path', '')
        doc_id = path.strip('/').replace('/', '_')

        return {
            '_id': f"DE_KEK_{doc_id}",
            '_source': 'DE/KEK',
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        urls = self._get_all_article_urls()
        for i, path in enumerate(urls):
            logger.info(f"[{i+1}/{len(urls)}] Fetching: {path}")
            article = self._extract_article(path)
            if article:
                yield self.normalize(article)
            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        urls = self._get_all_article_urls()
        for path in urls:
            article = self._extract_article(path)
            if article:
                if article.get('date') and article['date'] < since:
                    break
                yield self.normalize(article)
            time.sleep(1.5)

    def bootstrap_sample(self, count: int = 15) -> List[Dict[str, Any]]:
        urls = self._get_all_article_urls()
        # Sample from beginning, middle, end
        indices = list(range(min(5, len(urls))))
        mid = len(urls) // 2
        indices.extend(range(mid, min(mid + 5, len(urls))))
        indices.extend(range(max(0, len(urls) - 5), len(urls)))
        seen = set()
        selected = []
        for i in indices:
            if i not in seen and i < len(urls):
                selected.append(urls[i])
                seen.add(i)
            if len(selected) >= count:
                break

        results = []
        for i, path in enumerate(selected):
            logger.info(f"[Sample {i+1}/{len(selected)}] Fetching: {path}")
            article = self._extract_article(path)
            if article:
                results.append(self.normalize(article))
            time.sleep(1.5)

        return results


def main():
    if len(sys.argv) < 2:
        print("Usage: bootstrap.py <command> [options]")
        sys.exit(1)

    if sys.argv[1] == 'bootstrap':
        fetcher = KEKFetcher()
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

            texts = [r.get('text', '') for r in records]
            non_empty = sum(1 for t in texts if len(t) > 100)
            avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
            dates = sum(1 for r in records if r.get('date'))
            print(f"\n=== Validation Summary ===")
            print(f"Records fetched: {len(records)}")
            print(f"Records with substantial text (>100 chars): {non_empty}")
            print(f"Records with dates: {dates}")
            print(f"Average text length: {avg_len:.0f} chars")

            if non_empty < len(records) * 0.8:
                print("WARNING: Less than 80% of records have substantial text!")
            else:
                print("OK: Text extraction looks good.")

        elif '--full' in sys.argv:
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
