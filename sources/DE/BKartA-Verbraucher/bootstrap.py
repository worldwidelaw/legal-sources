#!/usr/bin/env python3
"""
German Consumer Protection (vzbv) Fetcher

Publications, position papers, court ruling summaries, and policy statements from
the Verbraucherzentrale Bundesverband (vzbv) - the German Federation of Consumer
Organizations.

Source: https://www.vzbv.de
Discovery: XML Sitemap (3 pages)
Content: HTML article pages with full text

Content types:
- /meldungen/ — Policy news and statements
- /urteile/ — Court ruling summaries from vzbv litigation
- /publikationen/ — Position papers, fact sheets, reports
- /pressemitteilungen/ — Press releases
- /stellungnahmen/ — Official position statements

Data is public domain official works under German law (§ 5 UrhG).
"""

import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.vzbv.de"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
SITEMAP_PAGES = 3

# URL path prefixes that contain doctrine content
CONTENT_PREFIXES = (
    '/meldungen/',
    '/urteile/',
    '/publikationen/',
    '/pressemitteilungen/',
    '/stellungnahmen/',
)

# Map URL path prefix to content type
CONTENT_TYPE_MAP = {
    '/meldungen/': 'policy_statement',
    '/urteile/': 'court_ruling_summary',
    '/publikationen/': 'publication',
    '/pressemitteilungen/': 'press_release',
    '/stellungnahmen/': 'position_paper',
}


class VzbvFetcher:
    """Fetcher for German Consumer Protection (vzbv) publications"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_sitemap_urls(self) -> List[Dict[str, str]]:
        """Fetch all content URLs from the sitemap pages"""
        all_urls = []

        for page in range(1, SITEMAP_PAGES + 1):
            url = f"{SITEMAP_URL}?page={page}"
            logger.info(f"Fetching sitemap page {page}: {url}")

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()

                root = ET.fromstring(resp.content)
                ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

                for url_elem in root.findall('sm:url', ns):
                    loc = url_elem.find('sm:loc', ns)
                    lastmod = url_elem.find('sm:lastmod', ns)

                    if loc is not None and loc.text:
                        parsed = urlparse(loc.text)
                        path = parsed.path

                        if any(path.startswith(prefix) for prefix in CONTENT_PREFIXES):
                            entry = {
                                'url': loc.text,
                                'path': path,
                                'lastmod': lastmod.text if lastmod is not None else None,
                            }
                            all_urls.append(entry)

                time.sleep(1.0)

            except Exception as e:
                logger.error(f"Error fetching sitemap page {page}: {e}")

        logger.info(f"Found {len(all_urls)} content URLs in sitemap")
        return all_urls

    def _get_content_type(self, path: str) -> str:
        """Determine content type from URL path"""
        for prefix, ctype in CONTENT_TYPE_MAP.items():
            if path.startswith(prefix):
                return ctype
        return 'unknown'

    def _extract_article(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract full text from an article page"""
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
                title = title_tag.get_text(strip=True).replace(' | vzbv', '')

        if not title:
            return None

        # Extract date - vzbv uses <span class="date">Datum: DD.MM.YYYY</span>
        date_str = None
        for date_elem in soup.find_all(class_='date'):
            text = date_elem.get_text(strip=True)
            if text.startswith('Datum:'):
                # Primary article date
                m = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
                if m:
                    date_str = m.group(1)
                break

        # Fallback: first date-class element with a date pattern
        if not date_str:
            for date_elem in soup.find_all(class_='date'):
                text = date_elem.get_text(strip=True)
                m = re.match(r'^(\d{2}\.\d{2}\.\d{4})$', text)
                if m:
                    date_str = m.group(1)
                    break

        # Fallback: meta tags
        if not date_str:
            for meta_name in ['article:published_time', 'dcterms.date']:
                meta = soup.find('meta', attrs={'property': meta_name}) or soup.find('meta', attrs={'name': meta_name})
                if meta and meta.get('content'):
                    date_str = meta['content']
                    break

        # Normalize date to ISO format
        date_iso = self._normalize_date(date_str)

        # Extract main article body text
        text_parts = []

        # Try various content containers common in Drupal
        content_selectors = [
            'article',
            '.field--name-body',
            '.node__content',
            '.content-area',
            'main .text-long',
            'main',
        ]

        content_elem = None
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                break

        if content_elem:
            # Remove navigation, scripts, styles, forms
            for unwanted in content_elem.find_all(['script', 'style', 'nav', 'form', 'iframe', 'noscript']):
                unwanted.decompose()
            # Remove share buttons, cookie banners, etc.
            for unwanted in content_elem.select('.share-buttons, .cookie-banner, .social-media, .breadcrumb, .sidebar'):
                unwanted.decompose()

            # Extract text from paragraphs and headings
            for elem in content_elem.find_all(['p', 'h2', 'h3', 'h4', 'li', 'blockquote', 'td']):
                text = elem.get_text(strip=True)
                if text and len(text) > 5:
                    text_parts.append(text)

        # Fallback: get all paragraphs from the page
        if not text_parts:
            for p in soup.find_all('p'):
                text = p.get_text(strip=True)
                if text and len(text) > 20:
                    text_parts.append(text)

        full_text = '\n\n'.join(text_parts)

        # Strip any residual HTML tags
        full_text = re.sub(r'<[^>]+>', '', full_text)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text).strip()

        # Skip if no substantial text
        if len(full_text) < 100:
            logger.warning(f"Insufficient text content for {url} ({len(full_text)} chars)")
            return None

        # Extract category/topic from breadcrumbs or tags
        category = None
        breadcrumb = soup.select_one('.breadcrumb, nav[aria-label="Breadcrumb"]')
        if breadcrumb:
            crumbs = breadcrumb.find_all('a')
            if len(crumbs) >= 2:
                category = crumbs[-1].get_text(strip=True)

        # Try meta keywords/tags
        if not category:
            meta_kw = soup.find('meta', attrs={'name': 'keywords'})
            if meta_kw and meta_kw.get('content'):
                category = meta_kw['content'].split(',')[0].strip()

        return {
            'title': title,
            'text': full_text,
            'date': date_iso,
            'url': url,
            'category': category,
        }

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """Convert various date formats to ISO 8601"""
        if not date_str:
            return None

        date_str = date_str.strip()

        # Already ISO format
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]

        # DD.MM.YYYY (German format)
        m = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})', date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

        # Try common formats
        for fmt in ['%d. %B %Y', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
            try:
                dt = datetime.strptime(date_str[:19], fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue

        return None

    def normalize(self, raw: Dict[str, Any], url_info: Dict[str, str]) -> Dict[str, Any]:
        """Normalize extracted article into standard schema"""
        path = url_info.get('path', '')
        content_type = self._get_content_type(path)

        # Generate stable ID from URL path
        doc_id = path.strip('/').replace('/', '_')

        return {
            '_id': f"DE_BKartA-Verbraucher_{doc_id}",
            '_source': 'DE/BKartA-Verbraucher',
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'category': raw.get('category'),
            'content_type': content_type,
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all documents with full text"""
        urls = self._get_sitemap_urls()
        logger.info(f"Processing {len(urls)} content URLs")

        for i, url_info in enumerate(urls):
            url = url_info['url']
            logger.info(f"[{i+1}/{len(urls)}] Fetching: {url}")

            article = self._extract_article(url)
            if article:
                yield self.normalize(article, url_info)

            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents modified since a given date"""
        urls = self._get_sitemap_urls()

        for url_info in urls:
            lastmod = url_info.get('lastmod')
            if lastmod and lastmod[:10] >= since:
                url = url_info['url']
                logger.info(f"Fetching updated: {url}")

                article = self._extract_article(url)
                if article:
                    yield self.normalize(article, url_info)

                time.sleep(1.5)

    def bootstrap_sample(self, count: int = 15) -> List[Dict[str, Any]]:
        """Fetch a sample of documents for testing"""
        urls = self._get_sitemap_urls()

        # Get a diverse sample: pick from different content types
        samples_by_type: Dict[str, List] = {}
        for url_info in urls:
            ctype = self._get_content_type(url_info['path'])
            samples_by_type.setdefault(ctype, []).append(url_info)

        selected = []
        per_type = max(2, count // len(samples_by_type)) if samples_by_type else count
        for ctype, type_urls in samples_by_type.items():
            selected.extend(type_urls[:per_type])
            if len(selected) >= count:
                break

        selected = selected[:count]

        results = []
        for i, url_info in enumerate(selected):
            url = url_info['url']
            logger.info(f"[Sample {i+1}/{len(selected)}] Fetching: {url}")

            article = self._extract_article(url)
            if article:
                record = self.normalize(article, url_info)
                results.append(record)

            time.sleep(1.5)

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
        fetcher = VzbvFetcher()
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
            print(f"\n=== Validation Summary ===")
            print(f"Records fetched: {len(records)}")
            print(f"Records with substantial text (>100 chars): {non_empty}")
            print(f"Average text length: {avg_len:.0f} chars")
            print(f"Content types: {set(r.get('content_type') for r in records)}")

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
