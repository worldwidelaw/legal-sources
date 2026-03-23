#!/usr/bin/env python3
"""
Finnish Tax Administration (Vero) Data Fetcher

Fetches tax guidance, preliminary rulings (KVL), decisions, and statements
from vero.fi via sitemap discovery + HTML scraping.

~1,790 documents. No API available.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SITEMAP_URL = "https://www.vero.fi/sitemap.xml"
BASE_URL = "https://www.vero.fi"

DOC_TYPE_MAP = {
    'ohje-hakusivu': 'guidance',
    'ennakkoratkaisut': 'preliminary_ruling',
    'paatokset': 'decision',
    'kannanotot': 'statement',
}


class VeroFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept-Language': 'fi,en;q=0.5',
        })

    def _get(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.warning(f"GET {url} attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None

    def _discover_urls(self) -> List[Dict[str, str]]:
        """Get doctrine URLs from sitemap."""
        resp = self._get(SITEMAP_URL)
        if not resp:
            return []

        root = ET.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        results = []
        for url_elem in root.findall('.//sm:url', ns):
            loc = url_elem.find('sm:loc', ns)
            lastmod = url_elem.find('sm:lastmod', ns)
            if loc is None:
                continue
            url = loc.text
            if url and '/syventavat-vero-ohjeet/' in url:
                # Skip listing pages
                path = url.replace(BASE_URL, '').strip('/')
                segments = path.split('/')
                if len(segments) >= 3:  # syventavat-vero-ohjeet/type/slug
                    results.append({
                        'url': url,
                        'lastmod': lastmod.text if lastmod is not None else None,
                    })

        logger.info(f"Discovered {len(results)} doctrine URLs from sitemap")
        return results

    def _classify_url(self, url: str) -> str:
        """Determine document type from URL."""
        for key, val in DOC_TYPE_MAP.items():
            if key in url:
                return val
        return 'other'

    def _extract_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract document content."""
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Title
        title = ''
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            title = re.sub(r'\s*[-–|]\s*[Vv]ero\.fi.*$', '', title)
            title = re.sub(r'\s*[-–|]\s*[Ss]katt.*$', '', title)

        # Page ID
        page_id_meta = soup.find('meta', attrs={'name': 'pageID'})
        page_id = page_id_meta['content'] if page_id_meta and page_id_meta.get('content') else None

        # Date and metadata from hero section
        date = None
        diary_number = ''
        hero = soup.find(class_='precise-information-hero-element') or soup.find('header')

        if hero:
            hero_text = hero.get_text()
            # Finnish date: DD.MM.YYYY
            date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', hero_text)
            if date_match:
                d, m, y = date_match.groups()
                try:
                    date = f"{y}-{int(m):02d}-{int(d):02d}"
                except ValueError:
                    pass

            # Diary number
            diary_match = re.search(r'VH/[\d/]+\.\d+/\d{4}', hero_text)
            if diary_match:
                diary_number = diary_match.group()

        # Extract main content
        main = soup.find('main')
        if not main:
            main = soup.find('div', class_='contentarea-main')
        if not main:
            main = soup.find('article')

        text = ''
        if main:
            for tag in main.find_all(['nav', 'aside', 'script', 'style', 'header', 'footer']):
                tag.decompose()
            # Also remove breadcrumbs and hero if inside main
            for cls in ['breadcrumbs', 'hero', 'sidebar']:
                for el in main.find_all(class_=re.compile(cls, re.I)):
                    el.decompose()

            text = main.get_text(separator='\n', strip=True)
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            text = text.strip()

        if not text or len(text) < 100:
            return None

        # Generate ID
        slug = url.rstrip('/').split('/')[-1]
        doc_id = page_id or slug

        return {
            'doc_id': doc_id,
            'title': title,
            'text': text,
            'date': date,
            'url': url,
            'doc_type': self._classify_url(url),
            'diary_number': diary_number,
        }

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all Vero doctrine documents."""
        urls = self._discover_urls()
        if not urls:
            return

        fetched = 0
        for entry in urls:
            if max_docs is not None and fetched >= max_docs:
                break

            url = entry['url']
            logger.info(f"Fetching {url} ({fetched+1}/{max_docs or len(urls)})...")

            doc = self._extract_document(url)
            if doc and len(doc.get('text', '')) >= 100:
                yield doc
                fetched += 1
            else:
                logger.warning(f"Insufficient content for {url}")

            time.sleep(1)

        logger.info(f"Fetched {fetched} Vero documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    if datetime.fromisoformat(doc['date']) >= since:
                        yield doc
                except Exception:
                    yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            '_id': raw['doc_id'],
            '_source': 'FI/VERO',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_id': raw['doc_id'],
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'doc_type': raw.get('doc_type', ''),
            'diary_number': raw.get('diary_number', ''),
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = VeroFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        max_docs = 15 if is_sample else 100

        logger.info(f"Fetching {'sample' if is_sample else 'batch'} Vero documents...")

        count = 0
        for raw in fetcher.fetch_all(max_docs=max_docs):
            normalized = fetcher.normalize(raw)
            if len(normalized.get('text', '')) < 100:
                continue

            filename = re.sub(r'[^\w\-.]', '_', normalized['document_id'])[:80] + '.json'
            with open(sample_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['title'][:80]}... ({len(normalized['text'])} chars)")
            count += 1

        logger.info(f"Bootstrap complete. {count} documents saved to {sample_dir}")
        if count > 0:
            files = list(sample_dir.glob('*.json'))
            total = sum(len(json.load(open(f)).get('text', '')) for f in files)
            logger.info(f"Average text length: {total // len(files):,} chars/doc")
    else:
        fetcher = VeroFetcher()
        print("Testing Vero fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(max_docs=3)):
            n = fetcher.normalize(raw)
            print(f"\n--- {i+1} ---")
            print(f"ID: {n['document_id']}, Type: {n['doc_type']}")
            print(f"Title: {n['title'][:100]}")
            print(f"Date: {n['date']}, Text: {len(n['text']):,} chars")
            if i >= 2:
                break


if __name__ == '__main__':
    main()
