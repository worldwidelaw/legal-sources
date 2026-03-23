#!/usr/bin/env python3
"""
NO/KONKURR - Norwegian Competition Authority (Konkurransetilsynet) Doctrine Fetcher

Fetches competition enforcement decisions, merger notifications, consultation
responses, and publications from the Norwegian Competition Authority.

Data source: https://konkurransetilsynet.no
Enumeration: XML sitemaps (decisions, fusjoner, publications)
Content: WordPress custom post types, extracted from HTML <article> tags

License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://konkurransetilsynet.no"
SITEMAP_INDEX = f"{BASE_URL}/sitemap_index.xml"
SOURCE_ID = "NO/KONKURR"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

# Sitemaps containing doctrine content
DOCTRINE_SITEMAPS = [
    "decisions-sitemap.xml",
    "fusjoner-sitemap.xml",
    "publications-sitemap.xml",
]


class KonkurransetilsynetFetcher:
    """Fetcher for Norwegian Competition Authority decisions and publications."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5',
        })

    def _get_sitemap_urls(self, sitemap_url: str) -> List[str]:
        """Fetch and parse a sitemap XML to get all URLs."""
        logger.info(f"Fetching sitemap: {sitemap_url}")
        try:
            resp = self.session.get(sitemap_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch sitemap {sitemap_url}: {e}")
            return []

        urls = []
        try:
            root = ET.fromstring(resp.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            for url_el in root.findall('.//sm:url/sm:loc', ns):
                if url_el.text:
                    urls.append(url_el.text.strip())
        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")

        logger.info(f"  Found {len(urls)} URLs in sitemap")
        return urls

    def get_all_urls(self) -> List[str]:
        """Get all doctrine URLs from sitemaps."""
        all_urls = []
        for sitemap_name in DOCTRINE_SITEMAPS:
            sitemap_url = f"{BASE_URL}/{sitemap_name}"
            urls = self._get_sitemap_urls(sitemap_url)
            all_urls.extend(urls)
        logger.info(f"Total URLs from sitemaps: {len(all_urls)}")
        return all_urls

    def _classify_url(self, url: str) -> str:
        """Classify URL into document type."""
        path = urlparse(url).path
        if '/fusjoner/' in path:
            return 'merger'
        elif '/publications/' in path:
            return 'publication'
        elif '/decisions/' in path:
            slug = path.rstrip('/').split('/')[-1]
            if slug.startswith('vedtak-'):
                return 'decision'
            elif slug.startswith('avgjorelse-') or '-a20' in slug:
                return 'ruling'
            elif slug.startswith('horingsuttalelse-'):
                return 'consultation'
            return 'decision'
        return 'other'

    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main article text from page."""
        # Try article tag first (primary content container)
        article = soup.find('article')
        if article:
            # Get the left-content div (main body)
            left = article.find('div', class_=re.compile(r'left-content'))
            if left:
                # Remove aside, nav, script
                for tag in left.find_all(['aside', 'nav', 'script', 'style']):
                    tag.decompose()
                text = left.get_text(separator="\n", strip=True)
                if len(text) > 30:
                    return self._clean_text(text)

            # Fallback: full article content
            content_div = article.find('div', class_=re.compile(r'content'))
            if content_div:
                for tag in content_div.find_all(['aside', 'nav', 'script', 'style']):
                    tag.decompose()
                text = content_div.get_text(separator="\n", strip=True)
                if len(text) > 30:
                    return self._clean_text(text)

            # Last fallback: entire article
            for tag in article.find_all(['nav', 'script', 'style']):
                tag.decompose()
            text = article.get_text(separator="\n", strip=True)
            if len(text) > 30:
                return self._clean_text(text)

        # Non-article page: try main content area
        main = soup.find('main') or soup.find('div', class_=re.compile(r'entry-content'))
        if main:
            for tag in main.find_all(['nav', 'footer', 'aside', 'script', 'style']):
                tag.decompose()
            text = main.get_text(separator="\n", strip=True)
            if len(text) > 30:
                return self._clean_text(text)

        return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        lines = [line.strip() for line in text.splitlines()]
        lines = [line for line in lines if line]
        return "\n".join(lines)

    def _extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract metadata from page."""
        meta = {'url': url}

        article = soup.find('article')
        if article:
            # Title
            h1 = article.find('h1')
            if h1:
                meta['title'] = h1.get_text(strip=True)

            # Legal provision (bestemmelse)
            best = article.find('p', class_='bestemmelse')
            if best:
                meta['legal_provision'] = best.get_text(strip=True).replace('Bestemmelse:', '').strip()

            # Document date (brevdato/dokumentdato)
            brevdato = article.find('p', class_='brevdato')
            if brevdato:
                text = brevdato.get_text(strip=True)
                date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
                if date_match:
                    d, m, y = date_match.groups()
                    meta['date'] = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

            # Status (for mergers)
            status = article.find('p', class_='status')
            if status:
                meta['status'] = status.get_text(strip=True).replace('Status:', '').strip()

            # Published date from <time pubdate>
            time_el = article.find('time', attrs={'pubdate': True})
            if time_el:
                date_text = time_el.get_text(strip=True)
                date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text)
                if date_match:
                    d, m, y = date_match.groups()
                    meta.setdefault('date', f"{y}-{m.zfill(2)}-{d.zfill(2)}")

        if 'title' not in meta:
            h1 = soup.find('h1')
            if h1:
                meta['title'] = h1.get_text(strip=True)
            else:
                title = soup.find('title')
                if title:
                    meta['title'] = title.get_text(strip=True).split('|')[0].strip()

        # Fallback date from page text
        if 'date' not in meta:
            page_text = soup.get_text()
            for pattern in [
                r'Dokumentdato:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})',
                r'Dato mottatt:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})',
                r'Publisert:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})',
            ]:
                match = re.search(pattern, page_text, re.DOTALL)
                if match:
                    d, m, y = match.groups()
                    meta['date'] = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
                    break

        return meta

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into standard schema."""
        url = raw['url']
        path = urlparse(url).path
        doc_id = hashlib.md5(path.encode()).hexdigest()[:12]
        doc_type = self._classify_url(url)

        return {
            '_id': f"NO-KONKURR-{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': url,
            'doc_subtype': doc_type,
            'legal_provision': raw.get('legal_provision', ''),
            'status': raw.get('status', ''),
            'language': 'nob',
        }

    def fetch_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single page."""
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')
        meta = self._extract_metadata(soup, url)

        # Re-parse for content extraction (since extract_content may modify soup)
        soup2 = BeautifulSoup(resp.text, 'html.parser')
        text = self._extract_content(soup2)
        if not text:
            logger.debug(f"No content extracted from {url}")
            return None

        meta['text'] = text
        return meta

    def fetch_all(self, max_pages: int = 10000) -> Iterator[Dict[str, Any]]:
        """Fetch all doctrine documents."""
        urls = self.get_all_urls()
        count = 0
        for i, url in enumerate(urls[:max_pages]):
            raw = self.fetch_page(url)
            if raw and raw.get('text') and len(raw['text']) > 30:
                record = self.normalize(raw)
                count += 1
                if count % 50 == 0:
                    logger.info(f"[{count}/{len(urls)}] {record['title'][:60]}")
                yield record
            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch documents modified since a given date."""
        for record in self.fetch_all():
            if record.get('date') and record['date'] >= since:
                yield record

    def bootstrap_sample(self, count: int = 15) -> list:
        """Fetch a sample of documents for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        samples = []

        # Get URLs from each sitemap type for variety
        all_urls = {}
        for sitemap_name in DOCTRINE_SITEMAPS:
            sitemap_url = f"{BASE_URL}/{sitemap_name}"
            urls = self._get_sitemap_urls(sitemap_url)
            all_urls[sitemap_name] = urls

        # Take samples from each type
        sample_urls = []
        for name, urls in all_urls.items():
            # Take first 5 from each sitemap (most recent)
            sample_urls.extend(urls[:5])

        for url in sample_urls:
            if len(samples) >= count:
                break
            logger.info(f"Fetching sample: {url}")
            raw = self.fetch_page(url)
            if raw and raw.get('text') and len(raw['text']) > 30:
                record = self.normalize(raw)
                samples.append(record)

                filename = f"sample_{len(samples):03d}.json"
                filepath = SAMPLE_DIR / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"  Saved: {filename} - {record['title'][:50]} ({len(record['text'])} chars)")
            time.sleep(1.5)

        return samples


def main():
    parser = argparse.ArgumentParser(description='NO/KONKURR - Konkurransetilsynet Doctrine Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument('--max-pages', type=int, default=10000,
                        help='Maximum pages to fetch')
    parser.add_argument('--count', type=int, default=15,
                        help='Number of sample records')

    args = parser.parse_args()
    fetcher = KonkurransetilsynetFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            samples = fetcher.bootstrap_sample(count=args.count)
            print(f"\nBootstrap complete: {len(samples)} sample documents saved to {SAMPLE_DIR}")

            texts = [s for s in samples if s.get('text') and len(s['text']) > 30]
            dates = [s for s in samples if s.get('date')]
            print(f"  With full text: {len(texts)}/{len(samples)}")
            print(f"  With dates: {len(dates)}/{len(samples)}")
            if texts:
                avg_len = sum(len(s['text']) for s in texts) // len(texts)
                print(f"  Average text length: {avg_len} chars")
        else:
            count = 0
            for record in fetcher.fetch_all(max_pages=args.max_pages):
                count += 1
            print(f"\nFetch complete: {count} documents")

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since date required for updates command")
            sys.exit(1)
        count = 0
        for record in fetcher.fetch_updates(args.since):
            count += 1
        print(f"\nUpdates since {args.since}: {count} documents")

    elif args.command == 'fetch':
        count = 0
        for record in fetcher.fetch_all(max_pages=args.max_pages):
            count += 1
        print(f"\nFetch complete: {count} documents")


if __name__ == '__main__':
    main()
