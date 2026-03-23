#!/usr/bin/env python3
"""
NO/DTIL - Norwegian Data Protection Authority (Datatilsynet) Doctrine Fetcher

Fetches data protection guidance, regulatory doctrine, and sector-specific
privacy guidance from Datatilsynet.

Data source: https://www.datatilsynet.no
Sections crawled:
  - /rettigheter-og-plikter/ (rights and obligations)
  - /personvern-pa-ulike-omrader/ (privacy in various areas)
  - /regelverk-og-verktoy/ (regulations and tools)

No API or sitemap available — uses recursive HTML crawl from seed pages.
License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.datatilsynet.no"
SOURCE_ID = "NO/DTIL"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

SEED_PATHS = [
    "/rettigheter-og-plikter/",
    "/personvern-pa-ulike-omrader/",
    "/regelverk-og-verktoy/",
]

# Skip these path prefixes — not doctrine content
SKIP_PREFIXES = [
    "/aktuelt/",
    "/om-datatilsynet/",
    "/sok/",
    "/globalassets/",
    "/contentassets/",
]

# Skip file extensions
SKIP_EXTENSIONS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".png", ".jpg", ".jpeg", ".gif", ".svg"}


class DatatilsynetFetcher:
    """Fetcher for Norwegian Data Protection Authority guidance documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5',
        })
        self.visited: Set[str] = set()

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication."""
        parsed = urlparse(url)
        path = parsed.path.rstrip("/") + "/" if parsed.path != "/" else "/"
        # Remove query params and fragments for dedup
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    def _is_valid_link(self, href: str) -> bool:
        """Check if a link should be followed."""
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            return False
        parsed = urlparse(href)
        # Only follow links on same host
        if parsed.netloc and parsed.netloc != "www.datatilsynet.no":
            return False
        path = parsed.path.lower()
        # Skip file downloads
        for ext in SKIP_EXTENSIONS:
            if path.endswith(ext):
                return False
        # Skip non-doctrine sections
        for prefix in SKIP_PREFIXES:
            if path.startswith(prefix):
                return False
        return True

    def _is_in_scope(self, url: str) -> bool:
        """Check if URL falls within our seed path scopes."""
        parsed = urlparse(url)
        path = parsed.path
        return any(path.startswith(seed) for seed in SEED_PATHS)

    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main article text from page HTML."""
        # Try common content containers
        content = None
        for selector in [
            'main',
            'article',
            ('div', {'class': re.compile(r'article|content|body|text', re.I)}),
            ('div', {'role': 'main'}),
        ]:
            if isinstance(selector, str):
                content = soup.find(selector)
            else:
                content = soup.find(*selector)
            if content:
                break

        if not content:
            # Fallback: get everything after skiplinktarget
            skip_link = soup.find(id='skiplinktarget')
            if skip_link:
                content = skip_link.find_parent()

        if not content:
            return None

        # Remove navigation, footer, sidebar elements
        for tag in content.find_all(['nav', 'footer', 'aside', 'script', 'style', 'noscript']):
            tag.decompose()
        for tag in content.find_all(class_=re.compile(r'nav|menu|sidebar|footer|breadcrumb|cookie|banner', re.I)):
            tag.decompose()

        # Extract text
        text = content.get_text(separator="\n", strip=True)
        # Clean up excessive whitespace
        lines = [line.strip() for line in text.splitlines()]
        lines = [line for line in lines if line]
        text = "\n".join(lines)

        return text if len(text) > 50 else None

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract metadata from page."""
        meta = {}

        # Title
        h1 = soup.find('h1')
        if h1:
            meta['title'] = h1.get_text(strip=True)
        else:
            title_tag = soup.find('title')
            if title_tag:
                meta['title'] = title_tag.get_text(strip=True).split('|')[0].strip()

        # Published/modified dates from meta tags
        for tag in soup.find_all('meta'):
            name = (tag.get('name') or tag.get('property') or '').lower()
            content_val = tag.get('content', '')
            if 'published' in name or 'date' in name:
                meta.setdefault('date_published', content_val)
            if 'modified' in name:
                meta['date_modified'] = content_val

        # Search full page text for Norwegian date labels (may have newlines between label and date)
        page_text = soup.get_text()
        pub_match = re.search(r'Publisert:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text, re.DOTALL)
        if pub_match:
            d, m, y = pub_match.groups()
            meta['date_published'] = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        mod_match = re.search(r'(?:Sist\s+)?endret:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text, re.DOTALL)
        if mod_match:
            d, m, y = mod_match.groups()
            meta['date_modified'] = f"{y}-{m.zfill(2)}-{d.zfill(2)}"

        # Description
        desc_tag = soup.find('meta', attrs={'name': 'description'})
        if desc_tag and desc_tag.get('content'):
            meta['description'] = desc_tag['content']

        return meta

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        # Already ISO format
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]
        # DD.MM.YYYY
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            d, m, y = match.groups()
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        # ISO datetime
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw document into standard schema."""
        url = raw['url']
        path = urlparse(url).path
        doc_id = hashlib.md5(path.encode()).hexdigest()[:12]

        date = self._parse_date(raw.get('date_published')) or self._parse_date(raw.get('date_modified'))

        return {
            '_id': f"NO-DTIL-{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': date,
            'url': url,
            'description': raw.get('description', ''),
            'path': path,
            'language': 'nob',
        }

    def discover_pages(self, max_pages: int = 2000) -> Iterator[str]:
        """Discover all doctrine pages by crawling from seed URLs."""
        queue = [BASE_URL + path for path in SEED_PATHS]
        discovered = set()

        while queue and len(discovered) < max_pages:
            url = queue.pop(0)
            normalized = self._normalize_url(url)

            if normalized in discovered:
                continue
            discovered.add(normalized)

            logger.info(f"Discovering links: {url} ({len(discovered)} found)")
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    continue
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                continue

            soup = BeautifulSoup(resp.text, 'html.parser')

            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if not self._is_valid_link(href):
                    continue
                full_url = urljoin(url, href)
                if self._is_in_scope(full_url):
                    norm = self._normalize_url(full_url)
                    if norm not in discovered:
                        queue.append(full_url)

            yield url
            time.sleep(0.5)

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
        # Extract metadata BEFORE content (content extraction modifies soup via decompose)
        meta = self._extract_metadata(soup)

        soup2 = BeautifulSoup(resp.text, 'html.parser')
        text = self._extract_content(soup2)
        if not text:
            logger.debug(f"No content extracted from {url}")
            return None

        meta['url'] = url
        meta['text'] = text

        return meta

    def fetch_all(self, max_pages: int = 2000) -> Iterator[Dict[str, Any]]:
        """Fetch all doctrine documents."""
        count = 0
        for url in self.discover_pages(max_pages=max_pages):
            raw = self.fetch_page(url)
            if raw and raw.get('text'):
                record = self.normalize(raw)
                if record['text'] and len(record['text']) > 50:
                    count += 1
                    logger.info(f"[{count}] {record['title'][:60]} ({len(record['text'])} chars)")
                    yield record
            time.sleep(1.0)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch documents modified since a given date."""
        # No date filtering available via HTML — refetch all and filter
        for record in self.fetch_all():
            if record.get('date') and record['date'] >= since:
                yield record

    def bootstrap_sample(self, count: int = 15) -> list:
        """Fetch a sample of documents for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        samples = []

        # Fetch specific known pages to get a good sample quickly
        sample_urls = [
            f"{BASE_URL}/rettigheter-og-plikter/den-registrertes-rettigheter/rett-til-innsyn/",
            f"{BASE_URL}/rettigheter-og-plikter/den-registrertes-rettigheter/rett-til-sletting/",
            f"{BASE_URL}/rettigheter-og-plikter/den-registrertes-rettigheter/rett-til-retting/",
            f"{BASE_URL}/rettigheter-og-plikter/den-registrertes-rettigheter/rett-til-dataportabilitet/",
            f"{BASE_URL}/rettigheter-og-plikter/den-registrertes-rettigheter/rett-til-a-protestere/",
            f"{BASE_URL}/rettigheter-og-plikter/den-registrertes-rettigheter/rett-til-begrensning-av-behandling/",
            f"{BASE_URL}/rettigheter-og-plikter/virksomhetenes-plikter/informasjonsplikt/",
            f"{BASE_URL}/rettigheter-og-plikter/virksomhetenes-plikter/personvernombud/",
            f"{BASE_URL}/rettigheter-og-plikter/virksomhetenes-plikter/vurdering-av-personvernkonsekvenser/",
            f"{BASE_URL}/rettigheter-og-plikter/virksomhetenes-plikter/overforing-av-personopplysninger-ut-av-eos/",
            f"{BASE_URL}/rettigheter-og-plikter/virksomhetenes-plikter/avvikshandtering/",
            f"{BASE_URL}/rettigheter-og-plikter/hva-er-personvern/",
            f"{BASE_URL}/rettigheter-og-plikter/personopplysninger/",
            f"{BASE_URL}/rettigheter-og-plikter/personvernprinsippene/",
            f"{BASE_URL}/personvern-pa-ulike-omrader/personvern-pa-arbeidsplassen/",
            f"{BASE_URL}/personvern-pa-ulike-omrader/overvaking-og-sporing/",
            f"{BASE_URL}/personvern-pa-ulike-omrader/internett-og-apper/",
            f"{BASE_URL}/personvern-pa-ulike-omrader/skole-barn-unge/",
            f"{BASE_URL}/regelverk-og-verktoy/",
        ]

        for url in sample_urls:
            if len(samples) >= count:
                break
            logger.info(f"Fetching sample: {url}")
            raw = self.fetch_page(url)
            if raw and raw.get('text') and len(raw['text']) > 50:
                record = self.normalize(raw)
                samples.append(record)

                # Save to file
                filename = f"sample_{len(samples):03d}.json"
                filepath = SAMPLE_DIR / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"  Saved: {filename} - {record['title'][:50]} ({len(record['text'])} chars)")
            time.sleep(1.5)

        return samples


def main():
    parser = argparse.ArgumentParser(description='NO/DTIL - Datatilsynet Doctrine Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument('--max-pages', type=int, default=2000,
                        help='Maximum pages to crawl')
    parser.add_argument('--count', type=int, default=15,
                        help='Number of sample records')

    args = parser.parse_args()
    fetcher = DatatilsynetFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            samples = fetcher.bootstrap_sample(count=args.count)
            print(f"\nBootstrap complete: {len(samples)} sample documents saved to {SAMPLE_DIR}")

            # Validation
            texts = [s for s in samples if s.get('text') and len(s['text']) > 50]
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
