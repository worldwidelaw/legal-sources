#!/usr/bin/env python3
"""
Finnish Competition and Consumer Authority (KKV) Data Fetcher

Fetches competition, merger, and consumer protection decisions from kkv.fi.
Uses sitemap for discovery, HTML scraping for inline text, and PDF extraction
for decisions with attached PDF documents.

~2,009 decisions across competition and consumer categories.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SITEMAP_URL = "https://www.kkv.fi/sitemap.xml"
BASE_URL = "https://www.kkv.fi"

# Decision URL patterns
DECISION_PATHS = [
    '/paatokset/kilpailuasiat/',
    '/paatokset/kuluttaja-asiat/',
    '/paatokset/vireilla-olevat-yrityskaupat/',
]

# Category mapping from URL path
CATEGORY_MAP = {
    'yrityskauppavalvonta': 'merger_control',
    'muut-paatokset': 'other_competition',
    'kuluttaja-asiamiehen-ratkaisut': 'consumer_ombudsman',
    'kuluttaja-asiamiehen-seuraamusmaksuesitykset': 'consumer_penalty',
    'hankintojen-valvonta': 'procurement_supervision',
    'poikkeusluvat-ja-puuttumattomuustodistukset': 'exemptions',
    'esitykset-markkinaoikeudelle': 'market_court_proposals',
    'kuluttaja-asiamiehen-maaraamat-kiellot': 'consumer_prohibitions',
    'kielto-sitoumus-tai-toimitusvelvoiteratkaisut': 'prohibition_commitment',
    'kuluttaja-asiamiehen-avustusasiat': 'consumer_assistance',
    'kilpailuneutraliteetti': 'competition_neutrality',
}


class KKVFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept-Language': 'fi,en;q=0.5',
        })

    def _get(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """GET with retry."""
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

    def _discover_decision_urls(self) -> List[str]:
        """Discover decision URLs from sitemap."""
        resp = self._get(SITEMAP_URL)
        if not resp:
            logger.error("Failed to fetch sitemap")
            return []

        # Parse sitemap XML
        root = ET.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        urls = []
        for url_elem in root.findall('.//sm:url/sm:loc', ns):
            url = url_elem.text
            if url and any(path in url for path in DECISION_PATHS):
                # Skip listing pages (those ending with the category path)
                path = url.replace(BASE_URL, '')
                segments = [s for s in path.strip('/').split('/') if s]
                # Decision pages have at least 4 segments (paatokset/area/type/decision-slug)
                if len(segments) >= 4:
                    urls.append(url)

        # If sitemap doesn't have sub-sitemaps, check for sitemap index
        if not urls:
            for sitemap_elem in root.findall('.//sm:sitemap/sm:loc', ns):
                sub_url = sitemap_elem.text
                if sub_url:
                    sub_resp = self._get(sub_url)
                    if sub_resp:
                        sub_root = ET.fromstring(sub_resp.content)
                        for url_elem in sub_root.findall('.//sm:url/sm:loc', ns):
                            url = url_elem.text
                            if url and any(path in url for path in DECISION_PATHS):
                                path = url.replace(BASE_URL, '')
                                segments = [s for s in path.strip('/').split('/') if s]
                                if len(segments) >= 4:
                                    urls.append(url)

        logger.info(f"Discovered {len(urls)} decision URLs from sitemap")
        return urls

    def _extract_category(self, url: str) -> str:
        """Extract category from URL path."""
        for key, value in CATEGORY_MAP.items():
            if key in url:
                return value
        return 'other'

    def _extract_decision(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract decision content from a page."""
        resp = self._get(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract title
        title_elem = soup.find('h1')
        title = title_elem.get_text(strip=True) if title_elem else ''

        # Extract date - look for date patterns in the page
        date = None
        # Check for structured date in meta or time elements
        time_elem = soup.find('time')
        if time_elem and time_elem.get('datetime'):
            date = time_elem['datetime'][:10]

        # Look for date in common patterns
        if not date:
            date_patterns = [
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # DD.MM.YYYY (Finnish format)
            ]
            article = soup.find('article') or soup.find('main') or soup.body
            if article:
                text_content = article.get_text()
                for pattern in date_patterns:
                    m = re.search(pattern, text_content)
                    if m:
                        day, month, year = m.groups()
                        try:
                            date = f"{year}-{int(month):02d}-{int(day):02d}"
                        except ValueError:
                            pass
                        break

        # Extract case reference (KKV/NNN/14.00.10/YYYY pattern)
        case_ref = None
        text_full = soup.get_text()
        ref_match = re.search(r'KKV/\d+/[\d.]+/\d{4}', text_full)
        if ref_match:
            case_ref = ref_match.group()

        # Extract article/main content text
        article = soup.find('article')
        if not article:
            article = soup.find('div', class_='entry-content') or soup.find('main')

        text = ''
        if article:
            # Remove navigation, sidebar, etc.
            for tag in article.find_all(['nav', 'aside', 'script', 'style', 'header', 'footer']):
                tag.decompose()
            text = article.get_text(separator='\n', strip=True)
            # Clean up
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            text = text.strip()

        # Try to find and extract PDF if inline text is short
        pdf_text = ''
        if len(text) < 500:
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
            for link in pdf_links:
                href = link.get('href', '')
                if '/uploads/' in href or '/r-' in href.lower():
                    pdf_url = href if href.startswith('http') else BASE_URL + href
                    pdf_text = self._extract_pdf_text(pdf_url)
                    if pdf_text and len(pdf_text) > 100:
                        break

        # Combine text
        if pdf_text and len(pdf_text) > len(text):
            full_text = pdf_text
        else:
            full_text = text

        if not full_text or len(full_text) < 50:
            return None

        # Generate ID from URL slug or case ref
        slug = url.rstrip('/').split('/')[-1]
        decision_id = case_ref or slug

        return {
            'decision_id': decision_id,
            'title': title,
            'text': full_text,
            'date': date,
            'url': url,
            'category': self._extract_category(url),
            'case_ref': case_ref,
        }

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download and extract text from PDF."""
        try:
            resp = self._get(pdf_url, timeout=30)
            if not resp:
                return ''

            # Try PyPDF2
            try:
                from PyPDF2 import PdfReader
                reader = PdfReader(BytesIO(resp.content))
                pages = []
                for page in reader.pages:
                    t = page.extract_text()
                    if t:
                        pages.append(t)
                return '\n\n'.join(pages)
            except ImportError:
                pass

            # Try pdfplumber
            try:
                import pdfplumber
                with pdfplumber.open(BytesIO(resp.content)) as pdf:
                    pages = []
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            pages.append(t)
                    return '\n\n'.join(pages)
            except ImportError:
                pass

            logger.warning("No PDF extraction library available (PyPDF2 or pdfplumber)")
            return ''
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ''

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all KKV decisions."""
        urls = self._discover_decision_urls()
        if not urls:
            logger.error("No decision URLs found")
            return

        fetched = 0
        for url in urls:
            if max_docs is not None and fetched >= max_docs:
                break

            logger.info(f"Fetching {url} ({fetched+1}/{max_docs or len(urls)})...")
            decision = self._extract_decision(url)

            if decision and len(decision.get('text', '')) >= 100:
                yield decision
                fetched += 1
            else:
                logger.warning(f"Insufficient content for {url}")

            time.sleep(1)

        logger.info(f"Fetched {fetched} KKV decisions total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions updated since a date."""
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = datetime.fromisoformat(doc['date'])
                    if doc_date >= since:
                        yield doc
                except Exception:
                    yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize to standard schema."""
        return {
            '_id': raw['decision_id'],
            '_source': 'FI/FCCA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'decision_id': raw['decision_id'],
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'category': raw.get('category', ''),
            'case_ref': raw.get('case_ref'),
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = KKVFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        max_docs = 15 if is_sample else 100

        logger.info(f"Fetching {'sample' if is_sample else 'batch'} KKV decisions...")

        count = 0
        for raw in fetcher.fetch_all(max_docs=max_docs):
            normalized = fetcher.normalize(raw)

            if len(normalized.get('text', '')) < 100:
                continue

            filename = re.sub(r'[^\w\-.]', '_', normalized['decision_id']) + '.json'
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
        fetcher = KKVFetcher()
        print("Testing KKV fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(max_docs=3)):
            n = fetcher.normalize(raw)
            print(f"\n--- {i+1} ---")
            print(f"ID: {n['decision_id']}, Category: {n['category']}")
            print(f"Title: {n['title'][:100]}")
            print(f"Date: {n['date']}, Text: {len(n['text']):,} chars")
            if i >= 2:
                break


if __name__ == '__main__':
    main()
