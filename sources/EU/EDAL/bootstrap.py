#!/usr/bin/env python3
"""
EU/EDAL Data Fetcher
European Database of Asylum Law - Structured case summaries

HTML scraping of asylumlawdatabase.eu (Drupal 7).
Extracts structured fields: headnote, facts, decision/reasoning, outcome,
observations. ~900 cases from 22 EU member states, frozen at 2021.
Crawl delay: 10 seconds (per robots.txt).
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.asylumlawdatabase.eu"
SEARCH_URL = f"{BASE_URL}/en/case-law-search"
CRAWL_DELAY = 10  # per robots.txt


class EDALFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)'
        })

    def _get(self, url: str, retries: int = 3) -> Optional[requests.Response]:
        """GET with retries and crawl delay."""
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code == 200:
                    return r
                logger.warning(f"Status {r.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        return None

    def _discover_case_urls(self, max_pages: int = 180) -> List[str]:
        """Discover case URLs by paginating search results."""
        urls = []
        for page in range(max_pages):
            r = self._get(f"{SEARCH_URL}?page={page}")
            if not r:
                break

            soup = BeautifulSoup(r.text, 'html.parser')
            links = soup.select('a[href*="/en/case-law/"]')

            page_urls = set()
            for link in links:
                href = link.get('href', '')
                if '/case-law-search' in href or not href:
                    continue
                # Remove #content anchor
                href = href.split('#')[0]
                full_url = urljoin(BASE_URL, href)
                if full_url not in page_urls:
                    page_urls.add(full_url)

            if not page_urls:
                logger.info(f"No cases on page {page}, stopping pagination")
                break

            urls.extend(page_urls)
            logger.info(f"Page {page}: found {len(page_urls)} cases (total: {len(urls)})")

            time.sleep(CRAWL_DELAY)

        return urls

    def _extract_field(self, soup: BeautifulSoup, field_name: str) -> str:
        """Extract text from a Drupal field."""
        el = soup.select_one(f'.field-name-{field_name} .field-item')
        if el:
            return el.get_text(strip=True)
        return ''

    def _extract_field_links(self, soup: BeautifulSoup, field_name: str) -> List[str]:
        """Extract link texts from a Drupal field."""
        els = soup.select(f'.field-name-{field_name} .field-item a')
        return [a.get_text(strip=True) for a in els if a.get_text(strip=True)]

    def _parse_title_metadata(self, title: str) -> Dict[str, str]:
        """Extract country, court, date from the page title."""
        # Title format: "Country – Court, Date, Citation"
        # or "Country: Court, Date, Citation"
        result = {'country': '', 'court': '', 'date_str': ''}

        # Try to extract country (before first – or :)
        m = re.match(r'^([A-Za-z ]+?)[\s]*[-–:]\s*(.+)', title)
        if m:
            result['country'] = m.group(1).strip()

        return result

    def _parse_case_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Parse a single case page and extract all fields."""
        r = self._get(url)
        if not r:
            return None

        soup = BeautifulSoup(r.text, 'html.parser')

        # Get title from h1
        h1 = soup.select_one('h1.title, h1#page-title, h1')
        title = h1.get_text(strip=True) if h1 else ''

        # Extract structured fields
        headnote = self._extract_field(soup, 'field-headnote')
        facts = self._extract_field(soup, 'field-facts')
        decision = self._extract_field(soup, 'field-decision')
        outcome = self._extract_field(soup, 'field-outcome')
        subproc = self._extract_field(soup, 'field-subproc')
        observations = self._extract_field(soup, 'field-observations')
        citation = self._extract_field(soup, 'field-ncn')
        court = self._extract_field(soup, 'field-court-name')
        country = self._extract_field(soup, 'field-tcod')
        legislation = self._extract_field(soup, 'field-leg-applicable')
        other_sources = self._extract_field(soup, 'field-other-sources')
        keywords = self._extract_field_links(soup, 'field-keywords')

        # Extract date
        date_el = soup.select_one('.field-name-field-date-dd .date-display-single')
        date_str = date_el.get_text(strip=True) if date_el else ''

        # Compose full text from structured sections
        text_parts = []
        if headnote:
            text_parts.append(f"HEADNOTE\n{headnote}")
        if facts:
            text_parts.append(f"FACTS\n{facts}")
        if decision:
            text_parts.append(f"DECISION & REASONING\n{decision}")
        if outcome:
            text_parts.append(f"OUTCOME\n{outcome}")
        if subproc:
            text_parts.append(f"SUBSEQUENT PROCEEDINGS\n{subproc}")
        if observations:
            text_parts.append(f"OBSERVATIONS\n{observations}")

        text = '\n\n'.join(text_parts)

        if not text:
            logger.warning(f"No text content for {url}")
            return None

        # Parse date to ISO format
        parsed_date = self._parse_date(date_str, title)

        # If country not from field, try from title
        if not country:
            meta = self._parse_title_metadata(title)
            country = meta.get('country', '')

        return {
            'title': title,
            'text': text,
            'headnote': headnote,
            'facts': facts,
            'decision': decision,
            'outcome': outcome,
            'observations': observations,
            'date_str': parsed_date,
            'court': court,
            'country': country,
            'citation': citation,
            'keywords': keywords,
            'legislation': legislation,
            'other_sources': other_sources,
            'url': url,
        }

    def _parse_date(self, date_str: str, title: str = '') -> str:
        """Try to parse date to ISO format from various formats."""
        # Try common date patterns
        for text in [date_str, title]:
            if not text:
                continue
            # Try various date formats
            patterns = [
                (r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', '%d %B %Y'),
                (r'(\d{1,2})/(\d{1,2})/(\d{4})', None),  # d/m/y
                (r'(\d{4})-(\d{2})-(\d{2})', None),  # ISO
            ]
            for pattern, fmt in patterns:
                m = re.search(pattern, text)
                if m:
                    if fmt:
                        try:
                            return datetime.strptime(m.group(0), fmt).strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                    elif '/' in m.group(0):
                        parts = m.group(0).split('/')
                        try:
                            return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                        except (IndexError, ValueError):
                            continue
                    else:
                        return m.group(0)
        return ''

    def fetch_all(self, max_docs: int = None, max_pages: int = 180) -> Iterator[Dict[str, Any]]:
        """Yield all case law documents."""
        logger.info("Discovering case URLs...")
        case_urls = self._discover_case_urls(max_pages=max_pages)
        logger.info(f"Found {len(case_urls)} case URLs")

        fetched = 0
        for i, url in enumerate(case_urls):
            if max_docs and fetched >= max_docs:
                break

            doc = self._parse_case_page(url)
            if doc:
                fetched += 1
                if fetched % 10 == 0:
                    logger.info(f"Fetched {fetched}/{len(case_urls)} documents")
                yield doc

            time.sleep(CRAWL_DELAY)

        logger.info(f"fetch_all complete. Total: {fetched}")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date (limited for static site)."""
        for doc in self.fetch_all():
            if doc.get('date_str'):
                try:
                    doc_date = datetime.strptime(doc['date_str'], '%Y-%m-%d')
                    if doc_date >= since:
                        yield doc
                except ValueError:
                    yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        # Create a stable ID from the URL slug
        slug = raw_doc['url'].rstrip('/').split('/')[-1]
        _id = f"EDAL-{slug[:80]}"

        return {
            '_id': _id,
            '_source': 'EU/EDAL',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'headnote': raw_doc.get('headnote', ''),
            'outcome': raw_doc.get('outcome', ''),
            'date': raw_doc.get('date_str', ''),
            'court': raw_doc.get('court', ''),
            'country': raw_doc.get('country', ''),
            'citation': raw_doc.get('citation', ''),
            'keywords': raw_doc.get('keywords', []),
            'url': raw_doc['url'],
        }


def main():
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = EDALFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv

        if is_sample:
            target = 15
            max_pages = 5
            logger.info(f"Sample mode: fetching {target} documents from first {max_pages} pages")
        else:
            target = None
            max_pages = 180
            logger.info("Full mode: fetching all documents")

        count = 0
        for doc in fetcher.fetch_all(max_docs=target, max_pages=max_pages):
            normalized = fetcher.normalize(doc)
            filename = re.sub(r'[^a-zA-Z0-9_-]', '_', normalized['_id'])[:80]
            filepath = sample_dir / f"{filename}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            text_len = len(normalized.get('text', ''))
            logger.info(f"[{count}] {normalized['title'][:60]} - {text_len} chars")

        logger.info(f"Bootstrap complete: {count} documents saved to {sample_dir}")
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")


if __name__ == '__main__':
    main()
