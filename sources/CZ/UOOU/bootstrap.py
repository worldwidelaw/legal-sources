#!/usr/bin/env python3
"""
Czech Data Protection Authority (ÚOOÚ) Data Fetcher

Fetches completed inspection decisions from the Office for Personal Data
Protection (Úřad pro ochranu osobních údajů).

Website: https://uoou.gov.cz
Source: /cinnost/ochrana-osobnich-udaju/ukoncene-kontroly

The inspections are organized by year (2001-2024) with subcategories for:
- Personal data protection violations
- Unsolicited commercial communications
- Schengen information systems
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Set
from urllib.parse import urljoin

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://uoou.gov.cz"
INSPECTIONS_ROOT = "/cinnost/ochrana-osobnich-udaju/ukoncene-kontroly"


class HTMLContentExtractor(HTMLParser):
    """Extract clean text content from HTML"""

    def __init__(self):
        super().__init__()
        self.text_parts: List[str] = []
        self.in_content = False
        self.depth = 0
        self.skip_tags = {'script', 'style', 'nav', 'header', 'footer'}
        self.current_skip = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        # Look for the main content div
        if tag == 'div' and attrs_dict.get('class', '').startswith('_cms-content'):
            self.in_content = True
            self.depth = 1
        elif self.in_content:
            self.depth += 1

        if tag in self.skip_tags:
            self.current_skip = tag

    def handle_endtag(self, tag):
        if self.in_content:
            self.depth -= 1
            if self.depth == 0:
                self.in_content = False

        if tag == self.current_skip:
            self.current_skip = None

    def handle_data(self, data):
        if self.in_content and self.current_skip is None:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self) -> str:
        return ' '.join(self.text_parts)


def clean_html_text(html: str) -> str:
    """Extract clean text from HTML content"""
    parser = HTMLContentExtractor()
    try:
        parser.feed(html)
        return parser.get_text()
    except Exception:
        # Fallback: simple regex stripping
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


class UOOUFetcher:
    """Fetcher for Czech Data Protection Authority inspection decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'cs,en;q=0.5',
        })
        self._last_request = 0

    def _rate_limit(self, delay: float = 1.5):
        """Ensure rate limiting between requests"""
        now = time.time()
        elapsed = now - self._last_request
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with rate limiting"""
        self._rate_limit()
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _get_year_urls(self) -> List[str]:
        """Get list of yearly inspection page URLs"""
        root_url = urljoin(BASE_URL, INSPECTIONS_ROOT)
        html = self._fetch_page(root_url)
        if not html:
            return []

        # Find year links - pattern: kontroly-za-rok-YYYY
        pattern = r'href="([^"]*kontroly-za-rok-[^"]+)"'
        matches = re.findall(pattern, html)

        # Deduplicate and normalize
        urls = set()
        for match in matches:
            if match.startswith('http'):
                urls.add(match)
            else:
                urls.add(urljoin(BASE_URL, match))

        return sorted(urls, reverse=True)  # Most recent first

    def _get_all_links(self, url: str) -> List[str]:
        """Extract all href links from a page"""
        html = self._fetch_page(url)
        if not html:
            return []

        pattern = r'href="([^"]+)"'
        matches = re.findall(pattern, html)

        urls = []
        for match in matches:
            if match.startswith('http'):
                urls.append(match)
            elif match.startswith('/'):
                urls.append(urljoin(BASE_URL, match))

        return urls

    def _discover_decision_urls(self, year_url: str) -> Set[str]:
        """
        Recursively discover all decision URLs from a year page.
        Drills down through subcategories to find actual decision pages.
        """
        decision_urls: Set[str] = set()
        visited: Set[str] = set()
        to_visit = [year_url]

        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            visited.add(current)

            links = self._get_all_links(current)

            for link in links:
                # Skip non-inspection links
                if 'ukoncene-kontroly' not in link:
                    continue
                # Skip already visited
                if link in visited:
                    continue
                # Skip external links
                if not link.startswith(BASE_URL):
                    continue

                # Check if this is a decision page (contains a case identifier)
                slug = link.rstrip('/').split('/')[-1]

                # Decision pages have slugs like:
                # obchodni-spolecnost-0003724
                # obchodni-spolecnost-0208423-zalohove-faktury
                if re.search(r'obchodni|spolecnost.*\d{4,}|\d{5,}', slug, re.IGNORECASE):
                    decision_urls.add(link)
                # Otherwise it might be a category page to drill into
                elif slug.startswith('kontroly-') or slug.startswith('kontrolni-') or \
                     slug.startswith('nevyzadana') or slug.startswith('schengen'):
                    # Only drill down if within the same year
                    if year_url.split('/')[-1].split('-')[-1] in link or \
                       any(y in link for y in ['2024', '2023', '2022', '2021', '2020']):
                        to_visit.append(link)

        return decision_urls

    def _fetch_decision(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single decision page"""
        html = self._fetch_page(url)
        if not html:
            return None

        # Extract title from page
        title_match = re.search(r'<title>([^<]+)</title>', html)
        title = title_match.group(1).strip() if title_match else ""
        title = re.sub(r'\s*\|\s*Úřad pro ochranu osobních údajů$', '', title)

        # Extract case number from title or breadcrumb
        case_match = re.search(r'(\d{4,}/\d{2,})', title)
        if not case_match:
            case_match = re.search(r'(\d{4,}/\d{2,})', html)
        case_number = case_match.group(1) if case_match else ""

        # Extract main content
        text = clean_html_text(html)

        if not text or len(text) < 100:
            logger.debug(f"Skipping {url}: text too short ({len(text)} chars)")
            return None

        # Extract dates from metadata
        date_modified_match = re.search(r'"dateModified":\s*"([^"]+)"', html)
        date_published_match = re.search(r'"datePublished":\s*"([^"]+)"', html)

        date_str = None
        if date_modified_match:
            date_str = date_modified_match.group(1)[:10]
        elif date_published_match:
            date_str = date_published_match.group(1)[:10]

        # Determine year from URL
        year_match = re.search(r'kontroly-za-rok-(\d{4})', url)
        year = int(year_match.group(1)) if year_match else None

        # Determine category from URL
        category = 'personal_data'
        if 'obchodni-sdeleni' in url or 'nevyzadana' in url:
            category = 'commercial_communications'
        elif 'schengen' in url.lower():
            category = 'schengen'

        return {
            'url': url,
            'title': title,
            'case_number': case_number,
            'text': text,
            'date': date_str,
            'year': year,
            'category': category,
        }

    def fetch_all(self, start_year: int = None, end_year: int = None,
                  limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all inspection decisions.

        Args:
            start_year: Most recent year to start from
            end_year: Oldest year to go back to
            limit: Maximum number of documents to fetch

        Yields:
            Raw document dictionaries with full text
        """
        count = 0
        year_urls = self._get_year_urls()

        if not year_urls:
            logger.error("Could not find any year URLs")
            return

        logger.info(f"Found {len(year_urls)} year pages")

        for year_url in year_urls:
            # Extract year from URL
            year_match = re.search(r'(\d{4})', year_url)
            if year_match:
                year = int(year_match.group(1))

                # Filter by year range
                if start_year and year > start_year:
                    continue
                if end_year and year < end_year:
                    continue

            logger.info(f"Processing: {year_url}")

            # Discover all decision URLs recursively
            decision_urls = self._discover_decision_urls(year_url)

            logger.info(f"  Found {len(decision_urls)} decision URLs")

            # Fetch each decision
            for url in decision_urls:
                if limit and count >= limit:
                    logger.info(f"Reached limit of {limit} documents")
                    return

                decision = self._fetch_decision(url)
                if decision and decision.get('text'):
                    count += 1
                    yield decision

                    if count % 10 == 0:
                        logger.info(f"  Progress: {count} decisions fetched")

        logger.info(f"Total: {count} decisions fetched")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions modified since a given date."""
        current_year = datetime.now().year

        for doc in self.fetch_all(start_year=current_year, end_year=since.year):
            if doc.get('date'):
                try:
                    doc_date = datetime.fromisoformat(doc['date'])
                    if doc_date >= since:
                        yield doc
                except ValueError:
                    yield doc  # Include if we can't parse the date
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        url = raw_doc.get('url', '')
        case_number = raw_doc.get('case_number', '')

        # Create a unique ID from URL slug or case number
        url_slug = url.rstrip('/').split('/')[-1]
        doc_id = f"UOOU-{case_number}" if case_number else f"UOOU-{url_slug}"
        doc_id = re.sub(r'[^\w-]', '_', doc_id)

        return {
            '_id': doc_id,
            '_source': 'CZ/UOOU',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'case_number': case_number,
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date'),
            'year': raw_doc.get('year'),
            'category': raw_doc.get('category', 'personal_data'),
            'url': url,
            'language': 'cs',
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = UOOUFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 15 if is_sample else None

        if is_sample:
            logger.info("Fetching sample records...")
            sample_count = 0

            # Fetch from recent years for sample
            for raw_doc in fetcher.fetch_all(
                start_year=2024,
                end_year=2022,
                limit=target_count
            ):
                if sample_count >= target_count:
                    break

                normalized = fetcher.normalize(raw_doc)
                text_len = len(normalized.get('text', ''))

                if text_len < 200:
                    continue

                filename = f"{normalized['_id']}.json"
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
            # Full bootstrap - write to JSONL
            data_dir = Path(__file__).parent / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            jsonl_path = data_dir / 'records.jsonl'

            count = 0
            with open(jsonl_path, 'w', encoding='utf-8') as out:
                for raw_doc in fetcher.fetch_all():
                    normalized = fetcher.normalize(raw_doc)
                    line = json.dumps(normalized, ensure_ascii=False)
                    out.write(line + '\n')
                    print(line)
                    count += 1

            logger.info(f"Bootstrap complete: {count} records written to {jsonl_path}")
    else:
        # Test mode
        fetcher = UOOUFetcher()
        print("Testing ÚOOÚ fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(
            start_year=2024,
            end_year=2024,
            limit=3
        ):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title']}")
            print(f"Case Number: {normalized['case_number']}")
            print(f"Date: {normalized['date']}")
            print(f"Category: {normalized['category']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
