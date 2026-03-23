#!/usr/bin/env python3
"""
DE/NRW - North Rhine-Westphalia State Law (recht.nrw.de)

Fetches state legislation from the official NRW legal database.

Coverage:
- State laws (Gesetze)
- Legal ordinances (Rechtsverordnungen)
- Administrative directives (Verwaltungsvorschriften)
- Official announcements (Bekanntmachungen)

Data source: https://recht.nrw.de
Sitemap: https://recht.nrw.de/sitemap.xml (47 pages)

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://recht.nrw.de"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap.xml"
RATE_LIMIT_DELAY = 1.0  # seconds between requests
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/NRW"

# URL patterns for legislation documents
LRGV_PATTERN = re.compile(r'^https://recht\.nrw\.de/lrgv/')
LRMB_PATTERN = re.compile(r'^https://recht\.nrw\.de/lrmb/')


class NRWFetcher:
    """Fetcher for NRW state legislation from recht.nrw.de"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _fetch_page(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a webpage with rate limiting and retries."""
        for attempt in range(retries):
            try:
                time.sleep(RATE_LIMIT_DELAY)

                # Ensure URL ends with / (recht.nrw.de requires it)
                if not url.endswith('/'):
                    url = url + '/'

                response = self.session.get(url, timeout=30, allow_redirects=True)

                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    wait_time = 10 * (attempt + 1)
                    print(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code >= 500:
                    print(f"Server error {response.status_code}, retrying...")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"Error: {response.status_code} for {url}")
                    return None

            except requests.exceptions.Timeout:
                print(f"Timeout for {url}, attempt {attempt + 1}/{retries}")
                time.sleep(5)
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                time.sleep(5)

        return None

    def _fetch_sitemap_index(self) -> List[str]:
        """Fetch the sitemap index to get list of sub-sitemaps."""
        print(f"Fetching sitemap index from {SITEMAP_INDEX_URL}")
        try:
            response = self.session.get(SITEMAP_INDEX_URL, timeout=60)
            response.raise_for_status()

            root = ET.fromstring(response.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            sitemap_urls = []
            for sitemap in root.findall('.//sm:sitemap', ns):
                loc = sitemap.find('sm:loc', ns)
                if loc is not None and loc.text:
                    sitemap_urls.append(loc.text)

            print(f"Found {len(sitemap_urls)} sub-sitemaps")
            return sitemap_urls

        except Exception as e:
            print(f"Error fetching sitemap index: {e}")
            return []

    def _fetch_sitemap(self, sitemap_url: str) -> List[str]:
        """Fetch a single sitemap and extract legislation URLs."""
        try:
            time.sleep(0.5)  # Rate limit
            response = self.session.get(sitemap_url, timeout=60)
            response.raise_for_status()

            root = ET.fromstring(response.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            urls = []
            for url_elem in root.findall('.//sm:url', ns):
                loc = url_elem.find('sm:loc', ns)
                if loc is not None and loc.text:
                    url = loc.text
                    # Only include legislation URLs (/lrgv/ or /lrmb/)
                    if LRGV_PATTERN.match(url) or LRMB_PATTERN.match(url):
                        urls.append(url)

            return urls

        except Exception as e:
            print(f"Error fetching sitemap {sitemap_url}: {e}")
            return []

    def _get_all_legislation_urls(self, limit: int = None) -> List[str]:
        """Fetch all legislation URLs from sitemaps."""
        sitemap_urls = self._fetch_sitemap_index()
        all_urls = []

        for i, sitemap_url in enumerate(sitemap_urls):
            print(f"Processing sitemap {i+1}/{len(sitemap_urls)}: {sitemap_url}")
            urls = self._fetch_sitemap(sitemap_url)
            all_urls.extend(urls)
            print(f"  Found {len(urls)} legislation URLs")

            if limit and len(all_urls) >= limit:
                all_urls = all_urls[:limit]
                break

        print(f"Total legislation URLs: {len(all_urls)}")
        return all_urls

    def _extract_document_id(self, url: str) -> str:
        """Extract document ID from URL."""
        # URL format: https://recht.nrw.de/lrgv/gesetz/16012026-verfassung-...
        match = re.search(r'/(?:lrgv|lrmb)/[^/]+/([^/]+)/?$', url)
        if match:
            return match.group(1)
        return url.split('/')[-1].rstrip('/')

    def _extract_document_type(self, url: str) -> str:
        """Extract document type from URL."""
        # URL format: /lrgv/gesetz/... or /lrmb/verwaltungsvorschrift/...
        match = re.search(r'/(?:lrgv|lrmb)/([^/]+)/', url)
        if match:
            return match.group(1)
        return "unknown"

    def _parse_html_content(self, html: str, url: str) -> Dict[str, Any]:
        """Parse legislation HTML to extract metadata and full text."""
        soup = BeautifulSoup(html, 'html.parser')

        result = {
            'url': url,
            'doc_id': self._extract_document_id(url),
            'doc_type': self._extract_document_type(url),
            'title': '',
            'vollzitat': '',
            'valid_from': None,
            'fundstelle': '',
            'text': '',
        }

        # Extract title from <h1>
        h1 = soup.find('h1', class_='font-size-h1-small')
        if h1:
            result['title'] = h1.get_text(strip=True)

        # Try title tag as fallback
        if not result['title']:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text()
                # Remove "| RECHT.NRW.DE" suffix
                result['title'] = re.sub(r'\s*\|\s*RECHT\.NRW\.DE.*$', '', title_text).strip()

        # Extract Vollzitat (full citation)
        vollzitat_title = soup.find('h2', class_='full-quote-title')
        if vollzitat_title:
            vollzitat_div = vollzitat_title.find_next('div', class_='full-quote-content')
            if vollzitat_div:
                p = vollzitat_div.find('p')
                if p:
                    result['vollzitat'] = p.get_text(strip=True)

        # Extract valid_from date from document ID (DDMMYYYY format)
        date_match = re.match(r'(\d{8})-', result['doc_id'])
        if date_match:
            date_str = date_match.group(1)
            try:
                # Parse DDMMYYYY format
                dt = datetime.strptime(date_str, "%d%m%Y")
                result['valid_from'] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract fundstelle from Vollzitat if available
        if result['vollzitat']:
            # Extract "(GV. NRW. S. XXX)" pattern
            fundstelle_match = re.search(r'\(GV\.\s*NRW\.?\s*S\.\s*\d+\)', result['vollzitat'])
            if fundstelle_match:
                result['fundstelle'] = fundstelle_match.group(0)

        # Extract full text content
        text_parts = []

        # Get preamble text (before articles)
        document_content = soup.find('article', class_='document-content')
        if document_content:
            # Find all legaldoc-article sections (individual articles)
            for article in document_content.find_all('section', class_='legaldoc-article'):
                # Extract article heading
                h2 = article.find('h2')
                if h2:
                    heading = h2.get_text(strip=True)
                    if heading and heading != "Der Link zum Pragraph wurde kopiert":
                        text_parts.append(f"\n{heading}")

                # Extract article text
                text_div = article.find('div', class_=re.compile(r'field--field_text'))
                if text_div:
                    article_text = self._clean_html_text(str(text_div))
                    if article_text:
                        text_parts.append(article_text)

        # If no structured content found, try getting all text from document-content
        if not text_parts:
            doc_content = soup.find('article', id='documentContent')
            if doc_content:
                # Remove navigation and metadata sections
                for unwanted in doc_content.find_all(['nav', 'script', 'style']):
                    unwanted.decompose()
                text_parts.append(self._clean_html_text(str(doc_content)))

        result['text'] = '\n'.join(text_parts).strip()
        return result

    def _clean_html_text(self, html: str) -> str:
        """Clean HTML to extract plain text."""
        if not html:
            return ""

        soup = BeautifulSoup(html, 'html.parser')

        # Remove script and style elements
        for elem in soup.find_all(['script', 'style', 'nav']):
            elem.decompose()

        # Get text with some structure preserved
        text = soup.get_text(separator='\n')

        # Unescape HTML entities
        text = unescape(text)

        # Clean up whitespace
        text = re.sub(r' +', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Remove common unwanted text
        text = re.sub(r'Der Link zum Pragraph wurde kopiert\n?', '', text)

        return text.strip()

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        doc_type_map = {
            'gesetz': 'Law',
            'rechtsverordnung': 'Ordinance',
            'verwaltungsvorschrift': 'Administrative Directive',
            'bekanntmachung': 'Announcement',
        }

        doc_type = raw_doc.get('doc_type', 'unknown')
        doc_type_display = doc_type_map.get(doc_type, doc_type.title())

        return {
            '_id': f"NRW-{raw_doc.get('doc_id', '')}",
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),

            # Required fields
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('valid_from'),
            'url': raw_doc.get('url', ''),

            # Document metadata
            'doc_id': raw_doc.get('doc_id', ''),
            'doc_type': doc_type,
            'doc_type_display': doc_type_display,
            'vollzitat': raw_doc.get('vollzitat', ''),
            'fundstelle': raw_doc.get('fundstelle', ''),

            # Jurisdiction
            'jurisdiction': 'North Rhine-Westphalia (Nordrhein-Westfalen)',
            'country': 'DE',
            'language': 'de',
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all legislation documents with full text."""
        urls = self._get_all_legislation_urls(limit=limit)

        count = 0
        for i, url in enumerate(urls):
            print(f"[{i+1}/{len(urls)}] Fetching: {url}")

            html = self._fetch_page(url)
            if not html:
                continue

            parsed = self._parse_html_content(html, url)

            if parsed.get('text') and len(parsed.get('text', '')) > 100:
                yield parsed
                count += 1

                if limit and count >= limit:
                    break
            else:
                print(f"  Skipping - insufficient text ({len(parsed.get('text', ''))} chars)")

        print(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent legislation updates."""
        # Would need to track what we've seen before
        # For now, fetch recent from first sitemap page
        yield from self.fetch_all(limit=50)


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records for validation."""
    fetcher = NRWFetcher()
    samples = []

    # Get all URLs from first few sitemaps
    sitemap_urls = fetcher._fetch_sitemap_index()[:3]  # First 3 sitemaps
    all_urls = []
    for sitemap_url in sitemap_urls:
        urls = fetcher._fetch_sitemap(sitemap_url)
        all_urls.extend(urls)

    print(f"Found {len(all_urls)} URLs to sample from")

    # Shuffle to get variety (different doc types)
    import random
    random.shuffle(all_urls)

    # Try to get variety of document types
    doc_type_counts = {}

    for url in all_urls:
        if len(samples) >= count:
            break

        # Check document type
        doc_type = fetcher._extract_document_type(url)

        # Skip if we have too many of this type
        if doc_type_counts.get(doc_type, 0) >= 5:
            continue

        print(f"Fetching: {url}")
        html = fetcher._fetch_page(url)
        if not html:
            continue

        parsed = fetcher._parse_html_content(html, url)
        text_len = len(parsed.get('text', ''))

        # Require substantial text
        if text_len < 500:
            print(f"  Skipping - text too short ({text_len} chars)")
            continue

        normalized = fetcher.normalize(parsed)
        samples.append(normalized)
        doc_type_counts[doc_type] = doc_type_counts.get(doc_type, 0) + 1
        print(f"  Sample {len(samples)}: {doc_type} ({text_len:,} chars)")

    return samples


def save_samples(samples: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Save individual records
    for i, record in enumerate(samples):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

    # Save all samples in one file
    all_samples_path = SAMPLE_DIR / "all_samples.json"
    with open(all_samples_path, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(samples)} samples to {SAMPLE_DIR}")


def validate_samples(samples: List[Dict]) -> bool:
    """Validate sample records meet requirements."""
    print("\n=== Sample Validation ===")

    issues = []

    # Check count
    if len(samples) < 10:
        issues.append(f"Only {len(samples)} samples, need at least 10")

    # Check required fields
    text_lengths = []
    for i, record in enumerate(samples):
        text = record.get("text", "")
        if not text:
            issues.append(f"Record {i}: missing 'text' field")
        elif len(text) < 200:
            issues.append(f"Record {i}: text too short ({len(text)} chars)")
        else:
            text_lengths.append(len(text))

        if not record.get("_id"):
            issues.append(f"Record {i}: missing '_id'")
        if not record.get("title"):
            issues.append(f"Record {i}: missing 'title'")

    # Report
    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Records with text: {len(text_lengths)}/{len(samples)}")
        print(f"Average text length: {avg_len:,.0f} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    # Check document types covered
    doc_types = set(r.get("doc_type") for r in samples if r.get("doc_type"))
    print(f"Document types covered: {len(doc_types)}")
    for dt in sorted(doc_types):
        count = sum(1 for r in samples if r.get("doc_type") == dt)
        print(f"  - {dt}: {count}")

    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False

    print("\n All validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="DE/NRW data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "status"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of sample records to fetch"
    )

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = fetch_sample(args.count)
            save_samples(samples)

            if validate_samples(samples):
                print("\n Bootstrap sample complete")
                return 0
            else:
                print("\n Validation failed")
                return 1
        else:
            print("Full bootstrap - fetching all legislation...")
            fetcher = NRWFetcher()
            count = 0
            for record in fetcher.fetch_all():
                normalized = fetcher.normalize(record)
                count += 1
                if count % 10 == 0:
                    print(f"Fetched {count} records...")
            print(f"Total: {count} records")

    elif args.command == "update":
        print("Fetching recent updates...")
        fetcher = NRWFetcher()
        count = 0
        for record in fetcher.fetch_updates(datetime.now(timezone.utc)):
            normalized = fetcher.normalize(record)
            count += 1
        print(f"Fetched {count} recent documents")

    elif args.command == "status":
        print("Checking sitemaps...")
        fetcher = NRWFetcher()
        sitemap_urls = fetcher._fetch_sitemap_index()
        print(f"\nDE/NRW Status:")
        print(f"  Sitemap pages: {len(sitemap_urls)}")

        # Count URLs in first sitemap
        if sitemap_urls:
            first_urls = fetcher._fetch_sitemap(sitemap_urls[0])
            print(f"  URLs in first sitemap: {len(first_urls)}")
            print(f"  Estimated total URLs: ~{len(first_urls) * len(sitemap_urls)}")

        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
