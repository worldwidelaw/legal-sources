#!/usr/bin/env python3
"""
Saxon State Law (REVOSAX) Fetcher

Official open data from revosax.sachsen.de
https://www.revosax.sachsen.de

This fetcher retrieves Saxon state legislation using:
- Sitemap XML for document discovery
- HTML pages for full text extraction

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.revosax.sachsen.de"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"


class SachsenFetcher:
    """Fetcher for Saxon state legislation from REVOSAX"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_vorschrift_urls_from_sitemap(self, limit: int = None) -> List[str]:
        """Fetch law URLs from sitemap.xml"""
        logger.info(f"Fetching sitemap from {SITEMAP_URL}")
        response = self.session.get(SITEMAP_URL, timeout=60)
        response.raise_for_status()

        # Parse sitemap XML
        root = ET.fromstring(response.content)
        # Sitemap namespace
        ns = {'': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        urls = []
        # Use {namespace}tag syntax for default namespace
        for url_elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
            loc = url_elem.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
            if loc is None:
                continue
            url = loc.text
            # Only include /vorschrift/ URLs (the actual laws)
            if url and '/vorschrift/' in url:
                urls.append(url)

                if limit and len(urls) >= limit:
                    break

        logger.info(f"Found {len(urls)} law URLs in sitemap")
        return urls

    def _extract_vorschrift_id(self, url: str) -> str:
        """Extract vorschrift ID from URL"""
        # URL format: https://www.revosax.sachsen.de/vorschrift/1813-Saechsisches-Verwaltungsvorschriftengesetz
        match = re.search(r'/vorschrift/(\d+)', url)
        if match:
            return match.group(1)
        return url.split('/')[-1]

    def _fetch_law_html(self, url: str) -> Optional[str]:
        """Fetch the HTML content of a law page.

        REVOSAX uses JavaScript to load content on /vorschrift/ pages.
        We first try to find and fetch the /vorschrift_gesamt/ URL which
        contains the full pre-rendered HTML text.
        """
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            html = response.text

            # Check if content is loaded via JS (article is empty or minimal)
            # Look for the vorschrift_gesamt link which has full pre-rendered content
            gesamt_match = re.search(r'href="(/vorschrift_gesamt/\d+/\d+\.html)"', html)
            if gesamt_match:
                gesamt_url = f"{BASE_URL}{gesamt_match.group(1)}"
                logger.debug(f"Using full HTML view: {gesamt_url}")
                gesamt_response = self.session.get(gesamt_url, timeout=60)
                gesamt_response.raise_for_status()
                return gesamt_response.text

            return html
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def _parse_law_html(self, html: str, url: str) -> Dict[str, Any]:
        """Parse law HTML to extract metadata and full text"""
        result = {
            'url': url,
            'vorschrift_id': self._extract_vorschrift_id(url),
            'title': '',
            'short_title': '',
            'vollzitat': '',
            'fundstelle': '',
            'gliederungsnummer': '',
            'valid_from': '',
            'text': '',
        }

        # Extract title from <h1>
        title_match = re.search(r'<h1[^>]*class="[^"]*mbottom[^"]*"[^>]*>([^<]+)</h1>', html)
        if title_match:
            result['title'] = unescape(title_match.group(1).strip())

        # Extract short title from <title>
        title_tag_match = re.search(r'<title>\s*REVOSax[^-]*-\s*([^<]+)</title>', html)
        if title_tag_match:
            result['short_title'] = unescape(title_tag_match.group(1).strip())

        # Extract Vollzitat (full citation)
        vollzitat_match = re.search(r'<p>Vollzitat:\s*([^<]+)</p>', html)
        if vollzitat_match:
            result['vollzitat'] = unescape(vollzitat_match.group(1).strip())

        # Extract Fundstelle (publication reference)
        fundstelle_match = re.search(r'<h3>Fundstelle[^<]*</h3>\s*<p>\s*([^<]+)', html)
        if fundstelle_match:
            result['fundstelle'] = unescape(fundstelle_match.group(1).strip())

        # Extract Gültigkeitszeitraum (validity period)
        valid_match = re.search(r'Fassung gültig ab:\s*(\d+\.\s*\w+\s*\d+)', html)
        if valid_match:
            result['valid_from'] = valid_match.group(1).strip()

        # Extract systematische Gliederungsnummer
        fsn_match = re.search(r'Fsn-Nr\.:\s*([\d-]+)', html)
        if fsn_match:
            result['gliederungsnummer'] = fsn_match.group(1).strip()

        # Extract the main text from <article id="lesetext">
        # The article tag structure is: <article id="lesetext">
        text_match = re.search(r'<article\s+id="lesetext"[^>]*>(.*?)</article>', html, re.DOTALL)
        if text_match:
            raw_text = text_match.group(1)
            result['text'] = self._clean_html_text(raw_text)
        else:
            # Try alternative patterns for older page layouts
            text_match2 = re.search(r'<div[^>]*class="[^"]*law_show[^"]*"[^>]*>(.*?)</div>\s*<div class="fixfloat"', html, re.DOTALL)
            if text_match2:
                raw_text = text_match2.group(1)
                result['text'] = self._clean_html_text(raw_text)

        return result

    def _clean_html_text(self, html: str) -> str:
        """Clean HTML to extract plain text"""
        # Remove script and style elements
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert some tags to newlines
        text = re.sub(r'</h[1-6]>', '\n\n', text)
        text = re.sub(r'<h[1-6][^>]*>', '\n', text)
        text = re.sub(r'</p>', '\n', text)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</section>', '\n\n', text)
        text = re.sub(r'</div>', '\n', text)
        text = re.sub(r'</li>', '\n', text)
        text = re.sub(r'</dd>', '\n', text)
        text = re.sub(r'</dt>', ' ', text)

        # Handle definition lists
        text = re.sub(r'<dt[^>]*class="td_1"[^>]*>\s*(\d+\.?)\s*</dt>', r'\n\1 ', text)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Unescape HTML entities
        text = unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)  # Collapse horizontal whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)  # Limit consecutive newlines
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)  # Remove leading whitespace per line

        return text.strip()

    def _parse_german_date(self, date_str: str) -> Optional[str]:
        """Convert German date string to ISO 8601"""
        if not date_str:
            return None

        # Map German month names
        months = {
            'Januar': '01', 'Februar': '02', 'März': '03', 'April': '04',
            'Mai': '05', 'Juni': '06', 'Juli': '07', 'August': '08',
            'September': '09', 'Oktober': '10', 'November': '11', 'Dezember': '12'
        }

        # Try parsing "DD. Month YYYY" format
        match = re.match(r'(\d+)\.\s*(\w+)\s*(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2)
            year = match.group(3)

            month = months.get(month_name)
            if month:
                return f"{year}-{month}-{day}"

        return date_str

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch Saxon laws with full text.

        Args:
            limit: Maximum number of laws to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        urls = self._get_vorschrift_urls_from_sitemap(limit=limit)

        count = 0
        for i, url in enumerate(urls):
            logger.info(f"[{i+1}/{len(urls)}] Fetching: {url}")

            html = self._fetch_law_html(url)
            if not html:
                continue

            parsed = self._parse_law_html(html, url)

            if parsed.get('text') and len(parsed.get('text', '')) > 100:
                yield parsed
                count += 1

                if limit and count >= limit:
                    break
            else:
                logger.warning(f"Skipping {url} - insufficient text ({len(parsed.get('text', ''))} chars)")

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Fetched {count} laws with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent laws (sitemap doesn't have dates, so fetch recent by ID)"""
        # For updates, we'd need to track which IDs we've seen before
        # For now, just yield from fetch_all with a limit
        yield from self.fetch_all(limit=50)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        vorschrift_id = raw_doc.get('vorschrift_id', '')

        # Use short_title if title is empty
        title = raw_doc.get('title') or raw_doc.get('short_title', '')
        if not title:
            title = f"Vorschrift {vorschrift_id}"

        # Parse date
        date = self._parse_german_date(raw_doc.get('valid_from', ''))

        return {
            '_id': f"SN-{vorschrift_id}",
            '_source': 'DE/Sachsen',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'vorschrift_id': vorschrift_id,
            'title': title,
            'short_title': raw_doc.get('short_title', ''),
            'text': raw_doc.get('text', ''),
            'date': date,
            'valid_from': raw_doc.get('valid_from', ''),
            'url': raw_doc.get('url', ''),
            'vollzitat': raw_doc.get('vollzitat', ''),
            'fundstelle': raw_doc.get('fundstelle', ''),
            'gliederungsnummer': raw_doc.get('gliederungsnummer', ''),
            'jurisdiction': 'Saxony (Sachsen)',
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = SachsenFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 50

        # For --sample mode, fetch more URLs and skip amendments to get diverse samples
        # Amendments ("Aend-") are usually very short references, not full laws
        sample_mode = '--sample' in sys.argv
        fetch_limit = 200 if sample_mode else target_count + 20  # More to filter from

        for raw_doc in fetcher.fetch_all(limit=fetch_limit):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            # In sample mode, require longer documents (skip amendments)
            min_text_len = 1000 if sample_mode else 100
            if text_len < min_text_len:
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_')
            filename = f"{doc_id}.json"
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
        # Test mode
        fetcher = SachsenFetcher()
        print("Testing Sachsen fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Short title: {normalized['short_title']}")
            print(f"Valid from: {normalized['valid_from']}")
            print(f"Date (ISO): {normalized['date']}")
            print(f"Fundstelle: {normalized['fundstelle']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
