#!/usr/bin/env python3
"""
RS/SluzbenGlasnik — Serbian Legislation Fetcher

Fetches Serbian legislation from paragraf.rs which mirrors content from the
official Službeni glasnik RS (Official Gazette).

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap            # Fetch 100 records
"""

import argparse
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
SOURCE_ID = "RS/SluzbenGlasnik"
BASE_URL = "https://www.paragraf.rs"
SITEMAP_URL = "https://www.paragraf.rs/sitemap-1.xml"
PROPISI_URL = "https://www.paragraf.rs/propisi.html"
REQUEST_DELAY = 2.0  # seconds between requests
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; +https://github.com/ZachLaik/LegalDataHunter)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


class SerbianLegislationFetcher:
    """Fetcher for Serbian legislation from paragraf.rs"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'sr,en;q=0.9',
        })

    def _make_request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """Make HTTP request with error handling"""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    def get_legislation_urls_from_sitemap(self, limit: int = None) -> List[str]:
        """Extract legislation URLs from sitemap"""
        logger.info(f"Fetching sitemap from {SITEMAP_URL}")
        response = self._make_request(SITEMAP_URL, timeout=60)
        if not response:
            return []

        try:
            root = ET.fromstring(response.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            urls = []
            for loc in root.findall('.//sm:loc', ns):
                url = loc.text
                if url and '/propisi/' in url and url.endswith('.html'):
                    # Skip archive versions and non-legislation pages
                    if '-2019.html' not in url and '-2020.html' not in url and '-2018.html' not in url:
                        urls.append(url)
                        if limit and len(urls) >= limit:
                            break

            logger.info(f"Found {len(urls)} legislation URLs in sitemap")
            return urls

        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            return []

    def get_legislation_urls_from_index(self) -> List[str]:
        """Extract legislation URLs from the index page as fallback"""
        logger.info(f"Fetching index from {PROPISI_URL}")
        response = self._make_request(PROPISI_URL)
        if not response:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        urls = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'propisi/' in href and href.endswith('.html'):
                full_url = urljoin(BASE_URL, href)
                if full_url not in urls:
                    urls.append(full_url)

        logger.info(f"Found {len(urls)} legislation URLs on index page")
        return urls

    def _extract_document_type(self, title: str) -> str:
        """Determine document type from title"""
        title_lower = title.lower()

        if 'zakon' in title_lower:
            return 'zakon'  # Law
        elif 'uredba' in title_lower:
            return 'uredba'  # Decree
        elif 'pravilnik' in title_lower:
            return 'pravilnik'  # Regulation
        elif 'odluka' in title_lower:
            return 'odluka'  # Decision
        elif 'statut' in title_lower:
            return 'statut'  # Statute
        elif 'kodeks' in title_lower:
            return 'kodeks'  # Code
        elif 'tarifa' in title_lower:
            return 'tarifa'  # Tariff
        elif 'poslovnik' in title_lower:
            return 'poslovnik'  # Rules of procedure
        elif 'uputstvo' in title_lower:
            return 'uputstvo'  # Instruction
        else:
            return 'propis'  # Generic regulation

    def _extract_gazette_reference(self, text: str) -> Optional[str]:
        """Extract Official Gazette reference from text"""
        # Pattern: "Sl. glasnik RS", br. XX/YYYY, XX/YYYY, ...
        pattern = r'"Sl\.\s*glasnik\s+RS"[^)]*br\.\s*([^)]+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            refs = match.group(1).strip()
            # Clean up and return first part
            refs = re.sub(r'\s+', ' ', refs)
            return f"Sl. glasnik RS, br. {refs}"
        return None

    def _extract_date_from_gazette(self, gazette_ref: str) -> Optional[str]:
        """Extract most recent date from gazette reference"""
        if not gazette_ref:
            return None

        # Find all year references (XX/YYYY)
        years = re.findall(r'/(\d{4})', gazette_ref)
        if years:
            # Return most recent year as date
            latest_year = max(years)
            return f"{latest_year}-01-01"
        return None

    def fetch_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single legislation document"""
        response = self._make_request(url)
        if not response:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract title from <title> tag
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove " | Paragraf" suffix
            title = re.sub(r'\s*\|\s*Paragraf\s*$', '', title)
            # Remove year prefix like "2022 | "
            title = re.sub(r'^\d{4}\s*\|\s*', '', title)
        else:
            title = ""

        # Extract full text from main content
        # The content is in <p> tags with specific classes
        text_parts = []

        # Find main content area
        main = soup.find('main')
        if main:
            # Extract text from paragraphs
            for p in main.find_all('p'):
                p_class = p.get('class', [])
                # Skip non-content paragraphs
                if 'print-hide' in ' '.join(p_class):
                    continue

                text = p.get_text(strip=True)
                if text and text != '&nbsp;':
                    # Handle article numbers specially
                    if 'clan' in ' '.join(p_class):
                        text_parts.append(f"\n\n{text}\n")
                    elif 'wyq050---odeljak' in ' '.join(p_class):
                        text_parts.append(f"\n\n### {text} ###\n")
                    elif 'wyq060---pododeljak' in ' '.join(p_class):
                        text_parts.append(f"\n\n## {text} ##\n")
                    elif 'wyq110---naslov-clana' in ' '.join(p_class):
                        text_parts.append(f"\n{text}\n")
                    else:
                        text_parts.append(text)

        full_text = '\n'.join(text_parts)

        # Clean up text
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r'[ \t]+', ' ', full_text)
        full_text = full_text.strip()

        if not full_text or len(full_text) < 100:
            logger.warning(f"Insufficient text for {url}: {len(full_text)} chars")
            return None

        # Extract Official Gazette reference
        gazette_ref = self._extract_gazette_reference(full_text)
        date = self._extract_date_from_gazette(gazette_ref)

        # Generate ID from URL
        doc_id = url.split('/')[-1].replace('.html', '')

        return {
            'url': url,
            'doc_id': doc_id,
            'title': title,
            'text': full_text,
            'gazette_ref': gazette_ref,
            'date': date,
            'document_type': self._extract_document_type(title),
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all legislation documents"""
        # Get URLs from sitemap
        urls = self.get_legislation_urls_from_sitemap(limit=limit * 2 if limit else None)

        if not urls:
            # Fallback to index page
            urls = self.get_legislation_urls_from_index()

        if limit:
            urls = urls[:limit]

        logger.info(f"Fetching {len(urls)} documents...")

        for i, url in enumerate(urls):
            logger.info(f"[{i+1}/{len(urls)}] Fetching: {url}")

            doc = self.fetch_document(url)
            if doc:
                yield doc

            # Rate limiting
            time.sleep(REQUEST_DELAY)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        return {
            '_id': raw_doc['doc_id'],
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw_doc['title'],
            'text': raw_doc['text'],
            'date': raw_doc.get('date'),
            'url': raw_doc['url'],
            'official_gazette_ref': raw_doc.get('gazette_ref'),
            'document_type': raw_doc.get('document_type', 'propis'),
            'language': 'sr',
        }


def main():
    parser = argparse.ArgumentParser(description="Serbian legislation fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    bootstrap_parser.add_argument("--count", type=int, default=100, help="Number of records to fetch")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    fetcher = SerbianLegislationFetcher()

    if args.command == "bootstrap":
        target_count = 15 if args.sample else args.count

        logger.info(f"Starting bootstrap - target: {target_count} records")

        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        records = []
        text_lengths = []

        for raw_doc in fetcher.fetch_all(limit=target_count + 10):
            if len(records) >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            records.append(normalized)
            text_lengths.append(text_len)

            # Save individual file
            filename = f"{normalized['_id']}.json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved [{len(records)}/{target_count}]: {normalized['title'][:60]}... ({text_len:,} chars)")

        # Save all samples in one file
        all_samples = SAMPLE_DIR / "all_samples.json"
        with open(all_samples, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        # Print summary
        if records:
            avg_len = sum(text_lengths) // len(text_lengths)
            logger.info(f"\n=== BOOTSTRAP COMPLETE ===")
            logger.info(f"  Documents saved: {len(records)}")
            logger.info(f"  Total text chars: {sum(text_lengths):,}")
            logger.info(f"  Average chars/doc: {avg_len:,}")
            logger.info(f"  Sample directory: {SAMPLE_DIR}")
        else:
            logger.error("No documents fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
