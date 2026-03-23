#!/usr/bin/env python3
"""
Brandenburg State Law (BRAVORS) Fetcher

Official open data from bravors.brandenburg.de
https://bravors.brandenburg.de

This fetcher retrieves Brandenburg state legislation using:
- Subject area navigation for document discovery
- HTML pages for full text extraction

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://bravors.brandenburg.de"
SUBJECT_AREAS = list(range(1, 10))  # Subject areas 1-9


class BrandenburgFetcher:
    """Fetcher for Brandenburg state legislation from BRAVORS"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_law_urls_from_subject_areas(self, limit: int = None) -> List[str]:
        """Fetch law URLs from all subject area pages"""
        all_urls: Set[str] = set()

        for subject_id in SUBJECT_AREAS:
            url = f"{BASE_URL}/de/vorschriften_fundstellennachweis_gesetzte_und_verordnungen_sachgebietlich/sachgebiet/{subject_id}"
            logger.info(f"Fetching subject area {subject_id}: {url}")

            try:
                response = self.session.get(url, timeout=60)
                response.raise_for_status()
                html = response.text

                # Extract law URLs - two patterns:
                # 1. /de/gesetze-{id} or /de/verordnungen-{id}
                # 2. /gesetze/{shortname} or /verordnungen/{shortname}
                pattern1 = re.findall(r'href="(/de/(?:gesetze|verordnungen)-\d+)"', html)
                pattern2 = re.findall(r'href="(/(gesetze|verordnungen)/[^"]+)"', html)

                for path in pattern1:
                    all_urls.add(f"{BASE_URL}{path}")

                for path, _ in pattern2:
                    # Skip navigation/overview pages
                    if '/fundstellen' not in path and '/sachgebiet' not in path:
                        all_urls.add(f"{BASE_URL}{path}")

                logger.info(f"Subject area {subject_id}: found {len(pattern1) + len(pattern2)} URLs (total unique: {len(all_urls)})")

                if limit and len(all_urls) >= limit:
                    break

                time.sleep(0.3)

            except requests.RequestException as e:
                logger.error(f"Error fetching subject area {subject_id}: {e}")
                continue

        urls = list(all_urls)
        if limit:
            urls = urls[:limit]

        logger.info(f"Total unique law URLs found: {len(urls)}")
        return urls

    def _extract_law_id(self, url: str) -> str:
        """Extract law ID from URL"""
        # Pattern 1: /de/gesetze-212792
        match = re.search(r'/(gesetze|verordnungen)-(\d+)', url)
        if match:
            return f"{match.group(1)}_{match.group(2)}"

        # Pattern 2: /gesetze/swg or /verordnungen/wo_swg_2014
        match2 = re.search(r'/(gesetze|verordnungen)/([^/?#]+)', url)
        if match2:
            return f"{match2.group(1)}_{match2.group(2)}"

        # Fallback: use last path segment
        return url.split('/')[-1].replace('-', '_')

    def _fetch_law_html(self, url: str) -> Optional[str]:
        """Fetch the HTML content of a law page"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def _parse_law_html(self, html: str, url: str) -> Dict[str, Any]:
        """Parse law HTML to extract metadata and full text"""
        result = {
            'url': url,
            'law_id': self._extract_law_id(url),
            'title': '',
            'fundstelle': '',
            'valid_from': '',
            'text': '',
        }

        # Extract title from <h2 id="norm_title">
        title_match = re.search(r'<h2\s+id="norm_title"[^>]*>([^<]+)</h2>', html)
        if title_match:
            result['title'] = unescape(title_match.group(1).strip())
        else:
            # Fallback: try <title> tag
            title_tag_match = re.search(r'<title>([^<]+)</title>', html)
            if title_tag_match:
                result['title'] = unescape(title_tag_match.group(1).strip())

        # Extract Fundstelle (publication reference) from reiterbox_innen_kopf
        fundstelle_match = re.search(r'Fundstelle[:\s]*([^<]+)<', html)
        if fundstelle_match:
            result['fundstelle'] = unescape(fundstelle_match.group(1).strip())

        # Extract validity date
        valid_match = re.search(r'(?:gültig\s+(?:ab|seit)[:.\s]*|Stand:\s*)(\d{1,2}\.\s*\w+\s*\d{4}|\d{1,2}\.\d{1,2}\.\d{4})', html, re.IGNORECASE)
        if valid_match:
            result['valid_from'] = valid_match.group(1).strip()

        # Extract the main text from reiterbox_innen_text div
        # The text ends with </div> followed by <div id="nachoben"> or similar
        text_match = re.search(
            r'<div\s+class="reiterbox_innen_text"[^>]*>(.*?)</div>\s*<div\s+(?:style="margin-top|id="nachoben")',
            html, re.DOTALL
        )
        if text_match:
            raw_text = text_match.group(1)
            result['text'] = self._clean_html_text(raw_text)
        else:
            # Alternative: find reiterbox_innen_text and take content until CONTENT MITTE - ENDE
            alt_match = re.search(
                r'class="reiterbox_innen_text"[^>]*>(.*?)<!--\s*CONTENT\s+MITTE\s+-\s+ENDE',
                html, re.DOTALL
            )
            if alt_match:
                raw_text = alt_match.group(1)
                result['text'] = self._clean_html_text(raw_text)

        return result

    def _clean_html_text(self, html: str) -> str:
        """Clean HTML to extract plain text"""
        # Remove script and style elements
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert headings to newlines with markers
        text = re.sub(r'</h[1-6]>', '\n\n', text)
        text = re.sub(r'<h[1-6][^>]*>', '\n', text)

        # Convert paragraph and break tags
        text = re.sub(r'</p>', '\n', text)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</div>', '\n', text)
        text = re.sub(r'</li>', '\n', text)

        # Remove remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Unescape HTML entities
        text = unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)  # Collapse horizontal whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)  # Limit consecutive newlines
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)  # Remove leading whitespace

        return text.strip()

    def _parse_german_date(self, date_str: str) -> Optional[str]:
        """Convert German date string to ISO 8601"""
        if not date_str:
            return None

        # Try DD.MM.YYYY format first
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month = match.group(2).zfill(2)
            year = match.group(3)
            return f"{year}-{month}-{day}"

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
        Fetch Brandenburg laws with full text.

        Args:
            limit: Maximum number of laws to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        urls = self._get_law_urls_from_subject_areas(limit=limit * 2 if limit else None)

        count = 0
        for i, url in enumerate(urls):
            if limit and count >= limit:
                break

            logger.info(f"[{i+1}/{len(urls)}] Fetching: {url}")

            html = self._fetch_law_html(url)
            if not html:
                continue

            parsed = self._parse_law_html(html, url)

            if parsed.get('text') and len(parsed.get('text', '')) > 100:
                yield parsed
                count += 1
            else:
                logger.warning(f"Skipping {url} - insufficient text ({len(parsed.get('text', ''))} chars)")

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Fetched {count} laws with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent laws"""
        # For updates, fetch a limited number of recent documents
        yield from self.fetch_all(limit=50)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        law_id = raw_doc.get('law_id', '')

        title = raw_doc.get('title', '')
        if not title:
            title = f"Vorschrift {law_id}"

        # Parse date
        date = self._parse_german_date(raw_doc.get('valid_from', ''))

        # All laws and regulations are classified as legislation
        doc_type = 'legislation'

        return {
            '_id': f"BB-{law_id}",
            '_source': 'DE/Brandenburg',
            '_type': doc_type,
            '_fetched_at': datetime.now().isoformat(),
            'law_id': law_id,
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': date,
            'valid_from': raw_doc.get('valid_from', ''),
            'url': raw_doc.get('url', ''),
            'fundstelle': raw_doc.get('fundstelle', ''),
            'jurisdiction': 'Brandenburg',
            'country': 'DE',
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BrandenburgFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target_count + 20):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            # Require substantial text content
            if text_len < 500:
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(' ', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['title'][:60]}... ({text_len:,} chars)")
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
        fetcher = BrandenburgFetcher()
        print("Testing Brandenburg fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Valid from: {normalized['valid_from']}")
            print(f"Date (ISO): {normalized['date']}")
            print(f"Fundstelle: {normalized['fundstelle']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
