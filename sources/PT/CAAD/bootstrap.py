#!/usr/bin/env python3
"""
PT/CAAD -- Portuguese Tax Arbitration Centre Fetcher

Fetches arbitration decisions from CAAD (Centro de Arbitragem Administrativa).

Strategy:
  - Discovery: Paginated listing at caad.org.pt/{area}/decisoes/index.php
    with listPageSize=100, extracting decision IDs from links
  - Full text: HTML body on decisao.php?id={ID} pages, in a large colspan <td>
  - Two areas: tributario (~10,100+ decisions), administrativo (~340+ decisions)
  - Total: ~10,400+ decisions with full text

License: Public (Portuguese arbitration open data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import hashlib
import html as html_mod
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://caad.org.pt"
PAGE_SIZE = 100

# Areas to scrape: (url_path, label)
AREAS = [
    ("tributario", "Tax"),
    ("administrativo", "Administrative"),
]


class CAADFetcher:
    """Fetcher for Portuguese Tax Arbitration Centre decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html',
        })

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML to plain text"""
        if not html_content:
            return ""
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = html_mod.unescape(text)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _fetch_with_retry(self, url: str, retries: int = 2, timeout: int = 60) -> Optional[requests.Response]:
        """Fetch URL with retries"""
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    wait = 3 * (attempt + 1)
                    logger.warning(f"Attempt {attempt+1} failed for {url}: {e}. Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to fetch {url} after {retries+1} attempts: {e}")
                    return None

    def _parse_listing_page(self, html_content: str, area: str) -> List[Dict[str, str]]:
        """Parse a listing page to extract decision metadata"""
        results = []
        # Each row has: <a href="decisao.php?...&id=NNNN">case_number</a>, date, tax_type, theme
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_content, re.DOTALL)

        for row in rows:
            # Must contain a decision link
            id_match = re.search(r'decisao\.php\?[^"]*id=(\d+)', row)
            if not id_match:
                continue

            doc_id = id_match.group(1)

            # Extract case number from link text
            case_match = re.search(r'id="listprocesso[^"]*"[^>]*>([^<]+)</a>', row)
            case_number = case_match.group(1).strip() if case_match else ""

            # Extract cells
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            # Cells: [case_number_link, date, tax_type, theme, articles]
            date = ""
            tax_type = ""
            theme = ""

            if len(cells) >= 2:
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', cells[1])
                date = date_match.group(1) if date_match else ""
            if len(cells) >= 3:
                tax_type = re.sub(r'<[^>]+>', '', cells[2]).strip().rstrip('\xa0').strip()
            if len(cells) >= 4:
                theme = re.sub(r'<[^>]+>', '', cells[3]).strip().rstrip('\xa0').strip()

            results.append({
                'doc_id': doc_id,
                'case_number': case_number,
                'date': date,
                'tax_type': tax_type,
                'theme': theme,
                'area': area,
            })

        return results

    def _fetch_decision_text(self, doc_id: str, area: str) -> Tuple[str, str]:
        """Fetch full text of a decision. Returns (text, title)."""
        url = f"{BASE_URL}/{area}/decisoes/decisao.php?id={doc_id}"
        response = self._fetch_with_retry(url)
        if not response:
            return "", ""

        page = response.text

        # Extract title (case number from header)
        title_match = re.search(r'Processo nº\s*([\d/\-A-Za-z]+)', page)
        title = f"Processo nº {title_match.group(1)}" if title_match else ""

        # Extract the large content block (decision text in colspan td)
        blocks = re.findall(r'<td[^>]*colspan[^>]*>(.*?)</td>', page, re.DOTALL)
        large_blocks = [b for b in blocks if len(b) > 500]

        if large_blocks:
            # Take the largest block as the decision text
            largest = max(large_blocks, key=len)
            text = self._clean_html(largest)
        else:
            # Fallback: try to find any large text area
            text = ""

        return text, title

    def discover_decisions(self, area: str, limit: int = None) -> Iterator[Dict[str, str]]:
        """Discover decisions from a listing area with pagination"""
        page_num = 1
        total_yielded = 0

        while True:
            if limit and total_yielded >= limit:
                return

            url = (f"{BASE_URL}/{area}/decisoes/index.php?"
                   f"listPageSize={PAGE_SIZE}&listPage={page_num}"
                   f"&listOrder=Sorter_data&listDir=DESC")

            response = self._fetch_with_retry(url)
            if not response:
                break

            items = self._parse_listing_page(response.text, area)
            if not items:
                break

            for item in items:
                if limit and total_yielded >= limit:
                    return
                yield item
                total_yielded += 1

            logger.info(f"[{area}] Page {page_num}: {len(items)} decisions (total: {total_yielded})")

            # Check if there are more pages
            if len(items) < PAGE_SIZE:
                break

            page_num += 1
            time.sleep(1)  # Rate limiting

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all decisions with full text"""
        count = 0

        for area, label in AREAS:
            if limit and count >= limit:
                return

            area_limit = limit - count if limit else None
            logger.info(f"Starting {label} ({area}) decisions...")

            for meta in self.discover_decisions(area, limit=area_limit):
                if limit and count >= limit:
                    return

                doc_id = meta['doc_id']
                logger.info(f"Fetching [{count+1}] {area} id={doc_id}: {meta['case_number']}...")

                text, title = self._fetch_decision_text(doc_id, area)
                if not text or len(text) < 100:
                    logger.warning(f"No text for {area} id={doc_id}")
                    continue

                meta['text'] = text
                meta['title'] = title or meta['case_number']
                yield meta
                count += 1

                time.sleep(1.5)  # Rate limiting

        logger.info(f"Fetched {count} decisions with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize decision to standard schema"""
        area = raw.get('area', 'tributario')
        doc_id = raw.get('doc_id', '')
        case_number = raw.get('case_number', '')

        return {
            '_id': f"PT-CAAD-{area}-{doc_id}",
            '_source': 'PT/CAAD',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw.get('title', case_number),
            'text': raw.get('text', ''),
            'date': raw.get('date', ''),
            'url': f"{BASE_URL}/{area}/decisoes/decisao.php?id={doc_id}",
            'language': 'pt',
            'case_number': case_number,
            'tax_type': raw.get('tax_type', ''),
            'theme': raw.get('theme', ''),
            'area': area,
        }


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = CAADFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None
        limit = target_count + 5 if target_count else None

        logger.info(f"Starting bootstrap (sample={is_sample})...")

        saved = 0
        for raw in fetcher.fetch_all(limit=limit):
            if target_count and saved >= target_count:
                break

            normalized = fetcher.normalize(raw)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            filename = f"{normalized['_id']}.json"
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{saved+1}]: {normalized['case_number']} ({text_len:,} chars)")
            saved += 1

        logger.info(f"Bootstrap complete. Saved {saved} documents to {sample_dir}")

        # Summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for fp in files:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        fetcher = CAADFetcher()
        print("Testing PT/CAAD fetcher...")
        count = 0
        for raw in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw)
            print(f"\n--- Decision {count+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Case: {normalized['case_number']}")
            print(f"Date: {normalized['date']}")
            print(f"Tax type: {normalized['tax_type']}")
            print(f"URL: {normalized['url']}")
            print(f"Text length: {len(normalized['text'])} chars")
            print(f"Text preview: {normalized['text'][:300]}...")
            count += 1


if __name__ == '__main__':
    main()
