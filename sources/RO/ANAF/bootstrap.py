#!/usr/bin/env python3
"""
RO/ANAF -- Romanian Tax Authority Doctrine Fetcher

Fetches tax doctrine from ANAF (Agenția Națională de Administrare Fiscală).

Strategy:
  - Fiscal bulletins: Archive index pages at static.anaf.ro with PDF links
    Years 2020-2026, ~40-50 bulletins per year
  - ANAF Orders (OPANAF): Static index pages for 2006-2019, enumeration for 2020+
  - Circulars: Lotus Domino JSON API at chat.anaf.ro (17 entries)
  - All PDFs on static.anaf.ro, text extracted via pypdf

License: Public (Romanian government open data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import html as html_mod
import io
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

try:
    import pypdf
except ImportError:
    pypdf = None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
STATIC_BASE = "https://static.anaf.ro/static/10/Anaf"
BULLETIN_ARCHIVE_URL = f"{STATIC_BASE}/legislatie/arhiva{{year}}_noutati_legislative.htm"
BULLETIN_YEARS = list(range(2026, 2019, -1))  # 2020-2026

# OPANAF order index pages (2006-2019 have static pages)
OPANAF_INDEX_URLS = {
    year: f"{STATIC_BASE}/Legislatie_R/Ordine_Anaf_{year}.htm"
    for year in range(2006, 2019)
}
OPANAF_INDEX_URLS[2019] = f"{STATIC_BASE}/Legislatie_R/Ordine_Anaf.htm"

CIRCULARS_URL = "https://chat.anaf.ro/Circulare.nsf/CirculareInternet?ReadViewEntries&outputformat=JSON"


class ANAFFetcher:
    """Fetcher for Romanian ANAF tax doctrine"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })

    def _extract_text_from_pdf(self, content: bytes) -> str:
        """Extract text from PDF bytes using pypdf"""
        if not pypdf:
            logger.warning("pypdf not available")
            return ""
        try:
            reader = pypdf.PdfReader(io.BytesIO(content))
            parts = []
            for page in reader.pages[:200]:
                page_text = page.extract_text()
                if page_text:
                    parts.append(page_text)
            return "\n\n".join(parts).strip()
        except Exception as e:
            logger.debug(f"PDF extraction failed: {e}")
            return ""

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
                    logger.warning(f"Attempt {attempt+1} failed: {e}. Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to fetch {url}: {e}")
                    return None

    def _fetch_pdf_text(self, url: str) -> str:
        """Download PDF and extract text"""
        response = self._fetch_with_retry(url, timeout=90)
        if not response:
            return ""
        if len(response.content) > 50_000_000:
            logger.warning(f"Skipping oversized PDF: {url}")
            return ""
        return self._extract_text_from_pdf(response.content)

    def discover_bulletins(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Discover fiscal bulletins from archive pages"""
        count = 0

        for year in BULLETIN_YEARS:
            if limit and count >= limit:
                return

            url = BULLETIN_ARCHIVE_URL.format(year=year)
            response = self._fetch_with_retry(url)
            if not response:
                continue

            # Parse HTML for bulletin entries
            html_text = response.text

            # Find all PDF URLs and their context
            items = re.findall(
                r'<li>(.*?)</li>',
                html_text, re.DOTALL
            )

            for item in items:
                if limit and count >= limit:
                    return

                # Extract PDF URL
                pdf_match = re.search(
                    r'(https://static\.anaf\.ro/[^"\'>\s]+\.pdf)',
                    item
                )
                if not pdf_match:
                    continue

                pdf_url = pdf_match.group(1)

                # Extract title text
                clean_text = re.sub(r'<[^>]+>', '', item).strip()
                clean_text = html_mod.unescape(clean_text)

                # Extract date
                date_match = re.search(r'din\s+(\d{1,2})\s+(\w+)\s+(\d{4})', clean_text)
                date = ""
                if date_match:
                    day = date_match.group(1)
                    month_name = date_match.group(2).lower()
                    year_str = date_match.group(3)
                    months = {
                        'ianuarie': '01', 'februarie': '02', 'martie': '03',
                        'aprilie': '04', 'mai': '05', 'iunie': '06',
                        'iulie': '07', 'august': '08', 'septembrie': '09',
                        'octombrie': '10', 'noiembrie': '11', 'decembrie': '12',
                    }
                    month = months.get(month_name, '01')
                    date = f"{year_str}-{month}-{int(day):02d}"

                # Extract bulletin number
                num_match = re.search(r'nr\.\s*(\d+)', clean_text)
                bulletin_num = num_match.group(1) if num_match else ""

                count += 1
                yield {
                    'doc_id': f"bulletin-{bulletin_num}-{year}",
                    'title': clean_text[:200],
                    'url': pdf_url,
                    'date': date,
                    'year': str(year),
                    'doc_type': 'bulletin',
                    'bulletin_number': bulletin_num,
                }

            logger.info(f"Year {year}: found {count} bulletins so far")
            time.sleep(0.5)

    def discover_opanaf_orders(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Discover OPANAF orders from static index pages"""
        count = 0

        for year in sorted(OPANAF_INDEX_URLS.keys(), reverse=True):
            if limit and count >= limit:
                return

            url = OPANAF_INDEX_URLS[year]
            response = self._fetch_with_retry(url)
            if not response:
                continue

            # Find PDF links to OPANAF documents
            pdf_urls = re.findall(
                r'(https?://static\.anaf\.ro/[^"\'>\s]*OPANAF[^"\'>\s]*\.pdf)',
                response.text
            )

            if not pdf_urls:
                # Try relative paths
                pdf_urls = re.findall(
                    r'href=["\']([^"\']*OPANAF[^"\']*\.pdf)',
                    response.text
                )
                # Convert relative to absolute
                pdf_urls = [
                    f"{STATIC_BASE}/legislatie/{u}" if not u.startswith('http') else u
                    for u in pdf_urls
                ]

            for pdf_url in pdf_urls:
                if limit and count >= limit:
                    return

                # Extract order number from URL
                order_match = re.search(r'OPANAF_(\d+)_(\d{4})', pdf_url)
                if not order_match:
                    continue

                order_num = order_match.group(1)
                order_year = order_match.group(2)

                count += 1
                yield {
                    'doc_id': f"OPANAF-{order_num}-{order_year}",
                    'title': f"OPANAF nr. {order_num}/{order_year}",
                    'url': pdf_url,
                    'date': f"{order_year}-01-01",
                    'year': order_year,
                    'doc_type': 'order',
                    'order_number': order_num,
                }

            logger.info(f"OPANAF {year}: found {count} orders so far")
            time.sleep(0.5)

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all documents with full text"""
        count = 0

        # Bulletins first (most recent and useful)
        logger.info("Fetching fiscal bulletins...")
        for meta in self.discover_bulletins(limit=limit):
            if limit and count >= limit:
                return

            logger.info(f"Downloading [{count+1}]: {meta['title'][:60]}...")
            text = self._fetch_pdf_text(meta['url'])
            if not text or len(text) < 100:
                logger.warning(f"No text for {meta['doc_id']}")
                continue

            meta['text'] = text
            yield meta
            count += 1
            time.sleep(1)

        # OPANAF orders
        if not limit or count < limit:
            order_limit = (limit - count) if limit else None
            logger.info("Fetching OPANAF orders...")
            for meta in self.discover_opanaf_orders(limit=order_limit):
                if limit and count >= limit:
                    return

                logger.info(f"Downloading [{count+1}]: {meta['title']}...")
                text = self._fetch_pdf_text(meta['url'])
                if not text or len(text) < 100:
                    logger.warning(f"No text for {meta['doc_id']}")
                    continue

                meta['text'] = text
                yield meta
                count += 1
                time.sleep(1)

        logger.info(f"Fetched {count} documents with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        return {
            '_id': f"RO-ANAF-{raw.get('doc_id', '')}",
            '_source': 'RO/ANAF',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date', ''),
            'url': raw.get('url', ''),
            'language': 'ro',
            'doc_type': raw.get('doc_type', ''),
        }


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ANAFFetcher()
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
                continue

            filename = f"{normalized['_id']}.json"
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{saved+1}]: {normalized['title'][:50]}... ({text_len:,} chars)")
            saved += 1

        logger.info(f"Bootstrap complete. Saved {saved} documents to {sample_dir}")

        files = list(sample_dir.glob('*.json'))
        total_chars = sum(
            len(json.load(open(fp)).get('text', ''))
            for fp in files
        )

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        fetcher = ANAFFetcher()
        print("Testing RO/ANAF fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(limit=3)):
            normalized = fetcher.normalize(raw)
            print(f"\n--- Document {i+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized['text'])} chars")
            print(f"Text preview: {normalized['text'][:300]}...")


if __name__ == '__main__':
    main()
