#!/usr/bin/env python3
"""
PL/URE -- Polish Energy Regulatory Office (Urząd Regulacji Energetyki) Decisions

Fetches energy regulatory decisions from the URE BIP (Public Information Bulletin):
  https://bip.ure.gov.pl/bip/taryfy-i-inne-decyzje-b

Data source: BIP website with PDF attachments
Strategy:
  1. Paginate through sector index pages to collect yearly archive URLs
  2. Parse each yearly archive page to extract decision metadata + PDF links
  3. Download PDFs and extract full text via pdfplumber
License: Public domain (official government decisions)
Coverage: ~3,000+ decisions across 5 energy sectors (1999-2026)

Usage:
  python bootstrap.py bootstrap --sample     # Fetch sample records
  python bootstrap.py bootstrap              # Full fetch
  python bootstrap.py test-api               # Connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://bip.ure.gov.pl"
SOURCE_ID = "PL/URE"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

SECTORS = [
    ("energia-elektryczna", "Electricity"),
    ("paliwa-gazowe", "Gas"),
    ("cieplo", "Heat"),
    ("paliwa-ciekle", "Liquid fuels"),
    ("inne-decyzje-informacj", "Other decisions"),
]

# Polish month names to numbers
PL_MONTHS = {
    'stycznia': 1, 'lutego': 2, 'marca': 3, 'kwietnia': 4,
    'maja': 5, 'czerwca': 6, 'lipca': 7, 'sierpnia': 8,
    'września': 9, 'października': 10, 'listopada': 11, 'grudnia': 12,
    'styczeń': 1, 'luty': 2, 'marzec': 3, 'kwiecień': 4,
    'maj': 5, 'czerwiec': 6, 'lipiec': 7, 'sierpień': 8,
    'wrzesień': 9, 'październik': 10, 'listopad': 11, 'grudzień': 12,
}


class UREFetcher:
    """Fetcher for URE energy regulatory decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pl-PL,pl;q=0.9,en;q=0.5',
        })

    def get_yearly_pages(self, sector_slug: str) -> List[Tuple[str, str]]:
        """Get all yearly archive page URLs for a sector.

        Returns list of (url_path, page_title) tuples.
        """
        results = []
        for page in range(10):
            url = f"{BASE_URL}/bip/taryfy-i-inne-decyzje-b/{sector_slug}?page={page}"
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch sector index page {page} for {sector_slug}: {e}")
                break

            pattern = rf'href="(/bip/taryfy-i-inne-decyzje-b/{re.escape(sector_slug)}/\d+,[^"]+\.html)"[^>]*>([^<]+)<'
            matches = re.findall(pattern, resp.text)
            if not matches:
                break
            for href, title in matches:
                results.append((href, unescape(title.strip())))
            time.sleep(0.5)

        return results

    def parse_yearly_page(self, url_path: str, sector_slug: str, sector_name: str) -> List[Dict[str, Any]]:
        """Parse a yearly archive page to extract decision entries."""
        url = f"{BASE_URL}{url_path}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch yearly page {url_path}: {e}")
            return []

        entries = []
        # Parse table rows
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', resp.text, re.DOTALL)

        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) < 3:
                continue

            # Cell 0: PDF link with file size
            pdf_match = re.search(
                r'href="(https?://bip\.ure\.gov\.pl/download/3/(\d+)/([^"]+\.pdf))"',
                cells[0], re.IGNORECASE
            )
            if not pdf_match:
                continue

            pdf_url = pdf_match.group(1)
            pdf_id = pdf_match.group(2)
            pdf_filename = pdf_match.group(3)

            # Cell 1: Decision number and date
            cell1_text = re.sub(r'<[^>]+>', ' ', cells[1]).strip()
            cell1_text = re.sub(r'\s+', ' ', cell1_text)

            # Extract decision number (e.g., "63/2026")
            dec_num_match = re.search(r'(\d+/\d{4})', cell1_text)
            decision_number = dec_num_match.group(1) if dec_num_match else ''

            # Extract date (e.g., "20 marca 2026")
            date_iso = self._parse_polish_date(cell1_text)

            # Cell 2: Description
            cell2_text = re.sub(r'<[^>]+>', ' ', cells[2]).strip()
            cell2_text = unescape(re.sub(r'\s+', ' ', cell2_text))

            entries.append({
                'pdf_url': pdf_url,
                'pdf_id': pdf_id,
                'pdf_filename': pdf_filename,
                'decision_number': decision_number,
                'date': date_iso,
                'description': cell2_text,
                'sector_slug': sector_slug,
                'sector_name': sector_name,
                'source_page': url,
            })

        return entries

    def _parse_polish_date(self, text: str) -> Optional[str]:
        """Parse Polish date like '20 marca 2026' to ISO format."""
        for month_name, month_num in PL_MONTHS.items():
            pattern = rf'(\d{{1,2}})\s+{re.escape(month_name)}\s+(\d{{4}})'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                day = int(match.group(1))
                year = int(match.group(2))
                try:
                    return datetime(year, month_num, day).strftime('%Y-%m-%d')
                except ValueError:
                    continue
        return None

    def extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="PL/URE",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def normalize(self, entry: Dict[str, Any], full_text: str) -> Dict[str, Any]:
        """Normalize a decision record to standard schema."""
        dec_num = entry.get('decision_number', '')
        desc = entry.get('description', '')

        # Build title from description or decision number
        if desc:
            title = desc[:200]
        elif dec_num:
            title = f"Decyzja Prezesa URE nr {dec_num}"
        else:
            title = f"URE Decision {entry['pdf_id']}"

        doc_id = f"PL_URE_{entry['pdf_id']}"

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': full_text,
            'date': entry.get('date'),
            'url': entry.get('source_page', ''),
            'decision_number': dec_num,
            'sector': entry.get('sector_name', ''),
            'pdf_url': entry.get('pdf_url', ''),
        }

    def fetch_all(self, sample: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all URE decisions with full text."""
        all_entries = []

        for sector_slug, sector_name in SECTORS:
            logger.info(f"Collecting yearly pages for {sector_name}...")
            yearly_pages = self.get_yearly_pages(sector_slug)
            logger.info(f"  Found {len(yearly_pages)} yearly pages")

            for url_path, page_title in yearly_pages:
                entries = self.parse_yearly_page(url_path, sector_slug, sector_name)
                logger.info(f"  {page_title}: {len(entries)} decisions")
                all_entries.extend(entries)
                time.sleep(0.5)

                if sample and len(all_entries) >= 20:
                    break
            if sample and len(all_entries) >= 20:
                break

        logger.info(f"Total entries collected: {len(all_entries)}")

        if sample:
            all_entries = all_entries[:15]
            logger.info(f"Sample mode: processing {len(all_entries)} decisions")

        for i, entry in enumerate(all_entries):
            logger.info(f"[{i+1}/{len(all_entries)}] Fetching PDF {entry['pdf_filename']}...")

            full_text = self.extract_pdf_text(entry['pdf_url'])
            if not full_text or len(full_text) < 50:
                logger.warning(f"  Empty/short PDF text, skipping")
                continue

            logger.info(f"  Extracted {len(full_text)} chars")
            time.sleep(1.0)

            record = self.normalize(entry, full_text)
            yield record

    def test_api(self) -> bool:
        """Test connectivity to URE BIP."""
        logger.info("Testing URE BIP connectivity...")

        # Test sector index
        try:
            url = f"{BASE_URL}/bip/taryfy-i-inne-decyzje-b/energia-elektryczna"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            logger.info(f"  Sector index: OK ({len(resp.text)} chars)")
        except Exception as e:
            logger.error(f"  Sector index: FAILED - {e}")
            return False

        # Test yearly page
        try:
            yearly = self.get_yearly_pages('energia-elektryczna')
            if not yearly:
                logger.error("  No yearly pages found")
                return False
            logger.info(f"  Yearly pages: OK ({len(yearly)} found)")

            entries = self.parse_yearly_page(yearly[0][0], 'energia-elektryczna', 'Electricity')
            if not entries:
                logger.error("  No entries found on yearly page")
                return False
            logger.info(f"  Entry parsing: OK ({len(entries)} entries on first page)")
        except Exception as e:
            logger.error(f"  Yearly page parsing: FAILED - {e}")
            return False

        # Test PDF download
        try:
            entry = entries[0]
            text = self.extract_pdf_text(entry['pdf_url'])
            if text and len(text) > 100:
                logger.info(f"  PDF extraction: OK ({len(text)} chars)")
            else:
                logger.error("  PDF extraction: Too short or empty")
                return False
        except Exception as e:
            logger.error(f"  PDF extraction: FAILED - {e}")
            return False

        logger.info("All API tests passed!")
        return True


def bootstrap_sample(fetcher: UREFetcher):
    """Fetch sample records and save to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetcher.fetch_all(sample=True):
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        text_len = len(record.get('text', ''))
        logger.info(f"  Saved {record['_id']} ({text_len} chars)")
        count += 1
    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def bootstrap_full(fetcher: UREFetcher):
    """Fetch all records and save to data/ directory."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetcher.fetch_all(sample=False):
        fname = DATA_DIR / f"{record['_id']}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        if count % 100 == 0:
            logger.info(f"  Progress: {count} records saved")
    logger.info(f"Full bootstrap complete: {count} records saved to {DATA_DIR}")
    return count


def main():
    parser = argparse.ArgumentParser(description='PL/URE Energy Regulatory Decisions Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test-api'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Only fetch sample records (15 decisions)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = UREFetcher()

    if args.command == 'test-api':
        success = fetcher.test_api()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        if args.sample:
            count = bootstrap_sample(fetcher)
        else:
            count = bootstrap_full(fetcher)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)

    elif args.command == 'update':
        logger.info("Update not supported; use full bootstrap")
        sys.exit(1)


if __name__ == '__main__':
    main()
