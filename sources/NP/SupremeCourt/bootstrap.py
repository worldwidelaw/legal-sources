#!/usr/bin/env python3
"""
NP/SupremeCourt -- Supreme Court of Nepal (via NKP) Fetcher

Fetches landmark Supreme Court decisions from Nepal Kanoon Patrika (NKP).
~10,400+ decisions with full text in HTML. Nepali language.

Strategy:
  - Sequential ID enumeration: https://nkp.gov.np/full_detail/{id}
  - IDs range from 2 to ~10,475 (302 redirect = invalid)
  - Full text is inline HTML, parsed with BeautifulSoup
  - Metadata extracted from structured page elements

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NP.SupremeCourt")

BASE_URL = "https://nkp.gov.np"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NP/SupremeCourt"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Known valid ID range
MIN_ID = 2
MAX_ID = 10500
REQUEST_DELAY = 2  # seconds between requests

# Sample IDs spread across the range for diverse sampling
SAMPLE_IDS = [10, 50, 100, 500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 10470]


class SupremeCourtScraper(BaseScraper):
    """Scraper for NP/SupremeCourt -- Nepal Supreme Court decisions via NKP."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {"last_id": MIN_ID - 1, "fetched_ids": []}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _fetch_decision(self, nkp_id: int) -> Optional[dict]:
        """Fetch a single decision by NKP ID."""
        url = f"{BASE_URL}/full_detail/{nkp_id}"
        time.sleep(REQUEST_DELAY)

        try:
            resp = self.session.get(url, timeout=30, allow_redirects=False)
            if resp.status_code == 302:
                return None  # Invalid ID
            resp.raise_for_status()
            resp.encoding = 'utf-8'
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')
        main_div = soup.select_one('.col-md-8.para-sections')
        if not main_div:
            return None

        # Extract title
        title_el = main_div.select_one('h1.post-title')
        title = title_el.get_text(strip=True) if title_el else ''
        if not title:
            return None

        # Extract full text
        full_text = main_div.get_text(separator='\n', strip=True)
        if len(full_text) < 100:
            logger.warning(f"  Very short text for ID {nkp_id}: {len(full_text)} chars")
            return None

        # Extract metadata from text
        decision_number = ''
        match = re.search(r'निर्णय नं\.\s*([\d]+)', title)
        if match:
            decision_number = match.group(1)

        # Extract decision date
        date = ''
        date_match = re.search(r'फैसला मिति\s*[:\s]*([\d/।\.]+)', full_text)
        if date_match:
            date = date_match.group(1).strip()

        # Extract case number (e.g., 079-WO-0927)
        case_number = ''
        case_match = re.search(r'(\d{2,3}-[A-Z]{1,3}-\d{2,5})', full_text)
        if case_match:
            case_number = case_match.group(1)

        # Extract case type
        case_type = ''
        type_match = re.search(r'मुद्दाः\s*(.+?)(?:\n|$)', full_text)
        if type_match:
            case_type = type_match.group(1).strip()

        # Extract judges
        judges = []
        for judge_match in re.finditer(r'माननीय न्यायाधीश[^\n]*?([^\n]+)', full_text):
            judge_name = judge_match.group(1).strip()
            if judge_name and len(judge_name) < 100:
                judges.append(judge_name)

        # Extract bench type
        bench = ''
        bench_match = re.search(r'सर्वोच्च अदालत,([^\n]+)', full_text)
        if bench_match:
            bench = bench_match.group(1).strip()

        return {
            'nkp_id': str(nkp_id),
            'title': title,
            'text': full_text,
            'decision_number': decision_number,
            'date': date,
            'case_number': case_number,
            'case_type': case_type,
            'judges': judges,
            'bench': bench,
            'url': url,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision into standard schema."""
        return {
            '_id': f"NP-SC-{raw['nkp_id']}",
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw['title'],
            'text': raw['text'],
            'decision_number': raw.get('decision_number', ''),
            'date': raw.get('date', ''),
            'case_number': raw.get('case_number', ''),
            'case_type': raw.get('case_type', ''),
            'judges': raw.get('judges', []),
            'bench': raw.get('bench', ''),
            'url': raw['url'],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all decisions by iterating through IDs."""
        if sample:
            yield from self._fetch_sample()
            return

        checkpoint = self._load_checkpoint()
        start_id = checkpoint.get('last_id', MIN_ID - 1) + 1
        fetched_ids = set(checkpoint.get('fetched_ids', []))
        total = 0
        consecutive_misses = 0

        for nkp_id in range(start_id, MAX_ID + 1):
            if str(nkp_id) in fetched_ids:
                continue

            raw = self._fetch_decision(nkp_id)
            if raw:
                record = self.normalize(raw)
                yield record
                total += 1
                consecutive_misses = 0
                fetched_ids.add(str(nkp_id))
                logger.info(f"[{total}] ID {nkp_id}: {raw['title'][:60]} ({len(raw['text'])} chars)")
            else:
                consecutive_misses += 1

            # Save checkpoint every 10 IDs
            if nkp_id % 10 == 0:
                checkpoint['last_id'] = nkp_id
                checkpoint['fetched_ids'] = list(fetched_ids)[-1000:]  # Keep last 1000
                self._save_checkpoint(checkpoint)

            # Stop if too many consecutive misses (past end of valid range)
            if consecutive_misses > 50:
                logger.info(f"50 consecutive misses at ID {nkp_id}, stopping")
                break

        checkpoint['last_id'] = nkp_id
        checkpoint['fetched_ids'] = list(fetched_ids)[-1000:]
        self._save_checkpoint(checkpoint)
        logger.info(f"\nTotal decisions fetched: {total}")

    def _fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a diverse sample of 15 decisions."""
        count = 0
        for nkp_id in SAMPLE_IDS:
            if count >= 15:
                break
            logger.info(f"Fetching sample ID {nkp_id}...")
            raw = self._fetch_decision(nkp_id)
            if raw:
                record = self.normalize(raw)
                yield record
                count += 1
                logger.info(f"  [{count}] {raw['title'][:60]} ({len(raw['text'])} chars)")
            else:
                logger.warning(f"  ID {nkp_id} not valid, skipping")
        logger.info(f"\nSample complete: {count} decisions fetched")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recent decisions (high IDs)."""
        checkpoint = self._load_checkpoint()
        start_id = max(checkpoint.get('last_id', MAX_ID - 100), MAX_ID - 100)

        for nkp_id in range(start_id, MAX_ID + 100):
            raw = self._fetch_decision(nkp_id)
            if raw:
                yield self.normalize(raw)

    def test(self):
        """Quick connectivity test."""
        logger.info("Testing connectivity to nkp.gov.np...")
        try:
            raw = self._fetch_decision(10470)
            if raw and len(raw['text']) > 100:
                logger.info(f"OK: '{raw['title'][:60]}' ({len(raw['text'])} chars)")
                logger.info(f"Decision #{raw['decision_number']}, date: {raw['date']}")
                logger.info("Test PASSED")
                return True
            else:
                logger.error("Failed to fetch test decision")
                return False
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description='NP/SupremeCourt fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'])
    parser.add_argument('--sample', action='store_true', help='Fetch 15 sample records')
    args = parser.parse_args()

    scraper = SupremeCourtScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            out_file = SAMPLE_DIR / f"{record['_id']}.json"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")

    elif args.command == 'update':
        count = 0
        for record in scraper.fetch_updates():
            out_file = SAMPLE_DIR / f"{record['_id']}.json"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} new records")


if __name__ == '__main__':
    main()
