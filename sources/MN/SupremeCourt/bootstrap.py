#!/usr/bin/env python3
"""
MN/SupremeCourt -- Mongolia Supreme Court Decisions Fetcher

Fetches court decisions from shuukh.mn (Mongolian Court Decisions Electronic Database).
Covers civil (type=1), criminal (type=2), and administrative (type=3) cases at the
supervisory/cassation (Supreme Court) level (court_cat=3).

~22,900+ decisions with full text in Mongolian.

Strategy:
  - Sequential ID enumeration per case type: /single_case/{id}?id={type}&court_cat=3
  - Three separate ID spaces: civil, criminal, administrative
  - Full text is in <div class="undsen"> parsed from HTML
  - Metadata from structured page elements

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
logger = logging.getLogger("legal-data-hunter.MN.SupremeCourt")

BASE_URL = "https://shuukh.mn"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "MN/SupremeCourt"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "mn-MN,mn;q=0.9,en;q=0.5",
}

# Case type codes: 1=civil, 2=criminal, 3=administrative
# Court level 3 = supervisory/cassation (Supreme Court)
CASE_TYPES = {
    1: "civil",
    2: "criminal",
    3: "administrative",
}

# ID ranges per case type at Supreme Court level (court_cat=3)
# Civil: ~13,663 cases, Criminal: ~4,938, Administrative: ~4,301
TYPE_MAX_IDS = {
    1: 16000,
    2: 6000,
    3: 5000,
}

REQUEST_DELAY = 1.5  # seconds between requests

# Mongolian month names for date parsing
MN_DATE_PATTERN = re.compile(r'(\d{4})\s*оны\s*(\d{1,2})\s*сарын\s*(\d{1,2})\s*өдөр')

# Sample IDs spread across types and ranges
SAMPLE_IDS = [
    (1, 3, 2000), (1, 3, 5000), (1, 3, 8000), (1, 3, 10000), (1, 3, 13000),
    (2, 3, 1000), (2, 3, 2000), (2, 3, 3000), (2, 3, 5000),
    (3, 3, 500), (3, 3, 1000), (3, 3, 2000), (3, 3, 3000), (3, 3, 4000),
    (1, 3, 15000),
]


class SupremeCourtScraper(BaseScraper):
    """Scraper for MN/SupremeCourt -- Mongolia Supreme Court decisions via shuukh.mn."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _parse_date(self, date_str: str) -> str:
        """Parse Mongolian date format to ISO 8601."""
        match = MN_DATE_PATTERN.search(date_str)
        if match:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1990 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
        return ""

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract full decision text from the undsen div."""
        undsen = soup.select_one('div.undsen')
        if not undsen:
            return ""
        # Remove style and script tags
        for tag in undsen.find_all(['style', 'script']):
            tag.decompose()
        # Remove MS Office XML comments
        text = undsen.get_text(separator='\n', strip=True)
        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _fetch_decision(self, case_type: int, court_cat: int, case_id: int) -> Optional[dict]:
        """Fetch a single decision by type, court level, and ID."""
        url = f"{BASE_URL}/single_case/{case_id}?start_date=&end_date=&id={case_type}&court_cat={court_cat}&bb="
        time.sleep(REQUEST_DELAY)

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch type={case_type} cat={court_cat} id={case_id}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract full text
        text = self._extract_text(soup)
        if len(text) < 50:
            return None  # Empty or placeholder decision

        # Extract court name from decisionfullimg
        court_name = ""
        court_el = soup.select_one('div.decisionfullimg p')
        if court_el:
            court_name = court_el.get_text(strip=True)

        # Extract date
        date_str = ""
        date_el = soup.select_one('div.decisionfulltitle .col-md-4.fleft p')
        if date_el:
            date_str = date_el.get_text(strip=True)

        # Extract case number
        case_number = ""
        num_el = soup.select_one('div.decisionfulltitle .col-md-4.fcenter p')
        if num_el:
            case_number = num_el.get_text(strip=True)
            # Remove "Дугаар " prefix if present
            if case_number.startswith("Дугаар"):
                case_number = case_number[len("Дугаар"):].strip()

        iso_date = self._parse_date(date_str)

        # Skip 1970 dates (placeholder/empty entries)
        if iso_date and iso_date.startswith("1970"):
            return None

        return {
            'case_id': str(case_id),
            'case_type_code': case_type,
            'case_type': CASE_TYPES.get(case_type, 'unknown'),
            'court_cat': court_cat,
            'court_name': court_name,
            'case_number': case_number,
            'date_raw': date_str,
            'date': iso_date,
            'text': text,
            'url': f"{BASE_URL}/single_case/{case_id}?id={case_type}&court_cat={court_cat}",
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision into standard schema."""
        title = raw.get('court_name', '')
        if raw.get('case_number'):
            title = f"{title} - {raw['case_number']}" if title else raw['case_number']

        return {
            '_id': f"MN-SC-t{raw['case_type_code']}-c{raw['court_cat']}-{raw['case_id']}",
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': raw['text'],
            'date': raw.get('date', ''),
            'case_number': raw.get('case_number', ''),
            'case_type': raw.get('case_type', ''),
            'case_type_code': raw.get('case_type_code'),
            'court_name': raw.get('court_name', ''),
            'court_level': 'supervisory',
            'url': raw['url'],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all Supreme Court decisions by iterating through IDs per type."""
        if sample:
            yield from self._fetch_sample()
            return

        checkpoint = self._load_checkpoint()
        total = 0

        for case_type, type_name in CASE_TYPES.items():
            max_id = TYPE_MAX_IDS[case_type]
            ck_key = f"type_{case_type}_last_id"
            start_id = checkpoint.get(ck_key, 0) + 1
            consecutive_misses = 0

            logger.info(f"Fetching {type_name} cases (type={case_type}), IDs {start_id}-{max_id}")

            for case_id in range(start_id, max_id + 1):
                raw = self._fetch_decision(case_type, 3, case_id)
                if raw:
                    record = self.normalize(raw)
                    yield record
                    total += 1
                    consecutive_misses = 0
                    logger.info(f"[{total}] {type_name} ID {case_id}: {raw['case_number']} ({len(raw['text'])} chars)")
                else:
                    consecutive_misses += 1

                # Save checkpoint every 20 IDs
                if case_id % 20 == 0:
                    checkpoint[ck_key] = case_id
                    self._save_checkpoint(checkpoint)

                if consecutive_misses > 100:
                    logger.info(f"100 consecutive misses at {type_name} ID {case_id}, moving to next type")
                    break

            checkpoint[ck_key] = max_id
            self._save_checkpoint(checkpoint)
            logger.info(f"Completed {type_name} cases, total so far: {total}")

        logger.info(f"\nTotal decisions fetched: {total}")

    def _fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a diverse sample of decisions across case types."""
        count = 0
        for case_type, court_cat, case_id in SAMPLE_IDS:
            if count >= 15:
                break
            type_name = CASE_TYPES.get(case_type, 'unknown')
            logger.info(f"Fetching sample: {type_name} ID {case_id}...")
            raw = self._fetch_decision(case_type, court_cat, case_id)
            if raw:
                record = self.normalize(raw)
                yield record
                count += 1
                logger.info(f"  OK: {raw['case_number']} ({len(raw['text'])} chars)")
            else:
                logger.warning(f"  Skip: {type_name} ID {case_id} (empty)")
        logger.info(f"Sample complete: {count} records")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Fetch recent decisions (new IDs beyond checkpoint)."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    scraper = SupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to shuukh.mn...")
        raw = scraper._fetch_decision(1, 3, 5000)
        if raw:
            logger.info(f"SUCCESS: {raw['case_number']} - {len(raw['text'])} chars")
        else:
            logger.error("FAILED: Could not fetch test decision")
            sys.exit(1)

    elif command == "bootstrap":
        SAMPLE_DIR.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            if sample:
                fname = SAMPLE_DIR / f"{record['_id']}.json"
                with open(fname, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Bootstrap complete: {count} records")

    elif command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
