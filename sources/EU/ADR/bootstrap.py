#!/usr/bin/env python3
"""
EU Domain Dispute Resolution (.eu) Fetcher

Panel decisions from the Czech Arbitration Court (CAC) for .eu domain disputes.

Source: https://eu.adr.eu
Discovery: Paginated HTML listing at /decisions/list (155 pages, 10 per page)
Content: Full panel decision text from /decisions/detail?id=<ID>

Data is publicly published by the Czech Arbitration Court.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://eu.adr.eu"
LISTING_URL = f"{BASE_URL}/decisions/list"
DETAIL_URL = f"{BASE_URL}/decisions/detail"
TOTAL_PAGES = 16  # ~1550 decisions at 100 per page


class EUADRFetcher:
    """Fetcher for EU .eu domain dispute decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,de;q=0.8',
        })

    def _get_listing_page(self, page: int) -> List[str]:
        """Fetch decision IDs from a listing page"""
        params = {
            'grid-page': str(page),
            'grid-perPage': '100',
            'grid-sort[published]': 'DESC',
            'do': 'grid-page',
        }
        url = LISTING_URL

        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch listing page {page}: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')

        decision_ids = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            m = re.search(r'decisions/detail\?id=([a-f0-9]+)', href)
            if m:
                did = m.group(1)
                if did not in decision_ids:
                    decision_ids.append(did)

        return decision_ids

    def _extract_decision(self, decision_id: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract full text from a decision detail page"""
        url = f"{DETAIL_URL}?id={decision_id}"

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract title (contains case number)
        h1 = soup.find('h1')
        title = h1.get_text(strip=True) if h1 else None
        if not title:
            return None

        # Extract case number from title
        case_number = None
        m = re.search(r'CAC-ADREU-\d+', title)
        if m:
            case_number = m.group(0)

        # Extract metadata from tables
        domain_name = None
        filing_date = None
        tables = soup.find_all('table')
        for table in tables:
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    if 'domain' in label:
                        domain_name = value
                    elif 'filing' in label or 'time' in label:
                        filing_date = value

        # Parse date
        date_iso = None
        if filing_date:
            m = re.match(r'(\d{4}-\d{2}-\d{2})', filing_date)
            if m:
                date_iso = m.group(1)

        # Extract full decision text
        text_parts = []

        # The decision content is in div.print-form
        content_elem = soup.find('div', class_='print-form')
        if not content_elem:
            content_elem = soup.find('div', class_='container')
        if not content_elem:
            content_elem = soup.find('main') or soup.find('body')

        if content_elem:
            # Remove nav, scripts, etc.
            for unwanted in content_elem.find_all(['script', 'style', 'nav', 'iframe', 'noscript']):
                unwanted.decompose()
            for unwanted in content_elem.select('.navbar, .footer, .breadcrumb'):
                unwanted.decompose()

            # Extract full text from the content element
            full_text_raw = content_elem.get_text(separator='\n')
            for line in full_text_raw.split('\n'):
                line = line.strip()
                if line and len(line) > 3:
                    text_parts.append(line)

        full_text = '\n\n'.join(text_parts)
        full_text = re.sub(r'<[^>]+>', '', full_text)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text).strip()

        if len(full_text) < 200:
            logger.warning(f"Insufficient text for {url} ({len(full_text)} chars)")
            return None

        return {
            'title': title,
            'text': full_text,
            'date': date_iso,
            'url': url,
            'case_number': case_number,
            'domain_name': domain_name,
            'decision_id': decision_id,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        case_num = raw.get('case_number', raw.get('decision_id', 'unknown'))
        doc_id = case_num.replace('-', '_')

        return {
            '_id': f"EU_ADR_{doc_id}",
            '_source': 'EU/ADR',
            '_type': 'case_law',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'case_number': raw.get('case_number'),
            'domain_name': raw.get('domain_name'),
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        for page in range(1, TOTAL_PAGES + 1):
            logger.info(f"Listing page {page}/{TOTAL_PAGES}")
            ids = self._get_listing_page(page)

            for did in ids:
                logger.info(f"  Fetching decision: {did}")
                decision = self._extract_decision(did)
                if decision:
                    yield self.normalize(decision)
                time.sleep(1.5)

            time.sleep(1.0)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        for page in range(1, TOTAL_PAGES + 1):
            ids = self._get_listing_page(page)
            found_old = False

            for did in ids:
                decision = self._extract_decision(did)
                if decision:
                    if decision.get('date') and decision['date'] < since:
                        found_old = True
                        break
                    yield self.normalize(decision)
                time.sleep(1.5)

            if found_old:
                break
            time.sleep(1.0)

    def bootstrap_sample(self, count: int = 15) -> List[Dict[str, Any]]:
        results = []
        # Sample from different pages for diversity
        pages_to_check = [1, 5, 10, 15]
        per_page = max(2, count // len(pages_to_check))

        for page in pages_to_check:
            if len(results) >= count:
                break

            logger.info(f"Listing page {page}")
            ids = self._get_listing_page(page)

            for did in ids[:per_page]:
                if len(results) >= count:
                    break

                logger.info(f"  Fetching decision: {did}")
                decision = self._extract_decision(did)
                if decision:
                    results.append(self.normalize(decision))
                time.sleep(1.5)

            time.sleep(1.0)

        return results


def main():
    if len(sys.argv) < 2:
        print("Usage: bootstrap.py <command> [options]")
        sys.exit(1)

    if sys.argv[1] == 'bootstrap':
        fetcher = EUADRFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        if '--sample' in sys.argv:
            count = 15
            logger.info(f"Fetching {count} sample records...")
            records = fetcher.bootstrap_sample(count)

            for record in records:
                filename = re.sub(r'[^\w\-]', '_', record['_id'])[:100] + '.json'
                filepath = sample_dir / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved {len(records)} sample records to {sample_dir}")

            texts = [r.get('text', '') for r in records]
            non_empty = sum(1 for t in texts if len(t) > 200)
            avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
            dates = sum(1 for r in records if r.get('date'))
            domains = sum(1 for r in records if r.get('domain_name'))
            print(f"\n=== Validation Summary ===")
            print(f"Records fetched: {len(records)}")
            print(f"Records with substantial text (>200 chars): {non_empty}")
            print(f"Records with dates: {dates}")
            print(f"Records with domain names: {domains}")
            print(f"Average text length: {avg_len:.0f} chars")

            if non_empty < len(records) * 0.8:
                print("WARNING: Less than 80% of records have substantial text!")
            else:
                print("OK: Text extraction looks good.")

        elif '--full' in sys.argv:
            count = 0
            for record in fetcher.fetch_all():
                filename = re.sub(r'[^\w\-]', '_', record['_id'])[:100] + '.json'
                filepath = sample_dir / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
            logger.info(f"Saved {count} records to {sample_dir}")


if __name__ == '__main__':
    main()
