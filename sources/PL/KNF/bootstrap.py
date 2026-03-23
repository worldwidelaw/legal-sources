#!/usr/bin/env python3
"""
PL/KNF - Polish Financial Supervision Authority (KNF) Doctrine Fetcher

Fetches official journal documents from the KNF's Dziennik Urzedowy:
  - Komunikaty (communications)
  - Uchwaly (resolutions)
  - Ogloszenia (announcements)
  - Decyzje (decisions)
  - Obwieszczenia (notices)
  - Zarzadzenia (orders)

Data source: https://dziennikurzedowy.knf.gov.pl
API: REST JSON (SearchLegalAct) + PDF downloads (GetActPdf.ashx)
License: Public domain (official government journal)
"""

import argparse
import hashlib
import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pdfplumber
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://dziennikurzedowy.knf.gov.pl"
SOURCE_ID = "PL/KNF"
SAMPLE_DIR = Path(__file__).parent / "sample"


class KNFFetcher:
    """Fetcher for KNF Official Journal documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pl-PL,pl;q=0.9,en;q=0.5',
        })

    def fetch_all_acts(self) -> List[Dict[str, Any]]:
        """Fetch the complete index of all legal acts via SearchLegalAct API."""
        url = f"{BASE_URL}/api/SearchLegalAct"
        logger.info(f"Fetching act index from {url}")
        try:
            resp = self.session.post(url, json={"query": ""}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            acts = data.get('LegalActs', []) if isinstance(data, dict) else data
            logger.info(f"Retrieved {len(acts)} acts from index (total: {data.get('TotalItems', '?')})")
            return acts
        except requests.RequestException as e:
            logger.error(f"Failed to fetch act index: {e}")
            return []

    def fetch_pdf_text(self, year: int, position: int) -> Optional[str]:
        """Download a PDF and extract its text content."""
        url = f"{BASE_URL}/GetActPdf.ashx?year={year}&book=0&position={position}"
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for PDF: year={year}, pos={position}")
                return None
            if 'application/pdf' not in resp.headers.get('Content-Type', ''):
                logger.warning(f"Not a PDF response for year={year}, pos={position}")
                return None
        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF year={year}, pos={position}: {e}")
            return None

        try:
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                full_text = "\n\n".join(pages_text)
                return full_text if len(full_text) > 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for year={year}, pos={position}: {e}")
            return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse API date string to ISO 8601."""
        if not date_str:
            return None
        try:
            # Handle "2024-01-10T10:21:09.843" format
            dt = datetime.fromisoformat(date_str.split('.')[0])
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    def normalize(self, act: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize an act into standard schema."""
        year = act.get('Year', 0)
        position = act.get('Position', 0)
        oid = act.get('Oid', 0)
        doc_id = f"{year}-{position}"

        act_date = self._parse_date(act.get('ActDate'))
        pub_date = self._parse_date(act.get('PublicationDate'))
        date = act_date or pub_date

        title = act.get('Title') or act.get('Subject') or ''
        act_type = act.get('LegalActType', '')
        pdf_url = f"{BASE_URL}/GetActPdf.ashx?year={year}&book=0&position={position}"

        return {
            '_id': f"PL-KNF-{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text,
            'date': date,
            'url': pdf_url,
            'act_type': act_type,
            'year': year,
            'position': position,
            'oid': oid,
            'publisher': act.get('PublishersListFlat', ''),
            'language': 'pol',
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all normalized documents."""
        acts = self.fetch_all_acts()
        for i, act in enumerate(acts):
            year = act.get('Year', 0)
            position = act.get('Position', 0)
            logger.info(f"Fetching PDF {i+1}/{len(acts)}: year={year}, pos={position}")

            text = self.fetch_pdf_text(year, position)
            if not text:
                logger.warning(f"  No text extracted, skipping")
                continue

            yield self.normalize(act, text)
            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents published since a given date."""
        since_dt = datetime.fromisoformat(since)
        acts = self.fetch_all_acts()
        for act in acts:
            pub_date = self._parse_date(act.get('PublicationDate'))
            if pub_date:
                try:
                    act_dt = datetime.fromisoformat(pub_date)
                    if act_dt < since_dt:
                        continue
                except (ValueError, TypeError):
                    pass

            year = act.get('Year', 0)
            position = act.get('Position', 0)
            text = self.fetch_pdf_text(year, position)
            if text:
                yield self.normalize(act, text)
            time.sleep(1.5)

    def bootstrap_sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of documents for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        acts = self.fetch_all_acts()
        if not acts:
            logger.error("No acts found in index")
            return []

        # Group by type and pick diverse samples
        by_type: Dict[str, list] = {}
        for act in acts:
            act_type = act.get('LegalActType', 'Unknown')
            by_type.setdefault(act_type, []).append(act)

        selected = []
        types = list(by_type.keys())
        per_type = max(1, n // len(types)) if types else n
        for act_type in types:
            for act in by_type[act_type][:per_type]:
                selected.append(act)
            if len(selected) >= n:
                break

        # Fill remaining from largest categories
        if len(selected) < n:
            for act_type in sorted(types, key=lambda t: len(by_type[t]), reverse=True):
                for act in by_type[act_type][per_type:]:
                    if len(selected) >= n:
                        break
                    selected.append(act)
                if len(selected) >= n:
                    break

        selected = selected[:n]
        results = []

        for i, act in enumerate(selected):
            year = act.get('Year', 0)
            position = act.get('Position', 0)
            logger.info(f"Sample {i+1}/{len(selected)}: {act.get('LegalActType')} year={year} pos={position}")

            text = self.fetch_pdf_text(year, position)
            if not text:
                logger.warning(f"  No text, skipping")
                continue

            normalized = self.normalize(act, text)
            results.append(normalized)

            fname = f"{normalized['_id']}.json"
            with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            logger.info(f"  Saved: {fname} ({len(text)} chars)")

            time.sleep(1.5)

        logger.info(f"Sample complete: {len(results)} documents saved to {SAMPLE_DIR}")
        return results


def main():
    parser = argparse.ArgumentParser(description='PL/KNF - KNF Official Journal Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (ISO format)')
    parser.add_argument('--limit', type=int, default=15,
                        help='Max documents for sample')
    args = parser.parse_args()

    fetcher = KNFFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            results = fetcher.bootstrap_sample(n=args.limit)
            print(f"\nSample: {len(results)} documents fetched")
            for r in results:
                text_len = len(r.get('text', ''))
                print(f"  {r['_id']} | {r['act_type']:15s} | {text_len:>6} chars | {r['title'][:60]}")
        else:
            count = 0
            for doc in fetcher.fetch_all():
                count += 1
                if count % 50 == 0:
                    logger.info(f"Fetched {count} documents")
            print(f"Total: {count} documents fetched")

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since required for updates command", file=sys.stderr)
            sys.exit(1)
        count = 0
        for doc in fetcher.fetch_updates(args.since):
            count += 1
        print(f"Updates: {count} documents fetched since {args.since}")

    elif args.command == 'fetch':
        count = 0
        for doc in fetcher.fetch_all():
            count += 1
        print(f"Total: {count} documents fetched")


if __name__ == '__main__':
    main()
