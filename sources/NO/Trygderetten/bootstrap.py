#!/usr/bin/env python3
"""
NO/Trygderetten - Norwegian National Insurance Court Fetcher

Fetches Trygderetten (social security tribunal) decisions from Lovdata.
~66,631 anonymized rulings from 1971-present. Free public access.

Data source: https://lovdata.no/register/trygderetten
Registry pagination: ?year=YYYY&offset=N (20 items per page)
License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

BASE_URL = "https://lovdata.no"
REGISTRY_URL = "https://lovdata.no/register/trygderetten"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NO/Trygderetten"

# Trygderetten decisions available from 1971
START_YEAR = 1971


class TrygderettenScraper(BaseScraper):
    """Scraper for NO/Trygderetten -- Norwegian social security tribunal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {'completed_years': [], 'last_year': None, 'last_offset': 0, 'fetched_ids': []}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _fetch_registry_page(self, year: int, offset: int = 0) -> tuple:
        """Fetch a page of decisions from the registry."""
        url = f"{REGISTRY_URL}?year={year}&offset={offset}"
        print(f"  Fetching registry: year={year}, offset={offset}")

        time.sleep(1.5)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract total count
        total = 0
        count_text = soup.find(string=re.compile(r'Viser \d+ - \d+ av [\d\s,.]+ treff'))
        if count_text:
            match = re.search(r'av ([\d\s,.]+) treff', count_text)
            if match:
                total = int(re.sub(r'[\s,.]', '', match.group(1)))

        # Extract decision URLs
        items = []
        for article in soup.find_all('article'):
            link = article.find('a', href=re.compile(r'/dokument/.*/avgjorelse/'))
            if link and link.get('href'):
                doc_url = urljoin(BASE_URL, link['href'])
                case_id_match = re.search(r'(TRR-\d+-\d+)', link.get_text())
                case_id = case_id_match.group(1) if case_id_match else ''

                items.append({
                    'case_id': case_id,
                    'url': doc_url,
                })

        return items, total

    def _extract_text(self, html: str) -> str:
        """Extract clean text from HTML document body."""
        soup = BeautifulSoup(html, 'html.parser')

        doc_body = soup.find(id='documentBody')
        if not doc_body:
            doc_body = soup.find('div', class_='documentContent')
            if not doc_body:
                doc_body = soup.find(id='lovdataDocument')

        if not doc_body:
            return ''

        for element in doc_body(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()

        text = doc_body.get_text(separator='\n', strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    def _extract_metadata(self, html: str) -> dict:
        """Extract metadata from the document page."""
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {}

        meta_table = soup.find('table', class_='meta')
        if meta_table:
            for row in meta_table.find_all('tr'):
                th = row.find('th', class_='metafieldLabel')
                td = row.find('td', class_='metavalue')
                if th and td:
                    field_name = th.get_text(strip=True).lower()
                    field_value = td.get_text(strip=True)

                    if 'instans' in field_name:
                        metadata['court'] = field_value
                    elif 'dato' in field_name:
                        metadata['date'] = field_value
                    elif 'publisert' in field_name:
                        metadata['published_id'] = field_value
                    elif 'stikkord' in field_name:
                        metadata['keywords'] = field_value
                    elif 'sammendrag' in field_name:
                        metadata['summary'] = field_value
                    elif 'saksgang' in field_name:
                        metadata['case_history'] = field_value

        title_h1 = soup.find('h1')
        if title_h1:
            metadata['title'] = title_h1.get_text(strip=True)

        return metadata

    def _fetch_decision(self, url: str) -> Optional[dict]:
        """Fetch a single decision and extract content."""
        try:
            print(f"  Fetching: {url}")
            time.sleep(1.5)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            html = resp.text

            if 'Full tekst til avgjørelsen er ikke tilgjengelig' in html:
                print(f"    -> Full text not available")
                return None

            text = self._extract_text(html)
            metadata = self._extract_metadata(html)

            if not text or len(text) < 100:
                print(f"    -> Text too short ({len(text)} chars)")
                return None

            case_id_match = re.search(r'/avgjorelse/(trr-\d+-\d+)', url, re.I)
            case_id = case_id_match.group(1).upper() if case_id_match else ''

            return {
                'case_id': case_id,
                'url': url,
                'text': text,
                'metadata': metadata,
            }

        except requests.RequestException as e:
            print(f"    -> Error: {e}")
            return None

    def normalize(self, raw: dict) -> dict:
        case_id = raw.get('case_id', '')
        metadata = raw.get('metadata', {})

        date = metadata.get('date', '')
        title = metadata.get('title', case_id)
        if metadata.get('keywords'):
            title = f"{case_id} - {metadata['keywords'][:100]}"

        return {
            '_id': case_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': raw.get('text', ''),
            'date': date,
            'url': raw.get('url', ''),
            'court': metadata.get('court', 'Trygderetten'),
            'case_id': case_id,
            'keywords': metadata.get('keywords'),
            'summary': metadata.get('summary'),
            'case_history': metadata.get('case_history'),
            'language': 'nob',
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all decisions using registry pagination."""
        checkpoint = self._load_checkpoint()
        completed_years = set(checkpoint.get('completed_years', []))
        fetched_ids = set(checkpoint.get('fetched_ids', []))

        current_year = datetime.now().year
        years = list(range(current_year, START_YEAR - 1, -1))

        print(f"Fetching Trygderetten decisions from {START_YEAR} to {current_year}")
        count = 0

        for year in years:
            if year in completed_years:
                continue

            offset = 0
            if checkpoint.get('last_year') == year:
                offset = checkpoint.get('last_offset', 0)

            items, total = self._fetch_registry_page(year, offset)
            if total == 0:
                continue

            print(f"  Year {year}: {total} total decisions")

            while offset < total:
                if offset > 0:
                    items, _ = self._fetch_registry_page(year, offset)

                for item in items:
                    cid = item.get('case_id', '')
                    if not cid or cid in fetched_ids:
                        continue

                    raw = self._fetch_decision(item['url'])
                    if raw and raw.get('text') and len(raw['text']) >= 100:
                        yield self.normalize(raw)
                        count += 1
                        fetched_ids.add(cid)
                        print(f"    -> {len(raw['text']):,} chars")

                        if count % 50 == 0:
                            checkpoint['fetched_ids'] = list(fetched_ids)
                            self._save_checkpoint(checkpoint)

                offset += 20
                checkpoint['last_year'] = year
                checkpoint['last_offset'] = offset
                self._save_checkpoint(checkpoint)

            if year not in checkpoint['completed_years']:
                checkpoint['completed_years'].append(year)
            checkpoint['last_year'] = None
            checkpoint['last_offset'] = 0
            self._save_checkpoint(checkpoint)

        print(f"\nTotal records: {count}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        items, total = self._fetch_registry_page(2025, 0)
        if not items:
            print("ERROR: No items found on registry page")
            return False
        print(f"Registry OK: {total} decisions for 2025, {len(items)} on first page")

        raw = self._fetch_decision(items[0]['url'])
        if not raw or not raw.get('text') or len(raw['text']) < 100:
            print("ERROR: Could not extract full text")
            return False

        print(f"Document OK: {raw['case_id']} ({len(raw['text']):,} chars)")
        return True


def main():
    parser = argparse.ArgumentParser(description="NO/Trygderetten decision fetcher")
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    args = parser.parse_args()

    scraper = TrygderettenScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        if args.sample:
            count = 0
            # Sample from a few different years
            for year in [2025, 2020, 2015, 2010]:
                if count >= 15:
                    break
                items, total = scraper._fetch_registry_page(year, 0)
                for item in items[:4]:
                    if count >= 15:
                        break
                    raw = scraper._fetch_decision(item['url'])
                    if raw and raw.get('text') and len(raw['text']) >= 100:
                        record = scraper.normalize(raw)
                        out_path = SAMPLE_DIR / f"{count:04d}.json"
                        with open(out_path, 'w', encoding='utf-8') as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)
                        print(f"  [{count+1}] {record['case_id']}: {len(record['text']):,} chars")
                        count += 1

            print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")
        else:
            count = 0
            for record in scraper.fetch_all():
                out_path = SAMPLE_DIR / f"{count:04d}.json"
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    print(f"Saved {count} records...")
            print(f"Bootstrap complete: {count} records saved to {SAMPLE_DIR}")

    elif args.command == 'update':
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        print(f"Update complete: {count} records")


if __name__ == '__main__':
    main()
