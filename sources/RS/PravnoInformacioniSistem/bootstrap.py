#!/usr/bin/env python3
"""
RS/PravnoInformacioniSistem — Serbia Consolidated Legislation Fetcher

Fetches consolidated Serbian legislation from the official Pravno-Informacioni
Sistem (Legal Information System) REST API. ~19,800 regulations across 358
legal topic areas.

Strategy:
  - Fetch the full menu tree via /api/menu
  - For each leaf area, fetch acts via /api/lawsAndSubareas/{areaId}
  - For each act, fetch full HTML text via /api/viewAct/{uuid}
  - Strip HTML to plain text

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SOURCE_ID = "RS/PravnoInformacioniSistem"
REG_BASE = "https://reg.pravno-informacioni-sistem.rs/api"
DI_BASE = "https://di.pravno-informacioni-sistem.rs"
SITE_URL = "https://www.pravno-informacioni-sistem.rs"
REQUEST_DELAY = 1.5
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; +https://github.com/ZachLaik/LegalDataHunter)"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


class SerbiaLegislationFetcher:
    """Fetcher for Serbian legislation from Pravno-Informacioni Sistem."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'application/json, text/html',
            'Accept-Language': 'sr,en;q=0.9',
        })

    def _get(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"GET failed {url}: {e}")
            return None

    def _post(self, url: str, payload: dict, timeout: int = 30) -> Optional[requests.Response]:
        try:
            resp = self.session.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"POST failed {url}: {e}")
            return None

    def get_menu_tree(self) -> List[dict]:
        """Fetch the full area hierarchy."""
        resp = self._get(f"{REG_BASE}/menu")
        if not resp:
            return []
        return resp.json()

    def collect_leaf_areas(self, menu: List[dict]) -> List[dict]:
        """Extract leaf areas (no children) from the menu tree."""
        leaves = []

        def walk(items, path):
            for item in items:
                current_path = path + [item['name']]
                children = item.get('children', [])
                if not children:
                    leaves.append({
                        'id': item['id'],
                        'name': item['name'],
                        'level': item['level'],
                        'path': ' > '.join(current_path),
                    })
                else:
                    walk(children, current_path)

        walk(menu, [])
        return leaves

    def get_acts_in_area(self, area_id: int) -> List[dict]:
        """Fetch all acts within a leaf area."""
        resp = self._get(f"{REG_BASE}/lawsAndSubareas/{area_id}")
        if not resp:
            return []
        data = resp.json()
        if isinstance(data, list):
            return data
        return []

    def get_full_text(self, uuid: str) -> Optional[str]:
        """Fetch full HTML text of an act and convert to plain text."""
        resp = self._get(f"{REG_BASE}/viewAct/{uuid}")
        if not resp:
            return None
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        text = soup.get_text(separator='\n', strip=True)
        # Clean up excessive blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip() if text.strip() else None

    def parse_title_info(self, raw_name: str) -> Tuple[str, Optional[str], Optional[str]]:
        """Parse the act name to extract title, gazette ref, and doc type hint.

        Format: "Title: gazette_ref1, gazette_ref2"
        """
        # Split on first colon to separate title from gazette references
        parts = raw_name.split(':', 1)
        title = parts[0].strip()
        gazette_ref = parts[1].strip() if len(parts) > 1 else None

        # Try to extract a year from gazette ref (e.g., "101/2007-5")
        year = None
        if gazette_ref:
            m = re.search(r'/(\d{4})', gazette_ref)
            if m:
                year = m.group(1)

        return title, gazette_ref, year

    def normalize(self, raw: dict) -> Dict[str, Any]:
        """Transform raw act data into standard schema."""
        title, gazette_ref, year = self.parse_title_info(raw.get('name', ''))

        date_str = None
        if year:
            date_str = f"{year}-01-01"

        doc_url = f"{SITE_URL}/#/reg-act/{raw['uuid']}" if raw.get('uuid') else None

        return {
            '_id': f"RS-PIS-{raw.get('regId', raw.get('uuid', ''))}",
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': raw.get('text', ''),
            'date': date_str,
            'url': doc_url,
            'reg_id': raw.get('regId'),
            'uuid': raw.get('uuid'),
            'gazette_reference': gazette_ref,
            'area_path': raw.get('area_path', ''),
            'language': 'sr',
        }

    def fetch_all(self, limit: int = 0) -> Iterator[Dict[str, Any]]:
        """Yield all legislation records with full text."""
        logger.info("Fetching menu tree...")
        menu = self.get_menu_tree()
        if not menu:
            logger.error("Failed to fetch menu tree")
            return

        leaves = self.collect_leaf_areas(menu)
        logger.info(f"Found {len(leaves)} leaf areas")

        count = 0
        for i, area in enumerate(leaves):
            logger.info(f"[{i+1}/{len(leaves)}] Fetching area: {area['name']} (id={area['id']})")
            acts = self.get_acts_in_area(area['id'])
            if not acts:
                continue

            logger.info(f"  Found {len(acts)} acts")
            for act in acts:
                if limit and count >= limit:
                    return

                uuid = act.get('url', '')  # 'url' field contains UUID
                reg_id = act.get('regId')
                name = act.get('name', '')

                if not uuid:
                    continue

                time.sleep(REQUEST_DELAY)
                text = self.get_full_text(uuid)
                if not text:
                    logger.warning(f"  No text for {name[:60]}...")
                    continue

                raw = {
                    'name': name,
                    'uuid': uuid,
                    'regId': reg_id,
                    'text': text,
                    'area_path': area['path'],
                }
                record = self.normalize(raw)
                count += 1
                yield record

            time.sleep(REQUEST_DELAY)

        logger.info(f"Total records fetched: {count}")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch updates since a date (not supported by this API — returns all)."""
        logger.warning("Incremental updates not supported; fetching all")
        yield from self.fetch_all()


def test_api():
    """Test API connectivity and endpoints."""
    fetcher = SerbiaLegislationFetcher()

    print("1. Testing menu endpoint...")
    menu = fetcher.get_menu_tree()
    if not menu:
        print("   FAILED: Could not fetch menu")
        return False
    leaves = fetcher.collect_leaf_areas(menu)
    print(f"   OK: {len(leaves)} leaf areas")

    print("2. Testing lawsAndSubareas for area 217 (Constitution)...")
    acts = fetcher.get_acts_in_area(217)
    if not acts:
        print("   FAILED: No acts returned")
        return False
    print(f"   OK: {len(acts)} acts")
    for a in acts[:3]:
        print(f"      {a['name'][:80]}")

    print("3. Testing viewAct full text...")
    uuid = acts[0].get('url', '')
    if uuid:
        text = fetcher.get_full_text(uuid)
        if text:
            print(f"   OK: {len(text)} chars")
            print(f"   Preview: {text[:200]}...")
        else:
            print("   FAILED: No text returned")
            return False

    print("\nAll tests passed!")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    fetcher = SerbiaLegislationFetcher()
    limit = 15 if sample else 0
    output_dir = SAMPLE_DIR if sample else SCRIPT_DIR / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetcher.fetch_all(limit=limit):
        if not record.get('text'):
            continue

        filename = re.sub(r'[^\w\-]', '_', record['_id'])[:100] + '.json'
        filepath = output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        count += 1
        logger.info(f"  [{count}] Saved: {record['title'][:60]}")

    logger.info(f"Bootstrap complete: {count} records saved to {output_dir}")
    return count


def main():
    parser = argparse.ArgumentParser(description='RS/PravnoInformacioniSistem fetcher')
    parser.add_argument('command', choices=['test-api', 'bootstrap'])
    parser.add_argument('--sample', action='store_true', help='Fetch only 15 sample records')
    parser.add_argument('--full', action='store_true', help='Fetch all records')
    args = parser.parse_args()

    if args.command == 'test-api':
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == 'bootstrap':
        count = bootstrap(sample=args.sample and not args.full)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)


if __name__ == '__main__':
    main()
