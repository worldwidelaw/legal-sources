#!/usr/bin/env python3
"""
NO/Sivilombudet - Norwegian Parliamentary Ombudsman Statements Fetcher

Fetches ombudsman statements (uttalelser) from sivilombudet.no.
~1,935 administrative law statements from 2007-present.

Data access strategy:
  1. WP REST API for statement listing + full text (/wp-json/wp/v2/uttalelser)
  2. HTML page scraping for case number and statement date
  3. Case types taxonomy for categorization

License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import logging
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NO.Sivilombudet")

BASE_URL = "https://www.sivilombudet.no"
API_URL = f"{BASE_URL}/wp-json/wp/v2/uttalelser"
CASE_TYPES_URL = f"{BASE_URL}/wp-json/wp/v2/case_types"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NO/Sivilombudet"

PER_PAGE = 100


def _strip_html(html_str: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    if not html_str:
        return ""
    text = re.sub(r'<[^>]+>', ' ', html_str)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _parse_no_date(date_str: str) -> Optional[str]:
    """Parse Norwegian date format d.m.yyyy to ISO yyyy-mm-dd."""
    if not date_str:
        return None
    m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return None


class SivilombudetScraper(BaseScraper):
    """Scraper for NO/Sivilombudet — Norwegian Parliamentary Ombudsman."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json,text/html,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5,nb;q=0.3",
        })
        self._case_type_map = None

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {'last_page': 0, 'fetched_ids': []}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _get_case_type_map(self) -> dict:
        """Fetch case_types taxonomy and build ID->name map."""
        if self._case_type_map is not None:
            return self._case_type_map

        self._case_type_map = {}
        page = 1
        while True:
            url = f"{CASE_TYPES_URL}?per_page=100&page={page}"
            try:
                time.sleep(1)
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                terms = resp.json()
                if not terms:
                    break
                for t in terms:
                    self._case_type_map[t['id']] = t['name']
                total_pages = int(resp.headers.get('X-WP-TotalPages', 1))
                if page >= total_pages:
                    break
                page += 1
            except Exception as e:
                logger.warning(f"Failed to fetch case_types page {page}: {e}")
                break

        logger.info(f"Loaded {len(self._case_type_map)} case type categories")
        return self._case_type_map

    def _fetch_api_page(self, page: int) -> tuple:
        """Fetch a page of statements from the WP REST API."""
        url = f"{API_URL}?per_page={PER_PAGE}&page={page}&orderby=date&order=asc"
        logger.info(f"Fetching API page {page}")
        time.sleep(1)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        total = int(resp.headers.get("X-WP-Total", 0))
        total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
        return resp.json(), total, total_pages

    def _extract_case_info(self, url: str) -> dict:
        """Scrape statement page for case number and statement date."""
        try:
            time.sleep(1)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            html = resp.text

            info = {}
            # Pattern: "Dato og saksnummer: 21.4.2026 (2025/7808)"
            # or "Dato for uttalelse: 21.4.2026" and case_number separately
            m = re.search(
                r'(\d{1,2}\.\d{1,2}\.\d{4})\s*\((\d{4}/\d+)\)',
                html
            )
            if m:
                info['statement_date'] = _parse_no_date(m.group(1))
                info['case_number'] = m.group(2)
            else:
                # Try separate patterns
                date_m = re.search(
                    r'Dato\s+(?:for\s+)?uttalelse[:\s]+(\d{1,2}\.\d{1,2}\.\d{4})',
                    html
                )
                if date_m:
                    info['statement_date'] = _parse_no_date(date_m.group(1))

                case_m = re.search(r'[Ss]aksnummer[:\s]+(\d{4}/\d+)', html)
                if case_m:
                    info['case_number'] = case_m.group(1)

            return info
        except Exception as e:
            logger.warning(f"Failed to scrape case info from {url}: {e}")
            return {}

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ombudsman statements with full text."""
        checkpoint = self._load_checkpoint()
        start_page = checkpoint.get('last_page', 0) + 1
        fetched_ids = set(checkpoint.get('fetched_ids', []))

        # Load case type taxonomy
        ct_map = self._get_case_type_map()

        page = start_page
        total_pages = None

        while True:
            try:
                statements, total, tp = self._fetch_api_page(page)
            except requests.RequestException as e:
                logger.error(f"API request failed on page {page}: {e}")
                break

            if total_pages is None:
                total_pages = tp
                logger.info(f"Total statements: {total}, pages: {total_pages}")

            if not statements:
                break

            for stmt in statements:
                wp_id = stmt.get('id')
                if wp_id in fetched_ids:
                    continue

                slug = stmt.get('slug', '')
                stmt_url = stmt.get('link', f"{BASE_URL}/uttalelser/{slug}/")
                title = _strip_html(stmt.get('title', {}).get('rendered', slug))
                content_html = stmt.get('content', {}).get('rendered', '')
                full_text = _strip_html(content_html)
                wp_date = stmt.get('date', '')

                # Resolve case type IDs to names
                type_ids = stmt.get('case_types', [])
                case_types = [ct_map.get(tid, '') for tid in type_ids if ct_map.get(tid)]

                # Scrape HTML for case number and statement date
                case_info = self._extract_case_info(stmt_url)
                case_number = case_info.get('case_number', '')
                statement_date = case_info.get('statement_date') or (wp_date[:10] if wp_date else None)

                record = {
                    '_id': f"Sivilombudet-{case_number}" if case_number else f"Sivilombudet-{wp_id}",
                    '_source': SOURCE_ID,
                    '_type': 'doctrine',
                    '_fetched_at': datetime.now(timezone.utc).isoformat(),
                    'title': title,
                    'text': full_text,
                    'date': statement_date,
                    'url': stmt_url,
                    'case_number': case_number,
                    'case_types': ', '.join(case_types),
                    'excerpt': _strip_html(stmt.get('excerpt', {}).get('rendered', '')),
                    'wp_id': wp_id,
                }

                yield record
                fetched_ids.add(wp_id)

            checkpoint = {
                'last_page': page,
                'fetched_ids': list(fetched_ids),
            }
            self._save_checkpoint(checkpoint)

            if page >= (total_pages or 1):
                break
            page += 1

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield statements modified since a date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
        ct_map = self._get_case_type_map()
        page = 1

        while True:
            try:
                url = (
                    f"{API_URL}?per_page={PER_PAGE}&page={page}"
                    f"&orderby=modified&order=desc&modified_after={since}"
                )
                time.sleep(1)
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                statements = resp.json()
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            except requests.RequestException as e:
                logger.error(f"API request failed: {e}")
                break

            if not statements:
                break

            for stmt in statements:
                slug = stmt.get('slug', '')
                wp_id = stmt.get('id')
                stmt_url = stmt.get('link', f"{BASE_URL}/uttalelser/{slug}/")
                title = _strip_html(stmt.get('title', {}).get('rendered', slug))
                full_text = _strip_html(stmt.get('content', {}).get('rendered', ''))

                type_ids = stmt.get('case_types', [])
                case_types = [ct_map.get(tid, '') for tid in type_ids if ct_map.get(tid)]

                case_info = self._extract_case_info(stmt_url)
                case_number = case_info.get('case_number', '')
                statement_date = case_info.get('statement_date') or stmt.get('date', '')[:10]

                yield {
                    '_id': f"Sivilombudet-{case_number}" if case_number else f"Sivilombudet-{wp_id}",
                    '_source': SOURCE_ID,
                    '_type': 'doctrine',
                    '_fetched_at': datetime.now(timezone.utc).isoformat(),
                    'title': title,
                    'text': full_text,
                    'date': statement_date,
                    'url': stmt_url,
                    'case_number': case_number,
                    'case_types': ', '.join(case_types),
                }

            if page >= total_pages:
                break
            page += 1

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to the standard schema."""
        return {
            '_id': raw.get('_id', ''),
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': raw.get('_fetched_at', datetime.now(timezone.utc).isoformat()),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'case_number': raw.get('case_number', ''),
            'case_types': raw.get('case_types', ''),
            'excerpt': raw.get('excerpt', ''),
        }


def bootstrap(sample: bool = False):
    """Bootstrap the NO/Sivilombudet data source."""
    scraper = SivilombudetScraper()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    max_records = 15 if sample else float('inf')

    for record in scraper.fetch_all():
        normalized = scraper.normalize(record)

        if sample:
            safe_id = re.sub(r'[/\\:]', '_', normalized['_id'])
            out_file = SAMPLE_DIR / f"{safe_id}.json"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            text_len = len(normalized.get('text', '') or '')
            logger.info(
                f"[{count + 1}] {normalized['_id']} — "
                f"{text_len} chars text, date={normalized.get('date')}"
            )

        count += 1
        if count >= max_records:
            break

    logger.info(f"Done. {count} records {'sampled' if sample else 'fetched'}.")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="NO/Sivilombudet bootstrap")
    parser.add_argument("action", choices=["bootstrap"], help="Action to perform")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.action == "bootstrap":
        bootstrap(sample=args.sample or not args.full)
