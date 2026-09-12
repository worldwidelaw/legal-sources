#!/usr/bin/env python3
"""
RS/Poverenik — Serbia Commissioner for Information & Data Protection

Fetches decisions from the Poverenik practice database (praksa.poverenik.rs).
Three domains: Freedom of Information, Data Protection, Harmonization.

Strategy:
  1. For each domain, submit search form per-year to discover decision GUIDs
  2. For years hitting the 25-result cap, drill down by document type
  3. Fetch each decision detail page for full text
  4. Extract case number, date, and decision text

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test
"""

import sys
import re
import json
import logging
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Set, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RS.Poverenik")

BASE_URL = "https://praksa.poverenik.rs"

# Domain IDs and their URL paths
DOMAINS = {
    "pristupinformacijama": {"id": "4", "label": "Freedom of Information"},
    "zastitapodataka": {"id": "1", "label": "Data Protection"},
    "harmonizacija": {"id": "3", "label": "Harmonization"},
}

YEARS = list(range(2015, 2027))  # 2015-2026
MAX_RESULTS_PER_QUERY = 25


class PoverenikScraper(BaseScraper):
    """
    Scraper for RS/Poverenik — Serbia Commissioner practice database.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,sr;q=0.8",
        })

    def _get_csrf_and_form(self, domain_path: str) -> Tuple[str, BeautifulSoup]:
        """GET a domain page and extract CSRF token."""
        url = f"{BASE_URL}/{domain_path}"
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        csrf_el = soup.find('input', {'name': '__RequestVerificationToken'})
        csrf = csrf_el['value'] if csrf_el else ''
        return csrf, soup

    def _extract_form_metadata(self, soup: BeautifulSoup) -> Dict:
        """Extract document type IDs and other filter metadata from form."""
        doc_types = []
        for i in range(20):
            id_f = soup.find('input', {'name': f'DocumentTypes[{i}].Id'})
            if not id_f:
                break
            name_f = soup.find('input', {'name': f'DocumentTypes[{i}].Name'})
            count_f = soup.find('input', {'name': f'DocumentTypes[{i}].Count'})
            doc_types.append({
                'idx': i,
                'id': id_f.get('value', ''),
                'name': name_f.get('value', '') if name_f else '',
                'count': int(count_f.get('value', '0')) if count_f else 0,
            })

        num_keywords = 0
        for i in range(50):
            if not soup.find('input', {'name': f'Keywords[{i}].Id'}):
                break
            num_keywords = i + 1

        num_contents = 0
        for i in range(50):
            if not soup.find('input', {'name': f'ContentsWords[{i}].Id'}):
                break
            num_contents = i + 1

        return {
            'doc_types': doc_types,
            'num_doc_types': len(doc_types),
            'num_keywords': num_keywords,
            'num_contents': num_contents,
        }

    def _build_form_data(self, csrf: str, domain_id: str, form_meta: Dict,
                         year_idx: int = None, doc_type_idx: int = None) -> Dict:
        """Build POST form data for a search query."""
        data = {
            '__RequestVerificationToken': csrf,
            'searchTerm': '',
            'authorityName': '',
            'Domain': domain_id,
        }

        # Year checkboxes
        for i in range(len(YEARS)):
            if year_idx is not None:
                data[f'Godine[{i}].IsChecked'] = 'true' if i == year_idx else 'false'
            else:
                data[f'Godine[{i}].IsChecked'] = 'true'
            data[f'Godine[{i}].BrojGodine'] = str(YEARS[i])

        # Document type checkboxes
        for i in range(form_meta['num_doc_types']):
            if doc_type_idx is not None:
                data[f'DocumentTypes[{i}].IsChecked'] = 'true' if i == doc_type_idx else 'false'
            else:
                data[f'DocumentTypes[{i}].IsChecked'] = 'false'

        # Keywords and content words — all unchecked
        for i in range(form_meta['num_keywords']):
            data[f'Keywords[{i}].IsChecked'] = 'false'
        for i in range(form_meta['num_contents']):
            data[f'ContentsWords[{i}].IsChecked'] = 'false'

        return data

    def _search(self, csrf: str, domain_id: str, form_meta: Dict,
                year_idx: int = None, doc_type_idx: int = None) -> Set[str]:
        """Submit search form and return set of decision GUIDs."""
        data = self._build_form_data(csrf, domain_id, form_meta, year_idx, doc_type_idx)
        self.rate_limiter.wait()
        resp = self.session.post(f"{BASE_URL}/", data=data, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')
        guids = set()
        for link in soup.find_all('a', href=re.compile(r'/predmet/detalji/')):
            guid = link['href'].split('/')[-1]
            if len(guid) > 20:  # Valid GUID
                guids.add(guid)
        return guids

    def _discover_all_guids(self, domain_path: str, domain_id: str) -> Set[str]:
        """Discover all decision GUIDs for a domain by iterating years and doc types."""
        all_guids = set()

        csrf, soup = self._get_csrf_and_form(domain_path)
        form_meta = self._extract_form_metadata(soup)

        logger.info(f"  Domain {domain_path}: {form_meta['num_doc_types']} doc types")

        for year_idx, year in enumerate(YEARS):
            # Need fresh CSRF for each request
            csrf, _ = self._get_csrf_and_form(domain_path)
            guids = self._search(csrf, domain_id, form_meta, year_idx=year_idx)

            if len(guids) > 0:
                logger.info(f"    {year}: {len(guids)} decisions")

            if len(guids) >= MAX_RESULTS_PER_QUERY and form_meta['num_doc_types'] > 0:
                # Hit the cap — drill down by document type
                logger.info(f"    {year}: hit cap, drilling down by doc type...")
                for dt in form_meta['doc_types']:
                    csrf, _ = self._get_csrf_and_form(domain_path)
                    dt_guids = self._search(csrf, domain_id, form_meta,
                                            year_idx=year_idx, doc_type_idx=dt['idx'])
                    new_count = len(dt_guids - all_guids - guids)
                    if new_count > 0:
                        logger.info(f"      +{new_count} new from doc type {dt['name'][:30]}")
                    guids.update(dt_guids)

            all_guids.update(guids)

        return all_guids

    def _fetch_decision(self, guid: str) -> Dict[str, Any]:
        """Fetch a single decision detail page and extract content."""
        url = f"{BASE_URL}/predmet/detalji/{guid}"
        self.rate_limiter.wait()

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error fetching {guid}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')
        page_text = soup.get_text()

        # Check for "not found" page
        if 'Предмет није пронађен' in page_text:
            logger.warning(f"Decision {guid} not found (404)")
            return None

        # Extract case number and date from <strong> tags
        case_number = None
        date_str = None

        for strong in soup.find_all('strong'):
            text = strong.get_text(strip=True)
            if text in ('Број:', 'Broj:'):
                next_text = strong.next_sibling
                if next_text:
                    case_number = str(next_text).strip()
            elif text in ('Датум:', 'Datum:'):
                next_text = strong.next_sibling
                if next_text:
                    date_str = str(next_text).strip()

        # Fallback: regex on full page text
        if not case_number:
            m = re.search(r'Број:\s*([\d\-/]+)', page_text)
            if m:
                case_number = m.group(1).strip()

        if not date_str:
            m = re.search(r'Датум:\s*([\d.]+)', page_text)
            if m:
                date_str = m.group(1).strip()

        # Parse date
        parsed_date = None
        if date_str:
            for fmt in ['%d.%m.%Y', '%d.%m.%Y.', '%Y-%m-%d']:
                try:
                    parsed_date = datetime.strptime(date_str.rstrip('.'), fmt).strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue

        # Extract full text from body, removing boilerplate
        for tag in soup.find_all(['nav', 'footer', 'script', 'style']):
            tag.decompose()

        body = soup.find('body')
        full_text = body.get_text(separator='\n', strip=True) if body else ''

        # Clean: extract content between the title/date header and the contact footer
        lines = full_text.split('\n')
        cleaned_lines = []
        in_content = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Start at case metadata or content marker
            if not in_content:
                if any(marker in line for marker in ['Датум:', 'Datum:', 'САДРЖАЈ']):
                    in_content = True
                    cleaned_lines.append(line)
                    continue
            if in_content:
                # Stop at footer boilerplate
                if line.startswith('Контакт') or (
                    'Булевар краља Александра' in line
                ):
                    break
                cleaned_lines.append(line)

        full_text = '\n'.join(cleaned_lines)

        # Extract title — the text right before "Датум:" in the page
        title = case_number or guid
        for el in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            t = el.get_text(strip=True)
            if t and len(t) > 5:
                title = t
                break
        # Also check col-md-9 div for title
        if title == (case_number or guid):
            col9 = soup.find('div', class_='col-md-9')
            if col9:
                first_text = col9.find(string=True, recursive=False)
                if first_text and len(first_text.strip()) > 5:
                    title = first_text.strip()
                else:
                    # Get first substantial text child
                    for child in col9.children:
                        if hasattr(child, 'get_text'):
                            t = child.get_text(strip=True)
                            if t and len(t) > 10 and 'Датум' not in t and 'Број' not in t:
                                title = t[:200]
                                break

        return {
            'guid': guid,
            'case_number': case_number or guid,
            'date': parsed_date,
            'date_raw': date_str,
            'title': title,
            'text': full_text,
            'url': url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Discover and yield all decisions from all domains."""
        all_guids = set()

        for domain_path, domain_info in DOMAINS.items():
            logger.info(f"Discovering decisions in {domain_info['label']}...")
            guids = self._discover_all_guids(domain_path, domain_info['id'])
            new = guids - all_guids
            logger.info(f"  {domain_info['label']}: {len(guids)} found, {len(new)} new")
            all_guids.update(guids)

        logger.info(f"Total unique decisions discovered: {len(all_guids)}")

        for i, guid in enumerate(sorted(all_guids)):
            yield {
                'guid': guid,
                '_index': i,
                '_total': len(all_guids),
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all records (no incremental update support)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Fetch decision detail and normalize."""
        guid = raw['guid']
        idx = raw.get('_index', 0)
        total = raw.get('_total', 0)

        logger.info(f"  [{idx+1}/{total}] Fetching {guid[:20]}...")
        decision = self._fetch_decision(guid)

        if not decision or not decision.get('text'):
            logger.warning(f"  No text for {guid}")
            return None

        text = decision['text']
        if len(text) < 50:
            logger.warning(f"  Text too short ({len(text)} chars) for {guid}")
            return None

        logger.info(f"  Extracted {len(text)} chars, case={decision.get('case_number','?')}")

        doc_id = f"poverenik_{decision['case_number']}".replace('/', '_').replace(' ', '_')

        return {
            "_id": doc_id,
            "_source": "RS/Poverenik",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": decision['title'],
            "text": text,
            "date": decision['date'],
            "url": decision['url'],
            "case_number": decision['case_number'],
            "issuing_body": "Commissioner for Information of Public Importance and Personal Data Protection",
            "language": "sr",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing RS/Poverenik endpoints...")

        print("\n1. Testing search form...")
        csrf, soup = self._get_csrf_and_form("pristupinformacijama")
        form_meta = self._extract_form_metadata(soup)
        print(f"   CSRF token: {csrf[:30]}...")
        print(f"   Document types: {form_meta['num_doc_types']}")

        print("\n2. Searching 2022 FOI decisions...")
        guids = self._search(csrf, "4", form_meta, year_idx=7)
        print(f"   Found {len(guids)} decisions")

        if guids:
            sample_guid = sorted(guids)[0]
            print(f"\n3. Fetching decision {sample_guid[:20]}...")
            decision = self._fetch_decision(sample_guid)
            if decision:
                print(f"   Case: {decision['case_number']}")
                print(f"   Date: {decision['date']}")
                print(f"   Text: {len(decision['text'])} chars")
                print(f"   Preview: {decision['text'][:300]}...")

        print("\nTest complete!")


def main():
    scraper = PoverenikScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
