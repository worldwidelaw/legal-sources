#!/usr/bin/env python3
"""
Czech Constitutional Court (Ústavní soud) Data Fetcher

Access to NALUS database of Constitutional Court decisions
https://nalus.usoud.cz

OPTIMIZED VERSION: Uses search API to discover valid case references,
avoiding brute-force enumeration of all possible case numbers.

The ECLI format for Czech Constitutional Court is:
ECLI:CZ:US:{year}:{senate}.US.{number}.{year}.{ordinal}
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Set

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
NALUS_BASE = "https://nalus.usoud.cz"
SEARCH_URL = f"{NALUS_BASE}/Search/Search.aspx"
RESULTS_URL = f"{NALUS_BASE}/Search/Results.aspx"
GETTEXT_URL = f"{NALUS_BASE}/Search/GetText.aspx"


class NALUSHTMLParser(HTMLParser):
    """Parser to extract structured content from NALUS decision HTML"""

    def __init__(self):
        super().__init__()
        self.text_parts: List[str] = []
        self.in_body = False

    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.in_body = True

    def handle_data(self, data):
        if self.in_body:
            stripped = data.strip()
            if stripped:
                self.text_parts.append(stripped)

    def get_full_text(self) -> str:
        return '\n'.join(self.text_parts)

    def parse_decision(self, html: str) -> Dict[str, Any]:
        self.feed(html)
        full_text = self.get_full_text()

        result = {
            'raw_text': full_text,
            'metadata': {}
        }

        ecli_match = re.search(r'ECLI:CZ:US:\d+:[^<\s]+', html)
        if ecli_match:
            result['metadata']['ecli'] = ecli_match.group(0)

        sz_match = re.search(r'([IVPl]+\.\s*ÚS\s*\d+/\d+)', full_text)
        if sz_match:
            result['metadata']['case_ref'] = sz_match.group(1)

        date_match = re.search(r'ze dne (\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})', full_text)
        if date_match:
            day, month, year = date_match.groups()
            result['metadata']['decision_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        if 'NÁLEZ' in full_text[:500]:
            result['metadata']['decision_type'] = 'NÁLEZ'
        elif 'USNESENÍ' in full_text[:500]:
            result['metadata']['decision_type'] = 'USNESENÍ'
        elif 'STANOVISKO' in full_text[:500]:
            result['metadata']['decision_type'] = 'STANOVISKO'

        return result


class ConstitutionalCourtFetcher:
    """Fetcher for Czech Constitutional Court decisions from NALUS"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'cs,en;q=0.5',
        })
        self._request_count = 0
        self._last_request = 0

    def _rate_limit(self, delay: float = 1.0):
        """Ensure rate limiting between requests"""
        now = time.time()
        elapsed = now - self._last_request
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()
        self._request_count += 1

    def _get_viewstate(self) -> Dict[str, str]:
        """Get ASP.NET form tokens from search page"""
        self._rate_limit(0.5)
        r = self.session.get(SEARCH_URL, timeout=30)
        r.raise_for_status()

        viewstate = re.search(r'__VIEWSTATE" value="([^"]+)"', r.text)
        viewstate_gen = re.search(r'__VIEWSTATEGENERATOR" value="([^"]+)"', r.text)
        eventval = re.search(r'__EVENTVALIDATION" value="([^"]+)"', r.text)

        if not all([viewstate, viewstate_gen, eventval]):
            raise ValueError("Could not extract ASP.NET form tokens")

        return {
            '__VIEWSTATE': viewstate.group(1),
            '__VIEWSTATEGENERATOR': viewstate_gen.group(1),
            '__EVENTVALIDATION': eventval.group(1),
        }

    def _search_by_date_range(self, date_from: str, date_to: str) -> Iterator[str]:
        """
        Search for decisions in a date range and yield case reference URLs.

        NALUS uses session-based search with simple URL-based pagination:
        - First, POST the search form to establish search context
        - Then, GET /Search/Results.aspx?page=N for subsequent pages (0-indexed)

        Args:
            date_from: Start date in DD.MM.YYYY format
            date_to: End date in DD.MM.YYYY format

        Yields:
            Case reference strings (e.g., "1-3249-24_1")
        """
        tokens = self._get_viewstate()

        # Submit search to establish session context
        data = {
            **tokens,
            'ctl00$MainContent$nalezy': 'on',
            'ctl00$MainContent$usneseni': 'on',
            'ctl00$MainContent$stanoviska_plena': 'on',
            'ctl00$MainContent$decidedFrom': date_from,
            'ctl00$MainContent$decidedTo': date_to,
            'ctl00$MainContent$naveti': 'on',
            'ctl00$MainContent$but_search': 'Vyhledat',
        }

        self._rate_limit(0.5)
        r = self.session.post(SEARCH_URL, data=data, timeout=30, allow_redirects=True)
        r.raise_for_status()

        # Extract total count
        total_match = re.search(r'celkem[:\s]*(\d+)', r.text, re.IGNORECASE)
        total = int(total_match.group(1)) if total_match else 0

        if total == 0:
            return

        logger.info(f"  Date range {date_from} - {date_to}: {total} decisions")

        # Extract case refs from first page (page 0)
        all_refs: Set[str] = set()
        refs = set(re.findall(r'GetText\.aspx\?sz=([^"&\s]+)', r.text))
        all_refs.update(refs)
        for ref in refs:
            yield ref

        # Paginate using URL-based pagination
        if total > 10:
            num_pages = (total + 9) // 10  # Ceiling division

            for page_num in range(1, num_pages):
                self._rate_limit(0.3)
                try:
                    page_url = f"{RESULTS_URL}?page={page_num}"
                    r = self.session.get(page_url, timeout=30)
                    r.raise_for_status()
                except requests.RequestException as e:
                    logger.warning(f"  Page {page_num} failed: {e}")
                    continue

                new_refs = set(re.findall(r'GetText\.aspx\?sz=([^"&\s]+)', r.text))

                # Yield only new refs (avoid duplicates)
                for ref in new_refs:
                    if ref not in all_refs:
                        all_refs.add(ref)
                        yield ref

                if (page_num + 1) % 50 == 0:
                    logger.info(f"    Page {page_num + 1}/{num_pages}, collected {len(all_refs)} refs so far...")

    def _fetch_decision(self, case_ref: str) -> Optional[Dict[str, Any]]:
        """Fetch a single decision by case reference"""
        self._rate_limit(1.0)

        url = f"{GETTEXT_URL}?sz={case_ref}"

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code != 200:
                return None

            html = response.text

            # Check for actual decision content
            if 'nenalezeno' in html.lower() or len(html) < 6000:
                return None

            # Must contain a case reference
            if not re.search(r'[IVPl]+\.?\s*ÚS', html):
                return None

            parser = NALUSHTMLParser()
            result = parser.parse_decision(html)

            result['url'] = url
            result['case_ref_param'] = case_ref

            return result

        except requests.RequestException as e:
            logger.warning(f"Error fetching {case_ref}: {e}")
            return None

    def fetch_all(self, start_year: int = None, end_year: int = 1993,
                  limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all decisions using search-based discovery.

        This approach searches by year and paginates through results to find
        valid case references, then fetches each decision. This is much faster
        than brute-forcing all possible case numbers.

        Args:
            start_year: Most recent year to start from (default: current year)
            end_year: Oldest year to go back to (default: 1993)
            limit: Maximum number of documents to fetch

        Yields:
            Raw document dictionaries with full text
        """
        if start_year is None:
            start_year = datetime.now().year

        count = 0

        for year in range(start_year, end_year - 1, -1):
            logger.info(f"Processing year {year}...")

            # Search for all decisions in this year
            date_from = f"1.1.{year}"
            date_to = f"31.12.{year}"

            year_refs: List[str] = []
            try:
                for ref in self._search_by_date_range(date_from, date_to):
                    year_refs.append(ref)
                    # For limited fetches, stop collecting refs once we have enough candidates
                    # (collect 3x limit to account for some being empty/too short)
                    if limit and len(year_refs) >= limit * 3:
                        logger.info(f"  Collected enough refs for limit={limit}, stopping search")
                        break
            except Exception as e:
                logger.error(f"Search failed for {year}: {e}")
                continue

            logger.info(f"  Found {len(year_refs)} case references for {year}")

            # Fetch each decision
            year_count = 0
            for i, ref in enumerate(year_refs):
                if limit and count >= limit:
                    logger.info(f"Reached limit of {limit} documents")
                    return

                result = self._fetch_decision(ref)

                if result:
                    text = result.get('raw_text', '')
                    if text and len(text) > 500:
                        count += 1
                        year_count += 1

                        # Parse case ref
                        parts = ref.replace('_', '-').split('-')
                        senate = parts[0] if parts else '1'
                        number = int(parts[1]) if len(parts) > 1 else 0
                        year_short = parts[2] if len(parts) > 2 else str(year)[-2:]

                        yield {
                            'case_ref': ref,
                            'senate': senate,
                            'number': number,
                            'year': year,
                            'text': text,
                            'metadata': result.get('metadata', {}),
                            'url': result.get('url', '')
                        }

                if (i + 1) % 100 == 0:
                    logger.info(f"  Progress: {i+1}/{len(year_refs)}, found {year_count}")

            logger.info(f"Year {year}: {year_count} decisions fetched (total: {count})")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions published since a given date."""
        current_year = datetime.now().year
        since_year = since.year

        yield from self.fetch_all(
            start_year=current_year,
            end_year=since_year
        )

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        metadata = raw_doc.get('metadata', {})

        # Build ECLI if not present
        ecli = metadata.get('ecli', '')
        if not ecli:
            year = raw_doc.get('year', '')
            senate = raw_doc.get('senate', '1')
            number = raw_doc.get('number', '')
            if year and number:
                ecli = f"ECLI:CZ:US:{year}:{senate}.US.{number}.{str(year % 100).zfill(2)}.1"

        # Build the human-readable case reference
        senate_roman = {
            '1': 'I', '2': 'II', '3': 'III', '4': 'IV', 'Pl': 'Pl'
        }.get(str(raw_doc.get('senate', '1')), 'I')

        case_ref_display = f"{senate_roman}. ÚS {raw_doc.get('number', '')}/{str(raw_doc.get('year', ''))[-2:]}"

        text = raw_doc.get('text', '')
        title = metadata.get('case_ref', case_ref_display)

        if text:
            lines = text.split('\n')
            for line in lines[:20]:
                if len(line) > 50 and not line.startswith('Ústavní'):
                    if 've věci' in line.lower():
                        title = line[:200]
                        break

        decision_date = metadata.get('decision_date', f"{raw_doc.get('year', '')}-01-01")

        return {
            '_id': ecli or raw_doc.get('case_ref', ''),
            '_source': 'CZ/ConstitutionalCourt',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'case_reference': case_ref_display,
            'ecli': ecli,
            'text': text,
            'decision_type': metadata.get('decision_type', ''),
            'date': decision_date,
            'year': raw_doc.get('year'),
            'senate': raw_doc.get('senate'),
            'url': raw_doc.get('url', f"{GETTEXT_URL}?sz={raw_doc.get('case_ref', '')}"),
            'language': 'cs'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ConstitutionalCourtFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 15 if is_sample else None

        if is_sample:
            logger.info("Fetching sample records...")
            # For sample, just get recent decisions from one year
            sample_count = 0

            for raw_doc in fetcher.fetch_all(
                start_year=2024,
                end_year=2024,
                limit=target_count
            ):
                if sample_count >= target_count:
                    break

                normalized = fetcher.normalize(raw_doc)
                text_len = len(normalized.get('text', ''))

                if text_len < 500:
                    continue

                case_ref = raw_doc.get('case_ref', str(sample_count))
                filename = f"{case_ref.replace('/', '_').replace('-', '_')}.json"
                filepath = sample_dir / filename

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(normalized, f, indent=2, ensure_ascii=False)

                logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['case_reference']} ({text_len:,} chars)")
                sample_count += 1

            logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

            # Print summary
            files = list(sample_dir.glob('*.json'))
            total_chars = 0
            for f in files:
                with open(f, 'r', encoding='utf-8') as fp:
                    data = json.load(fp)
                    total_chars += len(data.get('text', ''))

            print(f"\n=== SUMMARY ===")
            print(f"Sample files: {len(files)}")
            print(f"Total text chars: {total_chars:,}")
            print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")
        else:
            # Full bootstrap - stream to stdout as JSONL
            data_dir = Path(__file__).parent / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            jsonl_path = data_dir / 'records.jsonl'

            count = 0
            with open(jsonl_path, 'w', encoding='utf-8') as out:
                for raw_doc in fetcher.fetch_all():
                    normalized = fetcher.normalize(raw_doc)
                    line = json.dumps(normalized, ensure_ascii=False)
                    out.write(line + '\n')
                    print(line)
                    count += 1

            logger.info(f"Bootstrap complete: {count} records written to {jsonl_path}")
    else:
        # Test mode
        fetcher = ConstitutionalCourtFetcher()
        print("Testing Constitutional Court fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(
            start_year=2024,
            end_year=2024,
            limit=3
        ):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Case Ref: {normalized['case_reference']}")
            print(f"Date: {normalized['date']}")
            print(f"Type: {normalized.get('decision_type', 'unknown')}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
