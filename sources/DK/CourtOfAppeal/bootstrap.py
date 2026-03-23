#!/usr/bin/env python3
"""
Danish Courts Database (Domsdatabasen) Data Fetcher

Fetches court decisions from the Danish Courts Administration's public database.
Uses the public web API to access case metadata and full judgment texts.

Data includes:
- Supreme Court (Højesteret) decisions
- High Court (Østre/Vestre Landsret) decisions
- District Court (Byretter) decisions
- Maritime and Commercial Court decisions

Full text is provided as HTML, which is cleaned to plain text.

API Endpoints:
- RSS feeds: https://domsdatabasen.dk/webapi/api/Case/rss
- Case details: https://domsdatabasen.dk/webapi/api/Case/get/{id}
- Document content: https://domsdatabasen.dk/webapi/api/Case/document/{id}

License: Danish court decisions are public domain under Danish Copyright Law §9.
"""

import json
import logging
import os
import re
import sys
import time
import html
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from xml.etree import ElementTree

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://domsdatabasen.dk/webapi/api"
RSS_FEEDS_URL = f"{API_BASE}/rssfeeds"


class DomsdatabasenFetcher:
    """Fetcher for Danish court decisions from Domsdatabasen"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'application/json, application/xml, */*'
        })

    def _make_request(self, url: str, accept: str = 'application/json', retries: int = 2) -> Optional[requests.Response]:
        """Make a request with error handling and retries"""
        for attempt in range(retries + 1):
            try:
                headers = {'Accept': accept}
                response = self.session.get(url, headers=headers, timeout=120)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    wait = 3 * (attempt + 1)
                    logger.warning(f"Request attempt {attempt+1} failed for {url}: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Request failed for {url} after {retries+1} attempts: {e}")
                    return None

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content to plain text"""
        if not html_content:
            return ""

        # Remove style tags and their content
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

        # Remove script tags and their content
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Replace common HTML entities
        text = html.unescape(text)

        # Replace <br> and </p> with newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.strip()

        return text

    def fetch_rss_feeds(self) -> List[Dict[str, Any]]:
        """Fetch available RSS feed configurations"""
        response = self._make_request(RSS_FEEDS_URL)
        if response:
            return response.json()
        return []

    def fetch_cases_from_rss(self, time_years: int = 2) -> Iterator[str]:
        """
        Fetch case IDs from RSS feed.

        Args:
            time_years: Number of years to look back

        Yields:
            Case IDs
        """
        # Use the generic RSS feed for all recent cases (Title parameter is required)
        rss_url = f"{API_BASE}/Case/rss?Title=All%20Cases&SortingParameter=PublishDate&DescendingOrder=true&TimeAmount={time_years}&TimeType=Years"

        response = self._make_request(rss_url, accept='application/xml')
        if not response:
            return

        try:
            root = ElementTree.fromstring(response.content)
            for item in root.findall('.//item'):
                link = item.find('link')
                if link is not None and link.text:
                    # Extract case ID from link like https://domsdatabasen.dk/#sag/10382
                    match = re.search(r'#sag/(\d+)', link.text)
                    if match:
                        yield match.group(1)
        except ElementTree.ParseError as e:
            logger.error(f"Failed to parse RSS: {e}")

    def fetch_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single case with metadata and documents"""
        url = f"{API_BASE}/Case/get/{case_id}"
        response = self._make_request(url)
        if response:
            return response.json()
        return None

    def fetch_document_content(self, doc_id: str) -> Optional[str]:
        """Fetch full HTML content of a document"""
        url = f"{API_BASE}/Case/document/{doc_id}"
        response = self._make_request(url)
        if response:
            data = response.json()
            return data.get('contentHtml', '')
        return None

    def fetch_case_ids_advanced(self, page_size: int = 10) -> Iterator[str]:
        """
        Discover all case IDs via the advanced search endpoint (paginated).

        Yields:
            Case IDs as strings
        """
        page_index = 0
        total_yielded = 0
        total_count = None
        max_retries = 3
        consecutive_failures = 0
        max_consecutive_failures = 5

        while True:
            payload = {
                "searchValue": "",
                "pageIndex": page_index,
                "pageSize": page_size,
                "sortingParameter": 1,
                "descendingOrder": True
            }
            data = None
            for attempt in range(max_retries):
                try:
                    response = self.session.post(
                        f"{API_BASE}/Case/advanced",
                        json=payload,
                        headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
                        timeout=120
                    )
                    response.raise_for_status()
                    data = response.json()
                    consecutive_failures = 0
                    break
                except Exception as e:
                    logger.warning(f"Advanced search page {page_index} attempt {attempt+1}/{max_retries} failed: {e}")
                    if attempt < max_retries - 1:
                        wait = 5 * (attempt + 1)
                        logger.info(f"Retrying in {wait}s...")
                        time.sleep(wait)

            if data is None:
                consecutive_failures += 1
                logger.error(f"Advanced search page {page_index} failed after {max_retries} retries")
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({max_consecutive_failures}), stopping")
                    break
                # Skip this page and try the next one
                page_index += 1
                time.sleep(3)
                continue

            if total_count is None:
                total_count = data.get('totalCount', 0)
                logger.info(f"Advanced search: {total_count} total cases across {data.get('pageCount', '?')} pages")

            cases = data.get('cases', [])
            if not cases:
                break

            for case in cases:
                case_id = case.get('id')
                if case_id:
                    yield str(case_id)
                    total_yielded += 1

            page_index += 1
            time.sleep(2)  # Rate limiting between pages

        logger.info(f"Discovered {total_yielded} case IDs via advanced search")

    def fetch_all(self, limit: int = None, use_advanced: bool = True) -> Iterator[Dict[str, Any]]:
        """
        Fetch all court cases with full text.

        Args:
            limit: Maximum number of cases to fetch (None for all)
            use_advanced: If True, use advanced search for complete discovery.
                         If False, use RSS feed (for quick samples).

        Yields:
            Raw case dictionaries with documents
        """
        count = 0

        if use_advanced:
            # Use advanced search endpoint for complete case discovery
            seen_ids = set()
            for case_id in self.fetch_case_ids_advanced(page_size=10):
                if case_id in seen_ids:
                    continue
                seen_ids.add(case_id)

                logger.info(f"Fetching case {case_id} [{count + 1}]...")
                case_data = self.fetch_case(case_id)

                if not case_data:
                    time.sleep(0.5)
                    continue

                # Fetch full document content for each document
                documents = case_data.get('documents', [])
                for doc in documents:
                    doc_id = doc.get('id')
                    if doc_id:
                        full_html = self.fetch_document_content(doc_id)
                        if full_html:
                            doc['fullContentHtml'] = full_html
                        time.sleep(0.3)  # Rate limiting

                yield case_data
                count += 1

                if limit and count >= limit:
                    logger.info(f"Reached limit of {limit} cases")
                    return

                time.sleep(0.5)  # Rate limiting between cases
        else:
            # RSS-based discovery (for quick samples)
            seen_ids = set()
            logger.info("Fetching case IDs from RSS feed...")

            for case_id in self.fetch_cases_from_rss(time_years=5):
                if case_id in seen_ids:
                    continue
                seen_ids.add(case_id)

                logger.info(f"Fetching case {case_id} [{count + 1}]...")
                case_data = self.fetch_case(case_id)

                if not case_data:
                    continue

                # Fetch full document content for each document
                documents = case_data.get('documents', [])
                for doc in documents:
                    doc_id = doc.get('id')
                    if doc_id:
                        full_html = self.fetch_document_content(doc_id)
                        if full_html:
                            doc['fullContentHtml'] = full_html
                        time.sleep(0.3)  # Rate limiting

                yield case_data
                count += 1

                if limit and count >= limit:
                    logger.info(f"Reached limit of {limit} cases")
                    return

                time.sleep(0.5)  # Rate limiting between cases

        logger.info(f"Fetched {count} cases with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """
        Fetch cases updated since a given date.

        Args:
            since: Fetch cases updated after this date

        Yields:
            Raw case dictionaries
        """
        # Calculate days since the date
        days_since = (datetime.now() - since).days
        years = max(1, days_since // 365 + 1)

        for case_data in self.fetch_all():
            yield case_data

    def normalize(self, raw_case: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize case to standard schema"""
        case_id = raw_case.get('id', '')
        headline = raw_case.get('headline', '')
        court_case_number = raw_case.get('courtCaseNumber', '')
        office_name = raw_case.get('officeName', '')

        # Get profession (case type)
        profession = raw_case.get('profession', {})
        profession_text = profession.get('displayText', '') if isinstance(profession, dict) else ''

        # Get case type
        case_type = raw_case.get('caseType', {})
        case_type_text = case_type.get('displayText', '') if isinstance(case_type, dict) else ''

        # Get instance level
        instance = raw_case.get('instance', {})
        instance_text = instance.get('displayText', '') if isinstance(instance, dict) else ''

        # Get verdict status
        verdict_status = raw_case.get('verdictStatus', {})
        verdict_status_text = verdict_status.get('displayText', '') if isinstance(verdict_status, dict) else ''

        # Get case subjects/topics
        subjects = raw_case.get('caseSubjects', [])
        subject_texts = [s.get('displayText', '') for s in subjects if isinstance(s, dict) and s.get('displayText')]

        # Get participants
        participants = raw_case.get('participants', [])
        participant_info = []
        for p in participants:
            if isinstance(p, dict):
                role = p.get('role', {})
                role_text = role.get('displayText', '') if isinstance(role, dict) else ''
                name = p.get('name', '')
                if role_text and name:
                    participant_info.append(f"{role_text}: {name}")

        # Extract ECLI and verdict date from documents
        ecli = ''
        verdict_date = ''

        # Collect all document texts
        full_text_parts = []
        documents = raw_case.get('documents', [])

        for doc in documents:
            doc_type = doc.get('documentType', {})
            doc_type_text = doc_type.get('displayText', '') if isinstance(doc_type, dict) else ''

            # Get ECLI from Dom document
            if doc.get('ecli'):
                ecli = doc['ecli']

            # Get verdict date
            if doc.get('verdictDateTime'):
                verdict_date = doc['verdictDateTime'][:10]  # Take just the date part

            # Get full text content
            full_html = doc.get('fullContentHtml', '')
            if full_html:
                clean_text = self._clean_html(full_html)
                if clean_text:
                    full_text_parts.append(f"=== {doc_type_text or 'Document'} ===\n\n{clean_text}")
            elif doc.get('firstPageHtml'):
                # Fallback to first page preview
                clean_text = self._clean_html(doc['firstPageHtml'])
                if clean_text:
                    full_text_parts.append(f"=== {doc_type_text or 'Document'} (Preview) ===\n\n{clean_text}")

        # Combine all texts
        full_text = '\n\n'.join(full_text_parts)

        # Build URL
        url = f"https://domsdatabasen.dk/#sag/{case_id}" if case_id else ''

        # Determine document type
        doc_type = 'case_law'

        return {
            '_id': f"DK-DDB-{case_id}",
            '_source': 'DK/CourtOfAppeal',
            '_type': doc_type,
            '_fetched_at': datetime.now().isoformat(),
            'title': headline,
            'text': full_text,
            'date': verdict_date or '',
            'url': url,
            'language': 'da',
            # Additional metadata
            'ecli': ecli,
            'court_case_number': court_case_number,
            'court': office_name,
            'case_type': case_type_text,
            'profession': profession_text,
            'instance': instance_text,
            'verdict_status': verdict_status_text,
            'subjects': subject_texts,
            'participants': participant_info
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = DomsdatabasenFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None  # No limit for full bootstrap

        # Use advanced search for both sample and full (RSS only returns ~10 items)
        limit_arg = target_count + 10 if target_count else None

        for raw_case in fetcher.fetch_all(limit=limit_arg, use_advanced=True):
            if target_count and sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_case)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                logger.warning(f"Skipping case {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            case_id = raw_case.get('id', 'unknown')
            filename = f"{case_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['court']} - {normalized['title'][:50]}... ({text_len:,} chars)")
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
        # Test mode
        fetcher = DomsdatabasenFetcher()
        print("Testing Domsdatabasen fetcher...")

        count = 0
        for raw_case in fetcher.fetch_all(limit=2):
            normalized = fetcher.normalize(raw_case)
            print(f"\n--- Case {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Court: {normalized['court']}")
            print(f"Case Number: {normalized['court_case_number']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"ECLI: {normalized['ecli']}")
            print(f"Subjects: {', '.join(normalized['subjects'])}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
