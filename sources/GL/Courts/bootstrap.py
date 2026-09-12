#!/usr/bin/env python3
"""
Greenland Court Decisions (Domsdatabasen) Data Fetcher

Fetches court decisions from Greenland courts via the Danish Courts Administration's
public database (Domsdatabasen). Greenland courts are part of the Danish judiciary.

Courts covered:
- Grønlands landsret (Greenland High Court)
- Sermersooq Kredsret (Nuuk district court — capital/most populous district)
- Kujalleq Kredsret (district court)
- Qeqqa Kredsret (district court)
- Qaasuitsoq Kredsret (district court)

API Endpoints:
- Advanced search: POST https://domsdatabasen.dk/webapi/api/Case/advanced
- Case details: GET https://domsdatabasen.dk/webapi/api/Case/get/{id}
- Document content: GET https://domsdatabasen.dk/webapi/api/Case/document/{id}

License: Danish/Greenland court decisions are public domain under Danish Copyright Law §9.
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

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://domsdatabasen.dk/webapi/api"

GREENLAND_COURTS = [
    "Grønlands landsret",
    "Sermersooq Kredsret",
    "Kujalleq Kredsret",
    "Qeqqa Kredsret",
    "Qaasuitsoq Kredsret",
]


class GreenlandCourtsFetcher:
    """Fetcher for Greenland court decisions from Domsdatabasen"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })

    def _request_get(self, url: str, retries: int = 2) -> Optional[requests.Response]:
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, timeout=120)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    time.sleep(3 * (attempt + 1))
                else:
                    logger.error(f"GET {url} failed: {e}")
                    return None

    def _request_post(self, url: str, payload: dict, retries: int = 2) -> Optional[requests.Response]:
        for attempt in range(retries + 1):
            try:
                response = self.session.post(url, json=payload, timeout=120)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    time.sleep(3 * (attempt + 1))
                else:
                    logger.error(f"POST {url} failed: {e}")
                    return None

    def _clean_html(self, html_content: str) -> str:
        if not html_content:
            return ""
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = html.unescape(text)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def fetch_case_ids(self, page_size: int = 10) -> Iterator[str]:
        """Fetch all Greenland case IDs via advanced search (paginated)."""
        page = 1
        total_count = None

        while True:
            url = f"{API_BASE}/Case/advanced?SortingParameter=VerdictDateDesc&page={page}&pageSize={page_size}"
            payload = {"office": GREENLAND_COURTS}

            response = self._request_post(url, payload)
            if not response:
                break

            data = response.json()

            if total_count is None:
                total_count = data.get('totalCount', 0)
                logger.info(f"Total Greenland cases: {total_count}")

            cases = data.get('cases') or []
            if not cases:
                break

            for case in cases:
                case_id = case.get('id')
                if case_id:
                    yield str(case_id)

            page += 1
            if page > data.get('pageCount', 0):
                break

            time.sleep(1.5)

    def fetch_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        url = f"{API_BASE}/Case/get/{case_id}"
        response = self._request_get(url)
        if response:
            return response.json()
        return None

    def fetch_document_content(self, doc_id: str) -> Optional[str]:
        url = f"{API_BASE}/Case/document/{doc_id}"
        response = self._request_get(url)
        if response:
            data = response.json()
            return data.get('contentHtml', '')
        return None

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all Greenland court cases with full text."""
        count = 0
        seen = set()

        for case_id in self.fetch_case_ids():
            if case_id in seen:
                continue
            seen.add(case_id)

            logger.info(f"Fetching case {case_id} [{count + 1}]...")
            case_data = self.fetch_case(case_id)
            if not case_data:
                time.sleep(0.5)
                continue

            # Fetch document content
            documents = case_data.get('documents', [])
            for doc in documents:
                doc_id = doc.get('id')
                if doc_id:
                    full_html = self.fetch_document_content(doc_id)
                    if full_html:
                        doc['fullContentHtml'] = full_html
                    time.sleep(0.5)

            yield case_data
            count += 1

            if limit and count >= limit:
                break

            time.sleep(1)

        logger.info(f"Fetched {count} Greenland cases")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        for case_data in self.fetch_all():
            yield case_data

    def normalize(self, raw_case: Dict[str, Any]) -> Dict[str, Any]:
        case_id = raw_case.get('id', '')
        headline = raw_case.get('headline', '')
        court_case_number = raw_case.get('courtCaseNumber', '')
        office_name = raw_case.get('officeName', '')
        office_abbrev = raw_case.get('officeAbbreviation', '')

        profession = raw_case.get('profession', {})
        profession_text = profession.get('displayText', '') if isinstance(profession, dict) else ''

        case_type = raw_case.get('caseType', {})
        case_type_text = case_type.get('displayText', '') if isinstance(case_type, dict) else ''

        instance = raw_case.get('instance', {})
        instance_text = instance.get('displayText', '') if isinstance(instance, dict) else ''

        verdict_status = raw_case.get('verdictStatus', {})
        verdict_status_text = verdict_status.get('displayText', '') if isinstance(verdict_status, dict) else ''

        subjects = raw_case.get('caseSubjects', [])
        subject_texts = [s.get('displayText', '') for s in subjects if isinstance(s, dict) and s.get('displayText')]

        participants = raw_case.get('participants', [])
        participant_info = []
        for p in participants:
            if isinstance(p, dict):
                role = p.get('role', {})
                role_text = role.get('displayText', '') if isinstance(role, dict) else ''
                name = p.get('name', '')
                if role_text and name:
                    participant_info.append(f"{role_text}: {name}")

        ecli = ''
        verdict_date = ''
        full_text_parts = []
        documents = raw_case.get('documents', [])

        for doc in documents:
            doc_type = doc.get('documentType', {})
            doc_type_text = doc_type.get('displayText', '') if isinstance(doc_type, dict) else ''

            if doc.get('ecli'):
                ecli = doc['ecli']
            if doc.get('verdictDateTime'):
                verdict_date = doc['verdictDateTime'][:10]

            full_html = doc.get('fullContentHtml', '')
            if full_html:
                clean_text = self._clean_html(full_html)
                if clean_text:
                    full_text_parts.append(f"=== {doc_type_text or 'Document'} ===\n\n{clean_text}")

        full_text = '\n\n'.join(full_text_parts)
        url = f"https://domsdatabasen.dk/#sag/{case_id}" if case_id else ''

        return {
            '_id': f"GL-DDB-{case_id}",
            '_source': 'GL/Courts',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': headline,
            'text': full_text,
            'date': verdict_date or '',
            'url': url,
            'language': 'da',
            'ecli': ecli,
            'court_case_number': court_case_number,
            'court': office_name,
            'court_abbreviation': office_abbrev,
            'case_type': case_type_text,
            'profession': profession_text,
            'instance': instance_text,
            'verdict_status': verdict_status_text,
            'subjects': subject_texts,
            'participants': participant_info,
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = GreenlandCourtsFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting GL/Courts bootstrap...")

        sample_count = 0
        is_sample = '--sample' in sys.argv
        target_count = 15 if is_sample else None

        for raw_case in fetcher.fetch_all(limit=target_count + 5 if target_count else None):
            if target_count and sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_case)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                logger.warning(f"Skipping case {normalized['_id']} - text too short ({text_len} chars)")
                continue

            case_id = raw_case.get('id', 'unknown')
            filepath = sample_dir / f"{case_id}.json"

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}]: {normalized['court']} - {normalized['title'][:60]}... ({text_len:,} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. {sample_count} documents saved to {sample_dir}")

        files = list(sample_dir.glob('*.json'))
        total_chars = sum(len(json.loads(f.read_text()).get('text', '')) for f in files)
        logger.info(f"Summary: {len(files)} files, {total_chars:,} total chars, avg {total_chars // max(len(files), 1):,} chars/doc")


if __name__ == '__main__':
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
