#!/usr/bin/env python3
"""
Finnish Data Protection Authority (TSV) Data Fetcher

Fetches GDPR enforcement decisions from Tietosuojavaltuutetun toimisto
via the Finlex Open Data API (Akoma Ntoso XML format).

~370 decisions from 1999 to present. No authentication required.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from xml.etree import ElementTree as ET

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://opendata.finlex.fi/finlex/avoindata/v1"
LIST_URL = f"{API_BASE}/akn/fi/judgment/data-protection-ombudsman-decision/list"
DOC_URL = f"{API_BASE}/akn/fi/judgment/data-protection-ombudsman-decision/{{year}}/{{number}}/fin@"
FINLEX_WEB = "https://finlex.fi/fi/viranomaiset/tietosuojavaltuutettu/{year}/{number}"

# Akoma Ntoso namespace
AKN_NS = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0'}


class TSVFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })

    def _get(self, url: str, params: dict = None, timeout: int = 60) -> Optional[requests.Response]:
        """GET with retry."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.warning(f"GET {url} attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None

    def _list_decisions(self, page: int = 1, sort_desc: bool = False) -> List[Dict]:
        """List decisions from Finlex API."""
        params = {
            'format': 'json',
            'page': page,
            'limit': 10,
            'sortBy': 'dateIssued',
        }
        resp = self._get(LIST_URL, params=params)
        if not resp:
            return []

        try:
            data = resp.json()
            if isinstance(data, list):
                return data
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning(f"Failed to parse list response: {e}")
            return []

    def _fetch_decision_xml(self, year: int, number: int) -> Optional[str]:
        """Fetch individual decision XML."""
        url = DOC_URL.format(year=year, number=number)
        resp = self._get(url)
        if resp:
            return resp.text
        return None

    def _parse_akn_uri(self, uri: str) -> Optional[tuple]:
        """Parse year and number from AKN URI like /akn/fi/judgment/.../2024/1/fin@"""
        # Pattern: .../year/number/...
        match = re.search(r'/(\d{4})/(\d+)(?:/|$)', uri)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None

    def _extract_text_from_xml(self, xml_text: str) -> Dict[str, Any]:
        """Extract structured data from Akoma Ntoso XML."""
        result = {
            'title': '',
            'text': '',
            'date': None,
            'diary_number': '',
            'keywords': [],
            'legal_basis': '',
        }

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return result

        # Try with namespace
        ns = AKN_NS

        # Extract title
        title_elem = root.find('.//akn:judgment/akn:header/akn:docTitle', ns)
        if title_elem is not None:
            result['title'] = ''.join(title_elem.itertext()).strip()

        # Fallback: use first heading in judgment body
        if not result['title']:
            heading = root.find('.//akn:judgmentBody//akn:heading', ns)
            if heading is not None:
                result['title'] = ''.join(heading.itertext()).strip()

        # Fallback: use keywords as title
        if not result['title']:
            keywords = [kw.get('showAs', '') for kw in root.findall('.//akn:keyword', ns) if kw.get('showAs')]
            if keywords:
                result['title'] = ' / '.join(keywords[:3])

        # Extract date
        date_elem = root.find('.//akn:FRBRdate[@name="dateIssued"]', ns)
        if date_elem is None:
            date_elem = root.find('.//akn:FRBRdate', ns)
        if date_elem is not None:
            result['date'] = date_elem.get('date', '')[:10]

        # Extract diary number
        alias_elem = root.find('.//akn:FRBRalias[@name="diaryNumber"]', ns)
        if alias_elem is not None:
            result['diary_number'] = alias_elem.get('value', '')

        # Extract keywords
        for kw in root.findall('.//akn:keyword', ns):
            val = kw.get('value', '')
            if val:
                result['keywords'].append(val)

        # Extract full text from judgment body
        body = root.find('.//akn:judgmentBody', ns)
        if body is None:
            body = root.find('.//akn:judgment', ns)

        if body is not None:
            text_parts = []
            for elem in body.iter():
                if elem.text and elem.text.strip():
                    text_parts.append(elem.text.strip())
                if elem.tail and elem.tail.strip():
                    text_parts.append(elem.tail.strip())
            result['text'] = '\n'.join(text_parts)

        # If namespace parsing fails, try without namespace
        if not result['text']:
            # Strip namespaces for fallback
            xml_clean = re.sub(r'\sxmlns[^"]*"[^"]*"', '', xml_text)
            try:
                root2 = ET.fromstring(xml_clean)
                body2 = root2.find('.//judgmentBody') or root2.find('.//judgment')
                if body2 is not None:
                    text_parts = []
                    for elem in body2.iter():
                        if elem.text and elem.text.strip():
                            text_parts.append(elem.text.strip())
                        if elem.tail and elem.tail.strip():
                            text_parts.append(elem.tail.strip())
                    result['text'] = '\n'.join(text_parts)

                if not result['title']:
                    t = root2.find('.//docTitle')
                    if t is not None:
                        result['title'] = ''.join(t.itertext()).strip()
            except ET.ParseError:
                pass

        # Clean up text
        if result['text']:
            result['text'] = re.sub(r'\n{3,}', '\n\n', result['text'])
            result['text'] = result['text'].strip()

        return result

    def fetch_all(self, max_docs: int = None, start_page: int = 1) -> Iterator[Dict[str, Any]]:
        """Fetch all TSV decisions."""
        fetched = 0
        page = start_page

        while True:
            if max_docs is not None and fetched >= max_docs:
                break

            entries = self._list_decisions(page=page)
            if not entries:
                break

            for entry in entries:
                if max_docs is not None and fetched >= max_docs:
                    break

                uri = entry.get('akn_uri', '') if isinstance(entry, dict) else str(entry)
                parsed = self._parse_akn_uri(uri)
                if not parsed:
                    logger.warning(f"Cannot parse URI: {uri}")
                    continue

                year, number = parsed
                logger.info(f"Fetching decision {year}/{number} ({fetched+1}/{max_docs or '?'})...")

                xml_text = self._fetch_decision_xml(year, number)
                if not xml_text:
                    continue

                extracted = self._extract_text_from_xml(xml_text)
                if not extracted.get('text') or len(extracted['text']) < 50:
                    logger.warning(f"Insufficient text for {year}/{number}")
                    continue

                yield {
                    'year': year,
                    'number': number,
                    'title': extracted['title'],
                    'text': extracted['text'],
                    'date': extracted['date'],
                    'diary_number': extracted['diary_number'],
                    'keywords': extracted['keywords'],
                }
                fetched += 1
                time.sleep(0.5)

            page += 1
            if len(entries) < 10:
                break

        logger.info(f"Fetched {fetched} TSV decisions total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions updated since a date."""
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = datetime.fromisoformat(doc['date'])
                    if doc_date >= since:
                        yield doc
                except Exception:
                    yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize to standard schema."""
        year = raw['year']
        number = raw['number']

        return {
            '_id': f"TSV-{year}-{number}",
            '_source': 'FI/TSV',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'decision_id': f"{year}/{number}",
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': FINLEX_WEB.format(year=year, number=number),
            'diary_number': raw.get('diary_number', ''),
            'keywords': raw.get('keywords', []),
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = TSVFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        max_docs = 15 if is_sample else 100

        # For sample, start from recent decisions (page 30+)
        start_page = 30 if is_sample else 1
        logger.info(f"Fetching {'sample' if is_sample else 'batch'} TSV decisions...")

        count = 0
        for raw in fetcher.fetch_all(max_docs=max_docs, start_page=start_page):
            normalized = fetcher.normalize(raw)

            if len(normalized.get('text', '')) < 100:
                continue

            filename = f"TSV_{raw['year']}_{raw['number']}.json"
            with open(sample_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['title'][:80]}... ({len(normalized['text'])} chars)")
            count += 1

        logger.info(f"Bootstrap complete. {count} documents saved to {sample_dir}")

        if count > 0:
            files = list(sample_dir.glob('*.json'))
            total = sum(len(json.load(open(f)).get('text', '')) for f in files)
            logger.info(f"Average text length: {total // len(files):,} chars/doc")
    else:
        fetcher = TSVFetcher()
        print("Testing TSV fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(max_docs=3)):
            n = fetcher.normalize(raw)
            print(f"\n--- {i+1} ---")
            print(f"ID: {n['decision_id']}, Diary: {n['diary_number']}")
            print(f"Title: {n['title'][:100]}")
            print(f"Date: {n['date']}, Text: {len(n['text']):,} chars")
            if i >= 2:
                break


if __name__ == '__main__':
    main()
