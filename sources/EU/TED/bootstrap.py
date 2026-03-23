#!/usr/bin/env python3
"""
EU Tenders Electronic Daily (TED) Data Fetcher

Fetches EU public procurement notices via the TED Search API v3.
No authentication required for reading published notices.
~6.8 million notices from 1993 to present.

Uses the search API for discovery and individual notice XML for full text.
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from xml.etree import ElementTree as ET

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SEARCH_API = "https://api.ted.europa.eu/v3/notices/search"
NOTICE_XML_URL = "https://ted.europa.eu/en/notice/{nd}/xml"
NOTICE_URL = "https://ted.europa.eu/en/notice/-/detail/{nd}"


class TEDFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

    def _search_notices(self, query: str, page: int = 1, limit: int = 100) -> Dict:
        """Search TED notices via API v3."""
        payload = {
            "query": query,
            "fields": ["ND", "PD", "TI", "TD", "CY"],
            "page": page,
            "limit": limit,
            "scope": "ALL",
            "paginationMode": "PAGE_NUMBER",
        }

        for attempt in range(3):
            try:
                resp = self.session.post(SEARCH_API, json=payload, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Search attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def _fetch_notice_xml(self, nd: str) -> Optional[str]:
        """Fetch full notice XML from TED."""
        url = NOTICE_XML_URL.format(nd=nd)
        try:
            resp = self.session.get(url, timeout=60, headers={
                'Accept': 'application/xml, text/xml',
                'Content-Type': None,  # override JSON content-type
            })
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch XML for {nd}: {e}")
            return None

    def _extract_text_from_xml(self, xml_text: str) -> Dict[str, Any]:
        """Extract text content from TED eForms XML or legacy XML."""
        result = {'title': '', 'text': '', 'notice_type': '', 'buyer': ''}

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return result

        # Collect all text from the XML, stripping namespaces
        all_texts = []

        def get_tag(elem):
            """Get tag without namespace."""
            tag = elem.tag
            if '}' in tag:
                tag = tag.split('}', 1)[1]
            return tag

        def extract_recursive(elem, depth=0):
            """Recursively extract text from XML elements."""
            tag = get_tag(elem)
            text = (elem.text or '').strip()
            tail = (elem.tail or '').strip()

            if text:
                all_texts.append(f"{tag}: {text}")
            if tail:
                all_texts.append(tail)

            for child in elem:
                extract_recursive(child, depth + 1)

        extract_recursive(root)

        # Build full text from all extracted content
        full_text = '\n'.join(all_texts)

        # Try to extract title - look for common patterns
        title = ''
        for t in all_texts:
            if any(t.startswith(prefix) for prefix in [
                'TITLE_CONTRACT:', 'Title:', 'TI_TEXT:', 'ObjectDescription:',
                'cbc:Name:', 'ContractTitle:',
            ]):
                title = t.split(':', 1)[1].strip()
                if len(title) > 10:
                    break

        # Try specific eForms fields
        for elem in root.iter():
            tag = get_tag(elem)
            if tag == 'Name' and not title:
                text = (elem.text or '').strip()
                if len(text) > 10:
                    title = text
            elif tag in ('BuyerProfileURL', 'OrganizationName'):
                text = (elem.text or '').strip()
                if text and not result['buyer']:
                    result['buyer'] = text

        result['title'] = title or 'Procurement Notice'
        result['text'] = full_text
        return result

    def fetch_all(self, max_docs: int = None, start_date: str = None,
                  end_date: str = None) -> Iterator[Dict[str, Any]]:
        """Fetch procurement notices with full text."""
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            # Default to last 30 days for sample
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

        query = f"PD >= {start_date} AND PD <= {end_date}"
        logger.info(f"Searching TED: {query}")

        fetched = 0
        page = 1

        while True:
            if max_docs is not None and fetched >= max_docs:
                break

            result = self._search_notices(query, page=page, limit=100)
            notices = result.get('notices', [])
            total = result.get('totalNoticeCount', 0)

            if not notices:
                break

            logger.info(f"Page {page}: {len(notices)} notices (total: {total})")

            for notice in notices:
                if max_docs is not None and fetched >= max_docs:
                    break

                nd = notice.get('ND', notice.get('publication-number', ''))
                if not nd:
                    continue

                # Get English title from multilingual TI field
                ti = notice.get('TI', {})
                if isinstance(ti, dict):
                    search_title = ti.get('eng', ti.get('fra', next(iter(ti.values()), '')))
                else:
                    search_title = str(ti) if ti else ''

                logger.info(f"Fetching XML for notice {nd} ({fetched+1}/{max_docs or '?'})...")

                xml_text = self._fetch_notice_xml(nd)
                if not xml_text:
                    continue

                extracted = self._extract_text_from_xml(xml_text)
                if not extracted.get('text') or len(extracted['text']) < 100:
                    logger.warning(f"Insufficient text for {nd}, skipping")
                    continue

                # Use search title if XML didn't produce a good one
                title = extracted.get('title', '')
                if not title or title == 'Procurement Notice':
                    title = search_title or 'Procurement Notice'

                # Country can be a list
                cy = notice.get('CY', '')
                if isinstance(cy, list):
                    cy = ', '.join(cy)

                yield {
                    'nd': nd,
                    'title': title,
                    'text': extracted['text'],
                    'date': notice.get('PD', ''),
                    'buyer': extracted.get('buyer', ''),
                    'notice_type': notice.get('TD', ''),
                    'country': cy,
                }
                fetched += 1
                time.sleep(0.5)

            page += 1
            # PAGE_NUMBER mode: max 15000 results
            if page * 100 > min(total, 15000):
                break

        logger.info(f"Fetched {fetched} TED notices total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch notices published since a given date."""
        start = since.strftime('%Y%m%d')
        end = datetime.now().strftime('%Y%m%d')
        yield from self.fetch_all(start_date=start, end_date=end)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize to standard schema."""
        # Parse date - can be YYYYMMDD or ISO with timezone like "2026-03-02+01:00"
        date = None
        pd = raw.get('date', '')
        if pd:
            try:
                # Strip timezone offset if present
                clean = pd.split('+')[0].split('T')[0]
                if '-' in clean:
                    date = clean[:10]  # Already ISO format
                elif len(clean) >= 8:
                    date = f"{clean[:4]}-{clean[4:6]}-{clean[6:8]}"
            except Exception:
                pass

        return {
            '_id': raw['nd'],
            '_source': 'EU/TED',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'notice_id': raw['nd'],
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': date,
            'url': NOTICE_URL.format(nd=raw['nd']),
            'notice_type': raw.get('notice_type', ''),
            'country': raw.get('country', ''),
            'buyer': raw.get('buyer', ''),
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = TEDFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        max_docs = 15 if is_sample else 100

        logger.info(f"Fetching {'sample' if is_sample else 'batch'} TED notices...")

        count = 0
        for raw in fetcher.fetch_all(max_docs=max_docs):
            normalized = fetcher.normalize(raw)

            if len(normalized.get('text', '')) < 100:
                continue

            filename = f"{normalized['notice_id'].replace('/', '_').replace('-', '_')}.json"
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
        fetcher = TEDFetcher()
        print("Testing EU/TED fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(max_docs=3)):
            n = fetcher.normalize(raw)
            print(f"\n--- {i+1} ---")
            print(f"ID: {n['notice_id']}, Type: {n['notice_type']}")
            print(f"Title: {n['title'][:100]}")
            print(f"Date: {n['date']}, Text: {len(n['text']):,} chars")
            if i >= 2:
                break


if __name__ == '__main__':
    main()
