#!/usr/bin/env python3
"""
German Federal Social Court (BSG) Case Law Fetcher

Official open data from rechtsprechung-im-internet.de
https://www.rechtsprechung-im-internet.de

This fetcher retrieves case law from the Federal Social Court using:
- Table of Contents XML for document discovery
- ZIP downloads containing XML with full decision text

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import io
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.rechtsprechung-im-internet.de"
RSS_FEED_URL = f"{BASE_URL}/jportal/docs/feed/bsjrs-bsg.xml"
TOC_URL = f"{BASE_URL}/rii-toc.xml"


class BSGFetcher:
    """Fetcher for German Federal Social Court case law"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'application/xml,application/zip,*/*',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_bsg_entries_from_toc(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch BSG entries from the table of contents XML"""
        logger.info(f"Fetching table of contents from {TOC_URL}")
        response = self.session.get(TOC_URL, timeout=120)
        response.raise_for_status()

        # Parse TOC XML
        root = ET.fromstring(response.content)
        entries = []

        for item in root.findall('.//item'):
            gericht = item.findtext('gericht', '')

            # Only include BSG decisions
            if not gericht.startswith('BSG'):
                continue

            entsch_datum = item.findtext('entsch-datum', '')
            aktenzeichen = item.findtext('aktenzeichen', '')
            link = item.findtext('link', '')
            modified = item.findtext('modified', '')

            # Extract doc_id from link
            # Link format: http://www.rechtsprechung-im-internet.de/jportal/docs/bsjrs/jb-KSRE162770108.zip
            doc_id = None
            if link:
                match = re.search(r'/jb-([A-Z0-9]+)\.zip$', link)
                if match:
                    doc_id = match.group(1)

            if doc_id:
                entries.append({
                    'doc_id': doc_id,
                    'gericht': gericht,
                    'entsch_datum': entsch_datum,
                    'aktenzeichen': aktenzeichen,
                    'link': link,
                    'modified': modified
                })

                if limit and len(entries) >= limit:
                    break

        logger.info(f"Found {len(entries)} BSG entries in TOC")
        return entries

    def _get_bsg_entries_from_rss(self) -> List[Dict[str, Any]]:
        """Fetch BSG entries from RSS feed (for recent decisions)"""
        logger.info(f"Fetching RSS feed from {RSS_FEED_URL}")
        response = self.session.get(RSS_FEED_URL, timeout=60)
        response.raise_for_status()

        # Parse RSS XML
        root = ET.fromstring(response.content)
        entries = []

        for item in root.findall('.//item'):
            title = item.findtext('title', '')
            description = item.findtext('description', '')
            link = item.findtext('link', '')
            guid = item.findtext('guid', '')

            # Extract doc_id from guid
            doc_id = guid.replace('jb-', '') if guid else None

            if doc_id:
                entries.append({
                    'doc_id': doc_id,
                    'title': title,
                    'description': description,
                    'link': link
                })

        logger.info(f"Found {len(entries)} entries in RSS feed")
        return entries

    def _fetch_decision_xml(self, doc_id: str, zip_url: str = None) -> Optional[str]:
        """
        Fetch and extract the decision XML from a ZIP file.

        Args:
            doc_id: Document ID (e.g., KSRE162770108)
            zip_url: Direct ZIP URL (optional, will be constructed if not provided)

        Returns:
            XML content as string or None if error
        """
        if not zip_url:
            # Use HTTP (not HTTPS) as per TOC links - HTTPS redirects don't work well
            zip_url = f"http://www.rechtsprechung-im-internet.de/jportal/docs/bsjrs/jb-{doc_id}.zip"

        try:
            # Use a fresh request (not session) to avoid cookie interference from HTTPS RSS feed
            response = requests.get(zip_url, timeout=60, headers={
                'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
                'Accept': '*/*',
            })
            response.raise_for_status()

            # Extract XML from ZIP
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                # Find the XML file in the ZIP
                xml_files = [f for f in zf.namelist() if f.endswith('.xml')]
                if not xml_files:
                    logger.warning(f"No XML file found in ZIP for {doc_id}")
                    return None

                # Read the first (and usually only) XML file
                with zf.open(xml_files[0]) as xf:
                    return xf.read().decode('utf-8')

        except requests.RequestException as e:
            logger.error(f"Error fetching ZIP for {doc_id}: {e}")
            return None
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file for {doc_id}: {e}")
            return None

    def _parse_decision_xml(self, xml_content: str) -> Dict[str, Any]:
        """Parse the decision XML to extract metadata and full text"""
        root = ET.fromstring(xml_content)

        result = {
            'doknr': root.findtext('doknr', ''),
            'ecli': root.findtext('ecli', ''),
            'gertyp': root.findtext('gertyp', ''),
            'spruchkoerper': root.findtext('spruchkoerper', ''),
            'entsch_datum': root.findtext('entsch-datum', ''),
            'aktenzeichen': root.findtext('aktenzeichen', ''),
            'doktyp': root.findtext('doktyp', ''),
            'norm': root.findtext('norm', ''),
            'titelzeile': '',
            'leitsatz': '',
            'tenor': '',
            'tatbestand': '',
            'entscheidungsgruende': '',
            'text': ''
        }

        # Extract titelzeile with proper element handling (has nested HTML-like content)
        titelzeile_elem = root.find('titelzeile')
        if titelzeile_elem is not None:
            result['titelzeile'] = self._extract_text_from_element(titelzeile_elem)

        # Extract leitsatz (headnote)
        leitsatz_elem = root.find('leitsatz')
        if leitsatz_elem is not None:
            result['leitsatz'] = self._extract_text_from_element(leitsatz_elem)

        # Extract tenor (operative part)
        tenor_elem = root.find('tenor')
        if tenor_elem is not None:
            result['tenor'] = self._extract_text_from_element(tenor_elem)

        # Extract tatbestand (facts)
        tatbestand_elem = root.find('tatbestand')
        if tatbestand_elem is not None:
            result['tatbestand'] = self._extract_text_from_element(tatbestand_elem)

        # Extract entscheidungsgruende (reasoning) - try both tag names
        # Note: Both tags may exist but one may be empty, so check for actual content
        gruende_elem = root.find('entscheidungsgruende')
        gruende_text = ''
        if gruende_elem is not None and len(list(gruende_elem)) > 0:
            gruende_text = self._extract_text_from_element(gruende_elem)
        # Fall back to 'gruende' if entscheidungsgruende is empty
        if not gruende_text.strip():
            gruende_elem = root.find('gruende')
            if gruende_elem is not None and len(list(gruende_elem)) > 0:
                gruende_text = self._extract_text_from_element(gruende_elem)
        result['entscheidungsgruende'] = gruende_text

        # Build full text from all sections
        text_parts = []

        if result['titelzeile']:
            text_parts.append(result['titelzeile'])

        if result['leitsatz']:
            text_parts.append("\n\nLeitsatz:\n" + result['leitsatz'])

        if result['tenor']:
            text_parts.append("\n\nTenor:\n" + result['tenor'])

        if result['tatbestand']:
            text_parts.append("\n\nTatbestand:\n" + result['tatbestand'])

        if result['entscheidungsgruende']:
            text_parts.append("\n\nEntscheidungsgründe:\n" + result['entscheidungsgruende'])

        result['text'] = '\n'.join(text_parts)

        return result

    def _extract_text_from_element(self, element: ET.Element) -> str:
        """Extract text content from an XML element, stripping HTML tags"""
        # Get all text including from child elements
        text_parts = []

        def recurse(elem, depth=0):
            if elem.text:
                text_parts.append(elem.text.strip())
            for child in elem:
                # Handle paragraph numbers
                if child.tag == 'a' and child.get('name', '').startswith('rd_'):
                    num = child.get('name', '').replace('rd_', '')
                    text_parts.append(f"\n[{num}] ")
                elif child.tag in ('p', 'div'):
                    recurse(child, depth + 1)
                    text_parts.append("\n")
                elif child.tag == 'em':
                    if child.text:
                        text_parts.append(child.text)
                elif child.tag == 'br':
                    text_parts.append("\n")
                else:
                    recurse(child, depth + 1)

                if child.tail:
                    text_parts.append(child.tail.strip())

        recurse(element)
        text = ' '.join(text_parts)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'\s*\[(\d+)\]\s*', r'\n[\1] ', text)

        return text.strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert date string to ISO 8601"""
        if not date_str:
            return None

        # Format: YYYYMMDD
        if len(date_str) == 8 and date_str.isdigit():
            try:
                dt = datetime.strptime(date_str, '%Y%m%d')
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        return date_str

    def fetch_all(self, limit: int = None, use_rss: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Fetch BSG decisions with full text.

        Args:
            limit: Maximum number of decisions to fetch (None for all)
            use_rss: If True, use RSS feed (recent only). If False, use full TOC (6,285 decisions).

        Yields:
            Raw document dictionaries with full text
        """
        if use_rss:
            entries = self._get_bsg_entries_from_rss()
        else:
            entries = self._get_bsg_entries_from_toc(limit=limit)

        if limit:
            entries = entries[:limit]

        count = 0
        for i, entry in enumerate(entries):
            doc_id = entry['doc_id']
            # Only use link if it's a ZIP URL (from TOC), otherwise let _fetch_decision_xml construct it
            zip_url = entry.get('link') if entry.get('link', '').endswith('.zip') else None

            logger.info(f"[{i+1}/{len(entries)}] Fetching: {doc_id} ({entry.get('aktenzeichen', 'N/A')})")

            xml_content = self._fetch_decision_xml(doc_id, zip_url)

            if not xml_content:
                logger.warning(f"Could not fetch XML for {doc_id}")
                continue

            try:
                parsed = self._parse_decision_xml(xml_content)

                # Merge entry metadata
                if 'aktenzeichen' not in parsed or not parsed['aktenzeichen']:
                    parsed['aktenzeichen'] = entry.get('aktenzeichen', '')
                if 'description' in entry:
                    parsed['rss_description'] = entry['description']

                if parsed.get('text') and len(parsed.get('text', '')) > 100:
                    yield parsed
                    count += 1

                    if limit and count >= limit:
                        break
                else:
                    logger.warning(f"Skipping {doc_id} - insufficient text")

            except ET.ParseError as e:
                logger.error(f"XML parse error for {doc_id}: {e}")
                continue

            # Rate limiting
            time.sleep(1.5)

        logger.info(f"Fetched {count} decisions with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent decisions (RSS feed only)"""
        yield from self.fetch_all(use_rss=True)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        doc_id = raw_doc.get('doknr', '')

        # Build permalink URL
        url = f"{BASE_URL}/jportal/?quelle=jlink&docid={doc_id}&psml=bsjrsprod.psml"

        # Parse date
        date = self._parse_date(raw_doc.get('entsch_datum', ''))

        # Build title
        parts = []
        if raw_doc.get('gertyp'):
            parts.append(raw_doc['gertyp'])
        if raw_doc.get('spruchkoerper'):
            parts.append(raw_doc['spruchkoerper'])
        if raw_doc.get('doktyp'):
            parts.append(raw_doc['doktyp'])
        if date:
            parts.append(f"vom {date}")
        if raw_doc.get('aktenzeichen'):
            parts.append(raw_doc['aktenzeichen'])

        title = ', '.join(parts) if parts else doc_id

        return {
            '_id': doc_id,
            '_source': 'DE/BSG',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': date,
            'url': url,
            'ecli': raw_doc.get('ecli', ''),
            'aktenzeichen': raw_doc.get('aktenzeichen', ''),
            'court': raw_doc.get('gertyp', 'BSG'),
            'chamber': raw_doc.get('spruchkoerper', ''),
            'decision_type': raw_doc.get('doktyp', ''),
            'norms': raw_doc.get('norm', ''),
            'headnote': raw_doc.get('leitsatz', ''),
            'tenor': raw_doc.get('tenor', ''),
            'summary': raw_doc.get('rss_description', ''),
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BSGFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None  # No limit for full bootstrap

        # Always use TOC for full coverage (6,280+ BSG decisions)
        # RSS only contains ~35 recent decisions which is insufficient
        use_rss = False

        # For sample mode, fetch a few extra to account for skipped records
        limit_arg = target_count + 5 if target_count else None
        for raw_doc in fetcher.fetch_all(limit=limit_arg, use_rss=use_rss):
            if target_count and sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['aktenzeichen']} ({text_len:,} chars)")
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
        fetcher = BSGFetcher()
        print("Testing BSG fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3, use_rss=True):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"ECLI: {normalized['ecli']}")
            print(f"Court: {normalized['court']}")
            print(f"Chamber: {normalized['chamber']}")
            print(f"File: {normalized['aktenzeichen']}")
            print(f"Type: {normalized['decision_type']}")
            print(f"Date: {normalized['date']}")
            print(f"Title: {normalized['title'][:100]}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
