#!/usr/bin/env python3
"""
German Federal Court of Justice (BGH) Case Law Fetcher

Official open data from rechtsprechung-im-internet.de
https://www.rechtsprechung-im-internet.de

This fetcher retrieves case law from the Federal Court of Justice using:
- Table of Contents XML for complete document discovery (35,000+ decisions)
- RSS feed for recent decisions discovery
- ZIP downloads containing XML with full decision text

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import html
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
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.rechtsprechung-im-internet.de"
RSS_FEED_URL = f"{BASE_URL}/jportal/docs/feed/bsjrs-bgh.xml"
TOC_URL = f"{BASE_URL}/rii-toc.xml"
DECISION_BASE_URL = f"{BASE_URL}/jportal/portal/page/bsjrsprod.psml"


class BGHFetcher:
    """Fetcher for German Federal Court of Justice case law"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_decisions_from_rss(self) -> List[Dict[str, Any]]:
        """Fetch the list of recent decisions from the RSS feed"""
        logger.info(f"Fetching RSS feed from {RSS_FEED_URL}")
        response = self.session.get(RSS_FEED_URL, timeout=60)
        response.raise_for_status()

        # Parse RSS XML
        root = ET.fromstring(response.content)
        decisions = []

        for item in root.findall('.//item'):
            title = item.findtext('title', '')
            description = item.findtext('description', '')
            link = item.findtext('link', '')
            pub_date = item.findtext('pubDate', '')
            guid = item.findtext('guid', '')

            # Extract doc.id from link
            doc_id = None
            if link:
                parsed = urlparse(link)
                params = parse_qs(parsed.query)
                if 'doc.id' in params:
                    doc_id = params['doc.id'][0]
                    # Remove 'jb-' prefix if present
                    if doc_id.startswith('jb-'):
                        doc_id = doc_id[3:]

            if not doc_id and guid:
                # Use guid as fallback
                doc_id = guid.replace('jb-', '')

            if doc_id:
                decisions.append({
                    'doc_id': doc_id,
                    'title': title,
                    'description': description,
                    'link': link,
                    'pub_date': pub_date
                })

        logger.info(f"Found {len(decisions)} decisions in RSS feed")
        return decisions

    def _get_decisions_from_toc(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch BGH entries from the table of contents XML (complete discovery)"""
        logger.info(f"Fetching table of contents from {TOC_URL}")
        response = self.session.get(TOC_URL, timeout=120)
        response.raise_for_status()

        # Parse TOC XML
        root = ET.fromstring(response.content)
        entries = []

        for item in root.findall('.//item'):
            gericht = item.findtext('gericht', '')

            # Only include BGH decisions
            if not gericht.startswith('BGH'):
                continue

            entsch_datum = item.findtext('entsch-datum', '')
            aktenzeichen = item.findtext('aktenzeichen', '')
            link = item.findtext('link', '')
            modified = item.findtext('modified', '')

            # Extract doc_id from link
            # Link format: http://www.rechtsprechung-im-internet.de/jportal/docs/bsjrs/jb-KORE702612026.zip
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

        logger.info(f"Found {len(entries)} BGH entries in TOC")
        return entries

    def _fetch_decision_xml(self, doc_id: str, zip_url: str = None) -> Optional[str]:
        """
        Fetch and extract the decision XML from a ZIP file.

        Args:
            doc_id: Document ID (e.g., KORE702612026)
            zip_url: Direct ZIP URL (optional, will be constructed if not provided)

        Returns:
            XML content as string or None if error
        """
        if not zip_url:
            # Use HTTP (not HTTPS) as per TOC links
            zip_url = f"http://www.rechtsprechung-im-internet.de/jportal/docs/bsjrs/jb-{doc_id}.zip"

        try:
            # Use a fresh request to avoid cookie interference
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
            'titelzeile': root.findtext('titelzeile', ''),
            'leitsatz': '',
            'tenor': '',
            'tatbestand': '',
            'entscheidungsgruende': '',
            'text': ''
        }

        # Extract leitsatz (headnote)
        leitsatz_elem = root.find('leitsatz')
        if leitsatz_elem is not None:
            result['leitsatz'] = self._extract_text_from_xml_element(leitsatz_elem)

        # Extract tenor (operative part)
        tenor_elem = root.find('tenor')
        if tenor_elem is not None:
            result['tenor'] = self._extract_text_from_xml_element(tenor_elem)

        # Extract tatbestand (facts)
        tatbestand_elem = root.find('tatbestand')
        if tatbestand_elem is not None:
            result['tatbestand'] = self._extract_text_from_xml_element(tatbestand_elem)

        # Extract entscheidungsgruende (reasoning)
        gruende_elem = root.find('entscheidungsgruende')
        if gruende_elem is not None:
            result['entscheidungsgruende'] = self._extract_text_from_xml_element(gruende_elem)

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

    def _extract_text_from_xml_element(self, element: ET.Element) -> str:
        """Extract text content from an XML element, stripping HTML tags"""
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

    def _fetch_decision_page(self, doc_id: str, part: str = 'L') -> Optional[str]:
        """
        Fetch the decision page HTML.

        Args:
            doc_id: Document ID (e.g., KORE702612026)
            part: 'K' for Kurztext (short), 'L' for Langtext (full)

        Returns:
            HTML content or None if error
        """
        url = f"{DECISION_BASE_URL}?doc.id=jb-{doc_id}&doc.part={part}&showdoccase=1"

        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching decision {doc_id}: {e}")
            return None

    def _parse_decision_html(self, html_content: str, doc_id: str) -> Dict[str, Any]:
        """Parse the decision HTML page to extract metadata and full text"""
        soup = BeautifulSoup(html_content, 'html.parser')

        result = {
            'doc_id': doc_id,
            'text': '',
            'court': '',
            'decision_date': '',
            'aktenzeichen': '',
            'ecli': '',
            'decision_type': '',
            'norms': ''
        }

        # Extract metadata from the document header table
        header_table = soup.find('table', class_='documentHeader')
        if header_table:
            for row in header_table.find_all('tr'):
                cells = row.find_all('td')
                for i, cell in enumerate(cells):
                    strong = cell.find('strong')
                    if strong:
                        label = strong.get_text(strip=True).rstrip(':')
                        # Get the next td or text after the strong
                        value_cell = cells[i + 1] if i + 1 < len(cells) else cell
                        if value_cell:
                            value = value_cell.get_text(strip=True)

                            if label == 'Gericht':
                                result['court'] = value
                            elif label == 'Entscheidungsdatum':
                                result['decision_date'] = value
                            elif label == 'Aktenzeichen':
                                result['aktenzeichen'] = value
                            elif label == 'ECLI':
                                result['ecli'] = value
                            elif label == 'Dokumenttyp':
                                result['decision_type'] = value
                            elif label == 'Normen':
                                result['norms'] = value

        # Also try to find metadata in the nested structure
        for table in soup.find_all('table', class_='TableRahmenkpl'):
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text.startswith('Gericht:'):
                        # Get sibling cell
                        siblings = cell.find_parent('tr').find_all('td')
                        if len(siblings) >= 2:
                            result['court'] = siblings[1].get_text(strip=True)
                    elif text.startswith('Entscheidungsdatum:'):
                        siblings = cell.find_parent('tr').find_all('td')
                        if len(siblings) >= 2:
                            result['decision_date'] = siblings[1].get_text(strip=True)
                    elif text.startswith('Aktenzeichen:'):
                        siblings = cell.find_parent('tr').find_all('td')
                        if len(siblings) >= 2:
                            result['aktenzeichen'] = siblings[1].get_text(strip=True)
                    elif text.startswith('ECLI:'):
                        siblings = cell.find_parent('tr').find_all('td')
                        if len(siblings) >= 2:
                            result['ecli'] = siblings[1].get_text(strip=True)
                    elif text.startswith('Dokumenttyp:'):
                        siblings = cell.find_parent('tr').find_all('td')
                        if len(siblings) >= 2:
                            result['decision_type'] = siblings[1].get_text(strip=True)

        # Extract full text from docLayoutText div
        text_parts = []

        # Get title/summary from docLayoutTitel
        title_div = soup.find('div', class_='docLayoutTitel')
        if title_div:
            title_text = title_div.get_text(strip=True)
            if title_text:
                text_parts.append(title_text)

        # Get main text content from docLayoutText
        for text_div in soup.find_all('div', class_='docLayoutText'):
            # Find all h4 headers (sections like Tenor, Gründe)
            for h4 in text_div.find_all('h4', class_='doc'):
                section_title = h4.get_text(strip=True)
                text_parts.append(f"\n\n{section_title}\n")

            # Extract text from RspDL elements (the decision structure)
            for dl in text_div.find_all('dl', class_='RspDL'):
                for dd in dl.find_all('dd'):
                    # Get numbered reference if exists
                    dt = dd.find_previous_sibling('dt')
                    if dt:
                        anchor = dt.find('a')
                        if anchor and anchor.get('name', '').startswith('rd_'):
                            num = anchor.get('name', '').replace('rd_', '')
                            text_parts.append(f"\n[{num}] ")

                    # Get paragraph text
                    para = dd.find('p')
                    if para:
                        para_text = para.get_text(strip=True)
                        if para_text:
                            text_parts.append(para_text)
                    else:
                        # Check for nested h1/h2 headings
                        heading = dd.find(['h1', 'h2'])
                        if heading:
                            heading_text = heading.get_text(strip=True)
                            text_parts.append(f"\n{heading_text}\n")
                        else:
                            dd_text = dd.get_text(strip=True)
                            if dd_text:
                                text_parts.append(dd_text)

        # Join and clean text
        full_text = ' '.join(text_parts)
        full_text = self._clean_text(full_text)
        result['text'] = full_text

        return result

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        # Clean up numbered paragraph markers
        text = re.sub(r'\s*\[(\d+)\]\s*', r'\n[\1] ', text)

        # Remove any remaining HTML-like content
        text = re.sub(r'<[^>]+>', '', text)

        return text.strip()

    def _parse_german_date(self, date_str: str) -> Optional[str]:
        """Convert German date format (DD.MM.YYYY) or YYYYMMDD to ISO 8601"""
        if not date_str:
            return None

        try:
            # Try DD.MM.YYYY format
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            # Try YYYYMMDD format (from XML)
            if len(date_str) == 8 and date_str.isdigit():
                dt = datetime.strptime(date_str, '%Y%m%d')
                return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            # Try YYYY-MM-DD format (already ISO)
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        return date_str

    def fetch_all(self, limit: int = None, use_rss: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Fetch BGH decisions with full text.

        Args:
            limit: Maximum number of decisions to fetch (None for all)
            use_rss: If True, use RSS feed (recent only ~224). If False, use full TOC (35,000+).

        Yields:
            Raw document dictionaries with full text
        """
        if use_rss:
            # RSS-based discovery (legacy, for recent decisions only)
            decisions = self._get_decisions_from_rss()
            if limit:
                decisions = decisions[:limit]

            count = 0
            for i, decision in enumerate(decisions):
                doc_id = decision['doc_id']
                logger.info(f"[{i+1}/{len(decisions)}] Fetching: {decision['title'][:60]}...")

                # Fetch full text (Langtext)
                html_content = self._fetch_decision_page(doc_id, part='L')

                if not html_content:
                    # Try Kurztext as fallback
                    logger.warning(f"No Langtext for {doc_id}, trying Kurztext")
                    html_content = self._fetch_decision_page(doc_id, part='K')

                if not html_content:
                    continue

                parsed = self._parse_decision_html(html_content, doc_id)

                # Merge RSS metadata with parsed data
                parsed['rss_title'] = decision['title']
                parsed['rss_description'] = decision['description']
                parsed['link'] = decision['link']

                if parsed.get('text') and len(parsed.get('text', '')) > 100:
                    yield parsed
                    count += 1

                    if limit and count >= limit:
                        break

                # Rate limiting
                time.sleep(1.5)

            logger.info(f"Fetched {count} decisions with full text (RSS)")
        else:
            # TOC-based discovery (full coverage: 35,000+ decisions)
            entries = self._get_decisions_from_toc(limit=limit)

            count = 0
            for i, entry in enumerate(entries):
                doc_id = entry['doc_id']
                # Only use link if it's a ZIP URL (from TOC)
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

            logger.info(f"Fetched {count} decisions with full text (TOC)")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent decisions (RSS feed only)"""
        yield from self.fetch_all(use_rss=True)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Support both HTML-based doc_id and XML-based doknr
        doc_id = raw_doc.get('doknr') or raw_doc.get('doc_id', '')

        # Build permalink URL
        url = f"{BASE_URL}/jportal/?quelle=jlink&docid={doc_id}&psml=bsjrsprod.psml"

        # Parse date (support both XML format YYYYMMDD and HTML format DD.MM.YYYY)
        date_str = raw_doc.get('entsch_datum') or raw_doc.get('decision_date', '')
        date = self._parse_german_date(date_str)

        # Build title
        title = raw_doc.get('rss_title') or raw_doc.get('titelzeile', '')
        if not title:
            parts = []
            court = raw_doc.get('gertyp') or raw_doc.get('court')
            if court:
                parts.append(court)
            chamber = raw_doc.get('spruchkoerper')
            if chamber:
                parts.append(chamber)
            decision_type = raw_doc.get('doktyp') or raw_doc.get('decision_type')
            if decision_type:
                parts.append(decision_type)
            if date:
                parts.append(f"vom {date}")
            aktenzeichen = raw_doc.get('aktenzeichen')
            if aktenzeichen:
                parts.append(aktenzeichen)
            title = ', '.join(parts) if parts else doc_id

        return {
            '_id': doc_id,
            '_source': 'DE/BGH',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': date,
            'url': url,
            'ecli': raw_doc.get('ecli', ''),
            'aktenzeichen': raw_doc.get('aktenzeichen', ''),
            'court': raw_doc.get('gertyp') or raw_doc.get('court', 'BGH'),
            'chamber': raw_doc.get('spruchkoerper', ''),
            'decision_type': raw_doc.get('doktyp') or raw_doc.get('decision_type', ''),
            'norms': raw_doc.get('norm') or raw_doc.get('norms', ''),
            'headnote': raw_doc.get('leitsatz', ''),
            'tenor': raw_doc.get('tenor', ''),
            'summary': raw_doc.get('rss_description', ''),
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BGHFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None  # No limit for full bootstrap

        # Use RSS for sample (recent decisions), TOC for full bootstrap (35,000+)
        use_rss = is_sample

        # For full bootstrap with no limit, fetch all from TOC
        limit_arg = target_count + 10 if target_count else None
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
        print(f"Discovery method: {'RSS (recent)' if use_rss else 'TOC (full 35K+)'}")

    else:
        # Test mode - use TOC to demonstrate full coverage
        fetcher = BGHFetcher()
        print("Testing BGH fetcher (TOC-based discovery)...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3, use_rss=False):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"ECLI: {normalized['ecli']}")
            print(f"File: {normalized['aktenzeichen']}")
            print(f"Date: {normalized['date']}")
            print(f"Title: {normalized['title'][:100]}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
