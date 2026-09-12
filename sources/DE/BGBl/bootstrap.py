#!/usr/bin/env python3
"""
German Federal Law Data Fetcher (Gesetze im Internet)

Official open data from the Federal Ministry of Justice
https://www.gesetze-im-internet.de

This fetcher downloads XML files from gesetze-im-internet.de which contain
the full consolidated text of all German federal laws and regulations.

Data structure:
- gii-toc.xml: Table of contents with links to all law XMLs
- {identifier}/xml.zip: Individual law XMLs with full text

No authentication required. Data is public domain (amtliche Werke).
"""

import io
import json
import logging
import os
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.gesetze-im-internet.de"
TOC_URL = f"{BASE_URL}/gii-toc.xml"


class GermanLawFetcher:
    """Fetcher for German federal law from gesetze-im-internet.de"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)'
        })

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Fetch the table of contents with all available laws"""
        logger.info(f"Fetching table of contents from {TOC_URL}")
        response = self.session.get(TOC_URL, timeout=60)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.content)
        laws = []

        for item in root.findall('.//item'):
            title_elem = item.find('title')
            link_elem = item.find('link')

            if title_elem is not None and link_elem is not None:
                laws.append({
                    'title': title_elem.text or '',
                    'xml_url': link_elem.text or ''
                })

        logger.info(f"Found {len(laws)} laws in table of contents")
        return laws

    def _download_and_extract_xml(self, xml_url: str) -> Optional[str]:
        """Download a law's XML zip and extract the content"""
        try:
            response = self.session.get(xml_url, timeout=60)
            response.raise_for_status()

            # Extract XML from zip
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                # There should be exactly one XML file
                xml_files = [n for n in zf.namelist() if n.endswith('.xml')]
                if not xml_files:
                    logger.warning(f"No XML file found in {xml_url}")
                    return None

                xml_content = zf.read(xml_files[0])
                return xml_content.decode('utf-8')

        except Exception as e:
            logger.error(f"Error downloading {xml_url}: {e}")
            return None

    def _parse_law_xml(self, xml_content: str) -> Dict[str, Any]:
        """Parse a law's XML and extract metadata and text"""
        root = ET.fromstring(xml_content)

        # Get document number from root attributes
        doknr = root.get('doknr', '')

        # Get basic metadata from first norm element
        metadata = {}
        first_norm = root.find('.//norm/metadaten')
        if first_norm is not None:
            jurabk = first_norm.findtext('jurabk', '')
            langue = first_norm.findtext('langue', '')
            kurzue = first_norm.findtext('kurzue', '')
            ausfertigung_datum = first_norm.findtext('ausfertigung-datum', '')

            # Get publication info
            fundstelle = first_norm.find('fundstelle')
            if fundstelle is not None:
                periodikum = fundstelle.findtext('periodikum', '')
                zitstelle = fundstelle.findtext('zitstelle', '')
            else:
                periodikum = ''
                zitstelle = ''

            metadata = {
                'doknr': doknr,
                'jurabk': jurabk,
                'title': langue or kurzue,
                'short_title': kurzue,
                'date': ausfertigung_datum,
                'publication': f"{periodikum} {zitstelle}".strip()
            }

        # Extract full text from all norm elements
        texts = []
        for norm in root.findall('.//norm'):
            # Get section identifier
            norm_meta = norm.find('metadaten')
            section_id = ''
            if norm_meta is not None:
                enbez = norm_meta.findtext('enbez', '')
                gliederungseinheit = norm_meta.find('gliederungseinheit')
                if gliederungseinheit is not None:
                    gliederungsbez = gliederungseinheit.findtext('gliederungsbez', '')
                    gliederungstitel = gliederungseinheit.findtext('gliederungstitel', '')
                    if gliederungsbez or gliederungstitel:
                        section_id = f"{gliederungsbez} {gliederungstitel}".strip()
                if enbez:
                    section_id = enbez

            # Get text content
            textdaten = norm.find('textdaten')
            if textdaten is not None:
                text_elem = textdaten.find('text')
                if text_elem is not None:
                    content = text_elem.find('Content')
                    if content is not None:
                        # Extract all text, handling nested elements
                        text_parts = self._extract_text_recursive(content)
                        if text_parts:
                            if section_id:
                                texts.append(f"\n{section_id}\n")
                            texts.append(text_parts)

        full_text = '\n'.join(texts)

        # Clean up text
        full_text = self._clean_text(full_text)

        return {
            **metadata,
            'text': full_text
        }

    def _law_metadata(self, root) -> Dict[str, Any]:
        """Metadata of the law itself, taken from its first norm."""
        doknr = root.get('doknr', '')
        first_norm = root.find('.//norm/metadaten')
        if first_norm is None:
            return {'doknr': doknr}

        fundstelle = first_norm.find('fundstelle')
        periodikum = fundstelle.findtext('periodikum', '') if fundstelle is not None else ''
        zitstelle = fundstelle.findtext('zitstelle', '') if fundstelle is not None else ''
        kurzue = first_norm.findtext('kurzue', '')

        return {
            'doknr': doknr,
            'jurabk': first_norm.findtext('jurabk', ''),
            'title': first_norm.findtext('langue', '') or kurzue,
            'short_title': kurzue,
            'date': first_norm.findtext('ausfertigung-datum', ''),
            'publication': f"{periodikum} {zitstelle}".strip(),
        }

    def _parse_law_sections(self, xml_content: str) -> Dict[str, Any]:
        """Split a law's XML into one entry per section (§ / Artikel).

        Issue #1621: the BGB was stored as a single 2.5M-character record and
        never made it into the index, so "§ 195 BGB" resolved to nothing.
        `gliederungskennzahl` is a hierarchical fixed-width code (010, 010010,
        ...), which gives each section its book/chapter breadcrumb.
        """
        root = ET.fromstring(xml_content)
        law = self._law_metadata(root)

        sections = []
        outline = {}  # kennzahl length -> "Buch 1 Allgemeiner Teil"

        for norm in root.findall('.//norm'):
            norm_meta = norm.find('metadaten')
            if norm_meta is None:
                continue

            gliederung = norm_meta.find('gliederungseinheit')
            if gliederung is not None:
                kennzahl = gliederung.findtext('gliederungskennzahl', '') or ''
                label = ' '.join(x for x in (gliederung.findtext('gliederungsbez', ''),
                                             gliederung.findtext('gliederungstitel', '')) if x)
                if kennzahl and label:
                    outline = {k: v for k, v in outline.items() if k < len(kennzahl)}
                    outline[len(kennzahl)] = label.strip()

            enbez = (norm_meta.findtext('enbez', '') or '').strip()
            if not enbez or enbez.lower() in ('inhaltsübersicht', 'inhaltsverzeichnis'):
                continue

            content = norm.find('textdaten/text/Content')
            if content is None:
                continue
            text = self._clean_text(self._extract_text_recursive(content))
            if not text:
                continue

            sections.append({
                'doknr': norm.get('doknr', ''),
                'section': enbez,
                'heading': (norm_meta.findtext('titel', '') or '').strip(),
                'context': ' > '.join(outline[k] for k in sorted(outline)),
                'text': text,
            })

        return {'law': law, 'sections': sections}

    def _extract_text_recursive(self, element) -> str:
        """Recursively extract text from XML elements"""
        parts = []

        if element.text:
            parts.append(element.text.strip())

        for child in element:
            # Handle common XML elements
            tag = child.tag.lower() if isinstance(child.tag, str) else ''

            if tag == 'br':
                parts.append('\n')
            elif tag == 'p':
                child_text = self._extract_text_recursive(child)
                if child_text:
                    parts.append(child_text)
                    parts.append('\n')
            elif tag in ('dl', 'dt', 'dd', 'table', 'tr', 'td', 'th'):
                child_text = self._extract_text_recursive(child)
                if child_text:
                    parts.append(child_text)
                    parts.append(' ')
            else:
                child_text = self._extract_text_recursive(child)
                if child_text:
                    parts.append(child_text)

            if child.tail:
                parts.append(child.tail.strip())

        return ' '.join(parts)

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

        # Remove any remaining XML-like content
        text = re.sub(r'<[^>]+>', '', text)

        return text.strip()

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all German federal laws with full text.

        Args:
            limit: Maximum number of laws to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        laws = self._get_law_list()

        if limit:
            laws = laws[:limit]

        count = 0
        for i, law in enumerate(laws):
            xml_url = law['xml_url']

            logger.info(f"[{i+1}/{len(laws)}] Fetching: {law['title'][:60]}...")

            xml_content = self._download_and_extract_xml(xml_url)
            if not xml_content:
                continue

            for doc in self.expand_law(xml_content, law['title'], xml_url):
                yield doc
                count += 1

            if limit and count >= limit:
                break

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Fetched {count} records with full text")

    def expand_law(self, xml_content: str, toc_title: str, xml_url: str) -> Iterator[Dict[str, Any]]:
        """Yield one raw document per section, or the whole law if it has none."""
        parsed = self._parse_law_sections(xml_content)
        law, sections = parsed['law'], parsed['sections']

        if sections:
            for section in sections:
                yield {
                    'kind': 'section',
                    'law': law,
                    'toc_title': toc_title,
                    'xml_url': xml_url,
                    **section,
                }
            return

        # Short regulations carry their text outside any numbered section.
        whole = self._parse_law_xml(xml_content)
        if whole.get('text') and len(whole['text']) > 100:
            yield {'kind': 'law', 'law': law, 'toc_title': toc_title,
                   'xml_url': xml_url, **whole}

    # Codes used for the validation sample: the sections most often cited, so
    # the sample shows several laws instead of the first law's first pages.
    SAMPLE_LAWS = ['bgb', 'gg', 'stgb', 'hgb', 'stpo', 'ao_1977']

    def fetch_sample(self, per_law: int = 2, min_chars: int = 200) -> Iterator[Dict[str, Any]]:
        """Yield a couple of substantive sections from each well-known code."""
        for slug in self.SAMPLE_LAWS:
            xml_url = f"{BASE_URL}/{slug}/xml.zip"
            xml_content = self._download_and_extract_xml(xml_url)
            if not xml_content:
                continue
            emitted = 0
            for doc in self.expand_law(xml_content, slug.upper(), xml_url):
                if len(doc.get('text', '')) < min_chars:
                    continue
                yield doc
                emitted += 1
                if emitted >= per_law:
                    break
            time.sleep(0.5)

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch all laws (no date filtering available from this source)"""
        yield from self.fetch_all()

    @staticmethod
    def law_slug(xml_url: str, jurabk: str) -> str:
        """The gesetze-im-internet.de path segment, e.g. "bgb"."""
        match = re.search(r'gesetze-im-internet\.de/([^/]+)/xml\.zip', xml_url or '')
        if match:
            return match.group(1)
        return (jurabk or '').lower().replace(' ', '_').replace('/', '_')

    @staticmethod
    def section_anchor(section: str) -> Optional[str]:
        """Filename of a single section page ("§ 195" -> "__195.html")."""
        match = re.match(r'^§+\s*([0-9]+[a-zA-Z]*)', section)
        if match:
            return f"__{match.group(1)}.html"
        match = re.match(r'^Art(?:ikel)?\.?\s*([0-9]+[a-zA-Z]*)', section)
        if match:
            return f"art_{match.group(1)}.html"
        return None

    def normalize_section(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize one § / Artikel of a law."""
        law = raw_doc.get('law', {})
        jurabk = law.get('jurabk', '')
        law_title = law.get('title') or raw_doc.get('toc_title', '')
        section = raw_doc.get('section', '')
        heading = raw_doc.get('heading', '')

        slug = self.law_slug(raw_doc.get('xml_url', ''), jurabk)
        anchor = self.section_anchor(section)
        url = f"https://www.gesetze-im-internet.de/{slug}/"
        if anchor:
            url += anchor

        header = f"{law_title} ({jurabk})" if jurabk else law_title
        if raw_doc.get('context'):
            header += f"\n{raw_doc['context']}"
        label = f"{section} {heading}".strip()
        text = f"{header}\n\n{label}\n\n{raw_doc.get('text', '')}"

        number = ''
        match = re.match(r'^(?:§+|Art(?:ikel)?\.?)\s*([0-9]+[a-zA-Z]*)', section)
        if match:
            number = match.group(1)

        return {
            '_id': raw_doc.get('doknr') or f"{law.get('doknr', '')}/{section}",
            '_source': 'DE/BGBl',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': f"{jurabk} {label}".strip() if jurabk else label,
            'law_title': law_title,
            'short_title': law.get('short_title', ''),
            'abbreviation': jurabk,
            'section': section,
            'section_number': number,
            'heading': heading,
            'context': raw_doc.get('context', ''),
            'text': text,
            'date': self._iso_date(law.get('date', '')),
            'publication': law.get('publication', ''),
            'url': url,
            'law_url': f"https://www.gesetze-im-internet.de/{slug}/",
            'language': 'de',
        }

    @staticmethod
    def _iso_date(date_str: str) -> Optional[str]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            return date_str

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        if raw_doc.get('kind') == 'section':
            return self.normalize_section(raw_doc)

        law = raw_doc.get('law') or raw_doc
        doknr = raw_doc.get('doknr', '') or law.get('doknr', '')
        jurabk = raw_doc.get('jurabk', '') or law.get('jurabk', '')

        # Build URL to the law on gesetze-im-internet.de
        slug = self.law_slug(raw_doc.get('xml_url', ''), jurabk)
        url = f"https://www.gesetze-im-internet.de/{slug}/" if slug else "https://www.gesetze-im-internet.de"

        # Parse date
        date_str = raw_doc.get('date', '') or law.get('date', '')
        if date_str:
            try:
                # Format is typically YYYY-MM-DD
                date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                date = date_str
        else:
            date = None

        return {
            '_id': doknr or jurabk,
            '_source': 'DE/BGBl',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', raw_doc.get('toc_title', '')),
            'law_title': raw_doc.get('title', raw_doc.get('toc_title', '')),
            'short_title': raw_doc.get('short_title', ''),
            'abbreviation': jurabk,
            'section': '',
            'section_number': '',
            'heading': '',
            'context': '',
            'text': raw_doc.get('text', ''),
            'date': date,
            'publication': raw_doc.get('publication', ''),
            'url': url,
            'law_url': url,
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = GermanLawFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 100

        stream = (fetcher.fetch_sample() if '--sample' in sys.argv
                  else fetcher.fetch_all(limit=target_count + 20))
        for raw_doc in stream:
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['abbreviation']} - {normalized['title'][:50]} ({text_len} chars)")
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

    elif len(sys.argv) > 1 and sys.argv[1] == 'bootstrap-fast':
        # ── Fast bootstrap using concurrent downloads + batched writes ──
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Add project root to path for imports
        # sources/DE/BGBl/bootstrap.py -> sources/DE/BGBl -> sources/DE -> sources -> project_root
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        sys.path.insert(0, str(project_root))
        from common.storage import StorageManager
        from common.rate_limiter import AdaptiveRateLimiter

        fetcher = GermanLawFetcher()
        source_dir = Path(__file__).parent
        storage = StorageManager(source_dir / 'data')

        # Parse args
        max_workers = 10
        batch_size = 100
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == '--workers' and i + 1 < len(sys.argv):
                max_workers = int(sys.argv[i + 1])
            if arg == '--batch-size' and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])

        rate_limiter = AdaptiveRateLimiter(
            start_rate=5.0, min_rate=0.5, max_rate=50.0, burst=10
        )

        logger.info(f"Starting fast bootstrap (workers={max_workers}, batch={batch_size})...")

        # Phase 1: Get TOC (one request)
        laws = fetcher._get_law_list()
        logger.info(f"TOC loaded: {len(laws)} laws to fetch")

        # Phase 2: Concurrent download + parse + normalize
        stats = {"fetched": 0, "new": 0, "skipped": 0, "errors": 0}
        batch = []

        def _fetch_and_normalize(law):
            """Download, parse, normalize one law (runs in worker thread)."""
            try:
                xml_content = fetcher._download_and_extract_xml(law['xml_url'])
                if not xml_content:
                    return None
                records = [fetcher.normalize(doc) for doc in
                           fetcher.expand_law(xml_content, law['title'], law['xml_url'])]
                records = [r for r in records if len(r.get('text', '')) >= 100]
                return records or None
            except Exception as e:
                logger.warning(f"Error processing {law.get('title', '?')[:40]}: {e}")
                return None

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for i, law in enumerate(laws):
                rate_limiter.wait()
                future = executor.submit(_fetch_and_normalize, law)
                futures[future] = law

                # Drain completed futures
                if len(futures) >= max_workers * 2:
                    done = [f for f in futures if f.done()]
                    if not done:
                        done = [next(as_completed(futures.keys()))]

                    for fut in done:
                        del futures[fut]
                        records = fut.result()
                        if not records:
                            stats["errors"] += 1
                            continue

                        for record in records:
                            stats["fetched"] += 1
                            dedup_key = record['_id']

                            if storage.exists(dedup_key):
                                stats["skipped"] += 1
                            else:
                                batch.append((dedup_key, record))
                                stats["new"] += 1

                        if len(batch) >= batch_size:
                            storage.write_batch(batch)
                            elapsed = time.time() - start_time
                            rate = stats["fetched"] / elapsed if elapsed > 0 else 0
                            logger.info(
                                f"Progress: {stats['fetched']}/{len(laws)} fetched, "
                                f"{stats['new']} new, {stats['errors']} errors, "
                                f"{rate:.1f} rec/s, RL: {rate_limiter.stats().get('current_rate', '?')} req/s"
                            )
                            batch = []

            # Drain remaining
            for fut in as_completed(futures.keys()):
                records = fut.result()
                if not records:
                    stats["errors"] += 1
                    continue
                for record in records:
                    stats["fetched"] += 1
                    dedup_key = record['_id']
                    if storage.exists(dedup_key):
                        stats["skipped"] += 1
                    else:
                        batch.append((dedup_key, record))
                        stats["new"] += 1

        # Final batch
        if batch:
            storage.write_batch(batch)
        storage.flush()

        elapsed = time.time() - start_time
        rate = stats["fetched"] / elapsed if elapsed > 0 else 0

        print(f"\n{'='*60}")
        print(f"FAST BOOTSTRAP COMPLETE: DE/BGBl")
        print(f"{'='*60}")
        print(f"Records built: {stats['fetched']} from {len(laws)} laws")
        print(f"New records:   {stats['new']}")
        print(f"Skipped:       {stats['skipped']}")
        print(f"Errors:        {stats['errors']}")
        print(f"Duration:      {elapsed:.0f}s ({elapsed/60:.1f}m)")
        print(f"Throughput:    {rate:.1f} records/sec")
        print(f"Rate limiter:  {rate_limiter.stats()}")
        print(f"Storage:       {storage.count()} total records in index")

    else:
        # Test mode
        fetcher = GermanLawFetcher()
        print("Testing German Law fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Abbreviation: {normalized['abbreviation']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
