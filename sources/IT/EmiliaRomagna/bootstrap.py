#!/usr/bin/env python3
"""
IT/EmiliaRomagna -- Regional Legislation of Emilia-Romagna Data Fetcher

Fetches regional legislation from Emilia-Romagna via Demetra database ZIP downloads.

Strategy:
  - Download ZIP archives from Demetra database (XML format)
  - Parse NIR (Norme in Rete) XML to extract full text
  - Process all legislatures (1-12) and document types (LR, RR, RI)

Endpoints:
  - Download: https://demetra.regione.emilia-romagna.it/al/public/resources/themes/simple/class/download_archive.php
    Parameters: type=zip, formato=xml, tipo=LR|RR|RI, metaleg=1-12

Data:
  - Document types: Legge Regionale (LR), Regolamento Regionale (RR), Regolamento Interno (RI)
  - Coverage: 1971-present
  - License: CC BY 4.0

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent legislatures)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import zipfile
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from xml.etree import ElementTree as ET

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.emiliaromagna")

# API URLs
BASE_URL = "https://demetra.regione.emilia-romagna.it/al"
DOWNLOAD_URL = f"{BASE_URL}/public/resources/themes/simple/class/download_archive.php"

# Legislatures (1-12, XII is current 2020-2025)
LEGISLATURES = list(range(1, 13))

# Document types
DOC_TYPES = {
    "LR": "Legge Regionale",
    "RR": "Regolamento Regionale",
    "RI": "Regolamento Interno",
}


class EmiliaRomagnaScraper(BaseScraper):
    """
    Scraper for IT/EmiliaRomagna -- Emilia-Romagna Regional Legislation.
    Country: IT
    URL: https://demetra.regione.emilia-romagna.it

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "application/zip",
        })

    def _extract_text_from_xml(self, xml_content: str) -> str:
        """
        Parse NIR (Norme in Rete) XML and extract full text content.
        Same format as IT/Lombardia.
        """
        try:
            # Replace common HTML entities
            entity_map = {
                '&agrave;': 'à', '&egrave;': 'è', '&igrave;': 'ì', '&ograve;': 'ò', '&ugrave;': 'ù',
                '&Agrave;': 'À', '&Egrave;': 'È', '&Igrave;': 'Ì', '&Ograve;': 'Ò', '&Ugrave;': 'Ù',
                '&aacute;': 'á', '&eacute;': 'é', '&iacute;': 'í', '&oacute;': 'ó', '&uacute;': 'ú',
                '&nbsp;': ' ', '&laquo;': '«', '&raquo;': '»', '&deg;': '°',
                '&euro;': '€', '&pound;': '£', '&copy;': '©', '&reg;': '®',
                '&mdash;': '—', '&ndash;': '–', '&hellip;': '…',
                '&quot;': '"', '&apos;': "'", '&amp;': '&',
                '&rsquo;': '\u2019', '&lsquo;': '\u2018', '&rdquo;': '\u201D', '&ldquo;': '\u201C',
                '&bull;': '•', '&middot;': '·', '&times;': '×', '&divide;': '÷',
                '&sect;': '§', '&para;': '¶', '&dagger;': '†',
            }
            for entity, char in entity_map.items():
                xml_content = xml_content.replace(entity, char)

            # Handle numeric entities
            xml_content = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), xml_content)
            xml_content = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), xml_content)

            root = ET.fromstring(xml_content)
            text_parts = []

            def extract_text(elem, depth=0):
                """Recursively extract text from element."""
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

                # Skip metadata elements
                if tag_name in ('meta', 'descrittori', 'pubblicazione', 'redazione', 'urn',
                                'vigenza', 'relazioni', 'proprietario', 'disposizioni', 'keywords'):
                    return

                # Handle specific elements
                if tag_name in ('tipoDoc', 'dataDoc', 'numDoc'):
                    if elem.text:
                        text_parts.append(elem.text.strip())
                    return

                if tag_name == 'titoloDoc':
                    if elem.text:
                        text_parts.append(f"\n{elem.text.strip()}\n")
                    for child in elem:
                        extract_text(child, depth + 1)
                    if elem.tail and elem.tail.strip():
                        text_parts.append(elem.tail.strip())
                    return

                if tag_name == 'articolo':
                    text_parts.append("\n")

                if tag_name == 'num':
                    if elem.text:
                        text_parts.append(f"\n{elem.text.strip()} ")
                    return

                if tag_name == 'rubrica':
                    if elem.text:
                        text_parts.append(f"({elem.text.strip()})\n")
                    return

                if tag_name == 'comma':
                    text_parts.append("\n")

                # Handle element text
                if elem.text and elem.text.strip():
                    text_parts.append(elem.text.strip())

                # Process children
                for child in elem:
                    extract_text(child, depth + 1)

                # Handle tail text
                if elem.tail and elem.tail.strip():
                    text_parts.append(" " + elem.tail.strip())

            extract_text(root)

            # Join and clean up
            full_text = ' '.join(text_parts)
            full_text = re.sub(r'\s+', ' ', full_text)
            full_text = re.sub(r'\s+\n', '\n', full_text)
            full_text = re.sub(r'\n\s+', '\n', full_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)

            return full_text.strip()

        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            # Fallback: strip tags
            text = re.sub(r'<[^>]+>', ' ', xml_content)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception as e:
            logger.error(f"Failed to extract text from XML: {e}")
            return ""

    def _extract_metadata_from_xml(self, xml_content: str) -> dict:
        """Extract metadata (URN, date, title) from NIR XML."""
        metadata = {}
        try:
            root = ET.fromstring(xml_content)

            # Find URN
            for urn_elem in root.iter():
                if urn_elem.tag.endswith('urn') or urn_elem.tag == 'urn':
                    if urn_elem.text:
                        metadata['urn'] = urn_elem.text.strip()
                        break

            # Find date and title from intestazione
            for elem in root.iter():
                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

                if tag == 'dataDoc':
                    norm = elem.get('norm', '')
                    if norm:
                        # Format: YYYYMMDD -> YYYY-MM-DD
                        if len(norm) == 8:
                            metadata['date'] = f"{norm[:4]}-{norm[4:6]}-{norm[6:8]}"
                        else:
                            metadata['date'] = norm
                    elif elem.text:
                        metadata['date_text'] = elem.text.strip()

                if tag == 'numDoc' and elem.text:
                    metadata['number'] = elem.text.strip()

                if tag == 'titoloDoc' and elem.text:
                    # Get full title including child text
                    title_parts = [elem.text.strip()] if elem.text else []
                    for child in elem.iter():
                        if child.text and child != elem:
                            title_parts.append(child.text.strip())
                        if child.tail:
                            title_parts.append(child.tail.strip())
                    metadata['title'] = ' '.join(title_parts).strip()

                if tag == 'tipoDoc' and elem.text:
                    metadata['doc_type_text'] = elem.text.strip()

        except Exception as e:
            logger.warning(f"Metadata extraction error: {e}")

        return metadata

    def _download_zip(self, legislature: int, doc_type: str) -> Optional[bytes]:
        """
        Download ZIP archive for a legislature and document type.

        Returns ZIP content as bytes or None on failure.
        """
        params = {
            "type": "zip",
            "formato": "xml",
            "tipo": doc_type,
            "metaleg": str(legislature),
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.get(DOWNLOAD_URL, params=params, timeout=120)

            if resp.status_code != 200:
                logger.warning(f"ZIP download failed for leg={legislature}, type={doc_type}: HTTP {resp.status_code}")
                return None

            # Check content type
            content_type = resp.headers.get('Content-Type', '')
            if 'zip' not in content_type.lower() and 'octet-stream' not in content_type.lower():
                logger.warning(f"Unexpected content type: {content_type}")
                # Still try to process - some servers don't set correct content type

            if len(resp.content) < 100:
                logger.info(f"Empty or small response for leg={legislature}, type={doc_type}")
                return None

            return resp.content

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout downloading ZIP for leg={legislature}, type={doc_type}")
            return None
        except Exception as e:
            logger.error(f"Failed to download ZIP: {e}")
            return None

    def _process_zip(self, zip_content: bytes, legislature: int, doc_type: str) -> Generator[dict, None, None]:
        """
        Process ZIP archive and yield parsed documents.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                # Get list of XML files
                xml_files = [f for f in zf.namelist() if f.endswith('.xml')]
                logger.info(f"Found {len(xml_files)} XML files in ZIP for leg={legislature}, type={doc_type}")

                for xml_filename in xml_files:
                    try:
                        xml_content = zf.read(xml_filename).decode('utf-8')

                        # Extract metadata
                        metadata = self._extract_metadata_from_xml(xml_content)

                        # Extract full text
                        full_text = self._extract_text_from_xml(xml_content)

                        if not full_text or len(full_text) < 50:
                            logger.warning(f"Skipping {xml_filename}: insufficient text ({len(full_text)} chars)")
                            continue

                        # Parse filename for additional info
                        # Format: {legislature}_{type}_{year}_{number}.xml
                        match = re.match(r'(\d+)_([A-Z]+)_(\d{4})_(\d+)\.xml', xml_filename)
                        if match:
                            metadata['legislature'] = int(match.group(1))
                            metadata['doc_type_code'] = match.group(2)
                            metadata['year'] = match.group(3)
                            metadata['doc_number'] = match.group(4)

                        yield {
                            'filename': xml_filename,
                            'legislature': legislature,
                            'doc_type': doc_type,
                            'doc_type_name': DOC_TYPES.get(doc_type, doc_type),
                            'full_text': full_text,
                            **metadata,
                        }

                    except Exception as e:
                        logger.error(f"Error processing {xml_filename}: {e}")
                        continue

        except zipfile.BadZipFile:
            logger.error(f"Invalid ZIP file for leg={legislature}, type={doc_type}")
        except Exception as e:
            logger.error(f"Error processing ZIP: {e}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Emilia-Romagna.

        Downloads ZIP archives for all legislatures and document types.
        """
        for legislature in LEGISLATURES:
            for doc_type in DOC_TYPES.keys():
                logger.info(f"Fetching legislature {legislature}, type {doc_type}...")

                zip_content = self._download_zip(legislature, doc_type)
                if not zip_content:
                    continue

                yield from self._process_zip(zip_content, legislature, doc_type)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent legislatures.

        Since we download full ZIP archives, we focus on recent legislatures only.
        """
        # Focus on current and previous legislature for updates
        recent_legislatures = [11, 12]  # XI (2015-2020) and XII (2020-2025)

        for legislature in recent_legislatures:
            for doc_type in DOC_TYPES.keys():
                logger.info(f"Fetching updates from legislature {legislature}, type {doc_type}...")

                zip_content = self._download_zip(legislature, doc_type)
                if not zip_content:
                    continue

                for doc in self._process_zip(zip_content, legislature, doc_type):
                    # Check if document date is after 'since'
                    doc_date = doc.get('date')
                    if doc_date:
                        try:
                            doc_dt = datetime.strptime(doc_date, "%Y-%m-%d")
                            if doc_dt.replace(tzinfo=timezone.utc) >= since:
                                yield doc
                        except ValueError:
                            yield doc  # Include if we can't parse date
                    else:
                        yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        # Build unique ID
        urn = raw.get('urn', '')
        if not urn:
            # Fallback: construct from filename
            urn = f"urn:nir:regione.emilia.romagna:{raw.get('doc_type', 'legge').lower()}:{raw.get('date', '')}:{raw.get('doc_number', '')}"

        # Generate document ID
        doc_id = urn.replace(':', '_').replace(';', '_').replace('/', '_')
        if not doc_id:
            doc_id = raw.get('filename', '').replace('.xml', '')

        # Parse title
        title = raw.get('title', '')
        if not title:
            title = raw.get('doc_type_name', 'Legge Regionale')
            if raw.get('number'):
                title += f" n. {raw['number']}"
            if raw.get('date'):
                title += f" del {raw['date']}"

        # Build URL
        url = f"https://demetra.regione.emilia-romagna.it/al/ricerca/scheda/leggi-e-regolamenti?urn={urn}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IT/EmiliaRomagna",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get('full_text', ''),  # MANDATORY FULL TEXT
            "date": raw.get('date', ''),
            "url": url,
            # Additional metadata
            "urn": urn,
            "document_type": raw.get('doc_type_name', ''),
            "document_type_code": raw.get('doc_type', ''),
            "number": raw.get('number', raw.get('doc_number', '')),
            "legislature": raw.get('legislature', ''),
            "year": raw.get('year', ''),
            "language": "it",
            "region": "Emilia-Romagna",
            "country": "IT",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Emilia-Romagna Demetra endpoints...")

        # Test download endpoint with current legislature
        print("\n1. Testing ZIP download (Legislature XII, LR)...")
        try:
            zip_content = self._download_zip(12, "LR")
            if zip_content:
                print(f"   Downloaded {len(zip_content):,} bytes")

                # Try to open and list contents
                with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                    files = zf.namelist()
                    xml_files = [f for f in files if f.endswith('.xml')]
                    pdf_files = [f for f in files if f.endswith('.pdf')]
                    print(f"   ZIP contains: {len(xml_files)} XML files, {len(pdf_files)} PDF attachments")

                    if xml_files:
                        # Parse first XML
                        xml_content = zf.read(xml_files[0]).decode('utf-8')
                        metadata = self._extract_metadata_from_xml(xml_content)
                        text = self._extract_text_from_xml(xml_content)
                        print(f"\n   Sample document: {xml_files[0]}")
                        print(f"   URN: {metadata.get('urn', 'N/A')}")
                        print(f"   Title: {metadata.get('title', 'N/A')[:60]}...")
                        print(f"   Date: {metadata.get('date', 'N/A')}")
                        print(f"   Text length: {len(text):,} characters")
                        if text:
                            print(f"   Text sample: {text[:200]}...")
            else:
                print("   ERROR: No ZIP content returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test older legislature
        print("\n2. Testing ZIP download (Legislature X, LR)...")
        try:
            zip_content = self._download_zip(10, "LR")
            if zip_content:
                with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                    xml_files = [f for f in zf.namelist() if f.endswith('.xml')]
                    print(f"   Legislature X has {len(xml_files)} laws")
            else:
                print("   No content for Legislature X")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = EmiliaRomagnaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
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
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
