#!/usr/bin/env python3
"""
IT/Lombardia -- Regional Legislation of Lombardia Data Fetcher

Fetches regional legislation from Lombardia via Socrata SODA API + XML full text.

Strategy:
  - Use Socrata SODA API at dati.lombardia.it to get legislation metadata.
  - For each record, fetch full XML text from normelombardia.consiglio.regione.lombardia.it.
  - Parse XML (NIR format - Norme in Rete) to extract full text content.

Endpoints:
  - SODA API: https://www.dati.lombardia.it/resource/abjw-hhay.json
  - XML full text: http://normelombardia.consiglio.regione.lombardia.it/normelombardia/accessibile/xmain_xml.aspx?view=showdoc&iddoc={id}

Data:
  - Legislation types: Legge Regionale, Regolamento Regionale
  - 2,658+ records
  - License: CC0 1.0 Universal (Public Domain Dedication)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any
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
logger = logging.getLogger("legal-data-hunter.IT.lombardia")

# API URLs
SOCRATA_API = "https://www.dati.lombardia.it/resource/abjw-hhay.json"
XML_BASE_URL = "http://normelombardia.consiglio.regione.lombardia.it/normelombardia/accessibile/xmain_xml.aspx"


class LombardiaScraper(BaseScraper):
    """
    Scraper for IT/Lombardia -- Lombardia Regional Legislation.
    Country: IT
    URL: https://www.dati.lombardia.it/government/CRL-Leggi-Regionali-della-Lombardia/abjw-hhay

    Data types: legislation
    Auth: none (CC0 1.0 Universal)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _extract_text_from_xml(self, xml_content: str) -> str:
        """
        Parse NIR (Norme in Rete) XML and extract full text content.

        The XML structure includes articles, paragraphs, and legal references.
        """
        try:
            # Replace common HTML entities with Unicode before parsing
            # The NIR XML often contains HTML entities that aren't defined in XML
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

            # Also handle numeric entities
            xml_content = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), xml_content)
            xml_content = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), xml_content)

            # Parse XML
            root = ET.fromstring(xml_content)

            # Define namespace mappings (NIR uses custom namespaces)
            namespaces = {
                'h': 'http://www.w3.org/HTML/1998/html4',
                'xlink': 'http://www.w3.org/1999/xlink',
            }

            text_parts = []

            def extract_text(elem, depth=0):
                """Recursively extract text from element."""
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

                # Skip metadata elements
                if tag_name in ('meta', 'descrittori', 'pubblicazione', 'redazione', 'urn'):
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

                # Handle tail text (text after closing tag)
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
            # Fallback: strip tags and extract text
            text = re.sub(r'<[^>]+>', ' ', xml_content)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception as e:
            logger.error(f"Failed to extract text from XML: {e}")
            return ""

    def _fetch_xml_text(self, doc_id: str) -> str:
        """
        Fetch XML full text for a document.

        Returns the extracted text content or empty string on failure.
        """
        url = f"{XML_BASE_URL}?view=showdoc&iddoc={doc_id}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)

            if resp.status_code != 200:
                logger.warning(f"XML fetch failed for {doc_id}: HTTP {resp.status_code}")
                return ""

            content = resp.text

            # Check if it's valid XML
            if not content.strip().startswith('<?xml'):
                logger.warning(f"Invalid XML response for {doc_id}")
                return ""

            return self._extract_text_from_xml(content)

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching XML for {doc_id}")
            return ""
        except Exception as e:
            logger.error(f"Failed to fetch XML for {doc_id}: {e}")
            return ""

    def _fetch_soda_records(self, offset: int = 0, limit: int = 1000) -> list:
        """
        Fetch records from Socrata SODA API.

        Returns list of records or empty list on failure.
        """
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "data_legge DESC",  # Most recent first
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.get(SOCRATA_API, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"SODA API error at offset {offset}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Lombardia.

        Fetches metadata via SODA API, then full XML text for each record.
        """
        offset = 0
        batch_size = 100

        while True:
            logger.info(f"Fetching SODA records at offset {offset}...")
            records = self._fetch_soda_records(offset=offset, limit=batch_size)

            if not records:
                logger.info("No more records from SODA API")
                break

            for record in records:
                doc_id = record.get("ids", "")
                if not doc_id:
                    continue

                # Fetch full text from XML endpoint
                full_text = self._fetch_xml_text(doc_id)

                if not full_text:
                    logger.warning(f"No full text for {doc_id}, skipping")
                    continue

                if len(full_text) < 100:
                    logger.warning(f"Text too short for {doc_id} ({len(full_text)} chars), skipping")
                    continue

                record["full_text"] = full_text
                yield record

            offset += batch_size

            if len(records) < batch_size:
                logger.info("Reached end of records")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses data_ultima_modifica field for filtering.
        """
        since_str = since.strftime("%Y-%m-%dT00:00:00.000")

        params = {
            "$limit": 1000,
            "$where": f"data_ultima_modifica >= '{since_str}'",
            "$order": "data_ultima_modifica DESC",
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.get(SOCRATA_API, params=params, timeout=60)
            resp.raise_for_status()
            records = resp.json()

            logger.info(f"Found {len(records)} records modified since {since_str[:10]}")

            for record in records:
                doc_id = record.get("ids", "")
                if not doc_id:
                    continue

                full_text = self._fetch_xml_text(doc_id)
                if not full_text or len(full_text) < 100:
                    continue

                record["full_text"] = full_text
                yield record

        except Exception as e:
            logger.error(f"Update fetch failed: {e}")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("ids", "")
        title = raw.get("titolo", "")
        estremi = raw.get("estremi", "")  # e.g., "Legge Regionale 10 febbraio 2026 n. 4"
        full_text = raw.get("full_text", "")

        # Parse date
        date_legge = raw.get("data_legge", "")
        if date_legge:
            # Convert "2026-02-10T00:00:00.000" to "2026-02-10"
            date_legge = date_legge[:10]

        # Document type from estremi
        doc_type = "Legge Regionale"
        if estremi:
            if estremi.lower().startswith("regolamento"):
                doc_type = "Regolamento Regionale"
            elif estremi.lower().startswith("legge"):
                doc_type = "Legge Regionale"

        # Build URLs
        html_url = ""
        link_html = raw.get("link_testo_html", {})
        if isinstance(link_html, dict):
            html_url = link_html.get("url", "")
        elif isinstance(link_html, str):
            html_url = link_html

        if not html_url:
            html_url = f"http://normelombardia.consiglio.regione.lombardia.it/normelombardia/accessibile/main.aspx?view=showdoc&iddoc={doc_id}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IT/Lombardia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_legge,
            "url": html_url,
            # Additional metadata
            "estremi": estremi,
            "document_type": doc_type,
            "numero_legge": raw.get("numero_legge", ""),
            "data_burl": raw.get("data_burl", "")[:10] if raw.get("data_burl") else "",
            "numero_burl": raw.get("numero_burl", ""),
            "stato_legge": raw.get("stato_legge", ""),
            "language": "it",
            "region": "Lombardia",
            "country": "IT",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Lombardia legislation endpoints...")

        # Test SODA API
        print("\n1. Testing Socrata SODA API...")
        try:
            records = self._fetch_soda_records(limit=3)
            print(f"   Found {len(records)} sample records")
            if records:
                r = records[0]
                print(f"   Sample ID: {r.get('ids', 'N/A')}")
                print(f"   Title: {r.get('titolo', 'N/A')[:60]}...")
                print(f"   Date: {r.get('data_legge', 'N/A')[:10]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test XML endpoint
        print("\n2. Testing XML full text endpoint...")
        if records:
            doc_id = records[0].get("ids", "")
            try:
                full_text = self._fetch_xml_text(doc_id)
                print(f"   Document ID: {doc_id}")
                print(f"   Text length: {len(full_text)} characters")
                if full_text:
                    print(f"   Text sample: {full_text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        # Test record count
        print("\n3. Checking total record count...")
        try:
            resp = self.session.get(
                SOCRATA_API.replace('.json', '.json'),
                params={"$select": "count(*) as total"},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                print(f"   Total records: {data[0].get('total', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = LombardiaScraper()

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
