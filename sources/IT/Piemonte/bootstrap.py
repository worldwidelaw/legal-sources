#!/usr/bin/env python3
"""
IT/Piemonte -- Regional Legislation of Piemonte Data Fetcher

Fetches regional legislation from Piemonte via Arianna database XML API.

Strategy:
  - Iterate through law numbers (1-N) for each year
  - Fetch XML full text using NIR (Norme in Rete) format
  - Parse XML to extract full text content
  - HTTP 302 response indicates law doesn't exist (end of sequence)

Endpoints:
  - XML API: http://arianna.consiglioregionale.piemonte.it/ariaint/TESTO?LAYOUT=PRESENTAZIONE&TIPODOC=LEGGI&LEGGE={num}&LEGGEANNO={year}&TIPOVISUAL=XML

Data:
  - Legislation types: Legge Regionale
  - Coverage: 1970s to present
  - License: CC0 1.0 Universal (Italian public sector information)

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
from typing import Generator, Optional
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
logger = logging.getLogger("legal-data-hunter.IT.piemonte")

# API URL
XML_API_URL = "http://arianna.consiglioregionale.piemonte.it/ariaint/TESTO"

# Year range - Piemonte region created 1970, laws since then
START_YEAR = 1970
CURRENT_YEAR = datetime.now().year


class PiemonteScraper(BaseScraper):
    """
    Scraper for IT/Piemonte -- Piemonte Regional Legislation.
    Country: IT
    URL: https://arianna.consiglioregionale.piemonte.it/

    Data types: legislation
    Auth: none (CC0 1.0 Universal)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            "Accept": "application/xml, text/xml, */*",
        })

    @staticmethod
    def _parse_norm_date(norm: str) -> str:
        """Parse 8-digit date that may be YYYYMMDD or DDMMYYYY."""
        # If first 4 chars look like a year (19xx or 20xx), treat as YYYYMMDD
        if norm[:2] in ('19', '20'):
            return f"{norm[:4]}-{norm[4:6]}-{norm[6:8]}"
        # Otherwise DDMMYYYY
        return f"{norm[4:8]}-{norm[2:4]}-{norm[0:2]}"

    def _extract_text_from_xml(self, xml_content: str) -> tuple[str, dict]:
        """
        Parse NIR (Norme in Rete) XML and extract full text content and metadata.

        Returns tuple of (full_text, metadata_dict).
        """
        metadata = {}

        try:
            # Replace common HTML entities with Unicode before parsing
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

            # Extract metadata from intestazione
            for elem in root.iter():
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

                if tag_name == 'titoloDoc' and elem.text:
                    metadata['title'] = elem.text.strip()
                elif tag_name == 'numDoc' and elem.text:
                    metadata['law_number'] = elem.text.strip()
                elif tag_name == 'dataDoc':
                    norm = elem.get('norm', '')
                    if norm and len(norm) == 8:
                        metadata['date'] = self._parse_norm_date(norm)
                    elif elem.text:
                        metadata['date_str'] = elem.text.strip()
                elif tag_name == 'pubblicazione':
                    norm = elem.get('norm', '')
                    if norm and len(norm) == 8:
                        metadata['date_bur'] = self._parse_norm_date(norm)
                    metadata['bur_number'] = elem.get('num', '')
                elif tag_name == 'urn':
                    metadata['urn'] = elem.get('valore', '')

            text_parts = []

            def extract_text(elem, depth=0):
                """Recursively extract text from element."""
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag

                # Skip metadata elements
                if tag_name in ('meta', 'descrittori', 'pubblicazione', 'redazione', 'urn', 'intestazione'):
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

            # Find the articolato section (main content)
            for elem in root.iter():
                tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                if tag_name in ('articolato', 'formulainiziale', 'formulafinale'):
                    extract_text(elem)

            # Join and clean up
            full_text = ' '.join(text_parts)
            full_text = re.sub(r'\s+', ' ', full_text)
            full_text = re.sub(r'\s+\n', '\n', full_text)
            full_text = re.sub(r'\n\s+', '\n', full_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)

            return full_text.strip(), metadata

        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            # Fallback: strip tags and extract text
            text = re.sub(r'<[^>]+>', ' ', xml_content)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip(), metadata
        except Exception as e:
            logger.error(f"Failed to extract text from XML: {e}")
            return "", metadata

    def _fetch_law(self, year: int, number: int) -> Optional[dict]:
        """
        Fetch a single law by year and number.

        Returns dict with full_text and metadata, or None if law doesn't exist.
        """
        params = {
            "LAYOUT": "PRESENTAZIONE",
            "TIPODOC": "LEGGI",
            "LEGGE": number,
            "LEGGEANNO": year,
            "TIPOVISUAL": "XML",
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.get(XML_API_URL, params=params, timeout=60, allow_redirects=False)

            # 302 redirect means law doesn't exist
            if resp.status_code == 302:
                return None

            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for law {number}/{year}")
                return None

            content = resp.text

            # Check if it's valid XML
            if not content.strip().startswith('<?xml'):
                logger.warning(f"Invalid XML response for law {number}/{year}")
                return None

            full_text, metadata = self._extract_text_from_xml(content)

            if not full_text or len(full_text) < 100:
                logger.warning(f"Text too short for law {number}/{year}: {len(full_text) if full_text else 0} chars")
                return None

            return {
                "year": year,
                "number": number,
                "full_text": full_text,
                **metadata,
            }

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching law {number}/{year}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch law {number}/{year}: {e}")
            return None

    def _get_max_law_number(self, year: int) -> int:
        """
        Find the maximum law number for a given year.
        Uses binary search with fallback to linear scan.
        """
        # Try common max values first
        for test_num in [50, 30, 20, 10]:
            params = {
                "LAYOUT": "PRESENTAZIONE",
                "TIPODOC": "LEGGI",
                "LEGGE": test_num,
                "LEGGEANNO": year,
                "TIPOVISUAL": "XML",
            }
            try:
                self.rate_limiter.wait()
                resp = self.session.get(XML_API_URL, params=params, timeout=30, allow_redirects=False)
                if resp.status_code == 200:
                    # Found one that exists, search upward
                    max_found = test_num
                    for n in range(test_num + 1, test_num + 50):
                        self.rate_limiter.wait()
                        params["LEGGE"] = n
                        resp = self.session.get(XML_API_URL, params=params, timeout=30, allow_redirects=False)
                        if resp.status_code == 200:
                            max_found = n
                        else:
                            break
                    return max_found
            except Exception:
                pass

        # Linear scan from 1
        max_found = 0
        for n in range(1, 100):
            params = {
                "LAYOUT": "PRESENTAZIONE",
                "TIPODOC": "LEGGI",
                "LEGGE": n,
                "LEGGEANNO": year,
                "TIPOVISUAL": "XML",
            }
            try:
                self.rate_limiter.wait()
                resp = self.session.get(XML_API_URL, params=params, timeout=30, allow_redirects=False)
                if resp.status_code == 200:
                    max_found = n
                elif resp.status_code == 302 and max_found > 0:
                    # We've found all laws
                    break
            except Exception:
                break

        return max_found

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Piemonte.

        Iterates through years from START_YEAR to present.
        For each year, iterates through law numbers until 302 response.
        """
        for year in range(START_YEAR, CURRENT_YEAR + 1):
            logger.info(f"Fetching laws for year {year}...")
            law_count = 0

            for number in range(1, 200):  # Max 200 laws per year (safety limit)
                record = self._fetch_law(year, number)

                if record is None:
                    # No more laws for this year
                    logger.info(f"Year {year}: found {law_count} laws")
                    break

                law_count += 1
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent years.

        Fetches all laws from the year of 'since' to present.
        """
        start_year = since.year

        for year in range(start_year, CURRENT_YEAR + 1):
            logger.info(f"Fetching updates for year {year}...")

            for number in range(1, 200):
                record = self._fetch_law(year, number)

                if record is None:
                    break

                yield record

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        year = raw.get("year", "")
        number = raw.get("number", "")

        # Generate document ID
        normalized_id = f"piemonte_lr_{year}_{number}"

        # Get or construct title
        title = raw.get("title", "")
        if not title:
            title = f"Legge Regionale {number}/{year}"

        # Parse date
        date_iso = raw.get("date", "")

        # Build URL
        url = f"http://arianna.consiglioregionale.piemonte.it/ariaint/TESTO?LAYOUT=PRESENTAZIONE&TIPODOC=LEGGI&LEGGE={number}&LEGGEANNO={year}"

        return {
            # Required base fields
            "_id": normalized_id,
            "_source": "IT/Piemonte",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": url,
            # Additional metadata
            "law_number": str(number),
            "year": str(year),
            "bur_number": raw.get("bur_number", ""),
            "date_bur": raw.get("date_bur", ""),
            "urn": raw.get("urn", ""),
            "document_type": "Legge Regionale",
            "language": "it",
            "region": "Piemonte",
            "country": "IT",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Piemonte Arianna XML API...")

        # Test 1: Fetch a known law
        print("\n1. Testing XML API (law 5/2024)...")
        try:
            record = self._fetch_law(2024, 5)
            if record:
                print(f"   Title: {record.get('title', 'N/A')[:60]}...")
                print(f"   Date: {record.get('date', 'N/A')}")
                print(f"   URN: {record.get('urn', 'N/A')}")
                text_len = len(record.get('full_text', ''))
                print(f"   Text length: {text_len:,} characters")
                if text_len > 0:
                    print(f"   Text preview: {record['full_text'][:200]}...")
            else:
                print("   ERROR: Failed to fetch law")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 2: Count laws for recent years
        print("\n2. Counting laws per year (recent)...")
        try:
            for year in [2020, 2022, 2024]:
                count = 0
                for n in range(1, 100):
                    params = {
                        "LAYOUT": "PRESENTAZIONE",
                        "TIPODOC": "LEGGI",
                        "LEGGE": n,
                        "LEGGEANNO": year,
                        "TIPOVISUAL": "XML",
                    }
                    self.rate_limiter.wait()
                    resp = self.session.get(XML_API_URL, params=params, timeout=30, allow_redirects=False)
                    if resp.status_code == 200:
                        count += 1
                    elif resp.status_code == 302:
                        break
                print(f"   {year}: {count} laws")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 3: Verify full text extraction
        print("\n3. Testing full text extraction...")
        try:
            record = self._fetch_law(2023, 1)
            if record and record.get('full_text'):
                print(f"   Law 1/2023 text: {len(record['full_text']):,} chars")
                # Check for article markers
                if 'Art.' in record['full_text'] or 'articolo' in record['full_text'].lower():
                    print("   ✓ Contains article markers")
                else:
                    print("   ✗ No article markers found")
            else:
                print("   ERROR: No full text extracted")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = PiemonteScraper()

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
