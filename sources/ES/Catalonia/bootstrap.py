#!/usr/bin/env python3
"""
ES/Catalonia -- Catalan Regional Legislation Data Fetcher

Fetches regional legislation from Catalonia via SODA API + ELI Akoma Ntoso XML.

Strategy:
  - Use Socrata SODA API at analisi.transparenciacatalunya.cat for metadata.
  - Each record has ELI URIs for full text in multiple formats (XML, HTML, PDF).
  - Fetch Akoma Ntoso XML from ELI URI and parse for full text content.

Endpoints:
  - SODA API: https://analisi.transparenciacatalunya.cat/resource/n6hn-rmy7.json
  - ELI XML: https://portaljuridic.gencat.cat/eli/{path}/cat/xml

Data:
  - Legislation types: Llei, Decret, Ordre, Resolució, etc.
  - 30,000+ records since 1977
  - License: CC0 1.0 Universal
  - Languages: Catalan (ca) and Spanish (es)

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
import socket
import ssl
from pathlib import Path

# Issue #502: hard safety net against silent socket hangs
socket.setdefaulttimeout(120)
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from xml.etree import ElementTree as ET

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

try:
    from urllib3.util.ssl_ import create_urllib3_context
except ImportError:
    create_urllib3_context = None


class TLSAdapter(HTTPAdapter):
    """
    Custom HTTPAdapter that uses a permissive SSL context.

    Fixes SSL handshake failures with portaldogc.gencat.cat on some servers
    (especially VPS/datacenter environments with older OpenSSL).

    The Catalan government server (portaldogc.gencat.cat) uses TLSv1.2 with
    AES256-SHA cipher, which may not be in the default cipher list on some systems.
    """

    def __init__(self, *args, **kwargs):
        self.ssl_context = self._create_ssl_context()
        super().__init__(*args, **kwargs)

    def _create_ssl_context(self):
        """Create SSL context with permissive cipher settings."""
        if create_urllib3_context:
            # Use urllib3's context creator for better compatibility
            ctx = create_urllib3_context()
        else:
            # Fallback to standard ssl module
            ctx = ssl.create_default_context()

        # Enable broader cipher support for older servers
        # The server uses TLSv1.2 with AES256-SHA
        ctx.set_ciphers('DEFAULT:!aNULL:!eNULL:!MD5:!3DES:!DES:!RC4:!IDEA:!SEED:!aDSS:!SRP:!PSK')

        # Allow TLS 1.2 (required by portaldogc.gencat.cat)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        return ctx

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.catalonia")

# API URLs
SOCRATA_API = "https://analisi.transparenciacatalunya.cat/resource/n6hn-rmy7.json"
PORTAL_BASE = "https://portaljuridic.gencat.cat"


class CataloniaScraper(BaseScraper):
    """
    Scraper for ES/Catalonia -- Catalan Regional Legislation.
    Country: ES
    URL: https://analisi.transparenciacatalunya.cat/d/n6hn-rmy7

    Data types: legislation
    Auth: none (CC0 1.0 Universal)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "application/json, application/xml",
        })

        # Mount custom TLS adapter for compatibility with portaldogc.gencat.cat
        # This fixes SSL handshake failures on some VPS/datacenter environments
        tls_adapter = TLSAdapter()
        self.session.mount("https://portaldogc.gencat.cat", tls_adapter)
        self.session.mount("https://portaljuridic.gencat.cat", tls_adapter)

    def _extract_text_from_akn(self, xml_content: str) -> str:
        """
        Parse Akoma Ntoso XML and extract full text content.

        The Catalan ELI endpoint returns AKN 3.0 format with embedded HTML in the body.
        """
        try:
            # The body content in Catalan AKN is HTML-encoded in a CDATA-like format
            # Look for the <body> tag and extract the HTML content
            body_match = re.search(r'<body>(.*?)</body>', xml_content, re.DOTALL)
            if not body_match:
                logger.warning("No body tag found in XML")
                return ""

            body_content = body_match.group(1)

            # The content is often in <content period="..."> with HTML-encoded text
            content_match = re.search(r'<content\s+period="([^"]*)"', body_content)
            if content_match:
                encoded_html = content_match.group(1)
                # Decode HTML entities
                decoded = html.unescape(encoded_html)

                # Strip HTML tags
                text = re.sub(r'<[^>]+>', ' ', decoded)
                # Clean whitespace
                text = re.sub(r'\s+', ' ', text)
                text = text.strip()
                return text

            # Fallback: try to parse as standard XML
            root = ET.fromstring(xml_content)

            # Define AKN namespace
            ns = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0'}

            text_parts = []

            def extract_text(elem):
                """Recursively extract text from element."""
                if elem.text:
                    text_parts.append(elem.text.strip())
                for child in elem:
                    extract_text(child)
                if elem.tail:
                    text_parts.append(elem.tail.strip())

            # Find body and extract
            body = root.find('.//akn:body', ns) or root.find('.//body')
            if body is not None:
                extract_text(body)

            text = ' '.join(text_parts)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()

        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            # Fallback: strip tags and extract text
            text = re.sub(r'<[^>]+>', ' ', xml_content)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception as e:
            logger.error(f"Failed to extract text from AKN: {e}")
            return ""

    def _fetch_xml_text(self, xml_url: str) -> str:
        """
        Fetch Akoma Ntoso XML from ELI URI.

        Returns the extracted text content or empty string on failure.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(xml_url, timeout=(15, 60))

            if resp.status_code != 200:
                logger.warning(f"XML fetch failed: HTTP {resp.status_code} for {xml_url}")
                return ""

            content = resp.text

            # Check if it's valid XML
            if not content.strip().startswith('<?xml'):
                logger.warning(f"Invalid XML response for {xml_url}")
                return ""

            return self._extract_text_from_akn(content)

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching XML from {xml_url}")
            return ""
        except Exception as e:
            logger.error(f"Failed to fetch XML: {e}")
            return ""

    def _fetch_soda_records(self, offset: int = 0, limit: int = 100) -> list:
        """
        Fetch records from Socrata SODA API.

        Returns list of records or empty list on failure.
        """
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "data_del_document DESC",  # Most recent first
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.get(SOCRATA_API, params=params, timeout=(15, 60))
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"SODA API error at offset {offset}: {e}")
            return []

    def _get_xml_url(self, record: dict) -> Optional[str]:
        """Extract XML URL from record."""
        # Try url_format_xml field first (nested object with 'url' key)
        xml_field = record.get("url_format_xml")
        if isinstance(xml_field, dict):
            return xml_field.get("url")
        elif isinstance(xml_field, str):
            return xml_field

        # Try format_xml as fallback
        format_xml = record.get("format_xml")
        if isinstance(format_xml, dict):
            return format_xml.get("url")

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Catalonia.

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
                control_num = record.get("n_mero_de_control", "")
                if not control_num:
                    continue

                # Get XML URL
                xml_url = self._get_xml_url(record)
                if not xml_url:
                    logger.debug(f"No XML URL for {control_num}")
                    continue

                # Fetch full text from XML endpoint
                full_text = self._fetch_xml_text(xml_url)

                if not full_text:
                    logger.warning(f"No full text for {control_num}, skipping")
                    continue

                if len(full_text) < 100:
                    logger.warning(f"Text too short for {control_num} ({len(full_text)} chars), skipping")
                    continue

                record["full_text"] = full_text
                record["xml_url"] = xml_url
                yield record

            offset += batch_size

            if len(records) < batch_size:
                logger.info("Reached end of records")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses data_de_publicaci_del_diari field for filtering.
        """
        since_str = since.strftime("%Y-%m-%dT00:00:00.000")

        params = {
            "$limit": 1000,
            "$where": f"data_de_publicaci_del_diari >= '{since_str}'",
            "$order": "data_de_publicaci_del_diari DESC",
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.get(SOCRATA_API, params=params, timeout=(15, 60))
            resp.raise_for_status()
            records = resp.json()

            logger.info(f"Found {len(records)} records published since {since_str[:10]}")

            for record in records:
                control_num = record.get("n_mero_de_control", "")
                if not control_num:
                    continue

                xml_url = self._get_xml_url(record)
                if not xml_url:
                    continue

                full_text = self._fetch_xml_text(xml_url)
                if not full_text or len(full_text) < 100:
                    continue

                record["full_text"] = full_text
                record["xml_url"] = xml_url
                yield record

        except Exception as e:
            logger.error(f"Update fetch failed: {e}")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        control_num = str(raw.get("n_mero_de_control", ""))
        title = raw.get("t_tol_de_la_norma", "")
        title_es = raw.get("t_tol_de_la_norma_es", "")
        rang = raw.get("rang_de_norma", "")  # e.g., "Decret", "Llei"
        full_text = raw.get("full_text", "")

        # Parse date
        date_doc = raw.get("data_del_document", "")
        if date_doc:
            date_doc = date_doc[:10]

        # Get URL (prefer latest version)
        url = ""
        latest_html = raw.get("url_ltima_versi_format_html")
        if isinstance(latest_html, dict):
            url = latest_html.get("url", "")
        elif isinstance(latest_html, str):
            url = latest_html

        if not url:
            format_html = raw.get("format_html")
            if isinstance(format_html, dict):
                url = format_html.get("url", "")

        # ELI URI from URL
        eli_uri = ""
        if url and "/eli/" in url:
            # Extract ELI path from URL
            eli_match = re.search(r'/eli/([^?]+)', url)
            if eli_match:
                eli_uri = f"https://portaljuridic.gencat.cat/eli/{eli_match.group(1)}"

        # Document type mapping
        doc_type_map = {
            "Llei": "Law",
            "Decret": "Decree",
            "Decret legislatiu": "Legislative Decree",
            "Ordre": "Order",
            "Resolució": "Resolution",
            "Acord": "Agreement",
            "Edicte": "Edict",
        }
        doc_type = doc_type_map.get(rang, rang)

        # Publication info
        diari = raw.get("diari_oficial", "")
        num_diari = raw.get("n_mero_de_diari", "")
        pub_date = raw.get("data_de_publicaci_del_diari", "")
        if pub_date:
            pub_date = pub_date[:10]

        vigencia = raw.get("vig_ncia_de_la_norma", "")

        return {
            # Required base fields
            "_id": control_num,
            "_source": "ES/Catalonia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "title_es": title_es,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_doc,
            "url": url,
            "eli_uri": eli_uri,
            # Additional metadata
            "document_type": doc_type,
            "rang_norma": rang,
            "year": raw.get("any", ""),
            "gazette": diari,
            "gazette_number": num_diari,
            "publication_date": pub_date,
            "status": vigencia,
            "language": "ca",
            "region": "Catalonia",
            "country": "ES",
            "xml_url": raw.get("xml_url", ""),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Catalonia legislation endpoints...")

        # Test SODA API
        print("\n1. Testing Socrata SODA API...")
        try:
            records = self._fetch_soda_records(limit=3)
            print(f"   Found {len(records)} sample records")
            if records:
                r = records[0]
                print(f"   Control Number: {r.get('n_mero_de_control', 'N/A')}")
                title = r.get('t_tol_de_la_norma', 'N/A')
                print(f"   Title: {title[:70]}..." if len(title) > 70 else f"   Title: {title}")
                print(f"   Type: {r.get('rang_de_norma', 'N/A')}")
                print(f"   Date: {r.get('data_del_document', 'N/A')[:10]}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test XML endpoint
        print("\n2. Testing ELI XML endpoint...")
        if records:
            xml_url = self._get_xml_url(records[0])
            if xml_url:
                try:
                    full_text = self._fetch_xml_text(xml_url)
                    print(f"   XML URL: {xml_url}")
                    print(f"   Text length: {len(full_text)} characters")
                    if full_text:
                        print(f"   Text sample: {full_text[:150]}...")
                except Exception as e:
                    print(f"   ERROR: {e}")
            else:
                print("   No XML URL in sample record")

        # Test record count
        print("\n3. Checking total record count...")
        try:
            resp = self.session.get(
                SOCRATA_API,
                params={"$select": "count(*) as total"},
                timeout=(15, 30)
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                print(f"   Total records: {data[0].get('total', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = CataloniaScraper()

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
