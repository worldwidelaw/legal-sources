#!/usr/bin/env python3
"""
ES/CastillaLeon -- Castilla y León Regional Legislation (BOCYL)

Fetches legislation from the Boletín Oficial de Castilla y León via:
  1. OpenDataSoft API for structured metadata with pagination
  2. XML endpoints for full text extraction

Data:
  - Coverage from 1983 to present.
  - License: CC BY 4.0 (per OpenDataSoft dataset metadata).
  - Language: Spanish (es).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import socket
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from urllib.parse import quote

socket.setdefaulttimeout(120)

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.castillaleon")

API_BASE = "https://jcyl.opendatasoft.com/api/explore/v2.1/catalog/datasets/bocyl/records"
XML_BASE = "https://bocyl.jcyl.es"

# Subsections that contain legislation/regulatory content
LEGISLATION_SUBSECTIONS = [
    "A. DISPOSICIONES GENERALES",
    "A. DISPOSICIONES Y ACTOS",
    "D. OTRAS DISPOSICIONES",
]

PAGE_SIZE = 100


class CastillaLeonScraper(BaseScraper):
    """Scraper for ES/CastillaLeon -- BOCYL via OpenDataSoft API + XML full text."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "application/json, text/xml, */*",
        })
        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _build_where_clause(self, since: Optional[datetime] = None) -> str:
        """Build ODS where clause for legislation subsections."""
        parts = []
        sub_clauses = " OR ".join(
            f'subseccion = "{s}"' for s in LEGISLATION_SUBSECTIONS
        )
        parts.append(f"({sub_clauses})")
        if since:
            date_str = since.strftime("%Y-%m-%d")
            parts.append(f'fecha_publicacion >= "{date_str}"')
        return " AND ".join(parts)

    def _fetch_api_page(self, offset: int, where: str) -> Optional[dict]:
        """Fetch a page of records from the OpenDataSoft API."""
        self.rate_limiter.wait()
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "where": where,
            "order_by": "fecha_publicacion DESC",
        }
        try:
            resp = self.session.get(API_BASE, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"API request failed at offset {offset}: {e}")
            return None

    def _fetch_xml_text(self, xml_url: str) -> Optional[str]:
        """Fetch XML document and extract full text from <texto> element."""
        if not xml_url:
            return None

        # Ensure HTTPS
        url = xml_url.replace("http://", "https://")
        if not url.startswith("https://"):
            url = XML_BASE + url

        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            # XML is ISO-8859-15 encoded
            resp.encoding = "iso-8859-15"
            xml_content = resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch XML {url}: {e}")
            return None

        return self._extract_text_from_xml(xml_content)

    def _extract_text_from_xml(self, xml_content: str) -> str:
        """Extract clean text from BOCYL XML <texto> element."""
        if not xml_content:
            return ""

        # Extract content within <texto> tags
        texto_match = re.search(
            r'<texto[^>]*>(.*?)</texto>', xml_content, re.DOTALL
        )
        if not texto_match:
            return ""

        body = texto_match.group(1)

        # Replace block elements with newlines
        body = re.sub(r'</p>', '\n', body)
        body = re.sub(r'</div>', '\n', body)
        body = re.sub(r'<br\s*/?>', '\n', body)
        body = re.sub(r'</tr>', '\n', body)
        body = re.sub(r'</td>', ' | ', body)
        body = re.sub(r'</th>', ' | ', body)
        body = re.sub(r'</li>', '\n', body)

        # Strip remaining HTML/XML tags
        body = re.sub(r'<[^>]+>', '', body)

        # Decode HTML entities
        body = html_module.unescape(body)

        # Clean up whitespace
        lines = body.split('\n')
        cleaned = []
        for line in lines:
            line = line.strip()
            if line:
                cleaned.append(line)

        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _make_doc_id(self, record: dict) -> str:
        """Build a unique document ID from the record."""
        no_oficial = record.get("no_oficial", "")
        fecha_pub = record.get("fecha_publicacion", "")
        if no_oficial:
            safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', no_oficial)
            return f"bocyl-{safe_id}"
        # Fallback: use XML filename from URL
        xml_url = record.get("enlace_fichero_xml", "")
        if xml_url:
            fname = xml_url.rstrip("/").split("/")[-1]
            fname = fname.replace(".xml", "")
            return f"bocyl-{fname}"
        # Last fallback
        return f"bocyl-{fecha_pub}-{hash(record.get('titulo', '')) % 100000}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation from BOCYL via OpenDataSoft API (newest first)."""
        where = self._build_where_clause()
        offset = 0
        total = None

        while True:
            data = self._fetch_api_page(offset, where)
            if not data:
                break

            if total is None:
                total = data.get("total_count", 0)
                logger.info(f"Total records matching filter: {total}")

            results = data.get("results", [])
            if not results:
                break

            for record in results:
                xml_url = record.get("enlace_fichero_xml", "")
                text = self._fetch_xml_text(xml_url)

                if not text or len(text) < 50:
                    logger.warning(
                        f"Insufficient text for {record.get('no_oficial', 'unknown')}: "
                        f"{len(text) if text else 0} chars"
                    )
                    continue

                record["text"] = text
                yield record

            offset += PAGE_SIZE
            if offset >= total:
                break

            logger.info(f"Progress: {offset}/{total} records processed")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        where = self._build_where_clause(since=since)
        offset = 0
        total = None

        while True:
            data = self._fetch_api_page(offset, where)
            if not data:
                break

            if total is None:
                total = data.get("total_count", 0)
                logger.info(f"Update: {total} records since {since.isoformat()}")

            results = data.get("results", [])
            if not results:
                break

            for record in results:
                xml_url = record.get("enlace_fichero_xml", "")
                text = self._fetch_xml_text(xml_url)

                if not text or len(text) < 50:
                    continue

                record["text"] = text
                yield record

            offset += PAGE_SIZE
            if offset >= total:
                break

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = self._make_doc_id(raw)

        # Build URL — prefer HTML link, fallback to XML
        url = raw.get("enlace_fichero_html", "") or raw.get("enlace_fichero_xml", "")
        if url:
            url = url.replace("http://", "https://")

        return {
            "_id": doc_id,
            "_source": "ES/CastillaLeon",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("titulo", ""),
            "text": raw.get("text", ""),
            "date": raw.get("fecha_publicacion", ""),
            "date_disposition": raw.get("fecha_disposicion", ""),
            "url": url,
            "no_oficial": raw.get("no_oficial", ""),
            "rango": raw.get("rango", ""),
            "organismo": raw.get("organismo", ""),
            "seccion": raw.get("seccion", ""),
            "subseccion": raw.get("subseccion", ""),
            "language": "es",
            "region": "Castilla y León",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOCYL Castilla y León connection...")

        print("\n1. Testing OpenDataSoft API...")
        where = self._build_where_clause()
        data = self._fetch_api_page(0, where)
        if not data:
            print("   ERROR: Cannot reach OpenDataSoft API")
            return
        total = data.get("total_count", 0)
        print(f"   Total legislation records: {total}")

        results = data.get("results", [])
        if results:
            rec = results[0]
            print(f"   First record: {rec.get('no_oficial', 'N/A')} - {rec.get('rango', 'N/A')}")
            print(f"   Title: {rec.get('titulo', '')[:100]}...")

            print("\n2. Testing XML full text extraction...")
            xml_url = rec.get("enlace_fichero_xml", "")
            text = self._fetch_xml_text(xml_url)
            if text:
                print(f"   XML URL: {xml_url}")
                print(f"   Text length: {len(text)} chars")
                print(f"   First 200 chars: {text[:200]}...")
            else:
                print(f"   ERROR: Could not extract text from {xml_url}")
        else:
            print("   No records found")

        print("\nAll tests passed!")


def main():
    scraper = CastillaLeonScraper()

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
