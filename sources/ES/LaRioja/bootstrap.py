#!/usr/bin/env python3
"""
ES/LaRioja -- La Rioja Regional Legislation (BOR)

Fetches legislation from the Boletín Oficial de La Rioja via:
  1. BOE ELI sitemaps to discover all La Rioja documents (es-ri prefix)
  2. BOE ELI page to extract the BOE-A identifier
  3. BOE Open Data API for metadata (JSON) and full text (XML)

Data:
  - Coverage from 1982 to present.
  - License: CC BY 4.0.
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
import time
import socket
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Dict

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
logger = logging.getLogger("legal-data-hunter.ES.larioja")

BOE_BASE = "https://www.boe.es"
ELI_PREFIX = "es-ri"

ELI_TYPE_NAMES = {
    "a": "Acuerdo",
    "d": "Decreto",
    "l": "Ley",
    "o": "Orden",
    "reg": "Reglamento",
    "res": "Resolución",
}


class LaRiojaScraper(BaseScraper):
    """Scraper for ES/LaRioja -- BOE ELI + Open Data API for full text."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        adapter = HTTPAdapter(max_retries=3)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _fetch_page(self, url: str, accept: str = None) -> Optional[str]:
        """Fetch a page with rate limiting."""
        self.rate_limiter.wait()
        headers = {}
        if accept:
            headers["Accept"] = accept
        try:
            resp = self.session.get(url, timeout=60, headers=headers)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _discover_eli_uris(self) -> List[str]:
        """Discover all La Rioja ELI URIs from BOE sitemaps."""
        uris = []
        for i in range(3):
            url = f"{BOE_BASE}/eli/sitemap{i}.xml"
            logger.info(f"Fetching sitemap {url}")
            content = self._fetch_page(url)
            if not content:
                continue
            matches = re.findall(
                rf'<loc>(https://www\.boe\.es/eli/{ELI_PREFIX}/[^<]+)</loc>',
                content
            )
            uris.extend(matches)
            logger.info(f"  Found {len(matches)} La Rioja entries in sitemap{i}")
        logger.info(f"Total La Rioja ELI URIs discovered: {len(uris)}")
        return uris

    def _parse_eli_uri(self, uri: str) -> Optional[Dict]:
        """Parse an ELI URI into components."""
        m = re.match(
            rf'https://www\.boe\.es/eli/{ELI_PREFIX}/([a-z]+)/(\d{{4}})/(\d{{1,2}})/(\d{{1,2}})/(\d+)',
            uri
        )
        if not m:
            return None
        return {
            "doc_type": m.group(1),
            "year": int(m.group(2)),
            "month": int(m.group(3)),
            "day": int(m.group(4)),
            "number": m.group(5),
            "eli_url": uri,
        }

    def _get_boe_id(self, eli_url: str) -> Optional[str]:
        """Extract BOE-A identifier from an ELI document page."""
        page = self._fetch_page(eli_url)
        if not page:
            return None
        m = re.search(
            r'property="http://data\.europa\.eu/eli/ontology#id_local"\s+'
            r'content="(BOE-A-\d+-\d+)"',
            page
        )
        if m:
            return m.group(1)
        m = re.search(r'(BOE-A-\d+-\d+)', page)
        return m.group(1) if m else None

    def _get_metadata(self, boe_id: str) -> Optional[Dict]:
        """Get document metadata from BOE Open Data API."""
        url = f"{BOE_BASE}/datosabiertos/api/legislacion-consolidada/id/{boe_id}/metadatos"
        content = self._fetch_page(url, accept="application/json")
        if not content:
            return None
        try:
            data = json.loads(content)
            if data.get("status", {}).get("code") != "200":
                return None
            items = data.get("data", [])
            return items[0] if items else None
        except (json.JSONDecodeError, IndexError):
            return None

    def _get_full_text_xml(self, boe_id: str) -> Optional[str]:
        """Get full text XML from BOE Open Data API."""
        url = f"{BOE_BASE}/datosabiertos/api/legislacion-consolidada/id/{boe_id}/texto"
        return self._fetch_page(url, accept="application/xml")

    def _extract_text_from_xml(self, xml_content: str) -> str:
        """Extract clean text from BOE full text XML response."""
        if not xml_content:
            return ""

        texto_match = re.search(r'<texto>(.*)</texto>', xml_content, re.S)
        if not texto_match:
            texto_match = re.search(r'<data>(.*)</data>', xml_content, re.S)
            if not texto_match:
                return ""

        raw = texto_match.group(1)

        raw = re.sub(r'</p>', '\n', raw)
        raw = re.sub(r'<br\s*/?>', '\n', raw)
        raw = re.sub(r'</div>', '\n', raw)
        raw = re.sub(r'</li>', '\n', raw)
        raw = re.sub(r'</tr>', '\n', raw)
        raw = re.sub(r'</td>', ' | ', raw)
        raw = re.sub(r'</th>', ' | ', raw)
        raw = re.sub(r'<bloque[^>]*titulo="([^"]*)"[^>]*>', r'\n\1\n', raw)

        text = re.sub(r'<[^>]+>', '', raw)
        text = html_module.unescape(text)

        lines = text.split('\n')
        cleaned = [line.strip() for line in lines if line.strip()]
        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()

    def _fetch_document(self, eli_info: Dict) -> Optional[dict]:
        """Fetch a single document: get BOE ID, metadata, and full text."""
        eli_url = eli_info["eli_url"]
        doc_type = eli_info["doc_type"]

        boe_id = self._get_boe_id(eli_url)
        if not boe_id:
            logger.warning(f"  No BOE ID for {eli_url}")
            return None

        metadata = self._get_metadata(boe_id)

        xml_content = self._get_full_text_xml(boe_id)
        text = self._extract_text_from_xml(xml_content) if xml_content else ""

        if not text or len(text) < 50:
            logger.warning(
                f"  Insufficient text for {boe_id}: {len(text) if text else 0} chars"
            )
            return None

        title = ""
        rango = ""
        fecha_disposicion = ""
        departamento = ""
        numero_oficial = ""
        if metadata:
            title = metadata.get("titulo", "")
            rango = metadata.get("rango", {}).get("texto", "")
            fecha_disposicion = metadata.get("fecha_disposicion", "")
            departamento = metadata.get("departamento", {}).get("texto", "")
            numero_oficial = metadata.get("numero_oficial", "")

        if not title:
            type_name = ELI_TYPE_NAMES.get(doc_type, doc_type)
            title = f"{type_name} {eli_info['number']}/{eli_info['year']}"

        iso_date = ""
        if fecha_disposicion and len(fecha_disposicion) == 8:
            iso_date = f"{fecha_disposicion[:4]}-{fecha_disposicion[4:6]}-{fecha_disposicion[6:8]}"
        else:
            iso_date = f"{eli_info['year']}-{eli_info['month']:02d}-{eli_info['day']:02d}"

        return {
            "boe_id": boe_id,
            "doc_type": doc_type,
            "type_name": ELI_TYPE_NAMES.get(doc_type, doc_type),
            "year": eli_info["year"],
            "month": eli_info["month"],
            "day": eli_info["day"],
            "number": eli_info["number"],
            "title": title,
            "text": text,
            "date": iso_date,
            "rango": rango,
            "departamento": departamento,
            "numero_oficial": numero_oficial,
            "eli_url": eli_url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation from BOE ELI sitemaps for La Rioja."""
        uris = self._discover_eli_uris()
        total = len(uris)

        for i, uri in enumerate(uris):
            eli_info = self._parse_eli_uri(uri)
            if not eli_info:
                logger.warning(f"Could not parse ELI URI: {uri}")
                continue

            logger.info(
                f"[{i+1}/{total}] {eli_info['doc_type']}/{eli_info['year']}/"
                f"{eli_info['month']:02d}/{eli_info['day']:02d}/{eli_info['number']}"
            )

            record = self._fetch_document(eli_info)
            if record:
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        uris = self._discover_eli_uris()

        for uri in uris:
            eli_info = self._parse_eli_uri(uri)
            if not eli_info:
                continue

            doc_date = datetime(eli_info["year"], eli_info["month"], eli_info["day"])
            if doc_date < since.replace(tzinfo=None):
                continue

            record = self._fetch_document(eli_info)
            if record:
                yield record

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = f"boe-{raw.get('boe_id', '').lower()}"

        return {
            "_id": doc_id,
            "_source": "ES/LaRioja",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("eli_url", ""),
            "boe_id": raw.get("boe_id", ""),
            "doc_type": raw.get("doc_type", ""),
            "type_name": raw.get("type_name", ""),
            "rango": raw.get("rango", ""),
            "departamento": raw.get("departamento", ""),
            "numero_oficial": raw.get("numero_oficial", ""),
            "language": "es",
            "region": "La Rioja",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOE ELI / Open Data API for La Rioja...")

        print("\n1. Testing ELI sitemap discovery...")
        uris = self._discover_eli_uris()
        print(f"   Found {len(uris)} La Rioja ELI URIs")

        if not uris:
            print("   ERROR: No URIs found")
            return

        print("\n2. Testing BOE ID extraction...")
        test_uri = uris[0]
        print(f"   Test URI: {test_uri}")
        boe_id = self._get_boe_id(test_uri)
        print(f"   BOE ID: {boe_id}")

        if not boe_id:
            print("   ERROR: Could not extract BOE ID")
            return

        print("\n3. Testing metadata endpoint...")
        metadata = self._get_metadata(boe_id)
        if metadata:
            print(f"   Title: {metadata.get('titulo', 'N/A')[:100]}")
            print(f"   Rango: {metadata.get('rango', {}).get('texto', 'N/A')}")
        else:
            print("   WARNING: No metadata returned")

        print("\n4. Testing full text endpoint...")
        xml_content = self._get_full_text_xml(boe_id)
        if xml_content:
            text = self._extract_text_from_xml(xml_content)
            print(f"   Text length: {len(text)} chars")
            print(f"   First 200 chars: {text[:200]}...")
        else:
            print("   ERROR: No full text returned")

        print("\nAll tests passed!")


def main():
    scraper = LaRiojaScraper()

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
