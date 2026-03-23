#!/usr/bin/env python3
"""
ES/Madrid -- Madrid Regional Official Gazette Data Fetcher

Fetches legislation from the Boletín Oficial de la Comunidad de Madrid (BOCM).

Strategy:
  - Uses the BOCM RSS feeds to discover bulletins and documents.
  - Fetches full text via XML endpoint for each document.
  - XML structure: <documento> → <metadatos> + <analisis> + <texto>

Endpoints:
  - RSS bulletins: https://www.bocm.es/boletines.rss (last 20 bulletins)
  - RSS latest orders: https://www.bocm.es/ultimo-boletin.xml
  - Full XML: https://www.bocm.es/boletin/CM_Orden_BOCM/YYYY/MM/DD/BOCM-YYYYMMDD-N.xml

Data:
  - Types: Leyes, Decretos, Órdenes, Resoluciones, etc.
  - Language: Spanish
  - License: Open Data (free public access per Decree 2/2010)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (since last run)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.Madrid")

# Base URL for BOCM
BASE_URL = "https://www.bocm.es"

# Headers for requests
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "es",
}


class MadridScraper(BaseScraper):
    """
    Scraper for ES/Madrid -- Madrid Regional Official Gazette (BOCM).
    Country: ES
    URL: https://www.bocm.es

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            timeout=60,
        )

    def _parse_rss_bulletins(self, rss_content: str) -> List[Dict[str, Any]]:
        """
        Parse the bulletins RSS feed to extract bulletin dates and numbers.
        Returns list of dicts with date and bulletin_number.
        """
        bulletins = []
        try:
            root = ET.fromstring(rss_content)
            for item in root.findall(".//item"):
                link = item.find("link")
                if link is not None and link.text:
                    # Link format: https://www.bocm.es/boletin/bocm-20260318-65
                    match = re.search(r'bocm-(\d{8})-(\d+)', link.text)
                    if match:
                        date_str = match.group(1)
                        bulletin_num = match.group(2)
                        bulletins.append({
                            "date": date_str,
                            "bulletin_number": bulletin_num,
                            "url": link.text,
                        })
        except ET.ParseError as e:
            logger.error(f"Failed to parse bulletins RSS: {e}")
        return bulletins

    def _parse_latest_bulletin_rss(self, rss_content: str) -> List[str]:
        """
        Parse the latest bulletin RSS to extract document IDs.
        Returns list of document identifiers like BOCM-20260318-1.
        """
        doc_ids = []
        try:
            root = ET.fromstring(rss_content)
            for item in root.findall(".//item"):
                guid = item.find("guid")
                if guid is not None and guid.text:
                    # GUID format: https://www.bocm.es/bocm-20260318-1
                    match = re.search(r'(BOCM-\d{8}-\d+)', guid.text, re.IGNORECASE)
                    if match:
                        doc_ids.append(match.group(1).upper())
        except ET.ParseError as e:
            logger.error(f"Failed to parse latest bulletin RSS: {e}")
        return doc_ids

    def _fetch_document_xml(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch and parse a single document XML by its identifier.

        Args:
            doc_id: Document ID like BOCM-20260318-1

        Returns:
            Dict with document metadata and full text, or None on failure.
        """
        # Parse document ID: BOCM-YYYYMMDD-N
        match = re.match(r'BOCM-(\d{4})(\d{2})(\d{2})-(\d+)', doc_id)
        if not match:
            logger.warning(f"Invalid document ID format: {doc_id}")
            return None

        year, month, day, order = match.groups()

        # Construct XML URL
        url = f"/boletin/CM_Orden_BOCM/{year}/{month}/{day}/{doc_id}.xml"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            xml_content = resp.content.decode('utf-8', errors='replace')
            return self._parse_document_xml(xml_content, doc_id)

        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

    def _parse_document_xml(self, xml_content: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Parse a BOCM document XML to extract metadata and full text.
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.warning(f"Failed to parse XML for {doc_id}: {e}")
            return None

        doc = {"doc_id": doc_id}

        # Parse metadatos
        metadatos = root.find("metadatos")
        if metadatos is not None:
            for field in ["identificador", "origen_legislativo", "departamento",
                         "rango", "fecha_publicacion", "fecha_disposicion",
                         "titulo", "diario_numero", "seccion", "pagina_inicial",
                         "pagina_final", "url_html", "url_xml", "url_pdf"]:
                elem = metadatos.find(field)
                if elem is not None and elem.text:
                    doc[field] = elem.text.strip()

        # Parse analisis for additional metadata
        analisis = root.find("analisis")
        if analisis is not None:
            organismos = []
            for org in analisis.findall("organismo"):
                if org.text:
                    organismos.append(org.text.strip())
            if organismos:
                doc["organismos"] = organismos

            tipo = analisis.find("tipo_disposicion")
            if tipo is not None and tipo.text:
                doc["tipo_disposicion"] = tipo.text.strip()

            apartado = analisis.find("apartado")
            if apartado is not None and apartado.text:
                doc["apartado"] = apartado.text.strip()

        # Extract full text
        texto = root.find("texto")
        if texto is not None and texto.text:
            doc["full_text"] = self._clean_text(texto.text)
        else:
            # Try to get all text content from texto element
            texto_content = self._extract_element_text(texto) if texto is not None else ""
            doc["full_text"] = self._clean_text(texto_content)

        # Validate we have full text
        if not doc.get("full_text") or len(doc.get("full_text", "")) < 50:
            logger.warning(f"No substantial full text for {doc_id}")
            return None

        return doc

    def _extract_element_text(self, element) -> str:
        """Recursively extract all text from an XML element."""
        if element is None:
            return ""
        texts = []
        if element.text:
            texts.append(element.text)
        for child in element:
            texts.append(self._extract_element_text(child))
            if child.tail:
                texts.append(child.tail)
        return ' '.join(texts)

    def _clean_text(self, text: str) -> str:
        """Clean extracted text: decode entities, normalize whitespace."""
        if not text:
            return ""
        # Decode HTML entities
        text = html.unescape(text)
        # Remove remaining HTML/XML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Normalize whitespace (but preserve paragraph structure)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        # Strip leading/trailing whitespace
        text = text.strip()
        return text

    def _get_bulletin_documents(self, date_str: str) -> List[str]:
        """
        Get all document IDs for a specific bulletin date.

        Args:
            date_str: Date in YYYYMMDD format

        Returns:
            List of document IDs like BOCM-20260318-1, BOCM-20260318-2, etc.
        """
        # Try to fetch the bulletin page or discover documents by probing
        doc_ids = []

        # Start with order 1 and increment until we get 404s
        order = 1
        max_orders = 200  # Safety limit
        consecutive_failures = 0
        max_consecutive_failures = 3

        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        while order <= max_orders and consecutive_failures < max_consecutive_failures:
            doc_id = f"BOCM-{date_str}-{order}"
            url = f"/boletin/CM_Orden_BOCM/{year}/{month}/{day}/{doc_id}.xml"

            try:
                self.rate_limiter.wait()
                resp = self.client.get(url)

                if resp.status_code == 200:
                    doc_ids.append(doc_id)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

            except Exception:
                consecutive_failures += 1

            order += 1

        logger.info(f"Found {len(doc_ids)} documents for bulletin {date_str}")
        return doc_ids

    def _generate_date_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Generate list of dates in YYYYMMDD format between start and end."""
        dates = []
        current = start_date
        while current <= end_date:
            # BOCM is published Mon-Sat (not Sundays)
            if current.weekday() != 6:  # 6 = Sunday
                dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return dates

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from BOCM.

        Iterates through recent bulletins via RSS, then fetches full XML for each doc.
        For full bootstrap, would need to iterate through historical dates.
        """
        # For full bootstrap, start from recent and go back
        # BOCM data available from 1983, but we'll start with recent data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # Last year for bootstrap

        dates = self._generate_date_range(start_date, end_date)
        total_fetched = 0

        for date_str in dates:
            doc_ids = self._get_bulletin_documents(date_str)

            for doc_id in doc_ids:
                doc = self._fetch_document_xml(doc_id)
                if doc:
                    total_fetched += 1
                    yield doc

            # Progress logging every 10 dates
            if dates.index(date_str) % 10 == 0:
                logger.info(f"Progress: {dates.index(date_str)}/{len(dates)} dates, {total_fetched} documents")

        logger.info(f"Completed fetching {total_fetched} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.
        """
        end_date = datetime.now()
        dates = self._generate_date_range(since, end_date)

        for date_str in dates:
            doc_ids = self._get_bulletin_documents(date_str)

            for doc_id in doc_ids:
                doc = self._fetch_document_xml(doc_id)
                if doc:
                    yield doc

    def fetch_sample(self, n: int = 12) -> Generator[dict, None, None]:
        """
        Fetch a sample of recent documents for validation.
        """
        # Get recent bulletins from RSS
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/boletines.rss")
            resp.raise_for_status()
            bulletins = self._parse_rss_bulletins(resp.text)
        except Exception as e:
            logger.error(f"Failed to fetch bulletins RSS: {e}")
            bulletins = []

        if not bulletins:
            # Fallback: use today's date
            today = datetime.now().strftime("%Y%m%d")
            bulletins = [{"date": today}]

        fetched = 0
        for bulletin in bulletins:
            if fetched >= n:
                break

            date_str = bulletin["date"]
            doc_ids = self._get_bulletin_documents(date_str)

            for doc_id in doc_ids:
                if fetched >= n:
                    break

                doc = self._fetch_document_xml(doc_id)
                if doc:
                    fetched += 1
                    yield doc

        logger.info(f"Fetched {fetched} sample documents")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        identificador = raw.get("identificador", raw.get("doc_id", ""))
        title = raw.get("titulo", "")
        full_text = raw.get("full_text", "")

        # Parse date from fecha_publicacion (format: YYYY/MM/DD)
        fecha_pub = raw.get("fecha_publicacion", "")
        if fecha_pub:
            date_str = fecha_pub.replace("/", "-")
        else:
            # Extract from identificador: BOCM-YYYYMMDD-N
            match = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', identificador)
            if match:
                date_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            else:
                date_str = ""

        # URL
        url = raw.get("url_html") or f"{BASE_URL}/bocm-{identificador.lower().replace('bocm-', '')}"

        return {
            # Required base fields
            "_id": identificador,
            "_source": "ES/Madrid",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "identifier": identificador,
            "rango": raw.get("rango", ""),
            "departamento": raw.get("departamento", ""),
            "seccion": raw.get("seccion", ""),
            "apartado": raw.get("apartado", ""),
            "diario_numero": raw.get("diario_numero", ""),
            "fecha_disposicion": raw.get("fecha_disposicion", ""),
            "organismos": raw.get("organismos", []),
            "tipo_disposicion": raw.get("tipo_disposicion", ""),
            "origen_legislativo": raw.get("origen_legislativo", ""),
            "url_pdf": raw.get("url_pdf", ""),
            "language": "es",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOCM (Madrid) endpoints...")

        # Test RSS feed
        print("\n1. Testing bulletins RSS feed...")
        try:
            resp = self.client.get("/boletines.rss")
            print(f"   Status: {resp.status_code}")
            bulletins = self._parse_rss_bulletins(resp.text)
            print(f"   Found {len(bulletins)} bulletins")
            if bulletins:
                print(f"   Latest: {bulletins[0].get('date', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test latest bulletin RSS
        print("\n2. Testing latest bulletin RSS...")
        try:
            resp = self.client.get("/ultimo-boletin.xml")
            print(f"   Status: {resp.status_code}")
            doc_ids = self._parse_latest_bulletin_rss(resp.text)
            print(f"   Found {len(doc_ids)} documents in latest bulletin")
            if doc_ids:
                print(f"   Sample IDs: {doc_ids[:3]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document XML endpoint
        print("\n3. Testing document XML endpoint...")
        try:
            # Use a recent date
            today = datetime.now()
            date_str = today.strftime("%Y%m%d")
            doc_id = f"BOCM-{date_str}-1"
            doc = self._fetch_document_xml(doc_id)
            if doc:
                print(f"   Document ID: {doc.get('identificador', doc_id)}")
                print(f"   Title: {doc.get('titulo', 'N/A')[:60]}...")
                text_len = len(doc.get('full_text', ''))
                print(f"   Full text length: {text_len} characters")
                if text_len > 0:
                    print(f"   Text sample: {doc.get('full_text', '')[:200]}...")
            else:
                print(f"   No document found for {doc_id} (may be weekend/holiday)")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = MadridScraper()

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
