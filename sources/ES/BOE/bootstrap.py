#!/usr/bin/env python3
"""
ES/BOE -- Spanish Official State Gazette Data Fetcher

Fetches consolidated Spanish legislation from the Boletín Oficial del Estado (BOE).

Strategy:
  - Uses the official BOE Open Data API for structured access.
  - List endpoint: /datosabiertos/api/legislacion-consolidada returns metadata.
  - Full text endpoint: /datosabiertos/api/legislacion-consolidada/id/{id}/texto returns XML.
  - Parse XML to extract clean text content.

Endpoints:
  - List: https://www.boe.es/datosabiertos/api/legislacion-consolidada?limit=50&offset=0
  - Metadata: https://www.boe.es/datosabiertos/api/legislacion-consolidada/id/{id}/metadatos
  - Full text: https://www.boe.es/datosabiertos/api/legislacion-consolidada/id/{id}/texto
  - ELI: https://www.boe.es/eli/es/{type}/{year}/{month}/{day}/{number}

Data:
  - Types: Leyes, Real Decreto, Decreto-Ley, Orden, etc.
  - Languages: Spanish (some regional legislation in other languages)
  - License: Open Data (reutilización libre)

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
import time
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
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
logger = logging.getLogger("legal-data-hunter.ES.BOE")

# Base URL for BOE Open Data API
BASE_URL = "https://www.boe.es"
API_URL = "https://www.boe.es/datosabiertos/api"

# Headers for API requests
API_HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/xml",
    "Accept-Language": "es",
}


class BOEScraper(BaseScraper):
    """
    Scraper for ES/BOE -- Spanish Official State Gazette.
    Country: ES
    URL: https://www.boe.es

    Data types: legislation (consolidated)
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_URL,
            headers=API_HEADERS,
            timeout=60,
        )

    def _parse_list_response(self, xml_content: str) -> List[Dict[str, Any]]:
        """
        Parse the list API response to extract document metadata.

        Returns a list of dicts with: identifier, title, date, etc.
        """
        documents = []

        try:
            root = ET.fromstring(xml_content)

            # Check response status
            status = root.find(".//status/code")
            if status is not None and status.text != "200":
                logger.warning(f"API returned non-200 status: {status.text}")
                return documents

            # Parse each item
            for item in root.findall(".//item"):
                try:
                    doc = {}

                    # Extract basic fields
                    identifier = item.find("identificador")
                    if identifier is not None:
                        doc["identifier"] = identifier.text

                    titulo = item.find("titulo")
                    if titulo is not None:
                        doc["title"] = titulo.text

                    fecha_disposicion = item.find("fecha_disposicion")
                    if fecha_disposicion is not None and fecha_disposicion.text:
                        # Format: YYYYMMDD -> YYYY-MM-DD
                        d = fecha_disposicion.text
                        if len(d) == 8:
                            doc["date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                        else:
                            doc["date"] = d

                    fecha_publicacion = item.find("fecha_publicacion")
                    if fecha_publicacion is not None and fecha_publicacion.text:
                        d = fecha_publicacion.text
                        if len(d) == 8:
                            doc["publication_date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                        else:
                            doc["publication_date"] = d

                    fecha_vigencia = item.find("fecha_vigencia")
                    if fecha_vigencia is not None and fecha_vigencia.text:
                        d = fecha_vigencia.text
                        if len(d) == 8:
                            doc["effective_date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                        else:
                            doc["effective_date"] = d

                    # Nested fields - these have format: <element codigo="X">Text Value</element>
                    rango = item.find("rango")
                    if rango is not None:
                        doc["rango_codigo"] = rango.get("codigo")
                        doc["rango"] = rango.text.strip() if rango.text else ""

                    departamento = item.find("departamento")
                    if departamento is not None:
                        doc["departamento"] = departamento.text.strip() if departamento.text else ""

                    ambito = item.find("ambito")
                    if ambito is not None:
                        doc["ambito"] = ambito.text.strip() if ambito.text else ""

                    numero_oficial = item.find("numero_oficial")
                    if numero_oficial is not None:
                        doc["numero_oficial"] = numero_oficial.text

                    estado = item.find("estado_consolidacion")
                    if estado is not None:
                        doc["estado_consolidacion"] = estado.text.strip() if estado.text else ""

                    # URLs
                    url_eli = item.find("url_eli")
                    if url_eli is not None:
                        doc["url_eli"] = url_eli.text

                    url_html = item.find("url_html_consolidada")
                    if url_html is not None:
                        doc["url_html"] = url_html.text

                    # Only add if we have an identifier
                    if doc.get("identifier"):
                        documents.append(doc)

                except Exception as e:
                    logger.warning(f"Failed to parse item: {e}")
                    continue

        except ET.ParseError as e:
            logger.error(f"Failed to parse XML response: {e}")

        return documents

    def _extract_version_text(self, version) -> str:
        """Extract clean text from a version element, taking the latest version's content."""
        parts = []
        for p in version.findall(".//p"):
            # Skip footnotes (nota_pie class)
            css_class = p.get("class", "")
            if "nota_pie" in css_class:
                continue
            text = self._extract_element_text(p)
            if text and text.strip():
                clean = self._clean_text(text)
                if clean:
                    parts.append(clean)
        return '\n'.join(parts)

    def _fetch_full_text_and_articles(self, identifier: str) -> tuple:
        """
        Fetch the full text of a document and extract individual articles.

        Returns:
            (full_text: str, articles: list[dict])
            Each article dict has: article_id, article_title, text
        """
        try:
            self.rate_limiter.wait()
            url = f"/legislacion-consolidada/id/{identifier}/texto"
            resp = self.client.get(url)
            resp.raise_for_status()

            xml_content = resp.content.decode('utf-8', errors='replace')

            text_parts = []
            articles = []

            try:
                root = ET.fromstring(xml_content)

                status = root.find(".//status/code")
                if status is not None and status.text != "200":
                    logger.warning(f"Text API returned non-200 for {identifier}: {status.text}")
                    return "", []

                # Track structural context for articles
                current_title = ""
                current_chapter = ""

                for bloque in root.findall(".//bloque"):
                    bloque_id = bloque.get("id", "")
                    bloque_tipo = bloque.get("tipo", "")
                    bloque_title = bloque.get("titulo", "")

                    if bloque_title:
                        text_parts.append(f"\n{bloque_title}\n")

                    # Track structural headings
                    if bloque_tipo == "encabezado":
                        if bloque_title:
                            current_title = bloque_title

                    # Get the latest version (last in the list)
                    versions = bloque.findall(".//version")
                    latest = versions[-1] if versions else None

                    if latest is not None:
                        version_text = self._extract_version_text(latest)

                        # Add to full text
                        if version_text:
                            text_parts.append(version_text)

                        # Extract article-level records for precepto bloques
                        if bloque_tipo == "precepto" and version_text and len(version_text) > 10:
                            articles.append({
                                "article_id": bloque_id,
                                "article_title": bloque_title,
                                "text": version_text,
                                "chapter": current_title,
                            })
                    else:
                        # Fallback: extract from all versions
                        for version in versions:
                            for p in version.findall(".//p"):
                                text = self._extract_element_text(p)
                                if text and text.strip():
                                    clean_text = self._clean_text(text)
                                    if clean_text:
                                        text_parts.append(clean_text)

            except ET.ParseError as e:
                logger.warning(f"Failed to parse text XML for {identifier}: {e}")
                text_parts = self._extract_text_fallback(xml_content)

            full_text = '\n'.join(text_parts).strip()
            return full_text, articles

        except Exception as e:
            logger.warning(f"Failed to fetch full text for {identifier}: {e}")
            return "", []

    def _fetch_full_text(self, identifier: str) -> str:
        """Fetch the full text of a document (backward-compatible wrapper)."""
        full_text, _ = self._fetch_full_text_and_articles(identifier)
        return full_text

    def _extract_element_text(self, element) -> str:
        """Recursively extract all text from an XML element."""
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

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)

        # Strip leading/trailing whitespace
        text = text.strip()

        return text

    def _extract_text_fallback(self, xml_content: str) -> List[str]:
        """Fallback text extraction using regex when XML parsing fails."""
        text_parts = []

        # Extract text between p tags
        for match in re.findall(r'<p[^>]*>([^<]+(?:<[^>]+>[^<]*</[^>]+>[^<]*)*)</p>', xml_content, re.DOTALL):
            clean = self._clean_text(match)
            if clean and len(clean) > 10:
                text_parts.append(clean)

        return text_parts

    def _fetch_list(self, limit: int = 50, offset: int = 0, from_date: str = None, to_date: str = None) -> List[Dict[str, Any]]:
        """
        Fetch a page of consolidated legislation from the API.

        Args:
            limit: Number of records to fetch (max 50 recommended)
            offset: Starting offset for pagination
            from_date: Filter by update date (YYYYMMDD format)
            to_date: Filter by update date (YYYYMMDD format)

        Returns:
            List of document metadata dicts
        """
        params = f"limit={limit}&offset={offset}"
        if from_date:
            params += f"&from={from_date}"
        if to_date:
            params += f"&to={to_date}"

        url = f"/legislacion-consolidada?{params}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            xml_content = resp.content.decode('utf-8', errors='replace')
            documents = self._parse_list_response(xml_content)

            logger.info(f"Fetched {len(documents)} documents (offset={offset})")
            return documents

        except Exception as e:
            logger.error(f"Failed to fetch list at offset {offset}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the BOE consolidated legislation API.

        Paginates through the entire dataset, fetching full text for each document.
        Also yields individual article-level records for laws with precepto bloques.
        """
        offset = 0
        limit = 50
        total_fetched = 0
        total_articles = 0

        while True:
            documents = self._fetch_list(limit=limit, offset=offset)

            if not documents:
                logger.info(f"No more documents at offset {offset}, stopping")
                break

            for doc in documents:
                identifier = doc.get("identifier")
                if not identifier:
                    continue

                # Fetch full text and articles
                full_text, articles = self._fetch_full_text_and_articles(identifier)

                if not full_text:
                    logger.warning(f"No full text for {identifier}, skipping")
                    continue

                doc["full_text"] = full_text
                total_fetched += 1
                yield doc

                # Yield individual article records
                for article in articles:
                    article_rec = {
                        "_is_article": True,
                        "parent_identifier": identifier,
                        "parent_title": doc.get("title", ""),
                        "article_id": article["article_id"],
                        "article_title": article["article_title"],
                        "chapter": article["chapter"],
                        "full_text": article["text"],
                        "date": doc.get("date", ""),
                        "publication_date": doc.get("publication_date", ""),
                        "effective_date": doc.get("effective_date", ""),
                        "rango": doc.get("rango", ""),
                        "url_eli": doc.get("url_eli", ""),
                        "url_html": doc.get("url_html", ""),
                    }
                    total_articles += 1
                    yield article_rec

            offset += limit

            if offset > 100000:
                logger.warning("Reached maximum offset limit, stopping")
                break

        logger.info(f"Completed: {total_fetched} laws + {total_articles} articles")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses the API's from/to date filters.
        """
        from_date = since.strftime("%Y%m%d")
        to_date = datetime.now(timezone.utc).strftime("%Y%m%d")

        offset = 0
        limit = 50

        while True:
            documents = self._fetch_list(
                limit=limit,
                offset=offset,
                from_date=from_date,
                to_date=to_date
            )

            if not documents:
                break

            for doc in documents:
                identifier = doc.get("identifier")
                if not identifier:
                    continue

                full_text = self._fetch_full_text(identifier)

                if not full_text:
                    continue

                doc["full_text"] = full_text
                yield doc

            offset += limit

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        Handles both law-level and article-level records.
        CRITICAL: Includes full text in the 'text' field.
        """
        if raw.get("_is_article"):
            # Article-level record
            parent_id = raw.get("parent_identifier", "")
            article_id = raw.get("article_id", "")
            article_title = raw.get("article_title", "")
            parent_title = raw.get("parent_title", "")
            chapter = raw.get("chapter", "")

            # Build descriptive title: "Art. 1105 — Código Civil [CAPÍTULO II]"
            title = article_title
            if parent_title:
                title = f"{article_title} — {parent_title}"
            if chapter:
                title = f"{title} [{chapter}]"

            url = raw.get("url_eli") or raw.get("url_html") or f"{BASE_URL}/buscar/act.php?id={parent_id}"

            return {
                "_id": f"{parent_id}:{article_id}",
                "_source": "ES/BOE",
                "_type": "legislation",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": raw.get("full_text", ""),
                "date": raw.get("date", raw.get("publication_date", "")),
                "url": url,
                "identifier": f"{parent_id}:{article_id}",
                "parent_identifier": parent_id,
                "parent_title": parent_title,
                "article_id": article_id,
                "article_title": article_title,
                "chapter": chapter,
                "rango": raw.get("rango", ""),
                "publication_date": raw.get("publication_date", ""),
                "effective_date": raw.get("effective_date", ""),
                "url_eli": raw.get("url_eli", ""),
                "language": "es",
                "granularity": "article",
            }

        # Law-level record (existing behavior)
        identifier = raw.get("identifier", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", raw.get("publication_date", ""))

        url = raw.get("url_eli") or raw.get("url_html") or f"{BASE_URL}/buscar/act.php?id={identifier}"

        return {
            "_id": identifier,
            "_source": "ES/BOE",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": url,
            "identifier": identifier,
            "rango": raw.get("rango", ""),
            "departamento": raw.get("departamento", ""),
            "ambito": raw.get("ambito", ""),
            "numero_oficial": raw.get("numero_oficial", ""),
            "publication_date": raw.get("publication_date", ""),
            "effective_date": raw.get("effective_date", ""),
            "estado_consolidacion": raw.get("estado_consolidacion", ""),
            "url_eli": raw.get("url_eli", ""),
            "language": "es",
            "granularity": "law",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BOE Open Data API endpoints...")

        # Test list endpoint
        print("\n1. Testing list endpoint (legislacion-consolidada)...")
        try:
            resp = self.client.get("/legislacion-consolidada?limit=3")
            print(f"   Status: {resp.status_code}")
            xml_content = resp.content.decode('utf-8', errors='replace')
            docs = self._parse_list_response(xml_content)
            print(f"   Found {len(docs)} documents")
            if docs:
                print(f"   Sample ID: {docs[0].get('identifier', 'N/A')}")
                print(f"   Sample title: {docs[0].get('title', 'N/A')[:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test full text endpoint
        print("\n2. Testing full text endpoint...")
        try:
            test_id = "BOE-A-1989-22056"  # Known good ID
            text = self._fetch_full_text(test_id)
            print(f"   Text length: {len(text)} characters")
            if text:
                print(f"   Sample: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = BOEScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
