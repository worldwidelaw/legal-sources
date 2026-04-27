#!/usr/bin/env python3
"""
CL/BCNLinkedData -- Chile BCN Linked Open Data Fetcher

Fetches Chilean legislation via SPARQL metadata + LeyChile XML full text.

Strategy:
  - Query SPARQL endpoint for norm metadata (title, dates, leychileCode)
  - Download full text XML from LeyChile for each norm
  - Parse XML to extract clean text
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull (laws)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urlencode, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.BCNLinkedData")

SPARQL_ENDPOINT = "https://datos.bcn.cl/sparql"
LEYCHILE_XML_URL = "https://www.leychile.cl/Consulta/obtxml?opt=7&idNorma={id}"
LEYCHILE_HTML_URL = "https://www.bcn.cl/leychile/Navegar?idNorma={id}"

# SPARQL query to get latest version of laws with metadata
SPARQL_LAWS_QUERY = """
PREFIX bcnnorms: <http://datos.bcn.cl/ontologies/bcn-norms#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?norma ?title ?number ?publishDate ?promulgationDate ?leychileCode
WHERE {{
  ?norma a bcnnorms:NormInstance .
  ?norma bcnnorms:type <http://datos.bcn.cl/recurso/cl/norma/tipo#ley> .
  ?norma bcnnorms:isLatestVersion 1 .
  ?norma dc:title ?title .
  ?norma bcnnorms:hasNumber ?number .
  ?norma bcnnorms:leychileCode ?leychileCode .
  OPTIONAL {{ ?norma bcnnorms:publishDate ?publishDate }}
  OPTIONAL {{ ?norma bcnnorms:promulgationDate ?promulgationDate }}
}}
ORDER BY DESC(?publishDate)
LIMIT {limit} OFFSET {offset}
"""


class BCNLinkedDataScraper(BaseScraper):
    """Scraper for CL/BCNLinkedData -- Chilean legislation via SPARQL + XML."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=60)
        except ImportError:
            self.client = None

    def _http_get(self, url: str, params: dict = None, headers: dict = None) -> Optional[str]:
        """HTTP GET returning response text."""
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(url, headers=headers or {})
                    if resp.status_code == 200:
                        return resp.text
                    logger.warning(f"HTTP {resp.status_code} for {url[:100]}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers=headers or {})
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:100]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _sparql_query(self, query: str) -> Optional[list]:
        """Execute SPARQL query and return bindings."""
        headers = {"Accept": "application/sparql-results+json"}
        params = {"query": query, "format": "application/sparql-results+json"}
        text = self._http_get(SPARQL_ENDPOINT, params=params, headers=headers)
        if not text:
            return None
        try:
            data = json.loads(text, strict=False)
            return data.get("results", {}).get("bindings", [])
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return None

    def _fetch_xml_text(self, leychile_code: str) -> Optional[str]:
        """Download XML from LeyChile and extract clean text."""
        url = LEYCHILE_XML_URL.format(id=leychile_code)
        xml_content = self._http_get(url)
        if not xml_content:
            return None
        return self._parse_xml_text(xml_content)

    def _parse_xml_text(self, xml_content: str) -> Optional[str]:
        """Parse LeyChile XML and extract text content."""
        try:
            # Remove XML namespace for easier parsing
            xml_clean = re.sub(r'\sxmlns[^"]*"[^"]*"', '', xml_content, count=5)
            root = ET.fromstring(xml_clean)
        except ET.ParseError:
            # Try to extract text with regex as fallback
            return self._regex_extract_text(xml_content)

        parts = []

        # Look for text content in various XML structures
        for tag in ["Titulo", "Materia", "Texto", "Contenido", "Articulo",
                     "ArticuloTexto", "Inciso", "Cuerpo"]:
            for elem in root.iter(tag):
                text = self._get_all_text(elem).strip()
                if text:
                    parts.append(text)

        # If specific tags didn't work, get all text from the document
        if not parts:
            all_text = self._get_all_text(root).strip()
            if all_text:
                parts.append(all_text)

        if not parts:
            return self._regex_extract_text(xml_content)

        full_text = "\n\n".join(parts)
        # Clean up whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r'[ \t]+', ' ', full_text)
        return full_text.strip() if len(full_text) > 50 else None

    def _get_all_text(self, elem) -> str:
        """Recursively get all text from an XML element."""
        texts = []
        if elem.text:
            texts.append(elem.text.strip())
        for child in elem:
            texts.append(self._get_all_text(child))
            if child.tail:
                texts.append(child.tail.strip())
        return " ".join(t for t in texts if t)

    def _regex_extract_text(self, xml_content: str) -> Optional[str]:
        """Fallback: extract text using regex from XML."""
        # Strip all XML tags
        text = re.sub(r'<[^>]+>', ' ', xml_content)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&#\d+;', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        # Remove the XML header junk
        text = re.sub(r'^.*?(?=Art[ií]culo|TITULO|Decreto|LEY|Ley)', '', text, count=1)
        return text if len(text) > 100 else None

    def _sparql_value(self, binding: dict, key: str) -> str:
        """Extract value from SPARQL binding."""
        if key in binding and "value" in binding[key]:
            return binding[key]["value"]
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        number = raw.get("number", "")
        leychile_code = raw.get("leychile_code", "")
        publish_date = raw.get("publish_date", "")
        promulgation_date = raw.get("promulgation_date", "")
        text = raw.get("text", "")

        date = publish_date or promulgation_date or ""
        if date and len(date) > 10:
            date = date[:10]

        url = LEYCHILE_HTML_URL.format(id=leychile_code) if leychile_code else ""

        return {
            "_id": f"CL-BCN-{leychile_code}" if leychile_code else f"CL-BCN-{number}",
            "_source": "CL/BCNLinkedData",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "number": number,
            "leychile_code": leychile_code,
            "publish_date": publish_date[:10] if publish_date else "",
            "promulgation_date": promulgation_date[:10] if promulgation_date else "",
            "norm_type": "ley",
        }

    def fetch_all(self, limit_per_page: int = 50, max_pages: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch laws via SPARQL + XML full text."""
        offset = 0
        count = 0
        page = 0

        while True:
            query = SPARQL_LAWS_QUERY.format(limit=limit_per_page, offset=offset)
            bindings = self._sparql_query(query)
            if not bindings:
                logger.info(f"No more results at offset {offset}")
                break

            for b in bindings:
                leychile_code = self._sparql_value(b, "leychileCode")
                if not leychile_code:
                    continue

                title = self._sparql_value(b, "title")
                number = self._sparql_value(b, "number")
                publish_date = self._sparql_value(b, "publishDate")
                promulgation_date = self._sparql_value(b, "promulgationDate")

                time.sleep(1)
                full_text = self._fetch_xml_text(leychile_code)
                if not full_text:
                    logger.warning(f"No text for norm {leychile_code}: {title[:60]}")
                    continue

                raw = {
                    "title": title,
                    "number": number,
                    "leychile_code": leychile_code,
                    "publish_date": publish_date,
                    "promulgation_date": promulgation_date,
                    "text": full_text,
                }
                count += 1
                yield raw

            offset += limit_per_page
            page += 1
            if max_pages and page >= max_pages:
                break
            time.sleep(1)

        logger.info(f"Completed: {count} norms fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently published norms."""
        # Use a date filter in SPARQL
        date_filter = ""
        if since:
            date_filter = f'FILTER(?publishDate >= "{since}"^^xsd:date)'

        query = f"""
PREFIX bcnnorms: <http://datos.bcn.cl/ontologies/bcn-norms#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?norma ?title ?number ?publishDate ?promulgationDate ?leychileCode
WHERE {{
  ?norma a bcnnorms:NormInstance .
  ?norma bcnnorms:type <http://datos.bcn.cl/recurso/cl/norma/tipo#ley> .
  ?norma bcnnorms:isLatestVersion 1 .
  ?norma dc:title ?title .
  ?norma bcnnorms:hasNumber ?number .
  ?norma bcnnorms:leychileCode ?leychileCode .
  OPTIONAL {{ ?norma bcnnorms:publishDate ?publishDate }}
  OPTIONAL {{ ?norma bcnnorms:promulgationDate ?promulgationDate }}
  {date_filter}
}}
ORDER BY DESC(?publishDate)
LIMIT 200
"""
        bindings = self._sparql_query(query)
        if not bindings:
            return

        count = 0
        for b in bindings:
            leychile_code = self._sparql_value(b, "leychileCode")
            if not leychile_code:
                continue

            time.sleep(1)
            full_text = self._fetch_xml_text(leychile_code)
            if not full_text:
                continue

            raw = {
                "title": self._sparql_value(b, "title"),
                "number": self._sparql_value(b, "number"),
                "leychile_code": leychile_code,
                "publish_date": self._sparql_value(b, "publishDate"),
                "promulgation_date": self._sparql_value(b, "promulgationDate"),
                "text": full_text,
            }
            count += 1
            yield raw

        logger.info(f"Updates: {count} norms")

    def test(self) -> bool:
        """Quick connectivity test."""
        query = SPARQL_LAWS_QUERY.format(limit=1, offset=0)
        bindings = self._sparql_query(query)
        if not bindings:
            logger.error("SPARQL query returned no results")
            return False

        b = bindings[0]
        code = self._sparql_value(b, "leychileCode")
        title = self._sparql_value(b, "title")
        logger.info(f"SPARQL OK: {title[:80]} (code={code})")

        text = self._fetch_xml_text(code)
        if text:
            logger.info(f"XML full text OK: {len(text)} chars")
            return True
        else:
            logger.error("Could not fetch XML full text")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/BCNLinkedData data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BCNLinkedDataScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
if __name__ == "__main__":
    main()
