#!/usr/bin/env python3
"""
ES/BasqueCountry — Basque Country Regional Legislation (BOPV/EHAA)

Fetches legislation from the Basque Autonomous Community via SPARQL endpoint
and XML content endpoints. Uses ELI (European Legislation Identifier) URIs.

Data source: Open Data Euskadi (https://opendata.euskadi.eus)
SPARQL endpoint: https://api.euskadi.eus/sparql/
License: CC BY 4.0
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
from html import unescape
import xml.etree.ElementTree as ET

import requests

SPARQL_ENDPOINT = "https://api.euskadi.eus/sparql/"
XML_CONTENT_BASE = "https://www.legegunea.euskadi.eus/contenidos/"
REQUEST_DELAY = 1.0  # seconds between requests
MAX_RETRIES = 3
TIMEOUT = 30

# SPARQL query to get legislation with XML content URLs
# We query for Spanish language documents (more standardized)
SPARQL_QUERY_LEGISLATION_TEMPLATE = """
PREFIX eli: <https://data.europa.eu/eli/ontology#>
PREFIX schema: <https://schema.org/>
SELECT DISTINCT ?eli ?title ?xml_url WHERE {{
  ?eli a eli:LegalExpression ;
       eli:title ?title ;
       eli:language <https://www.elidata.es/mdr/authority/language/SPA> .
  ?format eli:embodies ?eli ;
          eli:format <https://www.iana.org/assignments/media-types/application/xml> ;
          eli:published_in_format ?xml_url .
}}
ORDER BY DESC(?eli)
LIMIT {limit}
OFFSET {offset}
"""


def make_id(eli_uri: str) -> str:
    """Generate a stable document ID from the ELI URI."""
    # Extract the meaningful part from the ELI URI
    # e.g., https://id.euskadi.eus/eli/es-pv/d/2020/09/29/198/dof/spa
    # -> es-pv_d_2020-09-29_198
    match = re.search(r'/eli/(es-pv)/(\w+)/(\d{4})/(\d{2})/(\d{2})/(\d+|\(\d+\))', eli_uri)
    if match:
        jurisdiction, doc_type, year, month, day, number = match.groups()
        number = number.strip('()')
        return f"{jurisdiction}_{doc_type}_{year}-{month}-{day}_{number}"
    # Fallback to hash
    return hashlib.sha256(eli_uri.encode()).hexdigest()[:16]


def clean_html_text(html_content: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html_content:
        return ""
    # Decode HTML entities
    text = unescape(html_content)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Fix encoding issues (common in XML CDATA)
    # ISO-8859-1 chars incorrectly displayed
    replacements = {
        'á': 'á', 'é': 'é', 'í': 'í', 'ó': 'ó', 'ú': 'ú',
        'ñ': 'ñ', 'Ñ': 'Ñ', 'ü': 'ü', 'Ü': 'Ü',
        'à': 'à', 'è': 'è', 'ì': 'ì', 'ò': 'ò', 'ù': 'ù',
        '€': '€', '«': '«', '»': '»',
    }
    for wrong, right in replacements.items():
        text = text.replace(wrong, right)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_date_from_eli(eli_uri: str) -> Optional[str]:
    """Extract publication date from ELI URI."""
    match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', eli_uri)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None


def extract_document_type(eli_uri: str) -> str:
    """Extract document type from ELI URI."""
    type_map = {
        'd': 'decreto',
        'o': 'orden',
        'res': 'resolucion',
        'l': 'ley',
        'ac': 'acuerdo',
        'df': 'decreto_foral',
    }
    match = re.search(r'/eli/es-pv/(\w+)/', eli_uri)
    if match:
        code = match.group(1)
        return type_map.get(code, code)
    return 'unknown'


def fetch_sparql(query: str, session: requests.Session) -> list:
    """Execute a SPARQL query and return results."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)"
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(
                SPARQL_ENDPOINT,
                data={"query": query},
                headers=headers,
                timeout=TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            return data.get("results", {}).get("bindings", [])
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"SPARQL query failed after {MAX_RETRIES} attempts: {e}")


def fetch_xml_content(xml_url: str, session: requests.Session) -> Optional[dict]:
    """Fetch and parse XML content from a legislation URL."""
    headers = {
        "User-Agent": "WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)"
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(xml_url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Parse XML (handling encoding)
            content = response.content
            # Try to fix encoding declaration if needed
            if b"encoding='ISO-8859-1'" in content:
                content = content.replace(b"encoding='ISO-8859-1'", b"encoding='utf-8'")
                try:
                    content = content.decode('iso-8859-1').encode('utf-8')
                except:
                    pass
            
            root = ET.fromstring(content)
            
            result = {}
            
            # Extract title
            titulo_elem = root.find('.//titulo')
            if titulo_elem is not None and titulo_elem.text:
                result['title'] = clean_html_text(titulo_elem.text)
            
            # Extract full text
            texto_elem = root.find('.//textoLegal/texto')
            if texto_elem is not None and texto_elem.text:
                result['text'] = clean_html_text(texto_elem.text)
            
            # Extract bulletin info
            boletin = root.find('.//boletin')
            if boletin is not None:
                result['boletin_numero'] = boletin.get('numero')
                result['boletin_disposicion'] = boletin.get('disposicion')
                result['boletin_id'] = boletin.get('id')
                result['fecha_publicacion'] = boletin.get('fechaPublicacion')
                result['fecha_orden'] = boletin.get('fechaOrden')
            
            # Extract department/entity
            entidad = root.find('.//entidad')
            if entidad is not None and entidad.text:
                result['entidad'] = entidad.text.strip()
            
            # Extract departments
            departamentos = []
            for dept in root.findall('.//departamentos/entidadOrganica'):
                if dept.text:
                    departamentos.append(dept.text.strip())
            if departamentos:
                result['departamentos'] = departamentos
            
            # Extract document type (rango)
            rango = root.find('.//rango')
            if rango is not None:
                result['rango_code'] = rango.get('code')
                result['rango_type'] = rango.get('type')
                if rango.text:
                    result['rango_name'] = rango.text.strip()
            
            # Extract categories
            categorias = []
            for cat in root.findall('.//catalogaciones/catalogacion'):
                nombre = cat.find('nombre')
                if nombre is not None and nombre.text:
                    categorias.append(nombre.text.strip())
            if categorias:
                result['categorias'] = categorias
            
            # Extract PDF URLs
            pdf_es = root.find('.//boletin/url/es')
            pdf_eu = root.find('.//boletin/url/eu')
            if pdf_es is not None and pdf_es.text:
                result['pdf_url_es'] = f"https://www.euskadi.eus{pdf_es.text.strip()}"
            if pdf_eu is not None and pdf_eu.text:
                result['pdf_url_eu'] = f"https://www.euskadi.eus{pdf_eu.text.strip()}"
            
            return result
            
        except ET.ParseError as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            print(f"  Warning: XML parse error for {xml_url}: {e}", file=sys.stderr)
            return None
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            print(f"  Warning: Failed to fetch {xml_url}: {e}", file=sys.stderr)
            return None


def normalize(raw: dict, eli_uri: str) -> dict:
    """Transform raw data into standardized schema."""
    doc_id = make_id(eli_uri)
    
    # Parse dates
    date_published = None
    if raw.get('fecha_publicacion'):
        try:
            # Format: DD/MM/YYYY
            parts = raw['fecha_publicacion'].split('/')
            if len(parts) == 3:
                date_published = f"{parts[2]}-{parts[1]}-{parts[0]}"
        except:
            pass
    if not date_published:
        date_published = extract_date_from_eli(eli_uri)
    
    return {
        "_id": doc_id,
        "_source": "ES/BasqueCountry",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        
        # Core required fields
        "title": raw.get('title', ''),
        "text": raw.get('text', ''),
        "date": date_published,
        "url": eli_uri,
        
        # ELI-specific
        "eli_uri": eli_uri,
        "document_type": extract_document_type(eli_uri),
        
        # Basque-specific metadata
        "boletin_numero": raw.get('boletin_numero'),
        "boletin_id": raw.get('boletin_id'),
        "entidad": raw.get('entidad'),
        "departamentos": raw.get('departamentos', []),
        "rango_name": raw.get('rango_name'),
        "categorias": raw.get('categorias', []),
        "pdf_url": raw.get('pdf_url_es'),
        
        # Language
        "language": "es",
        "jurisdiction": "es-pv",  # Basque Country
    }


def fetch_all(limit: int = None) -> Generator[dict, None, None]:
    """Fetch all legislation documents."""
    session = requests.Session()
    
    offset = 0
    page_size = 100
    total_fetched = 0
    
    while True:
        if limit and total_fetched >= limit:
            break
        
        current_limit = min(page_size, limit - total_fetched) if limit else page_size
        
        query = SPARQL_QUERY_LEGISLATION_TEMPLATE.format(limit=current_limit, offset=offset)
        
        print(f"Fetching legislation metadata (offset={offset}, limit={current_limit})...")
        results = fetch_sparql(query, session)
        
        if not results:
            print("No more results from SPARQL endpoint.")
            break
        
        for binding in results:
            eli_uri = binding.get('eli', {}).get('value', '')
            title = binding.get('title', {}).get('value', '')
            xml_url = binding.get('xml_url', {}).get('value', '')
            
            if not eli_uri or not xml_url:
                continue
            
            print(f"  Fetching: {eli_uri}")
            
            time.sleep(REQUEST_DELAY)
            
            xml_data = fetch_xml_content(xml_url, session)
            
            if xml_data and xml_data.get('text'):
                # Use title from SPARQL if XML doesn't have it
                if not xml_data.get('title'):
                    xml_data['title'] = title
                
                record = normalize(xml_data, eli_uri)
                total_fetched += 1
                yield record
            else:
                print(f"  Skipping (no full text): {eli_uri}", file=sys.stderr)
        
        offset += len(results)
        
        if len(results) < current_limit:
            break
    
    print(f"Total records fetched: {total_fetched}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    # For now, just use fetch_all with a date filter
    # The SPARQL endpoint doesn't have an obvious date filter
    # Could filter by ELI URI date component
    yield from fetch_all(limit=50)  # Fetch recent for updates


def save_sample(records: list, sample_dir: Path):
    """Save sample records to JSON files."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    for record in records:
        filename = f"{record['_id']}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"Saved: {filepath}")


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records meet requirements."""
    samples = list(sample_dir.glob("*.json"))
    
    if len(samples) < 10:
        print(f"ERROR: Only {len(samples)} samples, need at least 10")
        return False
    
    errors = []
    text_lengths = []
    
    for sample_path in samples:
        with open(sample_path, 'r', encoding='utf-8') as f:
            record = json.load(f)
        
        if not record.get('text'):
            errors.append(f"{sample_path.name}: missing 'text' field")
        elif len(record['text']) < 100:
            errors.append(f"{sample_path.name}: text too short ({len(record['text'])} chars)")
        else:
            text_lengths.append(len(record['text']))
        
        if not record.get('title'):
            errors.append(f"{sample_path.name}: missing 'title' field")
        
        if not record.get('_id'):
            errors.append(f"{sample_path.name}: missing '_id' field")
    
    if errors:
        print("Validation errors:")
        for err in errors:
            print(f"  - {err}")
        return False
    
    avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    print(f"Validation passed:")
    print(f"  - {len(samples)} sample records")
    print(f"  - All records have full text")
    print(f"  - Average text length: {avg_length:,.0f} chars")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="ES/BasqueCountry legislation fetcher"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # bootstrap command
    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help="Fetch sample data for testing"
    )
    bootstrap_parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch only sample data (12 records)"
    )
    bootstrap_parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="Number of records to fetch"
    )
    
    # validate command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate sample data"
    )
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    
    if args.command == "bootstrap":
        limit = args.limit if args.sample else None
        if args.sample:
            print(f"Fetching {args.limit} sample records...")
        else:
            print("Fetching all records...")
        
        records = []
        for record in fetch_all(limit=limit if args.sample else None):
            records.append(record)
            if args.sample and len(records) >= args.limit:
                break
        
        save_sample(records, sample_dir)
        
        if records:
            print(f"\nSample statistics:")
            print(f"  Total records: {len(records)}")
            text_lengths = [len(r['text']) for r in records if r.get('text')]
            if text_lengths:
                print(f"  Avg text length: {sum(text_lengths)/len(text_lengths):,.0f} chars")
                print(f"  Min text length: {min(text_lengths):,} chars")
                print(f"  Max text length: {max(text_lengths):,} chars")
        
        # Auto-validate
        validate_samples(sample_dir)
    
    elif args.command == "validate":
        if validate_samples(sample_dir):
            sys.exit(0)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
