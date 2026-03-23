#!/usr/bin/env python3
"""
AR/InfoLEG -- Sistema de Información Legislativa Data Fetcher

Fetches Argentine legislation from the InfoLEG open data portal.

Data source: https://datos.jus.gob.ar/dataset/base-de-datos-legislativos-infoleg
License: CC BY 4.0

Strategy:
  - Download CSV metadata from CKAN bulk export
  - For each record with texto_original URL, fetch full HTML text
  - Parse HTML to extract clean text content
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import csv
import io
import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AR/InfoLEG"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.InfoLEG")

# API Configuration
CKAN_BASE = "https://datos.jus.gob.ar"
DATASET_ID = "d9a963ea-8b1d-4ca3-9dd9-07a4773e8c23"
SAMPLE_CSV_URL = f"{CKAN_BASE}/dataset/{DATASET_ID}/resource/8b1c2310-564e-41e6-9a84-99cfa9939bbc/download/base-infoleg-normativa-nacional-muestreo.csv"
FULL_ZIP_URL = f"{CKAN_BASE}/dataset/{DATASET_ID}/resource/bf0ec116-ad4e-4572-a476-e57167a84403/download/base-infoleg-normativa-nacional.zip"

# Request headers
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""

    # Remove script and style tags with content
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)

    # Remove header/branding sections
    html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html)

    # Decode HTML entities
    text = unescape(text)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()

    return text


def fetch_full_text(url: str, timeout: int = 30) -> Optional[str]:
    """
    Fetch full text from InfoLEG HTML page.

    Args:
        url: URL to texto_original HTML page
        timeout: Request timeout in seconds

    Returns:
        Cleaned text content or None if fetch failed
    """
    try:
        # Handle both http and https
        if url.startswith("http://"):
            url_https = url.replace("http://", "https://", 1)
        else:
            url_https = url

        # Try HTTPS first, fall back to HTTP
        try:
            response = requests.get(url_https, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()

        # Handle encoding - try multiple encodings
        content = None
        for encoding in ['iso-8859-1', 'utf-8', 'latin-1', 'cp1252']:
            try:
                content = response.content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue

        if content is None:
            content = response.content.decode('utf-8', errors='replace')

        # Extract main content - look for the justify div
        match = re.search(r'<div[^>]*style="text-align:\s*justify[^"]*"[^>]*>(.*?)</div>\s*</body>',
                         content, re.DOTALL | re.IGNORECASE)
        if match:
            return clean_html(match.group(1))

        # Fallback: get body content
        match = re.search(r'<body[^>]*>(.*?)</body>', content, re.DOTALL | re.IGNORECASE)
        if match:
            return clean_html(match.group(1))

        return clean_html(content)

    except requests.RequestException as e:
        logger.warning(f"Failed to fetch full text from {url}: {e}")
        return None


def normalize(raw: dict, full_text: Optional[str] = None) -> dict:
    """
    Transform raw CSV record to standard schema.

    Args:
        raw: Raw CSV row as dict
        full_text: Optional pre-fetched full text

    Returns:
        Normalized record with standard fields
    """
    id_norma = raw.get("id_norma", "")
    tipo_norma = raw.get("tipo_norma", "")
    numero_norma = raw.get("numero_norma", "")

    # Generate ID if not present
    if not id_norma:
        # Create a synthetic ID from type and number
        id_norma = f"{tipo_norma}_{numero_norma}_{raw.get('fecha_sancion', 'unknown')}"
        id_norma = re.sub(r'[^a-zA-Z0-9_-]', '_', id_norma)

    # Build title
    title = raw.get("titulo_resumido", "")
    if not title:
        title = raw.get("titulo_sumario", "")
    if not title and tipo_norma and numero_norma:
        title = f"{tipo_norma} {numero_norma}"

    # Get text - prefer full text, fallback to texto_resumido
    text = full_text if full_text else ""
    if not text:
        text = raw.get("texto_resumido", "")

    # Parse date
    date = raw.get("fecha_sancion", "")
    if date:
        try:
            # Format: YYYY-MM-DD
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            date = None

    # URL
    url = raw.get("texto_original", "")
    if not url:
        url = raw.get("texto_actualizado", "")
    if not url and id_norma:
        # Construct URL from ID
        # ID format appears to be sequential within ranges like 370000-374999
        try:
            id_int = int(id_norma)
            range_start = (id_int // 5000) * 5000
            range_end = range_start + 4999
            url = f"http://servicios.infoleg.gob.ar/infolegInternet/anexos/{range_start:06d}-{range_end:06d}/{id_norma}/norma.htm"
        except (ValueError, TypeError):
            pass

    return {
        "_id": str(id_norma),
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "id_norma": str(id_norma),
        "tipo_norma": tipo_norma,
        "numero_norma": numero_norma,
        "clase_norma": raw.get("clase_norma", ""),
        "title": title or f"Norma {id_norma}",
        "text": text,
        "date": date,
        "organismo_origen": raw.get("organismo_origen", ""),
        "numero_boletin": raw.get("numero_boletin", ""),
        "fecha_boletin": raw.get("fecha_boletin", ""),
        "pagina_boletin": raw.get("pagina_boletin", ""),
        "titulo_sumario": raw.get("titulo_sumario", ""),
        "texto_resumido": raw.get("texto_resumido", ""),
        "observaciones": raw.get("observaciones", ""),
        "texto_actualizado": raw.get("texto_actualizado", ""),
        "url": url,
        "modificada_por": raw.get("modificada_por", ""),
        "modifica_a": raw.get("modifica_a", ""),
    }


def fetch_sample(count: int = 15) -> list:
    """
    Fetch sample documents from CSV and retrieve full text.

    Uses sample CSV (1000 records) and fetches full text for
    records that have texto_original URLs.
    """
    records = []

    logger.info(f"Downloading sample CSV from datos.jus.gob.ar...")

    try:
        response = requests.get(SAMPLE_CSV_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()

        # Handle BOM and encoding
        content = response.content.decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(content))

        # Collect records with texto_original URLs
        candidates = []
        for row in reader:
            if row.get("texto_original") and row.get("id_norma"):
                candidates.append(row)
                if len(candidates) >= count * 2:  # Get extras in case some fail
                    break

        logger.info(f"Found {len(candidates)} records with full text URLs")

        # Fetch full text for each
        for row in candidates:
            if len(records) >= count:
                break

            url = row["texto_original"]
            logger.info(f"  Fetching full text for {row.get('tipo_norma')} {row.get('numero_norma')}...")

            full_text = fetch_full_text(url)
            time.sleep(1)  # Rate limit

            if full_text and len(full_text) > 100:
                normalized = normalize(row, full_text)
                records.append(normalized)
                logger.info(f"  [{len(records)}/{count}] {normalized['title'][:50]}... ({len(full_text)} chars)")
            else:
                logger.warning(f"  Skipped - no/short text from {url}")

    except Exception as e:
        logger.error(f"Error fetching samples: {e}")
        raise

    return records


def fetch_all() -> Generator[dict, None, None]:
    """
    Fetch all documents from the full ZIP export.

    Downloads full ZIP, extracts CSV, and yields normalized records
    with full text fetched for each document.
    """
    logger.info("Downloading full InfoLEG ZIP archive...")

    response = requests.get(FULL_ZIP_URL, headers=HEADERS, timeout=300, stream=True)
    response.raise_for_status()

    # Read ZIP into memory
    zip_data = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_data) as zf:
        # Find the CSV file inside
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise ValueError("No CSV file found in ZIP archive")

        csv_name = csv_files[0]
        logger.info(f"Reading {csv_name} from archive...")

        with zf.open(csv_name) as f:
            content = f.read().decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(content))

            count = 0
            for row in reader:
                # Only process records with texto_original URLs
                if not row.get("texto_original") or not row.get("id_norma"):
                    continue

                url = row["texto_original"]
                full_text = fetch_full_text(url)
                time.sleep(1)  # Rate limit

                if full_text and len(full_text) > 50:
                    normalized = normalize(row, full_text)
                    count += 1
                    if count % 100 == 0:
                        logger.info(f"  Processed {count} records...")
                    yield normalized


def test_api():
    """Test API connectivity and response structure."""
    logger.info("Testing InfoLEG data availability...")

    try:
        # Test CKAN API
        api_url = f"{CKAN_BASE}/api/3/action/package_show?id=base-de-datos-legislativos-infoleg"
        response = requests.get(api_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("success"):
            logger.info("CKAN API OK - Dataset accessible")
            resources = data.get("result", {}).get("resources", [])
            logger.info(f"  Found {len(resources)} resources")

        # Test sample CSV download
        logger.info("Testing sample CSV download...")
        response = requests.get(SAMPLE_CSV_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()
        content = response.content.decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        logger.info(f"  Sample CSV OK - {len(rows)} rows")

        # Test full text fetch
        for row in rows:
            if row.get("texto_original"):
                logger.info(f"Testing full text fetch: {row['texto_original'][:60]}...")
                text = fetch_full_text(row["texto_original"])
                if text:
                    logger.info(f"  Full text OK - {len(text)} characters")
                    return True
                break

        logger.warning("Could not test full text fetch - no texto_original URLs found")
        return True

    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def bootstrap_sample():
    """Fetch and save sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    # Save individual files
    for i, record in enumerate(records, 1):
        filename = f"sample_{i:02d}_{record['id_norma']}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Print summary
    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    # Validate
    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0} chars")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0} chars")

    # Check data types
    types = set(r.get("tipo_norma", "") for r in records)
    logger.info(f"  - Norm types: {', '.join(sorted(types))}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="AR/InfoLEG Legislation Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)"
    )

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode - processing all records")
            count = 0
            for record in fetch_all():
                count += 1
                # Save to sample dir for now
                SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
                filepath = SAMPLE_DIR / f"record_{record['id_norma']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")
            sys.exit(0)


if __name__ == "__main__":
    main()
