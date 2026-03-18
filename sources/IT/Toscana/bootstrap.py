#!/usr/bin/env python3
"""
IT/Toscana - Toscana Regional Legislation Bootstrap

Fetches Tuscany regional laws and regulations from Raccolta Normativa.
Uses bulk ZIP downloads containing NIR XML format documents.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 10-20 sample records
    python bootstrap.py bootstrap --full     # Fetch all records
    python bootstrap.py fetch_updates --since YYYY-MM-DD
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from xml.etree import ElementTree as ET

import requests

# Configuration
BASE_URL = "https://raccoltanormativa.consiglio.regione.toscana.it"
DOWNLOAD_ENDPOINT = f"{BASE_URL}/class/download.php"
SOURCE_ID = "IT/Toscana"

# Document types to fetch
DOC_TYPES = ["legge", "regolamento.consiglio", "regolamento.giunta"]

# Legislatures (oldest to newest for fetch_all, newest first for sample)
LEGISLATURES = list(range(1, 11))  # 1-10

# Rate limiting
REQUEST_DELAY = 2.0

# Session for HTTP requests
session = requests.Session()
session.headers.update({
    "User-Agent": "WorldWideLaw/1.0 (Academic Research; https://github.com/worldwidelaw/legal-sources)"
})


def download_zip(doc_type: str, legislature: int) -> Optional[bytes]:
    """Download ZIP file for a specific document type and legislature."""
    params = {
        "type": "zip",
        "formato": "xml",  # NIR XML format
        "tipo": doc_type,
        "metaleg": str(legislature)
    }

    try:
        resp = session.get(DOWNLOAD_ENDPOINT, params=params, timeout=120)
        resp.raise_for_status()

        # Check if we got a ZIP file
        if resp.headers.get("Content-Type", "").startswith("application/"):
            return resp.content
        else:
            print(f"  Warning: Unexpected content type for {doc_type}/leg{legislature}")
            return None
    except requests.RequestException as e:
        print(f"  Error downloading {doc_type}/leg{legislature}: {e}")
        return None


def extract_text_from_nir(root: ET.Element) -> str:
    """Extract full text from NIR XML element, stripping tags."""
    # NIR format stores text in various elements within the structure
    text_parts = []

    # Define namespaces that might be used
    ns = {
        'h': 'http://www.w3.org/HTML/1998/html4',
        'xlink': 'http://www.w3.org/1999/xlink'
    }

    # Get all text content, preserving document structure
    def extract_text_recursive(element, depth=0):
        # Get element text
        if element.text and element.text.strip():
            text_parts.append(element.text.strip())

        # Recursively process children
        for child in element:
            extract_text_recursive(child, depth + 1)

        # Get tail text
        if element.tail and element.tail.strip():
            text_parts.append(element.tail.strip())

    extract_text_recursive(root)

    # Join with spaces and clean up
    text = " ".join(text_parts)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_nir_xml(xml_content: bytes, filename: str) -> Optional[dict]:
    """Parse NIR XML content and extract metadata and full text."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"  XML parse error for {filename}: {e}")
        return None

    # NIR root element contains document type (Legge, Regolamento, etc.)
    doc_elem = None
    doc_type_name = None

    for child in root:
        if child.tag in ("Legge", "Regolamento", "Decreto"):
            doc_elem = child
            doc_type_name = child.tag.lower()
            break

    if doc_elem is None:
        # Try to use root if it's the document itself
        doc_elem = root
        doc_type_name = root.tag.lower() if root.tag != "NIR" else "legge"

    # Extract metadata from <meta>/<descrittori>
    urn = None
    pub_date = None
    pub_num = None
    vigenza_start = None
    keywords = []

    meta = doc_elem.find(".//meta")
    if meta is not None:
        descrittori = meta.find("descrittori")
        if descrittori is not None:
            # URN
            urn_elem = descrittori.find("urn")
            if urn_elem is not None and urn_elem.text:
                urn = urn_elem.text.strip()

            # Publication info
            pub_elem = descrittori.find("pubblicazione")
            if pub_elem is not None:
                pub_date = pub_elem.get("norm")  # YYYYMMDD format
                pub_num = pub_elem.get("num")

            # Vigenza (validity)
            vig_elem = descrittori.find("vigenza")
            if vig_elem is not None:
                vigenza_start = vig_elem.get("inizio")

            # Keywords
            for kw_group in descrittori.findall("keywords"):
                for kw in kw_group.findall("keyword"):
                    val = kw.get("val")
                    if val:
                        keywords.append(val)

    # Extract header info from <intestazione>
    title = None
    doc_date = None
    doc_number = None
    emanante = None

    intestazione = doc_elem.find(".//intestazione")
    if intestazione is not None:
        # Title
        titolo = intestazione.find("titoloDoc")
        if titolo is not None:
            # Get all text including from child elements
            title = "".join(titolo.itertext()).strip()

        # Date
        data = intestazione.find("dataDoc")
        if data is not None:
            doc_date = data.get("norm")  # YYYYMMDD format

        # Number
        num = intestazione.find("numDoc")
        if num is not None and num.text:
            doc_number = num.text.strip()

        # Emanating body
        eman = intestazione.find("emanante")
        if eman is not None and eman.text:
            emanante = eman.text.strip()

    # Extract full text from entire document
    full_text = extract_text_from_nir(doc_elem)

    if not full_text or len(full_text) < 50:
        print(f"  Warning: Very short text for {filename}")

    # Format dates to ISO 8601
    def format_date(d):
        if d and len(d) == 8:
            try:
                return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            except:
                return None
        return None

    # Build document ID
    if urn:
        doc_id = urn
    elif doc_date and doc_number:
        doc_id = f"urn:nir:regione.toscana:{doc_type_name}:{format_date(doc_date)};{doc_number}"
    else:
        doc_id = f"IT/Toscana/{filename}"

    # Ensure title is never null - fall back to document identifier or filename
    if not title:
        if doc_type_name and doc_number and doc_date:
            title = f"{doc_type_name.capitalize()} n. {doc_number} del {format_date(doc_date)}"
        elif urn:
            # Extract meaningful info from URN like "urn:nir:regione.toscana:legge:2020-01-15;5"
            title = f"Documento {urn.split(':')[-1]}" if ':' in urn else f"Documento {filename}"
        else:
            # Last resort: use filename
            title = f"Documento {filename.replace('.xml', '').replace('_', ' ')}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "urn": urn,
        "title": title,
        "doc_type": doc_type_name,
        "number": doc_number,
        "date": format_date(doc_date),
        "publication_date": format_date(pub_date),
        "publication_number": pub_num,
        "vigenza_start": format_date(vigenza_start),
        "emanating_body": emanante or "Regione Toscana",
        "keywords": keywords if keywords else None,
        "text": full_text,
        "url": f"{BASE_URL}/articolo?urndoc={urn}" if urn else None,
        "format": "nir_xml",
        "language": "it"
    }


def process_zip(zip_content: bytes, doc_type: str, legislature: int) -> Generator[dict, None, None]:
    """Process a ZIP file and yield parsed documents."""
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "data.zip"
        zip_path.write_bytes(zip_content)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for name in zf.namelist():
                    # Only process XML files
                    if not name.endswith('.xml') and not name.startswith('legge-'):
                        continue

                    try:
                        xml_content = zf.read(name)
                        record = parse_nir_xml(xml_content, name)
                        if record and record.get("text"):
                            record["legislature"] = legislature
                            record["source_doc_type"] = doc_type
                            yield record
                    except Exception as e:
                        print(f"  Error processing {name}: {e}")
        except zipfile.BadZipFile:
            print(f"  Invalid ZIP file for {doc_type}/leg{legislature}")


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """
    Fetch all Tuscany regional legislation.

    Args:
        sample: If True, only fetch from the most recent legislature
    """
    legislatures = [10] if sample else LEGISLATURES
    doc_types = ["legge"] if sample else DOC_TYPES

    total_count = 0
    sample_limit = 15 if sample else float('inf')

    for leg in reversed(legislatures):  # Newest first
        for doc_type in doc_types:
            if total_count >= sample_limit:
                return

            print(f"Fetching {doc_type} legislature {leg}...")
            zip_content = download_zip(doc_type, leg)

            if zip_content:
                for record in process_zip(zip_content, doc_type, leg):
                    if total_count >= sample_limit:
                        return
                    total_count += 1
                    yield record

                    if total_count % 50 == 0:
                        print(f"  Processed {total_count} records...")

            time.sleep(REQUEST_DELAY)

    print(f"Total records fetched: {total_count}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """
    Fetch documents updated since a given date.

    Since the bulk download doesn't support filtering by date,
    we fetch the most recent legislature and filter by date.
    """
    since_date = datetime.strptime(since, "%Y-%m-%d")

    print(f"Fetching updates since {since}...")

    for doc_type in DOC_TYPES:
        print(f"Checking {doc_type} from recent legislatures...")

        # Check last 2 legislatures for updates
        for leg in [10, 9]:
            zip_content = download_zip(doc_type, leg)

            if zip_content:
                for record in process_zip(zip_content, doc_type, leg):
                    # Filter by date if available
                    doc_date_str = record.get("date") or record.get("publication_date")
                    if doc_date_str:
                        try:
                            doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d")
                            if doc_date >= since_date:
                                yield record
                        except ValueError:
                            # If date parsing fails, include the record
                            yield record

            time.sleep(REQUEST_DELAY)


def normalize(raw: dict) -> dict:
    """Normalize a raw record to the standard schema."""
    # Records from parse_nir_xml are already in normalized format
    return raw


def save_sample(records: list, sample_dir: Path):
    """Save sample records to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filename = f"sample_{i+1:03d}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} sample records to {sample_dir}")


def main():
    parser = argparse.ArgumentParser(description="IT/Toscana Regional Legislation Bootstrap")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch legislation")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Fetch all records")

    # Fetch updates command
    updates_parser = subparsers.add_parser("fetch_updates", help="Fetch recent updates")
    updates_parser.add_argument("--since", required=True, help="Date in YYYY-MM-DD format")

    args = parser.parse_args()

    # Determine sample directory
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        sample_mode = args.sample or not args.full

        records = list(fetch_all(sample=sample_mode))

        if sample_mode:
            save_sample(records, sample_dir)
        else:
            # For full mode, just print summary
            print(f"Fetched {len(records)} records")
            if records:
                avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
                print(f"Average text length: {avg_text_len:.0f} characters")

    elif args.command == "fetch_updates":
        records = list(fetch_updates(args.since))
        print(f"Found {len(records)} updated records since {args.since}")

        # Save as sample for verification
        if records:
            save_sample(records[:20], sample_dir)


if __name__ == "__main__":
    main()
