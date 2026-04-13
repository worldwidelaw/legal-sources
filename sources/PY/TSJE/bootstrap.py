#!/usr/bin/env python3
"""
PY/TSJE -- Paraguay Electoral Justice Tribunal

Fetches decisions, resolutions, and sentences from the Tribunal Superior
de Justicia Electoral del Paraguay.

Data source: https://www.tsje.gov.py/legislaciones/
License: Open Government Data (Paraguay)

Strategy:
  - POST to /legislaciones/buscar/ with year parameter to get all documents
  - Parse HTML response for document metadata and PDF links
  - Download PDFs from static path and extract full text
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "PY/TSJE"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PY.TSJE")

BASE_URL = "https://www.tsje.gov.py"
SEARCH_URL = f"{BASE_URL}/legislaciones/buscar/"
STATIC_PDF_BASE = f"{BASE_URL}/static/ups/legislaciones/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
    "Referer": f"{BASE_URL}/legislaciones/",
    "Origin": BASE_URL,
    "Content-Type": "application/x-www-form-urlencoded",
}

REQUEST_DELAY = 1.5
MIN_TEXT_LENGTH = 50
START_YEAR = 1991
END_YEAR = 2026

# Case law document types (judicial decisions)
CASE_LAW_TYPES = {
    "acuerdo y sentencia",
    "sentencia definitiva",
    "sentencia",
    "auto interlocutorio",
    "resolucion tsje",
    "resolucion presidencia",
    "resolucion tribunales",
}


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="PY/TSJE",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def download_pdf_text(url: str) -> str:
    """Download PDF and extract text."""
    if not url:
        return ""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=60,
        )
        resp.raise_for_status()
        if len(resp.content) < 100:
            return ""
        return extract_text_from_pdf(resp.content)
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to download PDF {url}: {e}")
        return ""


def clean_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_search_results(html: str) -> list:
    """Parse HTML search results into document records."""
    docs = []

    # Find document type sections: h4 tags that contain document type names
    # (skip navigation h4 tags that contain <a> or <i> icons)
    type_positions = []
    for m in re.finditer(r"<h4>([^<]+)</h4>", html):
        type_name = m.group(1).strip()
        if type_name:
            type_positions.append((m.start(), type_name))

    if not type_positions:
        return docs

    # For each type section, find all h5 entries until the next h4
    for i, (pos, doc_type) in enumerate(type_positions):
        end_pos = type_positions[i + 1][0] if i + 1 < len(type_positions) else len(html)
        section = html[pos:end_pos]

        # Find all h5 entries with their download links and descriptions
        # Pattern: h5 with doc number, then descarga.php link, then description span
        pattern = (
            r'<h5[^>]*>\s*'
            r'([\d]+/[\d]+)\s*'                          # doc number like 189/2025
            r'<span\s+class="descarga">'
            r'.*?'                                         # skip commented-out links
            r'<a\s+href="([^"]*descarga\.php\?id=[^"]+)"' # actual download link
            r'.*?</h5>\s*'
            r'<span[^>]*sprocket-lists-desc[^>]*>\s*'
            r'(.*?)\s*</span>'                            # description
        )

        for m in re.finditer(pattern, section, re.DOTALL):
            doc_number = m.group(1).strip()
            pdf_url = m.group(2).strip()
            description = clean_html(m.group(3)).strip()

            # Extract PDF filename
            pdf_filename = ""
            fname_match = re.search(r"id=([^&\"]+)", pdf_url)
            if fname_match:
                pdf_filename = fname_match.group(1)

            docs.append({
                "doc_type": doc_type,
                "doc_number": doc_number,
                "description": description,
                "pdf_url": pdf_url,
                "pdf_filename": pdf_filename,
            })

    return docs


def fetch_year(year: int) -> list:
    """Fetch all documents for a given year."""
    try:
        resp = requests.post(
            SEARCH_URL,
            data={"tipo": "", "anio": str(year)},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        return parse_search_results(resp.text)
    except Exception as e:
        logger.error(f"Failed to fetch year {year}: {e}")
        return []


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all TSJE documents with full text."""
    total = 0
    skipped = 0

    # Iterate years in reverse (newest first)
    for year in range(END_YEAR, START_YEAR - 1, -1):
        time.sleep(REQUEST_DELAY)
        docs = fetch_year(year)
        if not docs:
            logger.info(f"  {year}: no documents")
            continue

        logger.info(f"  {year}: {len(docs)} documents found")

        for doc in docs:
            time.sleep(REQUEST_DELAY)
            text = download_pdf_text(doc["pdf_url"])

            if not text or len(text) < MIN_TEXT_LENGTH:
                skipped += 1
                logger.debug(f"  Skipped {doc['doc_number']} (text too short: {len(text or '')} chars)")
                continue

            # Build title from type + number
            title = f"{doc['doc_type']} {doc['doc_number']}".strip()

            # Create unique ID from filename or number
            safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", doc.get("pdf_filename", doc["doc_number"]))

            record = {
                "_id": f"PY-TSJE-{safe_id}",
                "_source": SOURCE_ID,
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": text,
                "date": f"{year}-01-01",  # Year-level precision
                "url": doc["pdf_url"],
                "document_number": doc["doc_number"],
                "document_type": doc["doc_type"],
                "description": doc["description"],
                "year": year,
            }
            yield record
            total += 1

            if limit and total >= limit:
                logger.info(f"Reached limit of {limit} records")
                return

    logger.info(f"Total: {total} records fetched, {skipped} skipped (no text)")


def test_api():
    """Test TSJE API connectivity."""
    logger.info("Testing TSJE search endpoint...")

    # Test with recent year
    docs = fetch_year(2025)
    logger.info(f"2025: {len(docs)} documents")

    if docs:
        doc = docs[0]
        logger.info(f"  Sample: {doc['doc_type']} {doc['doc_number']}")
        logger.info(f"  Description: {doc['description'][:100]}")
        logger.info(f"  PDF: {doc['pdf_url']}")

        time.sleep(REQUEST_DELAY)
        text = download_pdf_text(doc["pdf_url"])
        logger.info(f"  Text extracted: {len(text)} chars")
        if text:
            logger.info(f"  Preview: {text[:200]}...")

    # Quick count across a few years
    time.sleep(REQUEST_DELAY)
    docs_2024 = fetch_year(2024)
    logger.info(f"2024: {len(docs_2024)} documents")

    time.sleep(REQUEST_DELAY)
    docs_2021 = fetch_year(2021)
    logger.info(f"2021: {len(docs_2021)} documents")


def bootstrap(sample: bool = False, full: bool = False):
    """Run bootstrap: fetch and save records."""
    limit = 15 if sample else None
    out_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    text_lengths = []

    for record in fetch_all(limit=limit):
        records.append(record)
        text_lengths.append(len(record.get("text", "")))

        safe_id = record["_id"].replace("/", "_").replace(" ", "_")
        filepath = out_dir / f"{safe_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {record['_id']} ({len(record.get('text', '')):,} chars)")

    if records:
        avg_text = sum(text_lengths) / len(text_lengths)
        logger.info(f"\n{'='*60}")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"Avg text length: {avg_text:,.0f} chars")
        logger.info(f"Min text length: {min(text_lengths):,} chars")
        logger.info(f"Max text length: {max(text_lengths):,} chars")
        logger.info(f"Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
        logger.info(f"Output directory: {out_dir}")
    else:
        logger.warning("No records fetched!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PY/TSJE data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
