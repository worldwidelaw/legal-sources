#!/usr/bin/env python3
"""
GT/CC -- Guatemala Corte de Constitucionalidad (Constitutional Court)

Fetches Guatemalan Constitutional Court decisions via the Elasticsearch-backed
jurisprudencia API. Full text is extracted from PDF documents.

API: POST https://jurisprudencia.cc.gob.gt/coredataretriever/api/jurisprudencia/V1
PDFs: https://jurisprudencia.cc.gob.gt/Sentencias/{id}.{expediente}.pdf

~65,000 decisions from 1986 to present.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "GT/CC"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GT.CC")

API_URL = "https://jurisprudencia.cc.gob.gt/coredataretriever/api/jurisprudencia/V1"
PDF_BASE = "https://jurisprudencia.cc.gob.gt/Sentencias"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.verify = False  # SSL cert issues on jurisprudencia.cc.gob.gt

# Suppress SSL warnings
import urllib3

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PAGE_SIZE = 100
RATE_LIMIT = 1.5  # seconds between requests


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="GT/CC",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def build_pdf_url(doc: dict) -> str:
    """Build HTTPS PDF URL from document data."""
    raw_pdf = doc.get("pdf", "")
    if not raw_pdf:
        doc_id = doc.get("id", "")
        exps = doc.get("expedientes", [])
        exp_str = exps[0] if exps else ""
        if doc_id and exp_str:
            return f"{PDF_BASE}/{doc_id}.{exp_str}.pdf"
        return ""

    # Convert HTTP IP-based URLs to HTTPS domain URLs
    # e.g. http://143.208.58.124/Sentencias/790964.61-98.pdf
    #   -> https://jurisprudencia.cc.gob.gt/Sentencias/790964.61-98.pdf
    parsed = urlparse(raw_pdf)
    filename = parsed.path.split("/")[-1] if parsed.path else ""
    if filename:
        return f"{PDF_BASE}/{filename}"
    return raw_pdf


def fetch_page(search_term: str, start: int, length: int) -> dict:
    """Fetch a page of results from the API."""
    payload = {
        "mainSearch": search_term,
        "draw": 1,
        "start": start,
        "length": length,
        "columns": [],
        "order": [],
        "search": {"value": ""},
    }
    resp = SESSION.post(API_URL, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_pdf_text(pdf_url: str) -> str:
    """Download PDF and extract text."""
    if not pdf_url:
        return ""
    try:
        pdf_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            "Accept": "application/pdf,*/*",
        }
        resp = SESSION.get(pdf_url, timeout=120, headers=pdf_headers)
        resp.raise_for_status()
        if len(resp.content) < 100:
            return ""
        return extract_text_from_pdf(resp.content)
    except Exception as e:
        logger.warning(f"Failed to fetch/extract PDF {pdf_url}: {e}")
        return ""


def normalize(doc: dict, full_text: str) -> dict:
    """Normalize a document to standard schema."""
    doc_id = doc.get("id", "")
    exps = doc.get("expedientes", [])
    exp_str = ", ".join(exps) if exps else str(doc_id)

    fecha = doc.get("fechaSentencia")
    date_str = None
    if fecha:
        try:
            date_str = fecha[:10]  # "2026-03-04T00:00:00Z" -> "2026-03-04"
        except Exception:
            date_str = None

    tipo = doc.get("tipoExpediente", "")
    pdf_url = build_pdf_url(doc)

    # Use full text from PDF, or fall back to intro
    text = full_text if full_text else doc.get("intro", "")

    title = f"Expediente {exp_str}"
    if tipo:
        title = f"{tipo} - Expediente {exp_str}"

    return {
        "_id": str(doc_id),
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": pdf_url or f"https://jurisprudencia.cc.gob.gt/ptmp/AtributoElastic.aspx?id={doc_id}",
        "expediente": exp_str,
        "tipo_expediente": tipo,
        "tema": doc.get("tema"),
        "sub_tema": doc.get("subTema"),
        "fecha_publicacion": doc.get("fechaPublicacion"),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all documents. If sample=True, fetch only ~15 documents."""
    search_term = "Guatemala"  # Matches ~65K docs (virtually all)
    max_docs = 15 if sample else 999999

    # Get total count
    first_page = fetch_page(search_term, 0, 1)
    total = first_page.get("recordsFiltered", 0)
    logger.info(f"Total documents available: {total}")

    if sample:
        total = min(total, max_docs)

    yielded = 0
    start = 0
    pdf_failures = 0

    while start < total and yielded < max_docs:
        batch_size = min(PAGE_SIZE, max_docs - yielded)
        logger.info(f"Fetching page at offset {start} (batch size {batch_size})")

        try:
            data = fetch_page(search_term, start, batch_size)
        except Exception as e:
            logger.error(f"API request failed at offset {start}: {e}")
            break

        docs = data.get("documentos", [])
        if not docs:
            logger.info("No more documents returned")
            break

        for doc in docs:
            if yielded >= max_docs:
                break

            pdf_url = build_pdf_url(doc)
            logger.info(f"Processing doc {doc.get('id')} - {doc.get('expedientes', [])}")

            # Download and extract PDF text
            full_text = fetch_pdf_text(pdf_url)
            if not full_text:
                pdf_failures += 1
                logger.warning(f"No text extracted from PDF for doc {doc.get('id')}")
                # Use intro as fallback (500 chars of actual text)
                full_text = doc.get("intro", "")

            record = normalize(doc, full_text)
            if record.get("text"):
                yield record
                yielded += 1
            else:
                logger.warning(f"Skipping doc {doc.get('id')} - no text available")

            time.sleep(RATE_LIMIT)

        start += len(docs)

    logger.info(f"Finished: yielded {yielded} documents, {pdf_failures} PDF extraction failures")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents published since a given date."""
    # The API doesn't support date filtering directly,
    # but fechaPublicacion is available for post-filtering
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))

    for record in fetch_all(sample=False):
        pub_date = record.get("fecha_publicacion")
        if pub_date:
            try:
                pub_dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                if pub_dt >= since_dt:
                    yield record
            except Exception:
                yield record
        else:
            yield record


def test_api():
    """Quick API test."""
    print("Testing GT/CC API...")

    # Test search API
    data = fetch_page("Guatemala", 0, 3)
    total = data.get("recordsFiltered", 0)
    docs = data.get("documentos", [])
    print(f"Total documents: {total}")
    print(f"Sample batch: {len(docs)} docs")

    for doc in docs:
        print(f"\n  ID: {doc.get('id')}")
        print(f"  Expediente: {doc.get('expedientes')}")
        print(f"  Tipo: {doc.get('tipoExpediente')}")
        print(f"  Fecha: {doc.get('fechaSentencia')}")
        print(f"  PDF: {build_pdf_url(doc)}")
        print(f"  Intro (first 200): {doc.get('intro', '')[:200]}")

    # Test PDF download and extraction
    if docs:
        pdf_url = build_pdf_url(docs[0])
        print(f"\nTesting PDF extraction: {pdf_url}")
        text = fetch_pdf_text(pdf_url)
        print(f"  Extracted text length: {len(text)}")
        print(f"  First 300 chars: {text[:300]}")

    print("\nAPI test complete.")


def bootstrap(sample: bool = False):
    """Run bootstrap to fetch and save documents."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    text_lengths = []

    for record in fetch_all(sample=sample):
        count += 1
        text_len = len(record.get("text", ""))
        text_lengths.append(text_len)

        if sample:
            outfile = SAMPLE_DIR / f"{record['_id']}.json"
            with open(outfile, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved sample {count}: {record['title']} (text: {text_len} chars)")
        else:
            # In full mode, write to stdout as JSONL
            print(json.dumps(record, ensure_ascii=False))

        if sample and count >= 15:
            break

    if text_lengths:
        avg_text = sum(text_lengths) / len(text_lengths)
        min_text = min(text_lengths)
        max_text = max(text_lengths)
        logger.info(f"Done: {count} records. Text length avg={avg_text:.0f}, min={min_text}, max={max_text}")
    else:
        logger.warning("No records fetched!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GT/CC data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample data")
    parser.add_argument("--full", action="store_true", help="Fetch all data")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample or not args.full)
