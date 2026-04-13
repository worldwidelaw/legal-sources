#!/usr/bin/env python3
"""
CO/Procuraduria - Colombia Procuraduria Disciplinary Decisions Fetcher

Fetches disciplinary decisions (fallos) from the Procuraduria General de la Nacion
via the datos.gov.co Socrata open data API, then extracts full text from linked
PDF/DOCX documents.

The dataset is denormalized: one document may appear multiple times with different
topic tags. We group by document URL and deduplicate.

Data source: https://www.datos.gov.co/resource/rhun-uf37.json
License: Open Data Colombia

Usage:
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import hashlib
import io
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from collections import defaultdict

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "CO/Procuraduria"
SOCRATA_URL = "https://www.datos.gov.co/resource/rhun-uf37.json"

FALLO_TYPES = [
    "FALLO SEGUNDA",
    "FALLO PRIMERA",
    "FALLO PRIMERA NO APELADO",
    "UNICA INSTANCIA",
    "REVOCATORIA DIRECTA",
]

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (Legal Research; Open Data Collection)",
    "Accept": "application/json",
}

SAMPLE_DIR = Path(__file__).parent / "sample"


def extract_docx_text(content: bytes) -> str:
    """Extract text from DOCX bytes."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(content))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        return "\n\n".join(paragraphs)
    except Exception as e:
        print(f"    DOCX extraction error: {e}")
        return ""


def fetch_document_text(doc_url: str, session: requests.Session, source_id: str) -> str:
    """Download and extract text from a PDF or DOCX document."""
    if not doc_url:
        return ""
    try:
        resp = session.get(doc_url, headers=HEADERS, timeout=60, allow_redirects=True)
        resp.raise_for_status()
        content = resp.content
        if len(content) < 100:
            return ""

        # Detect type by magic bytes first
        if content[:5] == b"%PDF-":
            return extract_pdf_markdown(
                source=SOURCE_ID, source_id=source_id,
                pdf_bytes=content, table="case_law",
            ) or ""

        # Try DOCX (PK magic bytes = ZIP = DOCX)
        if content[:2] == b"PK":
            return extract_docx_text(content)

        # Fallback based on URL/content-type
        ct = resp.headers.get("Content-Type", "").lower()
        if "pdf" in ct:
            return extract_pdf_markdown(
                source=SOURCE_ID, source_id=source_id,
                pdf_bytes=content, table="case_law",
            ) or ""
        if "word" in ct or "msword" in ct:
            return extract_docx_text(content)

        return ""
    except requests.RequestException as e:
        print(f"    Error fetching document: {e}")
        return ""


def fetch_and_group_records(session: requests.Session, limit: int = 50000,
                            where: str = None) -> dict:
    """Fetch records from Socrata and group by document URL to deduplicate."""
    params = {
        "$limit": limit,
        "$offset": 0,
        "$order": "fecha_documento DESC",
    }
    if where:
        params["$where"] = where

    grouped = {}  # url -> merged record info
    total = 0

    while True:
        resp = session.get(SOCRATA_URL, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        items = resp.json()
        if not items:
            break

        for item in items:
            url_obj = item.get("url_documento", {})
            doc_url = ""
            if isinstance(url_obj, dict):
                doc_url = url_obj.get("url", "") or ""
            elif isinstance(url_obj, str):
                doc_url = url_obj

            if not doc_url:
                continue

            tema = item.get("tema", "") or ""
            subtema = item.get("subtema", "") or ""

            if doc_url not in grouped:
                grouped[doc_url] = {
                    "tipo_documento": item.get("tipo_documento", ""),
                    "n_mero_documento": item.get("n_mero_documento", ""),
                    "dependencia": item.get("dependencia", ""),
                    "fecha_documento": item.get("fecha_documento", ""),
                    "url": doc_url,
                    "temas": [],
                }

            if tema:
                entry = tema
                if subtema:
                    entry += f": {subtema}"
                if entry not in grouped[doc_url]["temas"]:
                    grouped[doc_url]["temas"].append(entry)

        total += len(items)
        if len(items) < limit:
            break
        params["$offset"] = params.get("$offset", 0) + limit

    print(f"Fetched {total} raw rows, grouped into {len(grouped)} unique documents")
    return grouped


def normalize(doc_info: dict, doc_text: str = "") -> dict:
    """Transform grouped document info into standard schema."""
    tipo = doc_info.get("tipo_documento", "")
    numero = doc_info.get("n_mero_documento", "")
    dependencia = doc_info.get("dependencia", "")
    fecha = doc_info.get("fecha_documento", "")
    doc_url = doc_info.get("url", "")
    temas = doc_info.get("temas", [])

    date = fecha[:10] if fecha else ""

    # Title
    title_parts = [tipo]
    if numero and numero not in ("NA", "0", ""):
        title_parts.append(f"No. {numero}")
    if dependencia:
        title_parts.append(f"- {dependencia}")
    title = " ".join(title_parts)

    full_text = doc_text.strip() if doc_text else ""

    if not full_text:
        parts = []
        if tipo:
            parts.append(f"Tipo: {tipo}")
        if numero and numero not in ("NA", "0"):
            parts.append(f"Número: {numero}")
        if dependencia:
            parts.append(f"Dependencia: {dependencia}")
        if temas:
            parts.append(f"Temas: {'; '.join(temas)}")
        if date:
            parts.append(f"Fecha: {date}")
        full_text = "\n".join(parts)

    # Stable ID from URL
    doc_id = f"CO_PROC_{hashlib.md5(doc_url.encode()).hexdigest()[:12]}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": doc_url,
        "tipo_documento": tipo,
        "numero_documento": numero,
        "dependencia": dependencia,
        "temas": temas,
    }


def fetch_all(max_records: int = None, tipos: list = None,
              since: str = None) -> Generator[dict, None, None]:
    """Fetch all disciplinary decisions."""
    session = requests.Session()

    # Build where clause
    clauses = []
    if tipos:
        tipo_list = ",".join(f"'{t}'" for t in tipos)
        clauses.append(f"tipo_documento in ({tipo_list})")
    if since:
        clauses.append(f"fecha_documento >= '{since}'")
    where = " AND ".join(clauses) if clauses else None

    print(f"Fetching records with filter: {where}")

    grouped = fetch_and_group_records(session, where=where)

    count = 0
    errors = 0

    # Sort by date descending
    sorted_docs = sorted(grouped.values(),
                         key=lambda d: d.get("fecha_documento", ""), reverse=True)

    for doc_info in sorted_docs:
        if max_records and count >= max_records:
            return

        tipo = doc_info["tipo_documento"]
        numero = doc_info["n_mero_documento"]
        fecha = doc_info.get("fecha_documento", "")[:10]
        doc_url = doc_info["url"]
        print(f"  [{count+1}] {tipo} {numero} ({fecha})")

        time.sleep(1.5)
        sid = f"CO_PROC_{hashlib.md5(doc_url.encode()).hexdigest()[:12]}"
        doc_text = fetch_document_text(doc_url, session, sid)
        if doc_text:
            print(f"    Document: {len(doc_text):,} chars")
        else:
            print(f"    No text extracted")

        record = normalize(doc_info, doc_text)
        if record["text"] and len(record["text"]) >= 50:
            yield record
            count += 1
        else:
            print(f"    Skipped (insufficient text)")
            errors += 1

    print(f"\nTotal records: {count}, errors: {errors}")


def fetch_updates(since: datetime, **kwargs) -> Generator[dict, None, None]:
    """Fetch decisions updated since the given date."""
    since_str = since.strftime("%Y-%m-%dT00:00:00.000")
    yield from fetch_all(since=since_str, **kwargs)


def bootstrap_sample(sample_count: int = 15) -> bool:
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    errors = 0

    for record in fetch_all(max_records=sample_count + 5, tipos=FALLO_TYPES):
        if len(records) >= sample_count:
            break

        if record["text"] and len(record["text"]) >= 100:
            records.append(record)
            filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            print(f"    Saved: {len(record['text']):,} chars")
        else:
            errors += 1

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
    print(f"Errors: {errors}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        has_substantial = sum(1 for r in records if len(r.get("text", "")) > 500)
        print(f"Records with substantial text (>500 chars): {has_substantial}/{len(records)}")

        unique_ids = len(set(r["_id"] for r in records))
        print(f"Unique IDs: {unique_ids}/{len(records)}")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text") or len(r["text"]) < 100)
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description=f"{SOURCE_ID} disciplinary decisions fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap (all document types)")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            tipos = None if args.full else FALLO_TYPES
            for record in fetch_all(tipos=tipos):
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
