#!/usr/bin/env python3
"""
LU/Courts - Luxembourg Courts Decisions (justice.public.lu)

Covers both judicial and administrative court decisions:
- Judicial courts (~46,435): Cour supérieure de justice, Tribunaux d'arrondissement,
  Justices de Paix — PDFs from anon.public.lu
- Administrative courts (~31,779): Tribunal administratif, Cour administrative
  — PDFs from ja.public.lu

Full text extracted from PDFs using pdfplumber.
"""

import argparse
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


SOURCE_ID = "LU/Courts"

JUDICIAL_URL = "https://justice.public.lu/fr/jurisprudence/juridictions-judiciaires.html"
ADMIN_URL = "https://justice.public.lu/fr/jurisprudence/juridictions-administratives.html"

REQUEST_DELAY = 1.5  # seconds between PDF downloads
PAGE_SIZE = 20  # results per page on justice.public.lu

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def parse_listing_page(html: str, court_type: str) -> list:
    """Parse a justice.public.lu listing page and extract decision metadata."""
    results = []

    # Split by article blocks
    articles = re.findall(
        r'<article class="article search-result search-result--jurisprudence"[^>]*>(.*?)</article>',
        html, re.DOTALL
    )

    for article in articles:
        record = {"court_type": court_type}

        # Extract PDF URL
        pdf_match = re.search(r'href="(https?://[^"]+\.pdf)"', article)
        if not pdf_match:
            continue
        record["pdf_url"] = unescape(pdf_match.group(1))

        # Extract filename from URL
        record["filename"] = unquote(record["pdf_url"].split("/")[-1])

        # Extract date
        date_match = re.search(r'datetime="(\d{2}/\d{2}/\d{4})"', article)
        if date_match:
            d, m, y = date_match.group(1).split("/")
            record["date"] = f"{y}-{m}-{d}"

        # Extract description/summary
        desc_match = re.search(r'<div class="article-summary"[^>]*>\s*<p>(.*?)</p>', article, re.DOTALL)
        if desc_match:
            record["description"] = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()

        if court_type == "administrative":
            # Extract content type, instance, chamber
            type_match = re.search(r'Type de contentieux\s*:.*?<b>(.*?)</b>', article, re.DOTALL)
            if type_match:
                record["content_type"] = type_match.group(1).strip()

            inst_match = re.search(r'Instance\s*:.*?<b>(.*?)</b>', article, re.DOTALL)
            if inst_match:
                record["instance"] = inst_match.group(1).strip()

            chamber_match = re.search(r'Chambre\s*:.*?<b>(.*?)</b>', article, re.DOTALL)
            if chamber_match:
                record["chamber"] = chamber_match.group(1).strip()

            # Extract ECLI from description
            ecli_match = re.search(r'ECLI:(LU:[A-Z]+:\d{4}:\d+[A-Z]?)', record.get("description", ""))
            if ecli_match:
                record["ecli"] = "ECLI:" + ecli_match.group(1)

            # Extract case number from filename (e.g., 54429.pdf)
            case_match = re.match(r'(\d+[A-Z]?)\.pdf', record["filename"])
            if case_match:
                record["case_number"] = case_match.group(1)

        else:  # judicial
            # Extract jurisdiction
            jur_match = re.search(r'Juridiction\s*:.*?<b>(.*?)</b>', article, re.DOTALL)
            if jur_match:
                record["court"] = unescape(jur_match.group(1).strip())

            # Extract case number from filename
            # Pattern: YYYYMMDD_CODE_CASE-NUMBER_pseudonymisé-accessible.pdf
            case_match = re.search(r'_([A-Z]+-\d{4}-\d+)_', record["filename"])
            if case_match:
                record["case_number"] = case_match.group(1)
            else:
                # Try simpler pattern - just the number after the court code
                simple_match = re.search(r'_([A-Z]+\d*)_(\d+)_', record["filename"])
                if simple_match:
                    record["case_number"] = f"{simple_match.group(1)}-{simple_match.group(2)}"

        results.append(record)

    return results


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="LU/Courts",
        source_id="",
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""

def extract_title_from_text(text: str, filename: str, court_type: str) -> str:
    """Extract a meaningful title from the decision text."""
    if court_type == "administrative":
        # Pattern: "Tribunal administratif ... N° XXXXX du rôle"
        match = re.search(r'(Tribunal administratif|Cour administrative)[^\n]*N°\s*(\d+[A-Z]?)\s*du rôle', text[:500])
        if match:
            court = match.group(1)
            num = match.group(2)
            return f"{court} N° {num}"

    # Judicial: "Arrêt N° XX/YY" or "Jugement N° XX/YY"
    match = re.search(r'(Arrêt|Jugement|Ordonnance)\s*N°\s*(\d+/\d+)', text[:500])
    if match:
        return f"{match.group(1)} N° {match.group(2)}"

    # Fallback
    return filename.replace("-pseudonymisé-accessible.pdf", "").replace(".pdf", "").replace("_", " ").replace("-", " ")


def normalize(meta: dict, text: str) -> dict:
    """Transform raw metadata + text into normalized schema."""
    court_type = meta.get("court_type", "unknown")
    filename = meta.get("filename", "")

    # Build unique ID
    if court_type == "administrative":
        case_num = meta.get("case_number", filename.replace(".pdf", ""))
        _id = f"LU-ADM-{case_num}"
        court = f"Tribunal administratif - {meta.get('instance', 'Unknown')} - Chambre {meta.get('chamber', '?')}"
    else:
        case_num = meta.get("case_number", filename.replace(".pdf", ""))
        _id = f"LU-JUD-{case_num}"
        court = meta.get("court", "Unknown")

    title = extract_title_from_text(text, filename, court_type)

    return {
        "_id": _id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": meta.get("date"),
        "url": meta.get("pdf_url", ""),
        "court": court,
        "court_type": court_type,
        "case_number": meta.get("case_number"),
        "ecli": meta.get("ecli"),
        "chamber": meta.get("chamber"),
        "content_type": meta.get("content_type"),
        "jurisdiction": "Luxembourg",
        "language": "fr",
        "pdf_filename": filename,
    }


def fetch_page(base_url: str, offset: int) -> str:
    """Fetch a listing page with pagination."""
    params = {"b": offset} if offset > 0 else {}
    resp = requests.get(base_url, params=params, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.text


def fetch_decisions(court_type: str, limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch decisions from one court type (judicial or administrative)."""
    base_url = ADMIN_URL if court_type == "administrative" else JUDICIAL_URL
    label = court_type.capitalize()

    offset = 0
    count = 0

    print(f"\nFetching {label} court decisions from justice.public.lu...")

    while True:
        if limit and count >= limit:
            break

        print(f"  Page offset={offset}...")
        try:
            html = fetch_page(base_url, offset)
        except requests.RequestException as e:
            print(f"  Error fetching page: {e}", file=sys.stderr)
            break

        entries = parse_listing_page(html, court_type)
        if not entries:
            print(f"  No more results at offset {offset}")
            break

        for entry in entries:
            if limit and count >= limit:
                break

            pdf_url = entry.get("pdf_url", "")
            filename = entry.get("filename", "unknown")
            print(f"  [{count+1}] {filename}")

            try:
                pdf_resp = requests.get(pdf_url, headers=HEADERS, timeout=120)
                pdf_resp.raise_for_status()

                text = extract_text_from_pdf(pdf_resp.content)

                if not text or len(text) < 100:
                    print(f"    Skipping: insufficient text ({len(text)} chars)")
                    continue

                record = normalize(entry, text)
                print(f"    OK: {len(text)} chars")
                yield record
                count += 1

                time.sleep(REQUEST_DELAY)

            except requests.RequestException as e:
                print(f"    Error downloading PDF: {e}", file=sys.stderr)
                continue
            except Exception as e:
                print(f"    Error processing: {e}", file=sys.stderr)
                continue

        offset += PAGE_SIZE
        time.sleep(1.0)  # Be polite between pages

    print(f"  {label}: {count} records fetched")


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all decisions from both judicial and administrative courts."""
    judicial_limit = limit // 2 if limit else None
    admin_limit = limit - judicial_limit if limit else None

    yield from fetch_decisions("judicial", limit=judicial_limit)
    yield from fetch_decisions("administrative", limit=admin_limit)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date.

    Since the listing pages are sorted by date (newest first), we stop
    when we encounter a decision older than `since`.
    """
    for court_type in ["judicial", "administrative"]:
        base_url = ADMIN_URL if court_type == "administrative" else JUDICIAL_URL
        offset = 0

        while True:
            try:
                html = fetch_page(base_url, offset)
            except requests.RequestException:
                break

            entries = parse_listing_page(html, court_type)
            if not entries:
                break

            found_old = False
            for entry in entries:
                date_str = entry.get("date")
                if date_str:
                    try:
                        entry_date = datetime.fromisoformat(date_str)
                        if entry_date.date() < since.date():
                            found_old = True
                            break
                    except ValueError:
                        pass

                pdf_url = entry.get("pdf_url", "")
                try:
                    pdf_resp = requests.get(pdf_url, headers=HEADERS, timeout=120)
                    pdf_resp.raise_for_status()
                    text = extract_text_from_pdf(pdf_resp.content)
                    if text and len(text) >= 100:
                        yield normalize(entry, text)
                    time.sleep(REQUEST_DELAY)
                except Exception:
                    continue

            if found_old:
                break
            offset += PAGE_SIZE
            time.sleep(1.0)


def bootstrap_sample(sample_size: int = 15) -> None:
    """Fetch sample records and save to sample/ directory."""
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    print(f"Bootstrapping {sample_size} sample records...")

    count = 0
    total_text_len = 0

    for record in fetch_all(limit=sample_size):
        filename = re.sub(r'[^\w\-.]', '_', record["_id"]) + ".json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        text_len = len(record.get("text", ""))
        total_text_len += text_len
        count += 1
        print(f"  Saved: {filename} ({text_len} chars)")

    if count > 0:
        avg_len = total_text_len / count
        print(f"\nSample complete: {count} records, avg {avg_len:.0f} chars/doc")
    else:
        print("\nNo records fetched!")


def main():
    parser = argparse.ArgumentParser(
        description="LU/Courts - Luxembourg Courts Decisions fetcher"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "fetch", "test"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample data only (for bootstrap)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=15,
        help="Number of records to fetch (default: 15)"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap_sample(args.limit)
    elif args.command == "fetch":
        for record in fetch_all(limit=args.limit if args.limit else None):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "test":
        print("Testing connection to justice.public.lu...")
        for ct, url in [("judicial", JUDICIAL_URL), ("administrative", ADMIN_URL)]:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            total_match = re.search(r'(\d[\d\s.]*)\s*résultat', resp.text)
            total = total_match.group(0) if total_match else "unknown"
            print(f"  {ct}: {resp.status_code} - {total}")


if __name__ == "__main__":
    main()
