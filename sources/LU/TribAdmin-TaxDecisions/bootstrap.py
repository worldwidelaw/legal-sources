#!/usr/bin/env python3
"""
LU/TribAdmin-TaxDecisions - Luxembourg Administrative Tribunal Tax Decisions

Fetches fiscal/tax decisions from the Luxembourg Tribunal administratif
and Cour administrative via justice.public.lu, filtered by subject_type=fiscal.
Covers corporate tax, wealth tax, IP regime, holding companies (~3,560 decisions).
Full text extracted from PDFs hosted at ja.public.lu.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


SOURCE_ID = "LU/TribAdmin-TaxDecisions"

# Fiscal decisions from administrative courts
BASE_URL = "https://justice.public.lu/fr/jurisprudence/juridictions-administratives.html"
FISCAL_FILTER = "r=f%2Fja_subject_type%2Ffiscal"

REQUEST_DELAY = 1.5  # seconds between PDF downloads
PAGE_SIZE = 20  # results per page

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def parse_listing_page(html: str) -> list:
    """Parse a justice.public.lu listing page and extract fiscal decision metadata."""
    results = []

    # Split by article blocks (justice.public.lu uses article tags)
    articles = re.findall(
        r'<article class="article search-result search-result--jurisprudence"[^>]*>(.*?)</article>',
        html, re.DOTALL
    )

    for article in articles:
        record = {}

        # Extract PDF URL
        pdf_match = re.search(r'href="(https?://[^"]+\.pdf)"', article)
        if not pdf_match:
            continue
        record["pdf_url"] = re.sub(r'&amp;', '&', pdf_match.group(1))

        # Extract filename from URL
        record["filename"] = unquote(record["pdf_url"].split("/")[-1])

        # Extract date - try datetime attribute first, then plain text
        date_match = re.search(r'datetime="(\d{2}/\d{2}/\d{4})"', article)
        if date_match:
            d, m, y = date_match.group(1).split("/")
            record["date"] = f"{y}-{m}-{d}"
        else:
            # Fallback: look for DD/MM/YYYY in text
            date_match2 = re.search(r'(\d{2})/(\d{2})/(\d{4})', article)
            if date_match2:
                record["date"] = f"{date_match2.group(3)}-{date_match2.group(2)}-{date_match2.group(1)}"

        # Extract description/summary
        desc_match = re.search(r'<div class="article-summary"[^>]*>\s*<p>(.*?)</p>', article, re.DOTALL)
        if desc_match:
            record["description"] = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()

        # Extract instance (Tribunal / Cour / Référé)
        inst_match = re.search(r'Instance\s*:.*?<b>(.*?)</b>', article, re.DOTALL)
        if inst_match:
            record["instance"] = inst_match.group(1).strip()

        # Extract chamber
        chamber_match = re.search(r'Chambre\s*:.*?<b>(.*?)</b>', article, re.DOTALL)
        if chamber_match:
            record["chamber"] = chamber_match.group(1).strip()

        # Extract ECLI from description
        ecli_match = re.search(r'ECLI:(LU:[A-Z]+:\d{4}:\d+[A-Z]?)', record.get("description", ""))
        if ecli_match:
            record["ecli"] = "ECLI:" + ecli_match.group(1)

        # Extract case number from filename (e.g., 49122.pdf, 54047C.pdf)
        case_match = re.match(r'(\d+[A-Z]?)\.pdf', record["filename"])
        if case_match:
            record["case_number"] = case_match.group(1)

        results.append(record)

    return results


def extract_text_from_pdf(pdf_content: bytes, source_id: str = "") -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source=SOURCE_ID,
        source_id=source_id,
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""


def extract_title_from_text(text: str, filename: str) -> str:
    """Extract a meaningful title from the decision text."""
    # Pattern: "Tribunal administratif ... N° XXXXX du rôle"
    match = re.search(
        r'(Tribunal administratif|Cour administrative)[^\n]*N°\s*(\d+[A-Z]?)\s*du rôle',
        text[:800]
    )
    if match:
        return f"{match.group(1)} N° {match.group(2)} du rôle"

    # Try: "N° XXXXX du rôle" alone
    match2 = re.search(r'N°\s*(\d+[A-Z]?)\s*du rôle', text[:800])
    if match2:
        return f"Tribunal administratif N° {match2.group(1)} du rôle"

    # Fallback to filename
    return filename.replace(".pdf", "").replace("_", " ")


def normalize(meta: dict, text: str) -> dict:
    """Transform raw metadata + text into normalized schema."""
    case_num = meta.get("case_number", meta.get("filename", "").replace(".pdf", ""))
    _id = f"LU-TAX-{case_num}"

    instance = meta.get("instance", "Tribunal")
    chamber = meta.get("chamber", "")
    court = f"Tribunal administratif"
    if instance == "Cour":
        court = "Cour administrative"
    if chamber:
        court += f" - {chamber}e chambre"

    title = extract_title_from_text(text, meta.get("filename", ""))

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
        "case_number": meta.get("case_number"),
        "ecli": meta.get("ecli"),
        "instance": meta.get("instance"),
        "chamber": meta.get("chamber"),
        "subject_type": "fiscal",
        "jurisdiction": "Luxembourg",
        "language": "fr",
    }


def fetch_page(offset: int) -> str:
    """Fetch a listing page with the fiscal filter and pagination."""
    url = f"{BASE_URL}?{FISCAL_FILTER}"
    if offset > 0:
        url += f"&b={offset}"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.text


def fetch_decisions(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch fiscal tax decisions from administrative courts."""
    offset = 0
    count = 0

    print(f"Fetching Luxembourg administrative tax decisions (fiscal filter)...")

    while True:
        if limit and count >= limit:
            break

        print(f"  Page offset={offset}...")
        try:
            html = fetch_page(offset)
        except requests.RequestException as e:
            print(f"  Error fetching page: {e}", file=sys.stderr)
            break

        entries = parse_listing_page(html)
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

                text = extract_text_from_pdf(pdf_resp.content, entry.get("case_number", ""))

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
        time.sleep(1.0)

    print(f"  Total: {count} fiscal tax decisions fetched")


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all fiscal tax decisions."""
    yield from fetch_decisions(limit=limit)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    offset = 0

    while True:
        try:
            html = fetch_page(offset)
        except requests.RequestException:
            break

        entries = parse_listing_page(html)
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
                text = extract_text_from_pdf(pdf_resp.content, entry.get("case_number", ""))
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

    print(f"Bootstrapping {sample_size} sample tax decisions...")

    count = 0
    total_text_len = 0

    for record in fetch_decisions(limit=sample_size):
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
        description="LU/TribAdmin-TaxDecisions - Luxembourg Administrative Tax Decisions"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "fetch", "test"],
        help="Command to run"
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--limit", type=int, default=15, help="Number of records (default: 15)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap_sample(args.limit)
    elif args.command == "fetch":
        lim = None if args.full else args.limit
        for record in fetch_all(limit=lim):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "test":
        print("Testing connection to justice.public.lu (fiscal filter)...")
        url = f"{BASE_URL}?{FISCAL_FILTER}"
        resp = requests.get(url, headers=HEADERS, timeout=60)
        total_match = re.search(r'(\d[\d\s.]*)\s*résultat', resp.text)
        total = total_match.group(0) if total_match else "unknown"
        print(f"  Status: {resp.status_code} - {total}")


if __name__ == "__main__":
    main()
