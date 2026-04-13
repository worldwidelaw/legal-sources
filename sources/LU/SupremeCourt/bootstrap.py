#!/usr/bin/env python3
"""
LU/SupremeCourt - Luxembourg Court of Cassation (Cour de Cassation)

Data source: data.public.lu Open Data Portal
Dataset: cour-de-cassation (66b20d265447fda899e7d1a7)
Format: PDF documents, text extracted via pdfplumber
License: CC BY-ND
Records: ~2,346 decisions

The Cour de Cassation reviews decisions from tribunals and appellate courts
at both criminal and civil level. It only decides questions of law or
application of law, ensuring harmonious application of laws through its
jurisprudence.
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

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

# Configuration
SOURCE_ID = "LU/SupremeCourt"
DATASET_SLUG = "cour-de-cassation"
API_BASE = "https://data.public.lu/api/1"
REQUEST_DELAY = 1.0  # seconds between PDF downloads


def get_dataset_resources(page_size: int = 100) -> Generator[dict, None, None]:
    """Fetch all resources from the dataset via pagination."""
    page = 1
    while True:
        url = f"{API_BASE}/datasets/{DATASET_SLUG}/"
        params = {"page": page, "page_size": page_size}

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        resources = data.get("resources", [])
        if not resources:
            break

        for resource in resources:
            if resource.get("format", "").lower() == "pdf":
                yield resource

        # The API returns all resources in one call for datasets
        # so we don't need pagination for resources
        break


def extract_metadata_from_filename(filename: str) -> dict:
    """Extract case metadata from PDF filename.

    Filename pattern: YYYYMMDD-cas-YYYY-NNNNN-NN-pseudonymise-accessible.pdf
    Example: 20260129-cas-2025-00151-32-pseudonymise-accessible.pdf
    """
    metadata = {
        "date": None,
        "case_number": None,
        "decision_number": None,
    }

    # Try to extract date from beginning (YYYYMMDD)
    date_match = re.match(r"^(\d{4})(\d{2})(\d{2})", filename)
    if date_match:
        year, month, day = date_match.groups()
        metadata["date"] = f"{year}-{month}-{day}"

    # Try to extract case number (cas-YYYY-NNNNN or cass-NNNN)
    case_match = re.search(r"cas[s]?-(\d{4})-(\d+)", filename, re.IGNORECASE)
    if case_match:
        year, num = case_match.groups()
        metadata["case_number"] = f"CAS-{year}-{num.zfill(5)}"
    else:
        # Older format: cass-NNNN
        case_match = re.search(r"cass-(\d+)", filename, re.IGNORECASE)
        if case_match:
            metadata["case_number"] = f"CASS-{case_match.group(1)}"

    # Try to extract decision number (the number after the case number)
    # Pattern: -NN-pseudonymise or just -NN at the end before extension
    decision_match = re.search(r"-(\d+)-(?:pseudonymise|accessible)", filename, re.IGNORECASE)
    if decision_match:
        metadata["decision_number"] = decision_match.group(1)

    return metadata


def extract_title_from_text(text: str, filename: str) -> str:
    """Extract a meaningful title from the decision text."""
    # Try to find the decision number and date at the beginning
    # Pattern: "N° 32 / 2026 du 29.01.2026"
    title_match = re.search(r"N°\s*(\d+)\s*/\s*(\d{4})\s*(?:p[ée]n(?:al)?\.?)?\s*du\s*(\d{1,2}[./]\d{1,2}[./]\d{4})", text[:500])
    if title_match:
        num, year, date = title_match.groups()
        return f"Arrêt N° {num}/{year} du {date}"

    # Try simpler pattern
    simple_match = re.search(r"N°\s*(\d+)\s*/\s*(\d+)", text[:300])
    if simple_match:
        num, year = simple_match.groups()
        return f"Arrêt N° {num}/{year}"

    # Fallback to filename
    return filename.replace("-pseudonymise-accessible.pdf", "").replace("-", " ").title()


def normalize(resource: dict, text: str) -> dict:
    """Transform raw resource data into normalized schema."""
    filename = resource.get("title", "")
    url = resource.get("url", "")

    # Extract metadata from filename
    meta = extract_metadata_from_filename(filename)

    # Build unique ID
    case_num = meta.get("case_number") or "UNKNOWN"
    dec_num = meta.get("decision_number") or "0"
    _id = f"LU-CASS-{case_num.replace('CAS-', '').replace('CASS-', '')}-{dec_num}"

    # Extract title from text
    title = extract_title_from_text(text, filename)

    # Try to get date from resource metadata or filename
    date = meta.get("date")
    if not date and resource.get("last_modified"):
        # Use last modified as fallback (not ideal but better than nothing)
        lm = resource.get("last_modified", "")
        if lm:
            date = lm[:10]

    return {
        "_id": _id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "case_number": meta.get("case_number"),
        "decision_number": meta.get("decision_number"),
        "pdf_filename": filename,
        "court": "Cour de Cassation",
        "jurisdiction": "Luxembourg",
        "language": "fr",
    }


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all decisions from the dataset."""
    count = 0

    print(f"Fetching resources from dataset: {DATASET_SLUG}")

    for resource in get_dataset_resources():
        if limit and count >= limit:
            break

        url = resource.get("url", "")
        filename = resource.get("title", "")

        if not url or not filename.lower().endswith(".pdf"):
            continue

        print(f"  [{count+1}] Downloading: {filename}")

        try:
            # Build source_id from filename metadata
            meta = extract_metadata_from_filename(filename)
            case_num = meta.get("case_number") or "UNKNOWN"
            dec_num = meta.get("decision_number") or "0"
            source_id = f"LU-CASS-{case_num.replace('CAS-', '').replace('CASS-', '')}-{dec_num}"

            # Extract text from PDF via centralized extractor
            text = extract_pdf_markdown(
                source=SOURCE_ID, source_id=source_id,
                pdf_url=url, table="case_law",
            )

            if not text or len(text) < 100:
                print(f"    Skipping: no text extracted")
                continue

            # Normalize the record
            record = normalize(resource, text)

            print(f"    Text length: {len(text)} chars")
            yield record
            count += 1

            # Rate limiting
            time.sleep(REQUEST_DELAY)

        except requests.RequestException as e:
            print(f"    Error downloading: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"    Error processing: {e}", file=sys.stderr)
            continue

    print(f"\nTotal records fetched: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    for resource in get_dataset_resources():
        # Check last_modified date
        lm = resource.get("last_modified", "")
        if lm:
            try:
                resource_date = datetime.fromisoformat(lm.replace("Z", "+00:00"))
                if resource_date < since:
                    continue
            except ValueError:
                pass

        url = resource.get("url", "")
        filename = resource.get("title", "")

        if not url or not filename.lower().endswith(".pdf"):
            continue

        try:
            meta_u = extract_metadata_from_filename(filename)
            case_num_u = meta_u.get("case_number") or "UNKNOWN"
            dec_num_u = meta_u.get("decision_number") or "0"
            sid = f"LU-CASS-{case_num_u.replace('CAS-', '').replace('CASS-', '')}-{dec_num_u}"
            text = extract_pdf_markdown(
                source=SOURCE_ID, source_id=sid,
                pdf_url=url, table="case_law",
            )

            if text and len(text) >= 100:
                yield normalize(resource, text)
                time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"Error fetching {filename}: {e}", file=sys.stderr)
            continue


def bootstrap_sample(sample_size: int = 15) -> None:
    """Fetch sample records and save to sample/ directory."""
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    print(f"Bootstrapping {sample_size} sample records...")

    count = 0
    total_text_len = 0

    for record in fetch_all(limit=sample_size):
        # Save to sample directory
        filename = f"{record['_id']}.json"
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
        description="LU/SupremeCourt - Luxembourg Court of Cassation fetcher"
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

    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap_sample(args.limit)
    elif args.command == "fetch":
        for record in fetch_all(limit=args.limit if args.limit else None):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "test":
        # Quick test
        print("Testing connection to data.public.lu...")
        resp = requests.get(f"{API_BASE}/datasets/{DATASET_SLUG}/", timeout=30)
        data = resp.json()
        resources = data.get("resources", [])
        print(f"Dataset has {len(resources)} resources")

        # Test one PDF
        if resources:
            r = resources[0]
            print(f"\nTesting PDF: {r.get('title')}")
            text = extract_pdf_markdown(
                source=SOURCE_ID, source_id="test",
                pdf_url=r.get("url"), table="case_law", force=True,
            ) or ""
            print(f"Extracted {len(text)} chars")
            print(f"First 500 chars:\n{text[:500]}")


if __name__ == "__main__":
    main()
