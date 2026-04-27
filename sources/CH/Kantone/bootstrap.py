#!/usr/bin/env python3
"""
CH/Kantone - Swiss Cantonal Legislation (All 26 Cantons)
Fetches legislation from HuggingFace Datasets (rcds/swiss_legislation).
Data originally compiled from lexfind.ch.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional
from html import unescape

try:
    from datasets import load_dataset
except ImportError:
    print("Error: 'datasets' package not installed. Run: pip install datasets", file=sys.stderr)
    print("This package is required for CH/Kantone to access the HuggingFace dataset.", file=sys.stderr)
    print("It should be in requirements.txt. Try: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

# Configuration
DATASET_NAME = "rcds/swiss_legislation"
RATE_LIMIT_DELAY = 0.1  # seconds between records (minimal for local dataset)

# Canton code to name mapping
CANTON_NAMES = {
    "ag": "Aargau", "ai": "Appenzell Innerrhoden", "ar": "Appenzell Ausserrhoden",
    "be": "Bern", "bl": "Basel-Landschaft", "bs": "Basel-Stadt",
    "fr": "Fribourg", "ge": "Genève", "gl": "Glarus", "gr": "Graubünden",
    "ju": "Jura", "lu": "Luzern", "ne": "Neuchâtel", "nw": "Nidwalden",
    "ow": "Obwalden", "sg": "St. Gallen", "sh": "Schaffhausen", "so": "Solothurn",
    "sz": "Schwyz", "tg": "Thurgau", "ti": "Ticino", "ur": "Uri",
    "vd": "Vaud", "vs": "Valais", "zg": "Zug", "zh": "Zürich"
}


def clean_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)

    # Decode HTML entities
    text = unescape(text)

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def load_swiss_legislation_dataset():
    """Load the Swiss legislation dataset from HuggingFace."""
    print("Loading Swiss cantonal legislation dataset from HuggingFace...", file=sys.stderr)
    dataset = load_dataset(DATASET_NAME, "default", split="train")
    print(f"Loaded {len(dataset)} records", file=sys.stderr)
    return dataset


def extract_text(record: dict) -> str:
    """Extract full text from a record, preferring HTML over PDF content."""
    # Try HTML content first (cleaner)
    html_content = record.get("html_content", "")
    if html_content and len(html_content) > 100:
        return clean_html(html_content)

    # Fall back to PDF content
    pdf_content = record.get("pdf_content", "")
    if pdf_content and len(pdf_content) > 100:
        return pdf_content.strip()

    return ""


def fetch_all(dataset=None) -> Generator[dict, None, None]:
    """Yield all Swiss cantonal legislation documents with full text."""
    if dataset is None:
        dataset = load_swiss_legislation_dataset()

    for i, record in enumerate(dataset):
        if i > 0 and i % 1000 == 0:
            print(f"Processed {i}/{len(dataset)} records...", file=sys.stderr)

        # Extract text
        text = extract_text(record)
        if not text or len(text) < 50:
            continue

        yield {
            "uuid": record.get("uuid", ""),
            "canton": record.get("canton", ""),
            "canton_name": CANTON_NAMES.get(record.get("canton", "").lower(), "Unknown"),
            "language": record.get("language", ""),
            "title": record.get("title", ""),
            "short_name": record.get("short", ""),
            "abbreviation": record.get("abbreviation", ""),
            "sr_number": record.get("sr_number", ""),
            "is_active": record.get("is_active", True),
            "pdf_url": record.get("pdf_url", ""),
            "html_url": record.get("html_url", ""),
            "text": text,
            "version_found_at": record.get("version_found_at"),
            "version_inactive_since": record.get("version_inactive_since")
        }

        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Yield documents modified since a given date."""
    # The HuggingFace dataset is a snapshot; for updates, re-fetch and filter by date
    dataset = load_swiss_legislation_dataset()
    since_ts = int(since.timestamp())

    for record in fetch_all(dataset):
        version_ts = record.get("version_found_at")
        if version_ts and version_ts >= since_ts:
            yield record


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    canton = raw.get("canton", "").upper()
    uuid = raw.get("uuid", "")
    sr_number = raw.get("sr_number", "")

    # Create document ID
    doc_id = f"{canton}/{sr_number}" if sr_number else f"{canton}/{uuid[:16]}"

    # Determine URL (prefer html_url, fall back to pdf_url)
    url = raw.get("html_url") or raw.get("pdf_url") or f"https://www.lexfind.ch/fe/de/tol/{uuid}"

    # Format date from timestamp if available
    version_ts = raw.get("version_found_at")
    date_str = ""
    if version_ts:
        try:
            date_str = datetime.fromtimestamp(version_ts).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass

    return {
        "_id": doc_id,
        "_source": "CH/Kantone",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "uuid": uuid,
        "canton": canton,
        "canton_name": raw.get("canton_name", ""),
        "title": raw.get("title", ""),
        "short_name": raw.get("short_name", ""),
        "abbreviation": raw.get("abbreviation", ""),
        "sr_number": sr_number,
        "text": raw.get("text", ""),
        "language": raw.get("language", ""),
        "is_active": raw.get("is_active", True),
        "date": date_str,
        "url": url,
        "pdf_url": raw.get("pdf_url", ""),
        "html_url": raw.get("html_url", "")
    }


def bootstrap_sample(sample_dir: Path, sample_count: int = 15):
    """Fetch sample documents for testing."""
    print(f"Fetching {sample_count} sample documents...", file=sys.stderr)

    sample_dir.mkdir(parents=True, exist_ok=True)

    dataset = load_swiss_legislation_dataset()

    # Get samples from different cantons by shuffling indices
    count = 0
    total_chars = 0
    cantons_seen = set()

    # Sample indices spread across dataset for canton diversity
    indices = list(range(0, len(dataset), max(1, len(dataset) // (sample_count * 3))))

    for idx in indices:
        if count >= sample_count:
            break

        record = dataset[idx]

        # Extract text
        text = extract_text(record)
        if not text or len(text) < 100:
            continue

        raw_doc = {
            "uuid": record.get("uuid", ""),
            "canton": record.get("canton", ""),
            "canton_name": CANTON_NAMES.get(record.get("canton", "").lower(), "Unknown"),
            "language": record.get("language", ""),
            "title": record.get("title", ""),
            "short_name": record.get("short", ""),
            "abbreviation": record.get("abbreviation", ""),
            "sr_number": record.get("sr_number", ""),
            "is_active": record.get("is_active", True),
            "pdf_url": record.get("pdf_url", ""),
            "html_url": record.get("html_url", ""),
            "text": text,
            "version_found_at": record.get("version_found_at"),
            "version_inactive_since": record.get("version_inactive_since")
        }

        normalized = normalize(raw_doc)
        canton = raw_doc.get("canton", "").lower()
        cantons_seen.add(canton)

        # Save to sample directory
        safe_id = normalized["_id"].replace("/", "_")
        sample_file = sample_dir / f"{safe_id}.json"

        with open(sample_file, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        print(f"Saved: {normalized['_id']} - {canton.upper()} ({len(text)} chars)", file=sys.stderr)
        total_chars += len(text)
        count += 1

    avg_chars = total_chars // max(count, 1)
    print(f"\nBootstrap complete: {count} documents from {len(cantons_seen)} cantons, avg {avg_chars} chars/doc", file=sys.stderr)
    return count


def main():
    parser = argparse.ArgumentParser(description="CH/Kantone Swiss Cantonal Legislation Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch sample documents")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample data")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--full", action="store_true", help="Fetch all records")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show dataset statistics")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Use --sample flag to fetch sample data", file=sys.stderr)
    elif args.command == "status":
        dataset = load_swiss_legislation_dataset()
        print(f"Total records: {len(dataset)}")

        # Count by canton
        canton_counts = {}
        lang_counts = {}
        for record in dataset:
            canton = record.get("canton", "unknown")
            lang = record.get("language", "unknown")
            canton_counts[canton] = canton_counts.get(canton, 0) + 1
            lang_counts[lang] = lang_counts.get(lang, 0) + 1

        print(f"\nCantons: {len(canton_counts)}")
        for canton, cnt in sorted(canton_counts.items()):
            name = CANTON_NAMES.get(canton, canton)
            print(f"  {canton.upper()}: {cnt} ({name})")

        print(f"\nLanguages:")
        for lang, cnt in sorted(lang_counts.items(), key=lambda x: -x[1]):
            print(f"  {lang}: {cnt}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
