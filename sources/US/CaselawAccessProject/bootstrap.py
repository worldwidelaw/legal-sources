#!/usr/bin/env python3
"""
US/CaselawAccessProject - Harvard Caselaw Access Project via HuggingFace

Fetches case law from the Common Pile version of the Caselaw Access Project.
Uses HuggingFace Datasets library with streaming to avoid downloading the full 78GB.

Data coverage: 6.7M+ U.S. federal and state court decisions spanning 360 years.
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

from datasets import load_dataset


def parse_case_metadata(text: str, raw_metadata: dict) -> dict:
    """Extract structured metadata from case text."""
    result = {
        "court": None,
        "case_number": None,
        "decision_date": None,
        "parties": None,
    }

    # Try to extract court from text (usually in the header)
    lines = text.split("\n")[:20]  # Look at first 20 lines
    for line in lines:
        line = line.strip()
        if "Court of Appeals" in line or "Supreme Court" in line or "District Court" in line:
            result["court"] = line
            break
        if "United States Court" in line:
            result["court"] = line
            break

    # Try to extract case number
    case_num_patterns = [
        r"No\.\s*([\d\-]+)",
        r"Case No\.\s*([\d\-]+)",
        r"Docket No\.\s*([\d\-]+)",
    ]
    for pattern in case_num_patterns:
        match = re.search(pattern, text[:2000])
        if match:
            result["case_number"] = match.group(1)
            break

    # Try to extract date from text — covers both modern and historical formats
    date_patterns = [
        # "Decided January 15, 1885" / "Filed: Jan. 3, 1920"
        r"(?:Decided|Filed|Argued|Submitted|Dated)[:\s]+([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})",
        # "January 15, 1885" / "Jan. 15, 1985"
        r"([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})",
        # "15 January 1885" (British-style, common in older cases)
        r"(\d{1,2}\s+[A-Z][a-z]+\.?\s+\d{4})",
        # "1/15/1985"
        r"(\d{1,2}/\d{1,2}/\d{4})",
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text[:5000])
        if match:
            raw_date = match.group(1)
            for fmt in (
                "%B %d, %Y", "%B %d %Y", "%b. %d, %Y", "%b. %d %Y",
                "%b %d, %Y", "%b %d %Y", "%m/%d/%Y",
                "%d %B %Y", "%d %b %Y", "%d %b. %Y",
            ):
                try:
                    dt = datetime.strptime(raw_date, fmt)
                    result["decision_date"] = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
            else:
                result["decision_date"] = raw_date
            break

    # Get author from metadata if available
    if raw_metadata and "author" in raw_metadata:
        result["author"] = raw_metadata["author"]

    return result


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a CAP record to standard schema."""
    doc_id = raw.get("id", "")
    source = raw.get("source", "Caselaw Access Project")
    created = raw.get("created", "")
    added = raw.get("added", "")
    metadata = raw.get("metadata", {}) or {}
    text = raw.get("text", "")

    # Extract case metadata from text
    parsed = parse_case_metadata(text, metadata)

    # Build title from first meaningful line of text
    title = None
    if text:
        lines = text.strip().split("\n")
        for line in lines[:10]:
            line = line.strip()
            if len(line) > 20 and len(line) < 500:
                title = line
                break
    if not title:
        title = doc_id.replace("/", " - ").replace(".html", "")

    # Get URL from metadata if available
    url = metadata.get("url", f"https://case.law/search/?q={doc_id}")

    return {
        "_id": doc_id,
        "_source": "US/CaselawAccessProject",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": parsed.get("decision_date"),
        "url": url,
        "court": parsed.get("court"),
        "case_number": parsed.get("case_number"),
        "author": parsed.get("author") or metadata.get("author"),
        "license": metadata.get("license", "Public Domain"),
        "original_source": source,
        "created_at": created,
    }


def fetch_all(
    max_records: Optional[int] = None,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all cases from HuggingFace CAP dataset using streaming."""
    print("Loading dataset in streaming mode...")
    ds = load_dataset("common-pile/caselaw_access_project", split="train", streaming=True)

    fetched = 0
    for item in ds:
        if max_records and fetched >= max_records:
            return

        record = normalize(dict(item))

        # Only yield records with actual text content
        if record.get("text") and len(record["text"]) > 100:
            yield record
            fetched += 1

            if fetched % 100 == 0:
                print(f"  Fetched {fetched} records...")


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch cases added after a given date.

    Note: The HuggingFace dataset is a static snapshot, so this filters
    by the 'added' field which indicates when records were added to the dataset.
    """
    print(f"Fetching records added since {since.isoformat()}...")
    ds = load_dataset("common-pile/caselaw_access_project", split="train", streaming=True)

    for item in ds:
        added = item.get("added", "")
        if added:
            try:
                added_dt = datetime.fromisoformat(added.replace("Z", "+00:00"))
                if added_dt >= since:
                    record = normalize(dict(item))
                    if record.get("text"):
                        yield record
            except ValueError:
                continue


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample records and save to sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    print(f"Fetching {count} sample records from Caselaw Access Project...")

    for record in fetch_all(max_records=count):
        # Create safe filename from ID
        safe_id = record["_id"].replace("/", "_").replace("\\", "_")
        filename = f"{safe_id}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        title_preview = record.get("title", "")[:60]
        print(f"  Saved {filename} ({text_len:,} chars) - {title_preview}...")
        saved += 1

    return saved


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records meet requirements."""
    samples = list(sample_dir.glob("*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 500:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        # Check required fields
        for field in ["_id", "_source", "_type", "title"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

        # Check for raw HTML tags
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {sample_path.name} may contain HTML tags")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="US/CaselawAccessProject data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of records to fetch",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Fetch records since date (YYYY-MM-DD)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count)
            print(f"\nSaved {saved} sample records to {sample_dir}")

            # Also run validation
            print("\nValidating samples...")
            valid = validate_samples(sample_dir)
            sys.exit(0 if saved >= 10 and valid else 1)
        else:
            print("Use --sample for bootstrap mode")
            sys.exit(1)

    elif args.command == "validate":
        valid = validate_samples(sample_dir)
        sys.exit(0 if valid else 1)

    elif args.command == "fetch":
        if args.since:
            since = datetime.fromisoformat(args.since)
            for record in fetch_updates(since):
                print(json.dumps(record, ensure_ascii=False))
        else:
            for record in fetch_all(max_records=args.count):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
