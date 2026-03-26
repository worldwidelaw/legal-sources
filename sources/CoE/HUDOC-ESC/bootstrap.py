#!/usr/bin/env python3
"""
CoE/HUDOC-ESC - European Committee of Social Rights Decisions

Fetches decisions from HUDOC-ESC (https://hudoc.esc.coe.int).
Uses the JSON API with full text returned in contentbody field.

Data coverage:
- ~395 decisions on the merits (FOND)
- ~800 admissibility decisions (ADMIS)
- ~170 separate opinions (OPCC)
- ~3000+ follow-up conclusions/assessments (CCASST)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlencode

import requests

# Configuration
SEARCH_URL = "https://hudoc.esc.coe.int/app/query/results"
RATE_LIMIT_DELAY = 1.0

# Document types to fetch (DOCX-based decisions with full text)
DOC_TYPES = ["FOND", "ADMIS", "OPCC"]

# Fields to request from API (metadata only, contentbody fetched separately per-doc)
METADATA_FIELDS = (
    "id,esctitle,escrawdctype,escdctype,escstateparty,esccomplnum,"
    "esccomplorg,escdclanguage,escdatedec,escpublicationdate,"
    "escarticle,escarticleviolated,escarticlenotviolated,"
    "escimportance,escsession,esccharterid"
)

# Full text field
FULL_TEXT_FIELDS = METADATA_FIELDS + ",contentbody"


def clean_text(text: str) -> str:
    """Clean text content: strip HTML tags, normalize whitespace."""
    if not text:
        return ""
    # Remove any HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode common HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def search_esc(
    query: str = "contentsitename:ESC",
    start: int = 0,
    length: int = 50,
    sort: str = "escpublicationdate Descending",
    select: str = METADATA_FIELDS,
) -> Dict[str, Any]:
    """Search HUDOC-ESC for documents."""
    params = {
        "query": query,
        "select": select,
        "sort": sort,
        "start": start,
        "length": length,
    }

    url = f"{SEARCH_URL}?{urlencode(params)}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a HUDOC-ESC record to standard schema."""
    columns = raw.get("columns", raw)

    doc_id = columns.get("id", "")
    title = columns.get("esctitle", "")
    raw_type = columns.get("escrawdctype", "")
    state = columns.get("escstateparty", "")
    compl_num = columns.get("esccomplnum", "")
    compl_org = columns.get("esccomplorg", "")
    language = columns.get("escdclanguage", "ENG")
    date_dec = columns.get("escdatedec", "")
    pub_date = columns.get("escpublicationdate", "")
    articles = columns.get("escarticle", "")
    violated = columns.get("escarticleviolated", "")
    not_violated = columns.get("escarticlenotviolated", "")
    importance = columns.get("escimportance", "")
    content = columns.get("contentbody", "")

    # Parse date
    date_iso = None
    date_str = date_dec or pub_date
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_iso = date_str[:10] if len(date_str) >= 10 else date_str

    # Clean text
    text = clean_text(content)

    # Document type label
    type_labels = {
        "FOND": "merits_decision",
        "ADMIS": "admissibility_decision",
        "OPCC": "separate_opinion",
        "CCASST": "follow_up_assessment",
    }
    doc_type_label = type_labels.get(raw_type, raw_type.lower() if raw_type else "unknown")

    return {
        "_id": doc_id,
        "_source": "CoE/HUDOC-ESC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"https://hudoc.esc.coe.int/eng?i={doc_id}",
        "complaint_number": compl_num,
        "complainant": compl_org,
        "respondent_state": state,
        "language": language.lower() if language else "eng",
        "document_type": raw_type,
        "document_type_label": doc_type_label,
        "articles": articles,
        "articles_violated": violated,
        "articles_not_violated": not_violated,
        "importance": importance,
    }


def fetch_all(
    doc_types: Optional[list] = None,
    max_records: Optional[int] = None,
    language: str = "ENG",
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all decisions from HUDOC-ESC.

    Args:
        doc_types: Document types to fetch (default: FOND, ADMIS, OPCC)
        max_records: Optional limit on number of records
        language: Language filter (default: ENG)
    """
    if doc_types is None:
        doc_types = DOC_TYPES

    type_filter = " OR ".join([f"escrawdctype:{t}" for t in doc_types])
    query = f"contentsitename:ESC AND ({type_filter}) AND escdclanguage:{language}"

    batch_size = 50
    start = 0
    fetched = 0

    while True:
        if max_records and fetched >= max_records:
            return

        result = search_esc(
            query=query,
            start=start,
            length=batch_size,
            select=FULL_TEXT_FIELDS,
        )

        total = result.get("resultcount", 0)
        results = result.get("results", [])

        if not results:
            break

        if start == 0:
            print(f"Total matching documents: {total}")

        for item in results:
            if max_records and fetched >= max_records:
                return

            record = normalize(item)
            if record.get("text"):
                yield record
                fetched += 1

        start += batch_size
        if start >= total:
            break

        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """Fetch decisions published since a given date."""
    since_str = since.strftime("%Y-%m-%d")
    query = f"contentsitename:ESC AND escpublicationdate>={since_str}"

    batch_size = 50
    start = 0

    while True:
        result = search_esc(
            query=query,
            start=start,
            length=batch_size,
            select=FULL_TEXT_FIELDS,
            sort="escpublicationdate Descending",
        )

        total = result.get("resultcount", 0)
        results = result.get("results", [])

        if not results:
            break

        for item in results:
            record = normalize(item)
            if record.get("text"):
                yield record

        start += batch_size
        if start >= total:
            break

        time.sleep(RATE_LIMIT_DELAY)


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample records and save to sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    # Fetch a mix of document types
    for doc_type in DOC_TYPES:
        per_type = max(count // len(DOC_TYPES), 3)
        remaining = count - saved
        if remaining <= 0:
            break
        to_fetch = min(per_type, remaining)

        print(f"\nFetching {to_fetch} {doc_type} records...")

        for record in fetch_all(doc_types=[doc_type], max_records=to_fetch):
            doc_id = str(record["_id"]).replace("/", "_")
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            date = record.get("date", "N/A")
            print(f"  Saved {filename} (type={doc_type}, date={date}, {text_len:,} chars)")
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
        elif len(text) < 100:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        for field in ["_id", "_source", "_type", "title", "date"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="CoE/HUDOC-ESC data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--count", type=int, default=15, help="Number of records to fetch")
    parser.add_argument("--since", type=str, help="Fetch records since date (YYYY-MM-DD)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count)
            print(f"\nSaved {saved} sample records to {sample_dir}")
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
