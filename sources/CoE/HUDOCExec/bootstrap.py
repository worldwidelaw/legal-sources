#!/usr/bin/env python3
"""
CoE/HUDOCExec - ECHR Judgment Execution Monitoring

Fetches execution monitoring documents from HUDOC-EXEC (https://hudoc.exec.coe.int).
Uses the JSON API with full text returned in contentbody field.

Data coverage:
- ~93K total documents
- Action plans, action reports, CM decisions, observations, communications
- Case entry cards for all cases under supervision
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlencode

import requests

# Configuration
SEARCH_URL = "https://hudoc.exec.coe.int/app/query/results"
RATE_LIMIT_DELAY = 1.0

# Substantive document types (excluding CEC case entry cards which are mostly metadata)
SUBSTANTIVE_TYPES = ["CMDEC", "CMNOT", "apo", "acr", "obs", "gvo", "ngo", "nhri", "CMINF"]

# All types including case entry cards
ALL_TYPES = SUBSTANTIVE_TYPES + ["CEC"]

# Metadata fields for search
METADATA_FIELDS = (
    "id,exectitle,execdocumenttype,execdocumenttypecollection,execstate,"
    "execappno,execdocnamefromechr,execdocumentreference,execidentifier,"
    "execlanguage,execjudgmentdate,execpublisheddate,execisclosed,"
    "execsupervision,execviolations,exectype,execgroup"
)

FULL_TEXT_FIELDS = METADATA_FIELDS + ",contentbody"


def clean_text(text: str) -> str:
    """Clean text content: strip HTML tags, normalize whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def search_exec(
    query: str = "contentsitename:EXEC",
    start: int = 0,
    length: int = 50,
    sort: str = "execpublisheddate Descending",
    select: str = METADATA_FIELDS,
) -> Dict[str, Any]:
    """Search HUDOC-EXEC for documents."""
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
    """Normalize a HUDOC-EXEC record to standard schema."""
    columns = raw.get("columns", raw)

    doc_id = columns.get("id", "")
    title = columns.get("exectitle", "") or columns.get("execdocnamefromechr", "")
    doc_type = columns.get("execdocumenttype", "")
    state = columns.get("execstate", "")
    app_no = columns.get("execappno", "")
    case_name = columns.get("execdocnamefromechr", "")
    doc_ref = columns.get("execdocumentreference", "")
    identifier = columns.get("execidentifier", "")
    language = columns.get("execlanguage", "ENG")
    judgment_date = columns.get("execjudgmentdate", "")
    pub_date = columns.get("execpublisheddate", "")
    is_closed = columns.get("execisclosed", "")
    supervision = columns.get("execsupervision", "")
    violations = columns.get("execviolations", "")
    exec_type = columns.get("exectype", "")
    content = columns.get("contentbody", "")

    # Parse date (prefer publication date)
    date_iso = None
    date_str = pub_date or judgment_date
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_iso = date_str[:10] if len(date_str) >= 10 else date_str

    text = clean_text(content)

    # Document type labels
    type_labels = {
        "CMDEC": "cm_decision",
        "CMNOT": "cm_note",
        "CMINF": "cm_information",
        "apo": "action_plan",
        "acr": "action_report",
        "obs": "observation",
        "gvo": "government_communication",
        "ngo": "ngo_communication",
        "nhri": "nhri_communication",
        "CEC": "case_entry_card",
    }
    doc_type_label = type_labels.get(doc_type, doc_type.lower() if doc_type else "unknown")

    return {
        "_id": doc_id,
        "_source": "CoE/HUDOCExec",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"https://hudoc.exec.coe.int/eng?i={doc_id}",
        "application_number": app_no,
        "case_name": case_name,
        "document_reference": doc_ref,
        "respondent_state": state,
        "language": language.lower() if language else "eng",
        "document_type": doc_type,
        "document_type_label": doc_type_label,
        "supervision_type": supervision,
        "is_closed": is_closed,
        "violations": violations,
        "case_type": exec_type,
    }


def fetch_all(
    doc_types: Optional[list] = None,
    max_records: Optional[int] = None,
    language: Optional[str] = None,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch documents from HUDOC-EXEC.

    Args:
        doc_types: Document types to fetch (default: substantive types)
        max_records: Optional limit on number of records
        language: Optional language filter (e.g., "ENG")
    """
    if doc_types is None:
        doc_types = SUBSTANTIVE_TYPES

    type_filter = " OR ".join(
        [f"execdocumenttypecollection:{t}" for t in doc_types]
    )
    query = f"contentsitename:EXEC AND ({type_filter})"
    if language:
        query += f" AND execlanguage:{language}"

    batch_size = 50
    start = 0
    fetched = 0

    while True:
        if max_records and fetched >= max_records:
            return

        result = search_exec(
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
    """Fetch documents published since a given date."""
    since_str = since.strftime("%Y-%m-%d")
    query = f"contentsitename:EXEC AND execpublisheddate>={since_str}"

    batch_size = 50
    start = 0

    while True:
        result = search_exec(
            query=query,
            start=start,
            length=batch_size,
            select=FULL_TEXT_FIELDS,
            sort="execpublisheddate Descending",
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
    """Fetch sample records across document types."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    # Sample from key document types
    sample_types = ["CMDEC", "apo", "acr", "obs", "CMNOT"]
    per_type = max(count // len(sample_types), 3)

    for doc_type in sample_types:
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
    parser = argparse.ArgumentParser(description="CoE/HUDOCExec data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--count", type=int, default=15, help="Number of records to fetch")
    parser.add_argument("--since", type=str, help="Fetch records since date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

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
