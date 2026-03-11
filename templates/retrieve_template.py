#!/usr/bin/env python3
"""
World Wide Law — Retrieve Script Template

Copy this file to sources/{COUNTRY}/{SOURCE}/retrieve.py
and implement parse_reference(), match_record(), and generate_test_cases().

The same file serves as both retrieval tool and test runner:
    python retrieve.py "reference string"       # Resolve to _id
    python retrieve.py --test                    # Run all test cases
    python retrieve.py --generate-tests          # Generate test cases from samples
    python retrieve.py --all "reference string"  # Show all matches as JSON
    python retrieve.py --as-of 2020-01-15 "ref" # Resolve to version in force on date

Reference implementation: sources/FR/LegifranceCodes/retrieve.py

All you need to implement:
    1. parse_reference()     - parse citation string into structured fields
    2. match_record()        - check if a record matches parsed fields
    3. generate_test_cases() - create test entries from sample data

For consolidated legislation sources (where articles have multiple versions over
time), implement filter_by_date() and set SUPPORTS_AS_OF_DATE = True. Gazette and
official journal sources (where each law has a fixed date) should leave it False.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"
TESTS_FILE = SCRIPT_DIR / "retrieve_tests.json"

# Set to True for consolidated legislation sources where the same article/section
# can have multiple versions over time (each with date ranges). Leave False for
# gazette/official journal sources where each law entry has a fixed date.
SUPPORTS_AS_OF_DATE = False


def parse_reference(reference: str) -> dict | None:
    """Parse a common legal reference string into structured fields.

    TODO: Implement for this source's citation conventions.

    For legislation, return e.g.:
        {"code_name": "Code civil", "article_num": "1"}

    For case law, return e.g.:
        {"number": "21-10.123"}
        {"ecli": "ECLI:FR:CCASS:2021:CO00123"}

    Returns None if the reference cannot be parsed.
    """
    raise NotImplementedError("TODO: implement parse_reference()")


def match_record(record: dict, parsed: dict) -> bool:
    """Check if a record matches the parsed reference fields.

    TODO: Implement field-by-field matching logic.

    Args:
        record: A record dict loaded from sample/data files.
        parsed: The structured fields from parse_reference().

    Returns:
        True if the record matches the reference.
    """
    raise NotImplementedError("TODO: implement match_record()")


def filter_by_date(matches: list[dict], as_of_date: str) -> list[dict]:
    """Filter matches to only those in force on as_of_date (YYYY-MM-DD).

    Only called when SUPPORTS_AS_OF_DATE is True. Override for source-specific
    date fields (e.g., date_debut/date_fin, effective_from/effective_to).

    Default: return all matches (no filtering).
    """
    return matches


def disambiguate(matches: list[dict]) -> dict | None:
    """Pick the best match when multiple records match.

    Default: return first match. Override for source-specific logic
    (e.g., prefer VIGUEUR status, most recent date, etc.).
    """
    return matches[0] if matches else None


def _load_records() -> list[dict]:
    """Load all available records (samples + JSONL data)."""
    records = []
    for f in sorted(SAMPLE_DIR.glob("record_*.json")):
        with open(f) as fh:
            records.append(json.load(fh))
    jsonl = DATA_DIR / "records.jsonl"
    if jsonl.exists():
        with open(jsonl) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def retrieve(reference: str, as_of_date: str | None = None) -> str | None:
    """Given a common legal reference, return the matching _id or None.

    Args:
        reference: Human-readable legal citation string.
        as_of_date: Optional YYYY-MM-DD date for consolidated legislation sources.
                    When provided (and SUPPORTS_AS_OF_DATE is True), returns the
                    version in force on that date. Ignored for gazette sources.
    """
    parsed = parse_reference(reference)
    if not parsed:
        return None
    records = _load_records()
    matches = [r for r in records if match_record(r, parsed)]
    if SUPPORTS_AS_OF_DATE and as_of_date:
        matches = filter_by_date(matches, as_of_date)
    if not matches:
        return None
    best = disambiguate(matches)
    return best["_id"] if best else None


def retrieve_all(reference: str, as_of_date: str | None = None) -> list[dict]:
    """Return all matching records for debugging."""
    parsed = parse_reference(reference)
    if not parsed:
        return []
    records = _load_records()
    matches = [r for r in records if match_record(r, parsed)]
    if SUPPORTS_AS_OF_DATE and as_of_date:
        matches = filter_by_date(matches, as_of_date)
    return [
        {"_id": r["_id"], **{k: r.get(k) for k in parsed}}
        for r in matches
    ]


def generate_test_cases() -> list[dict]:
    """Read sample records and produce retrieve_tests.json.

    TODO: Implement reference generation from sample metadata.

    Each test case should be:
        {
            "reference": "canonical reference string",
            "expected_id": "the _id from the sample record",
            "variants": ["alt form 1", "alt form 2"]
        }
    """
    raise NotImplementedError("TODO: implement generate_test_cases()")


def run_tests() -> dict:
    """Run all test cases and return results."""
    if not TESTS_FILE.exists():
        print("No test file found. Run --generate-tests first.")
        return {"passed": 0, "failed": 0, "total": 0, "failures": []}
    with open(TESTS_FILE, encoding="utf-8") as f:
        tests = json.load(f)
    passed = 0
    failed = 0
    failures = []
    for tc in tests:
        # Test canonical reference
        result = retrieve(tc["reference"])
        if result == tc["expected_id"]:
            passed += 1
        else:
            failed += 1
            failures.append({"ref": tc["reference"], "expected": tc["expected_id"], "got": result})
        # Test variants
        for variant in tc.get("variants", []):
            result = retrieve(variant)
            if result == tc["expected_id"]:
                passed += 1
            else:
                failed += 1
                failures.append({"ref": variant, "expected": tc["expected_id"], "got": result})
    total = passed + failed
    return {"passed": passed, "failed": failed, "total": total, "failures": failures}


def main():
    parser = argparse.ArgumentParser(
        description="Retrieve records by common legal reference"
    )
    parser.add_argument("reference", nargs="?", help="Legal reference string")
    parser.add_argument("--test", action="store_true", help="Run all test cases")
    parser.add_argument("--generate-tests", action="store_true", help="Generate test cases from samples")
    parser.add_argument("--all", action="store_true", help="Show all matches as JSON")
    parser.add_argument("--as-of", metavar="YYYY-MM-DD",
                        help="Date for version lookup (consolidated legislation only)")
    args = parser.parse_args()

    if args.generate_tests:
        tests = generate_test_cases()
        with open(TESTS_FILE, "w", encoding="utf-8") as f:
            json.dump(tests, f, ensure_ascii=False, indent=2)
        print(f"Generated {len(tests)} test cases -> {TESTS_FILE}")
    elif args.test:
        results = run_tests()
        print(f"Tests: {results['passed']}/{results['total']} passed")
        for f in results.get("failures", []):
            print(f"  FAIL: '{f['ref']}' expected={f['expected']} got={f['got']}")
        sys.exit(0 if results["failed"] == 0 else 1)
    elif args.reference:
        if args.all:
            matches = retrieve_all(args.reference, as_of_date=args.as_of)
            print(json.dumps(matches, ensure_ascii=False, indent=2))
        else:
            result = retrieve(args.reference, as_of_date=args.as_of)
            if result:
                print(result)
            else:
                print("No match found", file=sys.stderr)
                sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
