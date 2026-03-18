#!/usr/bin/env python3
"""
World Wide Law — Retrieve Script Template

Copy this file to sources/{COUNTRY}/{SOURCE}/retrieve.py
and implement the resolve() and parse_reference() methods.

A retrieve script resolves human-readable legal citations to unique record IDs.

Usage:
    python retrieve.py "article 1 du code civil"        # Resolve a citation
    python retrieve.py --test                            # Run all tests
    python retrieve.py --test-verbose                    # Run tests with details

The resolve() method should:
  1. Parse the citation to extract relevant identifiers
  2. Search the local sample data or query the source
  3. Return the _id of the matching record, or None if not found
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

# Configuration - UPDATE THESE FOR YOUR SOURCE
SOURCE_ID = "XX/SourceName"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
TESTS_FILE = SCRIPT_DIR / "retrieve_tests.json"


def load_sample_records() -> list[dict]:
    """Load all sample records from the sample directory."""
    records = []
    all_samples = SAMPLE_DIR / "all_samples.json"
    if all_samples.exists():
        with open(all_samples, encoding="utf-8") as f:
            records = json.load(f)
    else:
        for filepath in SAMPLE_DIR.glob("record_*.json"):
            with open(filepath, encoding="utf-8") as f:
                records.append(json.load(f))
    return records


def parse_reference(citation: str) -> Optional[dict]:
    """
    Parse a legal citation into its components.

    Args:
        citation: Human-readable citation like "Article 1 du Code civil"

    Returns:
        Dict with parsed components, or None if citation cannot be parsed.

        Example return value:
        {
            "article_num": "1",
            "code_name": "Code civil",
            "section": None,
            "paragraph": None,
        }

    TODO: Implement parsing logic for your source's citation formats.
    Common patterns to handle:
        - Full form: "Article 1 du Code civil"
        - Abbreviated: "C. civ., art. 1"
        - With sections: "Article L. 1234-5 du Code du travail"
        - With paragraphs: "Article 1, alinéa 2"
    """
    raise NotImplementedError("TODO: implement parse_reference()")


def resolve(citation: str, records: Optional[list[dict]] = None) -> Optional[str]:
    """
    Resolve a legal citation to its unique record _id.

    Args:
        citation: Human-readable citation to resolve
        records: Optional pre-loaded records (for efficiency in batch mode)

    Returns:
        The _id of the matching record, or None if not found.

    TODO: Implement resolution logic:
        1. Call parse_reference() to extract components
        2. Search through records for a match
        3. Return the _id of the best match

    Example:
        >>> resolve("Article 1 du Code civil")
        "LEGIARTI000006419280"
    """
    if records is None:
        records = load_sample_records()

    parsed = parse_reference(citation)
    if not parsed:
        return None

    # TODO: Implement matching logic
    # for record in records:
    #     if matches(record, parsed):
    #         return record["_id"]

    raise NotImplementedError("TODO: implement resolve()")


def run_tests(verbose: bool = False) -> tuple[int, int]:
    """
    Run all tests from retrieve_tests.json.

    Returns:
        Tuple of (passed_count, total_count)
    """
    if not TESTS_FILE.exists():
        print(f"No tests found at {TESTS_FILE}")
        return 0, 0

    with open(TESTS_FILE, encoding="utf-8") as f:
        tests = json.load(f)

    records = load_sample_records()
    passed = 0
    total = 0

    for test in tests:
        expected_id = test["expected_id"]
        citations = test.get("citations", [test.get("citation")])

        for citation in citations:
            if not citation:
                continue
            total += 1
            result = resolve(citation, records)

            if result == expected_id:
                passed += 1
                if verbose:
                    print(f"✓ {citation} → {result}")
            else:
                print(f"✗ {citation}")
                print(f"  Expected: {expected_id}")
                print(f"  Got:      {result}")

    print(f"\n{passed}/{total} tests passed")
    return passed, total


def main():
    parser = argparse.ArgumentParser(description="Resolve legal citations to record IDs")
    parser.add_argument("citation", nargs="?", help="Citation to resolve")
    parser.add_argument("--test", action="store_true", help="Run all tests")
    parser.add_argument("--test-verbose", action="store_true", help="Run tests with details")

    args = parser.parse_args()

    if args.test or args.test_verbose:
        passed, total = run_tests(verbose=args.test_verbose)
        sys.exit(0 if passed == total else 1)

    if args.citation:
        result = resolve(args.citation)
        if result:
            print(result)
        else:
            print(f"Could not resolve: {args.citation}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
