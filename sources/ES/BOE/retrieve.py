#!/usr/bin/env python3
"""
ES/BOE — Retrieve by Common Reference

Resolves human-readable Spanish legislation references to BOE IDs.

Usage:
    python retrieve.py "Real Decreto 1095/1989"
    python retrieve.py "RD 1095/1989"
    python retrieve.py "Ley 10/2019"
    python retrieve.py "BOE-A-2020-3824"
    python retrieve.py --test
    python retrieve.py --generate-tests
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"
TESTS_FILE = SCRIPT_DIR / "retrieve_tests.json"

# ── Rango (document type) aliases ──
# Maps normalized aliases to canonical rango names as they appear in BOE records.
RANGO_ALIASES = {
    # Real Decreto
    "real decreto": "Real Decreto",
    "rd": "Real Decreto",
    "r.d.": "Real Decreto",
    # Real Decreto-ley
    "real decreto-ley": "Real Decreto-ley",
    "real decreto ley": "Real Decreto-ley",
    "rdl": "Real Decreto-ley",
    "r.d.l.": "Real Decreto-ley",
    "rd-ley": "Real Decreto-ley",
    "rd-l": "Real Decreto-ley",
    # Real Decreto Legislativo
    "real decreto legislativo": "Real Decreto Legislativo",
    "rdleg": "Real Decreto Legislativo",
    # Ley
    "ley": "Ley",
    "l": "Ley",
    # Ley Orgánica
    "ley organica": "Ley Orgánica",
    "ley orgánica": "Ley Orgánica",
    "lo": "Ley Orgánica",
    "l.o.": "Ley Orgánica",
    # Orden
    "orden": "Orden",
    # Decreto
    "decreto": "Decreto",
    # Decreto-ley
    "decreto-ley": "Decreto-ley",
    "decreto ley": "Decreto-ley",
    "dl": "Decreto-ley",
    "d.l.": "Decreto-ley",
    # Decreto Legislativo
    "decreto legislativo": "Decreto Legislativo",
    "dleg": "Decreto Legislativo",
}

# Reverse lookup: canonical rango → preferred abbreviation (for test generation)
REVERSE_ABBREV = {
    "Real Decreto": "RD",
    "Real Decreto-ley": "RDL",
    "Real Decreto Legislativo": "RDLeg",
    "Ley": "Ley",
    "Ley Orgánica": "LO",
    "Decreto-ley": "DL",
    "Decreto Legislativo": "DLeg",
    "Orden": "Orden",
    "Decreto": "Decreto",
}


def _strip_accents(s: str) -> str:
    """Remove accents for comparison."""
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def _normalize_text(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    return re.sub(r"\s+", " ", _strip_accents(s).lower()).strip()


def resolve_rango(raw: str) -> str | None:
    """Resolve a raw rango string to canonical form."""
    normalized = _normalize_text(raw)
    if normalized in RANGO_ALIASES:
        return RANGO_ALIASES[normalized]
    # Try accent-stripped keys
    for alias, canonical in RANGO_ALIASES.items():
        if _normalize_text(alias) == normalized:
            return canonical
    return None


def parse_reference(reference: str) -> dict | None:
    """Parse a common Spanish legal reference into structured fields.

    Supported formats:
      - "Real Decreto 1095/1989"
      - "RD 1095/1989"
      - "Ley 10/2019"
      - "Ley Orgánica 3/2018"
      - "Orden APA/315/2020"
      - "BOE-A-2020-3824"  (direct ID lookup)

    Returns dict with:
      - {"identifier": "BOE-A-2020-3824"}  for direct ID
      - {"rango": "Real Decreto", "numero_oficial": "1095/1989"}  for rango+number
    """
    ref = reference.strip()

    # Pattern 1: Direct BOE identifier — "BOE-A-2020-3824"
    m = re.match(r"^(BOE-[A-Z]-\d{4}-\d+)$", ref, re.IGNORECASE)
    if m:
        return {"identifier": m.group(1).upper()}

    # Pattern 2: Direct non-BOE identifiers — "DOGC-f-1997-90001", "BOJA-b-2020-90161"
    m = re.match(r"^([A-Z]+-[a-z]-\d{4}-\d+)$", ref, re.IGNORECASE)
    if m:
        return {"identifier": m.group(1)}

    # Pattern 3: "Orden MINISTRY/NUMBER/YEAR" — special case with ministry prefix
    m = re.match(
        r"^(?:orden)\s+([A-Z]+/\d+/\d{4})$",
        ref, re.IGNORECASE
    )
    if m:
        return {"rango": "Orden", "numero_oficial": m.group(1)}

    # Pattern 4: "RANGO NUMBER/YEAR" — main pattern
    m = re.match(
        r"^(.+?)\s+(\d+/\d{4})$",
        ref, re.IGNORECASE
    )
    if m:
        rango_raw, num = m.group(1).strip(), m.group(2)
        rango = resolve_rango(rango_raw)
        if rango:
            return {"rango": rango, "numero_oficial": num}

    return None


def match_record(record: dict, parsed: dict) -> bool:
    """Check if a record matches the parsed reference fields."""
    if "identifier" in parsed:
        return record.get("_id", "").upper() == parsed["identifier"].upper() or \
               record.get("identifier", "").upper() == parsed["identifier"].upper()

    if "rango" in parsed and "numero_oficial" in parsed:
        r_rango = record.get("rango", "")
        r_num = record.get("numero_oficial", "")
        return r_rango == parsed["rango"] and r_num == parsed["numero_oficial"]

    return False


def disambiguate(matches: list[dict]) -> dict | None:
    """Pick the best match when multiple records match.

    Prefer non-Finalizado status (active legislation), then most recent date.
    """
    if not matches:
        return None
    # Prefer active over finalizado
    active = [r for r in matches if r.get("estado_consolidacion") != "Finalizado"]
    pool = active if active else matches
    pool.sort(key=lambda r: r.get("date", ""), reverse=True)
    return pool[0]


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


def retrieve(reference: str) -> str | None:
    """Given a common legal reference, return the matching _id or None."""
    parsed = parse_reference(reference)
    if not parsed:
        return None
    records = _load_records()
    matches = [r for r in records if match_record(r, parsed)]
    if not matches:
        return None
    best = disambiguate(matches)
    return best["_id"] if best else None


def retrieve_all(reference: str) -> list[dict]:
    """Return all matching records for debugging."""
    parsed = parse_reference(reference)
    if not parsed:
        return []
    records = _load_records()
    return [
        {"_id": r["_id"], "rango": r.get("rango"), "numero_oficial": r.get("numero_oficial"),
         "date": r.get("date"), "estado_consolidacion": r.get("estado_consolidacion")}
        for r in records
        if match_record(r, parsed)
    ]


def generate_test_cases() -> list[dict]:
    """Read sample records and generate retrieve_tests.json."""
    tests = []
    seen = set()
    for f in sorted(SAMPLE_DIR.glob("record_*.json")):
        with open(f) as fh:
            rec = json.load(fh)
        _id = rec["_id"]
        rango = rec.get("rango", "")
        numero = rec.get("numero_oficial", "")
        identifier = rec.get("identifier", _id)

        if not rango or not numero:
            # Fall back to direct ID lookup
            if _id not in seen:
                seen.add(_id)
                tests.append({
                    "reference": _id,
                    "expected_id": _id,
                    "variants": [],
                })
            continue

        key = (rango, numero)
        if key in seen:
            continue
        seen.add(key)

        # Canonical reference: "Real Decreto 1095/1989"
        ref = f"{rango} {numero}"

        # Generate variants
        variants = []
        # Direct ID
        variants.append(identifier)
        # Abbreviation form
        abbrev = REVERSE_ABBREV.get(rango)
        if abbrev and abbrev != rango:
            variants.append(f"{abbrev} {numero}")

        tests.append({
            "reference": ref,
            "expected_id": _id,
            "variants": variants,
        })
    return tests


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
        description="ES/BOE — Retrieve legislation by common reference"
    )
    parser.add_argument("reference", nargs="?", help="Legal reference string")
    parser.add_argument("--test", action="store_true", help="Run all test cases")
    parser.add_argument("--generate-tests", action="store_true", help="Generate test cases from samples")
    parser.add_argument("--all", action="store_true", help="Show all matches as JSON")
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
            matches = retrieve_all(args.reference)
            print(json.dumps(matches, ensure_ascii=False, indent=2))
        else:
            result = retrieve(args.reference)
            if result:
                print(result)
            else:
                print("No match found", file=sys.stderr)
                sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
