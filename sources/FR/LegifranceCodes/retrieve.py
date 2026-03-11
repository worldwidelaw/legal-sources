#!/usr/bin/env python3
"""
FR/LegifranceCodes — Retrieve by Common Reference

Resolves human-readable French code article references to LEGIARTI IDs.

Usage:
    python retrieve.py "article 1 du code civil"
    python retrieve.py "C. civ., art. 1"
    python retrieve.py --as-of 2000-01-01 "article 1 du code civil"
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

# Consolidated legislation: each article can have multiple versions over time,
# each with date_debut/date_fin. Supports as_of_date to select a specific version.
SUPPORTS_AS_OF_DATE = True

# ── Code name aliases ──
# Maps normalized (lowercase, accent-stripped) aliases to canonical code names.
# Covers the 8 priority codes in samples plus standard abbreviations.
CODE_ALIASES = {
    # Code civil
    "code civil": "Code civil",
    "c. civ.": "Code civil",
    "c.civ.": "Code civil",
    "cc": "Code civil",
    # Code pénal
    "code penal": "Code pénal",
    "code pénal": "Code pénal",
    "c. pen.": "Code pénal",
    "c. pén.": "Code pénal",
    "c.pen.": "Code pénal",
    "c.pén.": "Code pénal",
    "cp": "Code pénal",
    # Code du travail
    "code du travail": "Code du travail",
    "c. trav.": "Code du travail",
    "c.trav.": "Code du travail",
    # Code de commerce
    "code de commerce": "Code de commerce",
    "c. com.": "Code de commerce",
    "c.com.": "Code de commerce",
    # Code de la consommation
    "code de la consommation": "Code de la consommation",
    "c. conso.": "Code de la consommation",
    "c.conso.": "Code de la consommation",
    # Code de procédure civile
    "code de procedure civile": "Code de procédure civile",
    "code de procédure civile": "Code de procédure civile",
    "c. pr. civ.": "Code de procédure civile",
    "c.pr.civ.": "Code de procédure civile",
    "cpc": "Code de procédure civile",
    # Code de procédure pénale
    "code de procedure penale": "Code de procédure pénale",
    "code de procédure pénale": "Code de procédure pénale",
    "c. pr. pen.": "Code de procédure pénale",
    "c. pr. pén.": "Code de procédure pénale",
    "c.pr.pen.": "Code de procédure pénale",
    "c.pr.pén.": "Code de procédure pénale",
    "cpp": "Code de procédure pénale",
    # Code général des impôts
    "code general des impots": "Code général des impôts",
    "code général des impôts": "Code général des impôts",
    "cgi": "Code général des impôts",
}

# Reverse lookup: canonical name → preferred abbreviation (for test generation)
REVERSE_ABBREV = {
    "Code civil": "C. civ.",
    "Code pénal": "C. pén.",
    "Code du travail": "C. trav.",
    "Code de commerce": "C. com.",
    "Code de la consommation": "C. conso.",
    "Code de procédure civile": "C. pr. civ.",
    "Code de procédure pénale": "C. pr. pén.",
    "Code général des impôts": "CGI",
}


def _strip_accents(s: str) -> str:
    """Remove accents for comparison."""
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def _normalize_text(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    return re.sub(r"\s+", " ", _strip_accents(s).lower()).strip()


def resolve_code_name(raw: str) -> str | None:
    """Resolve a raw code name/abbreviation to canonical form."""
    normalized = _normalize_text(raw)
    # Direct lookup
    if normalized in CODE_ALIASES:
        return CODE_ALIASES[normalized]
    # Try accent-stripped keys
    for alias, canonical in CODE_ALIASES.items():
        if _normalize_text(alias) == normalized:
            return canonical
    return None


def normalize_article_num(raw: str) -> str:
    """Normalize an article number to match sample data format.

    Samples use: "1", "111-1", "L1", "L110-1", "1 A", "liminaire", "préliminaire"
    """
    s = raw.strip()
    # Handle special article names
    s_lower = _strip_accents(s).lower()
    if s_lower in ("preliminaire", "préliminaire"):
        return "préliminaire"
    if s_lower == "liminaire":
        return "liminaire"
    # Strip dots after letter prefixes: "L. 110-1" -> "L110-1"
    s = re.sub(r"^([LRDA])\.\s*", r"\1", s, flags=re.IGNORECASE)
    # Remove leading/trailing whitespace but preserve internal spaces ("1 A")
    s = s.strip()
    # Uppercase the letter prefix if present
    if s and s[0].isalpha() and len(s) > 1 and (s[1].isdigit() or s[1] == '-'):
        s = s[0].upper() + s[1:]
    return s


def parse_reference(reference: str) -> tuple[str, str] | None:
    """Parse a common legal reference into (canonical_code_name, normalized_article_num).

    Supported formats:
      - "article 1 du code civil"
      - "art. 1 code civil"
      - "C. civ., art. 1"
      - "article L110-1 du code de commerce"
      - "Code civil, article 1"
    """
    ref = reference.strip()

    # Pattern 1: Abbreviation format — "C. civ., art. 1" or "CGI, art. 4 A"
    m = re.match(
        r"^(.+?),\s*art(?:icle)?\.?\s+(.+)$",
        ref, re.IGNORECASE
    )
    if m:
        code_raw, art_raw = m.group(1).strip(), m.group(2).strip()
        code = resolve_code_name(code_raw)
        if code:
            return code, normalize_article_num(art_raw)

    # Pattern 2: "article X du/de/des CODE" or "art. X code Y"
    m = re.match(
        r"^art(?:icle)?\.?\s+(.+?)\s+(?:du|de\s+la|de\s+l'|des|de|d')\s+(.+)$",
        ref, re.IGNORECASE
    )
    if m:
        art_raw, code_raw = m.group(1).strip(), m.group(2).strip()
        code = resolve_code_name(code_raw)
        if code:
            return code, normalize_article_num(art_raw)

    # Pattern 3: "art. X CODE" (no preposition)
    m = re.match(
        r"^art(?:icle)?\.?\s+(.+?)\s+(code\s+.+|c\.\s*.+|cgi|cc|cp|cpc|cpp)$",
        ref, re.IGNORECASE
    )
    if m:
        art_raw, code_raw = m.group(1).strip(), m.group(2).strip()
        code = resolve_code_name(code_raw)
        if code:
            return code, normalize_article_num(art_raw)

    # Pattern 4: "Code civil, article 1" (code name first, then article)
    m = re.match(
        r"^(.+?),\s*article\s+(.+)$",
        ref, re.IGNORECASE
    )
    if m:
        code_raw, art_raw = m.group(1).strip(), m.group(2).strip()
        code = resolve_code_name(code_raw)
        if code:
            return code, normalize_article_num(art_raw)

    return None


def filter_by_date(matches: list[dict], as_of_date: str) -> list[dict]:
    """Filter to versions in force on as_of_date (YYYY-MM-DD).

    Each record has date_debut and date_fin defining its validity window.
    """
    result = []
    for r in matches:
        debut = r.get("date_debut", "")
        fin = r.get("date_fin", "2999-01-01")
        if debut <= as_of_date < fin:
            result.append(r)
    return result


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
        reference: Human-readable French code article citation.
        as_of_date: Optional YYYY-MM-DD date. When provided, returns the
                    version of the article in force on that date instead of
                    the current (VIGUEUR) version.
    """
    parsed = parse_reference(reference)
    if not parsed:
        return None
    code_name, article_num = parsed
    records = _load_records()
    # Match on code_name (canonical) and article_num (normalized)
    matches = []
    for r in records:
        r_code = r.get("code_name", "")
        r_art = normalize_article_num(r.get("article_num", ""))
        if r_code == code_name and r_art == article_num:
            matches.append(r)
    if not matches:
        return None
    # If as_of_date is provided, filter to versions valid on that date
    if as_of_date:
        matches = filter_by_date(matches, as_of_date)
        if not matches:
            return None
        # Among date-filtered matches, prefer most recent date_debut
        matches.sort(key=lambda r: r.get("date_debut", ""), reverse=True)
        return matches[0]["_id"]
    # Default: prefer VIGUEUR, then most recent date_debut
    vigueur = [r for r in matches if r.get("etat") == "VIGUEUR"]
    if vigueur:
        matches = vigueur
    matches.sort(key=lambda r: r.get("date_debut", ""), reverse=True)
    return matches[0]["_id"]


def retrieve_all(reference: str, as_of_date: str | None = None) -> list[dict]:
    """Return all matching records for debugging."""
    parsed = parse_reference(reference)
    if not parsed:
        return []
    code_name, article_num = parsed
    records = _load_records()
    matches = [
        r for r in records
        if r.get("code_name") == code_name
        and normalize_article_num(r.get("article_num", "")) == article_num
    ]
    if as_of_date:
        matches = filter_by_date(matches, as_of_date)
    return [
        {"_id": r["_id"], "code_name": r.get("code_name"), "article_num": r.get("article_num"),
         "etat": r.get("etat"), "date_debut": r.get("date_debut"), "date_fin": r.get("date_fin")}
        for r in matches
    ]


def generate_test_cases() -> list[dict]:
    """Read sample records and generate retrieve_tests.json."""
    tests = []
    seen = set()
    for f in sorted(SAMPLE_DIR.glob("record_*.json")):
        with open(f) as fh:
            rec = json.load(fh)
        code = rec["code_name"]
        num = rec["article_num"]
        _id = rec["_id"]
        # Skip duplicates (same code + article)
        key = (code, num)
        if key in seen:
            continue
        seen.add(key)
        # Canonical reference
        ref = f"article {num} du {code}"
        # Generate variants
        variants = []
        abbrev = REVERSE_ABBREV.get(code)
        if abbrev:
            variants.append(f"{abbrev}, art. {num}")
        variants.append(f"art. {num} {code.lower()}")
        tests.append({
            "reference": ref,
            "expected_id": _id,
            "variants": variants,
        })
    with open(TESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(tests, f, ensure_ascii=False, indent=2)
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
        description="FR/LegifranceCodes — Retrieve article by common reference"
    )
    parser.add_argument("reference", nargs="?", help="Legal reference string")
    parser.add_argument("--test", action="store_true", help="Run all test cases")
    parser.add_argument("--generate-tests", action="store_true", help="Generate test cases from samples")
    parser.add_argument("--all", action="store_true", help="Show all matches as JSON")
    parser.add_argument("--as-of", metavar="YYYY-MM-DD",
                        help="Return version in force on this date")
    args = parser.parse_args()

    if args.generate_tests:
        tests = generate_test_cases()
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
