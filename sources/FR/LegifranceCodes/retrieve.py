#!/usr/bin/env python3
"""
FR/LegifranceCodes — Legal Citation Resolver

Resolves French legal citations to unique Légifrance article IDs.

Supported formats:
    - Full form: "Article 1 du Code civil"
    - Abbreviated: "C. civ., art. 1"
    - With prefix: "Article L. 1234-5 du Code du travail"
    - Informal: "art. 111-1 code pénal"
    - Short: "CC art. 1", "CP art. 111-1"

Usage:
    python retrieve.py "article 1 du code civil"
    python retrieve.py "C. pén., art. 111-1"
    python retrieve.py --test
"""

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Optional

SOURCE_ID = "FR/LegifranceCodes"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
TESTS_FILE = SCRIPT_DIR / "retrieve_tests.json"

# French code abbreviation mappings
# Maps abbreviations and variants to canonical code names
CODE_ALIASES = {
    # Code civil
    "code civil": "Code civil",
    "c. civ.": "Code civil",
    "c.civ.": "Code civil",
    "cc": "Code civil",
    "c civ": "Code civil",
    "civ.": "Code civil",

    # Code pénal
    "code penal": "Code pénal",
    "code pénal": "Code pénal",
    "c. pen.": "Code pénal",
    "c. pén.": "Code pénal",
    "c.pen.": "Code pénal",
    "c.pén.": "Code pénal",
    "cp": "Code pénal",
    "c pen": "Code pénal",
    "c pén": "Code pénal",
    "pen.": "Code pénal",
    "pén.": "Code pénal",

    # Code du travail
    "code du travail": "Code du travail",
    "c. trav.": "Code du travail",
    "c.trav.": "Code du travail",
    "ct": "Code du travail",
    "c trav": "Code du travail",
    "trav.": "Code du travail",

    # Code de commerce
    "code de commerce": "Code de commerce",
    "c. com.": "Code de commerce",
    "c.com.": "Code de commerce",
    "ccom": "Code de commerce",
    "c com": "Code de commerce",
    "com.": "Code de commerce",

    # Code de la consommation
    "code de la consommation": "Code de la consommation",
    "c. conso.": "Code de la consommation",
    "c.conso.": "Code de la consommation",
    "c conso": "Code de la consommation",
    "conso.": "Code de la consommation",

    # Code de procédure civile
    "code de procedure civile": "Code de procédure civile",
    "code de procédure civile": "Code de procédure civile",
    "c. pr. civ.": "Code de procédure civile",
    "c.pr.civ.": "Code de procédure civile",
    "cpc": "Code de procédure civile",
    "c pr civ": "Code de procédure civile",

    # Code de procédure pénale
    "code de procedure penale": "Code de procédure pénale",
    "code de procédure pénale": "Code de procédure pénale",
    "c. pr. pen.": "Code de procédure pénale",
    "c. pr. pén.": "Code de procédure pénale",
    "c.pr.pen.": "Code de procédure pénale",
    "c.pr.pén.": "Code de procédure pénale",
    "cpp": "Code de procédure pénale",
    "c pr pen": "Code de procédure pénale",
    "c pr pén": "Code de procédure pénale",

    # Code général des impôts
    "code general des impots": "Code général des impôts",
    "code général des impôts": "Code général des impôts",
    "cgi": "Code général des impôts",
    "c.g.i.": "Code général des impôts",
    "c. gen. imp.": "Code général des impôts",
}


def normalize_text(text: str) -> str:
    """Normalize text for comparison: lowercase, remove accents, normalize spaces."""
    text = text.lower().strip()
    # Normalize unicode (NFD decomposes accented chars, then we filter combining marks)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_code_name(code_ref: str) -> Optional[str]:
    """Convert a code reference (abbreviation or full name) to canonical name."""
    # First normalize the input
    normalized = normalize_text(code_ref)

    # Try exact match first
    if normalized in CODE_ALIASES:
        return CODE_ALIASES[normalized]

    # Try normalized version of aliases
    for alias, canonical in CODE_ALIASES.items():
        if normalize_text(alias) == normalized:
            return canonical

    # Try partial match (code name contains the reference)
    for alias, canonical in CODE_ALIASES.items():
        if normalized in normalize_text(alias) or normalize_text(alias) in normalized:
            return canonical

    return None


def parse_reference(citation: str) -> Optional[dict]:
    """
    Parse a French legal citation into its components.

    Handles formats like:
        - "Article 1 du Code civil"
        - "art. L. 1234-5 code du travail"
        - "C. civ., art. 1"
        - "CC art. 1"
        - "article 111-1 CP"

    Returns dict with:
        - article_num: The article number (e.g., "1", "L1", "111-1", "L. 1234-5")
        - code_name: The canonical code name (e.g., "Code civil")
    """
    citation = citation.strip()
    original = citation
    citation_lower = citation.lower()

    # Pattern 1: "Article X du Code Y" or "art. X du Code Y" or "art X code Y"
    # Also handles "article X Y" where Y is a code abbreviation at the end
    pattern1 = re.compile(
        r"(?:article|art\.?)\s*"
        r"([A-Za-zÀ-ÿ]?\.?\s*[\d\-]+(?:\s*[A-Za-zÀ-ÿ])?(?:\-\d+)*)"
        r"(?:\s+(?:du|de\s+la|de\s+l'|des|de|,)?\s*)"
        r"(.+)",
        re.IGNORECASE
    )

    # Pattern 2: "C. xxx., art. X" or "CC art. X" (code abbreviation first)
    # Handles: C. civ., C. pr. civ., C. pr. pén., CPP, CGI, etc.
    pattern2 = re.compile(
        r"((?:[A-Za-zÀ-ÿ]+\.?\s*)+?)(?<!\s),?\s*"
        r"(?:article|art\.?)\s*"
        r"([A-Za-zÀ-ÿ]?\.?\s*[\d\-]+(?:\s*[A-Za-zÀ-ÿ])?(?:\-\d+)*)",
        re.IGNORECASE
    )

    article_num = None
    code_ref = None

    # Try pattern 2 first (code abbreviation before article)
    match2 = pattern2.match(citation)
    if match2:
        code_ref = match2.group(1).strip()
        article_num = match2.group(2).strip()
    else:
        # Try pattern 1 (article before code)
        match1 = pattern1.match(citation)
        if match1:
            article_num = match1.group(1).strip()
            code_ref = match1.group(2).strip()

    if not article_num or not code_ref:
        return None

    # Normalize article number: remove extra spaces, handle "L." prefix
    article_num = re.sub(r"\s+", "", article_num)  # Remove all spaces
    article_num = re.sub(r"^([A-Za-z])\.?", r"\1", article_num)  # L. -> L
    article_num = article_num.upper() if article_num[0].isalpha() else article_num

    # Convert code reference to canonical name
    code_name = normalize_code_name(code_ref)
    if not code_name:
        return None

    return {
        "article_num": article_num,
        "code_name": code_name,
    }


def load_sample_records() -> list[dict]:
    """Load all sample records from the sample directory."""
    records = []
    all_samples = SAMPLE_DIR / "all_samples.json"
    if all_samples.exists():
        with open(all_samples, encoding="utf-8") as f:
            records = json.load(f)
    else:
        for filepath in sorted(SAMPLE_DIR.glob("record_*.json")):
            with open(filepath, encoding="utf-8") as f:
                records.append(json.load(f))
    return records


def normalize_article_num(article_num: str) -> str:
    """Normalize an article number for comparison."""
    # Remove all whitespace
    article_num = re.sub(r"\s+", "", article_num)
    # Uppercase prefix letters
    if article_num and article_num[0].isalpha():
        article_num = article_num[0].upper() + article_num[1:]
    # Remove dots after prefix letter
    article_num = re.sub(r"^([A-Z])\.?", r"\1", article_num)
    return article_num


def resolve(citation: str, records: Optional[list[dict]] = None) -> Optional[str]:
    """
    Resolve a French legal citation to its unique Légifrance article ID.

    Args:
        citation: Human-readable citation like "Article 1 du Code civil"
        records: Optional pre-loaded records (for efficiency)

    Returns:
        The LEGIARTI ID of the matching record, or None if not found.

    Examples:
        >>> resolve("Article 1 du Code civil")
        "LEGIARTI000006419280"
        >>> resolve("C. pén., art. 111-1")
        "LEGIARTI000006417175"
    """
    if records is None:
        records = load_sample_records()

    parsed = parse_reference(citation)
    if not parsed:
        return None

    target_article = normalize_article_num(parsed["article_num"])
    target_code = parsed["code_name"]

    for record in records:
        record_code = record.get("code_name", "")
        record_article = normalize_article_num(record.get("article_num", ""))

        if record_code == target_code and record_article == target_article:
            return record["_id"]

    return None


def run_tests(verbose: bool = False) -> tuple[int, int]:
    """Run all tests from retrieve_tests.json."""
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
        citations = test.get("citations", [])

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
    parser = argparse.ArgumentParser(
        description="Resolve French legal citations to Légifrance article IDs"
    )
    parser.add_argument("citation", nargs="?", help="Citation to resolve")
    parser.add_argument("--test", action="store_true", help="Run all tests")
    parser.add_argument("--test-verbose", action="store_true", help="Run tests verbosely")

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
