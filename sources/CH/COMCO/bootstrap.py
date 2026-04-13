#!/usr/bin/env python3
"""
CH/COMCO - Swiss Competition Commission (WEKO/COMCO)

Fetches competition decisions from the Swiss Competition Commission.
Decisions are published as PDF documents on their website.

Data source:
- Decisions page: https://www.weko.admin.ch/de/entscheide
- PDFs: https://www.weko.admin.ch/dam/de/sd-web/{id}/{filename}.pdf

The WEKO publishes decisions including:
- Competition cases (antitrust, cartels, dominance abuse)
- Merger control decisions
- Market investigations
- Advisory opinions

Languages: German (de), French (fr), Italian (it)
License: Public domain (Swiss federal government)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all decisions
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import unquote

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "CH/COMCO"
BASE_URL = "https://www.weko.admin.ch"
DECISIONS_URL = f"{BASE_URL}/de/entscheide"

RATE_LIMIT_DELAY = 2.0
MAX_PDF_SIZE_MB = 25.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"

# Decision type keywords (German/French)
DECISION_TYPE_KEYWORDS = {
    "Verfügung": "decision",
    "Décision": "decision",
    "Schlussbericht": "final_report",
    "Rapport final": "final_report",
    "Stellungnahme": "opinion",
    "Avis": "opinion",
    "Zusammenschluss": "merger",
    "Concentration": "merger",
    "Fusion": "merger",
    "Beratung": "advisory",
    "Consultation": "advisory",
    "Anregung": "recommendation",
    "Recommandation": "recommendation",
}

# Month names for date parsing
MONTHS_DE = {
    "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8, "september": 9,
    "oktober": 10, "november": 11, "dezember": 12,
}
MONTHS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-CH,de;q=0.9,en;q=0.8",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="CH/COMCO",
        source_id="",
        pdf_bytes=pdf_content,
        table="doctrine",
    ) or ""

def fetch_decisions_list(session: requests.Session) -> list[dict]:
    """Fetch the list of decisions from the WEKO website."""
    print(f"Fetching decisions list from {DECISIONS_URL}...", file=sys.stderr)

    try:
        response = session.get(DECISIONS_URL, timeout=30)
        response.raise_for_status()
        html = response.text
    except requests.RequestException as e:
        print(f"Error fetching decisions page: {e}", file=sys.stderr)
        return []

    # Extract PDF links with their metadata
    decisions = []

    # Pattern to match PDF links
    pdf_pattern = re.compile(
        r'href="(https://www\.weko\.admin\.ch/dam/[^"]+\.pdf)"',
        re.IGNORECASE
    )

    for match in pdf_pattern.finditer(html):
        pdf_url = match.group(1)

        # Extract filename from URL
        filename = unquote(pdf_url.split('/')[-1])

        # Parse title from filename (remove .pdf)
        title = filename.replace('.pdf', '').replace('_', ' ').replace('%20', ' ')

        # Extract document ID from URL path
        url_parts = pdf_url.split('/')
        doc_id = None
        for i, part in enumerate(url_parts):
            if part == "sd-web" and i + 1 < len(url_parts):
                doc_id = url_parts[i + 1]
                break

        if not doc_id:
            doc_id = filename.replace('.pdf', '').replace(' ', '_')[:50]

        decisions.append({
            'pdf_url': pdf_url,
            'filename': filename,
            'title': title,
            'doc_id': doc_id,
        })

    print(f"Found {len(decisions)} decisions", file=sys.stderr)
    return decisions


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    """Download PDF and return content."""
    try:
        # First check PDF size with HEAD request
        head_resp = session.head(url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > MAX_PDF_SIZE_MB:
                print(f"Skipping large PDF ({size_mb:.1f} MB): {url.split('/')[-1]}", file=sys.stderr)
                return None

        response = session.get(url, timeout=180)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Error downloading PDF: {e}", file=sys.stderr)
        return None


def parse_date_from_text(text: str) -> Optional[str]:
    """Extract date from text content (title or PDF body)."""
    # Pattern: "vom DD. MONTH YYYY" or "du DD MONTH YYYY"
    # German: "vom 25. März 2025"
    # French: "du 25 mars 2025"

    patterns = [
        # German: "vom 25. März 2025" or "vom 25 März 2025"
        r'vom\s+(\d{1,2})\.?\s+(\w+)\s+(\d{4})',
        # French: "du 25 mars 2025"
        r'du\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
        # Generic: DD.MM.YYYY or DD/MM/YYYY
        r'(\d{1,2})[./](\d{1,2})[./](\d{4})',
        # ISO-like: YYYY-MM-DD
        r'(\d{4})-(\d{2})-(\d{2})',
        # Year only as fallback
        r'\b(20\d{2})\b',
    ]

    text_lower = text.lower()

    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            groups = match.groups()

            if len(groups) == 3 and not groups[1].isdigit():
                # Month name pattern
                day = int(groups[0])
                month_name = groups[1].lower()
                year = int(groups[2])

                month = MONTHS_DE.get(month_name) or MONTHS_FR.get(month_name)
                if month:
                    return f"{year}-{month:02d}-{day:02d}"

            elif len(groups) == 3 and groups[0].isdigit() and len(groups[0]) == 4:
                # YYYY-MM-DD pattern
                year = int(groups[0])
                month = int(groups[1])
                day = int(groups[2])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return f"{year}-{month:02d}-{day:02d}"

            elif len(groups) == 3 and groups[1].isdigit():
                # DD.MM.YYYY pattern
                day = int(groups[0])
                month = int(groups[1])
                year = int(groups[2])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return f"{year}-{month:02d}-{day:02d}"

            elif len(groups) == 1:
                # Year only
                return f"{groups[0]}-01-01"

    return None


def detect_decision_type(title: str) -> str:
    """Detect decision type from title."""
    title_lower = title.lower()

    for keyword, dtype in DECISION_TYPE_KEYWORDS.items():
        if keyword.lower() in title_lower:
            return dtype

    return "decision"


def detect_language(title: str, text: str) -> str:
    """Detect document language."""
    combined = (title + " " + text[:500]).lower()

    # French indicators
    fr_words = ['du', 'décision', 'rapport', 'en matière de', 'concernant']
    fr_count = sum(1 for w in fr_words if w in combined)

    # German indicators
    de_words = ['vom', 'verfügung', 'schlussbericht', 'betreffend', 'zusammenschluss']
    de_count = sum(1 for w in de_words if w in combined)

    # Italian indicators
    it_words = ['del', 'decisione', 'rapporto', 'concernente']
    it_count = sum(1 for w in it_words if w in combined)

    counts = {'de': de_count, 'fr': fr_count, 'it': it_count}
    return max(counts, key=counts.get)


def normalize(raw: dict, text: str) -> dict:
    """Transform raw decision data into normalized schema."""
    title = raw.get('title', '')
    doc_id = raw.get('doc_id', '')
    pdf_url = raw.get('pdf_url', '')

    # Parse date from title first, then from text body
    date_str = parse_date_from_text(title)
    if not date_str and text:
        # Try first 1000 chars of PDF content
        date_str = parse_date_from_text(text[:1000])

    # Detect type and language
    decision_type = detect_decision_type(title)
    language = detect_language(title, text)

    # Clean up title
    clean_title = title.strip()

    # Create unique ID
    unique_id = f"{SOURCE_ID}/{doc_id}"

    return {
        '_id': unique_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': clean_title,
        'text': text,
        'date': date_str,
        'url': pdf_url,
        'language': language,
        'decision_type': decision_type,
        'doc_id': doc_id,
        'authority': 'Wettbewerbskommission (WEKO)',
        'country': 'CH',
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all WEKO decisions."""
    print("Fetching all WEKO decisions...", file=sys.stderr)

    decisions = fetch_decisions_list(session)

    for i, decision in enumerate(decisions):
        print(f"[{i+1}/{len(decisions)}] Processing: {decision['title'][:60]}...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

        pdf_content = download_pdf(session, decision['pdf_url'])
        if not pdf_content:
            continue

        text = extract_text_from_pdf(pdf_content)
        if not text or len(text) < 200:
            print(f"  Warning: Could not extract text", file=sys.stderr)
            continue

        record = normalize(decision, text)
        print(f"  Extracted {len(text)} chars", file=sys.stderr)
        yield record


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of decisions."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} decisions...", file=sys.stderr)

    decisions = fetch_decisions_list(session)

    for decision in decisions[:count * 2]:  # Try more to get enough
        if len(records) >= count:
            break

        print(f"Processing: {decision['title'][:60]}...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

        pdf_content = download_pdf(session, decision['pdf_url'])
        if not pdf_content:
            continue

        text = extract_text_from_pdf(pdf_content)
        if not text or len(text) < 200:
            print(f"  Could not extract text, skipping", file=sys.stderr)
            continue

        record = normalize(decision, text)
        records.append(record)

        # Save incrementally
        filepath = save_dir / f"record_{len(records)-1:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  Saved {len(text)} chars to {filepath.name}", file=sys.stderr)

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="CH/COMCO Swiss Competition Commission Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if not PDF_AVAILABLE:
        print("ERROR: pdfplumber or pypdf library required for PDF text extraction", file=sys.stderr)
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records...", file=sys.stderr)
            records = fetch_sample(session, args.count)

            if records:
                save_samples(records)

                # Print summary
                text_lengths = [len(r.get('text', '')) for r in records]
                types = {}
                langs = {}
                for r in records:
                    t = r.get('decision_type', 'unknown')
                    types[t] = types.get(t, 0) + 1
                    lang = r.get('language', 'unknown')
                    langs[lang] = langs.get(lang, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                print(f"\nBy type:", file=sys.stderr)
                for dtype, cnt in sorted(types.items(), key=lambda x: -x[1]):
                    print(f"  {dtype}: {cnt}", file=sys.stderr)
                print(f"\nBy language:", file=sys.stderr)
                for lang, cnt in sorted(langs.items(), key=lambda x: -x[1]):
                    print(f"  {lang}: {cnt}", file=sys.stderr)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...", file=sys.stderr)
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"Fetched: {record.get('title', 'unknown')[:50]}", file=sys.stderr)
            print(f"Total: {count} decisions", file=sys.stderr)


if __name__ == "__main__":
    main()
