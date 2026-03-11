#!/usr/bin/env python3
"""
Brčko District (Bosnia and Herzegovina) Legislation Fetcher.

Fetches legislation from the Brčko District Assembly website.
Source: https://skupstinabd.ba/3-zakon/ba/

Brčko District is an autonomous administrative unit with its own legal system,
separate from the Federation of BiH and Republika Srpska.

Access method: Directory listing of PDF files on the assembly website.
Each law has its own directory containing PDF files of the original law
and any amendments, plus consolidated texts (prečišćeni tekst).
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import unquote, urljoin

import requests

try:
    import PyPDF2
except ImportError:
    print("ERROR: PyPDF2 is required. Install with: pip install PyPDF2", file=sys.stderr)
    sys.exit(1)

# Configuration
BASE_URL = "https://skupstinabd.ba"
LAWS_DIR_URL = f"{BASE_URL}/3-zakon/ba/"
REQUEST_DELAY = 1.0  # seconds between requests


class DirectoryListingParser(HTMLParser):
    """Parse Apache/nginx directory listing HTML."""

    def __init__(self):
        super().__init__()
        self.links = []
        self.in_link = False

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href' and value and not value.startswith('?') and not value.startswith('/'):
                    self.links.append(unquote(value))


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WorldWideLaw/1.0 (legal research project)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "bs,hr,sr,en;q=0.5",
    })
    return session


def list_law_directories(session: requests.Session) -> list[str]:
    """
    Get list of law directories from the index.

    Returns:
        List of law directory names
    """
    try:
        response = session.get(LAWS_DIR_URL, timeout=90)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch law directory: {e}", file=sys.stderr)
        return []

    parser = DirectoryListingParser()
    parser.feed(response.text)

    # Filter to only directories (ending with /)
    directories = [link.rstrip('/') for link in parser.links
                   if link.endswith('/') and not link.startswith('..')]

    return directories


def list_pdf_files(session: requests.Session, law_dir: str) -> list[dict]:
    """
    Get list of PDF files for a law directory.

    Args:
        session: requests session
        law_dir: law directory name

    Returns:
        List of dicts with pdf info
    """
    url = f"{LAWS_DIR_URL}{law_dir}/"

    try:
        response = session.get(url, timeout=90)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {law_dir}: {e}", file=sys.stderr)
        return []

    parser = DirectoryListingParser()
    parser.feed(response.text)

    pdfs = []
    for link in parser.links:
        if link.lower().endswith('.pdf'):
            pdfs.append({
                "filename": link,
                "url": f"{LAWS_DIR_URL}{law_dir}/{link}",
                "is_consolidated": 'prec--is--c' in link.lower() or 'precis' in link.lower(),
            })

    return pdfs


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """
    Extract text content from a PDF.

    Args:
        pdf_content: PDF file bytes

    Returns:
        Extracted text
    """
    try:
        reader = PyPDF2.PdfReader(BytesIO(pdf_content))
        text_parts = []

        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)

        text = "\n\n".join(text_parts)

        # Clean up text
        text = re.sub(r'\n{3,}', '\n\n', text)  # Reduce multiple newlines
        text = re.sub(r'[ \t]+', ' ', text)  # Normalize spaces
        text = re.sub(r' \n', '\n', text)  # Remove trailing spaces

        return text.strip()

    except Exception as e:
        print(f"PDF extraction error: {e}", file=sys.stderr)
        return ""


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    """
    Download a PDF file.

    Args:
        session: requests session
        url: PDF URL

    Returns:
        PDF content bytes or None
    """
    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()

        # Verify it's a PDF
        if not response.content.startswith(b'%PDF'):
            print(f"Not a valid PDF: {url}", file=sys.stderr)
            return None

        return response.content

    except requests.RequestException as e:
        print(f"Failed to download PDF: {e}", file=sys.stderr)
        return None


def parse_law_title(directory_name: str) -> str:
    """
    Parse law title from directory name.

    Args:
        directory_name: Directory name with encoding

    Returns:
        Human-readable law title
    """
    title = directory_name

    # Decode special character escapes (--č--, --š--, etc.)
    replacements = {
        'c--': 'č',
        'c-': 'ć',
        's--': 'š',
        'z--': 'ž',
        'dz--': 'dž',
        'C--': 'Č',
        'C-': 'Ć',
        'S--': 'Š',
        'Z--': 'Ž',
    }

    for encoded, char in replacements.items():
        title = title.replace(encoded, char)

    return title


def parse_gazette_info(filename: str) -> dict:
    """
    Parse gazette information from filename.

    Filename format: NNBXX-YY ... .pdf
    - NN: Gazette issue number
    - B: Section (always B for laws)
    - XX: Document number within issue
    - YY: Year (2-digit)

    Args:
        filename: PDF filename

    Returns:
        Dict with gazette info
    """
    info = {
        "gazette_issue": None,
        "gazette_year": None,
        "doc_number": None,
    }

    # Match pattern like "007B34-19" or "03B12-25"
    match = re.match(r'^(\d+)B(\d+)-(\d+)\s+', filename)
    if match:
        info["gazette_issue"] = int(match.group(1))
        info["doc_number"] = int(match.group(2))
        year_2digit = int(match.group(3))
        info["gazette_year"] = 2000 + year_2digit if year_2digit < 50 else 1900 + year_2digit

    return info


def normalize(raw: dict) -> dict:
    """
    Normalize raw document data to standard schema.

    Args:
        raw: raw document dict

    Returns:
        Normalized document dict
    """
    law_dir = raw.get("law_dir", "")
    filename = raw.get("filename", "")
    gazette_info = raw.get("gazette_info", {})

    # Generate unique ID
    gazette_year = gazette_info.get("gazette_year", "")
    gazette_issue = gazette_info.get("gazette_issue", "")

    safe_dir = re.sub(r'[^a-zA-Z0-9]', '_', law_dir)[:50]
    _id = f"BA-BD-{gazette_year}-{gazette_issue:02d}-{safe_dir}" if gazette_year and gazette_issue else f"BA-BD-{safe_dir}"

    return {
        "_id": _id,
        "_source": "BA/Brcko",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": f"{gazette_info.get('gazette_year', '')}" if gazette_info.get('gazette_year') else None,
        "url": raw.get("url", ""),
        "gazette_issue": gazette_issue,
        "gazette_year": gazette_year,
        "is_consolidated": raw.get("is_consolidated", False),
        "filename": filename,
        "language": "bs",  # Bosnian (also hr, sr)
    }


def fetch_law(session: requests.Session, law_dir: str) -> Optional[dict]:
    """
    Fetch a single law with full text.

    Prefers consolidated text (prečišćeni tekst) if available,
    otherwise uses the most recent PDF.

    Args:
        session: requests session
        law_dir: law directory name

    Returns:
        Document dict or None
    """
    pdfs = list_pdf_files(session, law_dir)

    if not pdfs:
        return None

    # Prefer consolidated text
    consolidated = [p for p in pdfs if p["is_consolidated"]]
    if consolidated:
        # Sort by filename to get the most recent
        consolidated.sort(key=lambda x: x["filename"], reverse=True)
        pdf_info = consolidated[0]
    else:
        # Otherwise take the most recent PDF
        pdfs.sort(key=lambda x: x["filename"], reverse=True)
        pdf_info = pdfs[0]

    # Download and extract text
    pdf_content = download_pdf(session, pdf_info["url"])
    if not pdf_content:
        return None

    text = extract_text_from_pdf(pdf_content)
    if not text or len(text) < 100:
        print(f"  Text too short for {law_dir} ({len(text)} chars)", file=sys.stderr)
        return None

    # Parse info
    title = parse_law_title(law_dir)
    gazette_info = parse_gazette_info(pdf_info["filename"])

    return {
        "law_dir": law_dir,
        "title": title,
        "text": text,
        "url": pdf_info["url"],
        "filename": pdf_info["filename"],
        "is_consolidated": pdf_info["is_consolidated"],
        "gazette_info": gazette_info,
    }


def fetch_all(session: requests.Session, max_docs: int = 1000) -> Iterator[dict]:
    """
    Fetch all laws.

    Args:
        session: requests session
        max_docs: maximum documents to fetch

    Yields:
        Normalized document dicts
    """
    print("Listing law directories...", file=sys.stderr)
    law_dirs = list_law_directories(session)
    print(f"Found {len(law_dirs)} law directories", file=sys.stderr)

    count = 0
    for i, law_dir in enumerate(law_dirs):
        if count >= max_docs:
            break

        print(f"Fetching {i+1}/{len(law_dirs)}: {law_dir[:50]}...", file=sys.stderr)

        raw = fetch_law(session, law_dir)
        if raw:
            yield normalize(raw)
            count += 1

        time.sleep(REQUEST_DELAY)


def fetch_updates(session: requests.Session, since: str) -> Iterator[dict]:
    """
    Fetch documents modified since a date.

    Args:
        session: requests session
        since: ISO date string (YYYY-MM-DD)

    Yields:
        Normalized document dicts
    """
    # Directory listing includes modification dates, but for simplicity
    # we fetch all and could filter by date if needed
    yield from fetch_all(session, max_docs=50)


def bootstrap_sample(output_dir: Path, sample_size: int = 12):
    """
    Fetch sample documents and save to output directory.

    Args:
        output_dir: directory to save samples
        sample_size: number of samples to fetch
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    session = get_session()

    print(f"Fetching {sample_size} sample documents...", file=sys.stderr)
    print(f"Source: {LAWS_DIR_URL}", file=sys.stderr)

    # Get law directories
    law_dirs = list_law_directories(session)
    print(f"Found {len(law_dirs)} law directories", file=sys.stderr)

    if not law_dirs:
        print("ERROR: No law directories found", file=sys.stderr)
        return

    # Fetch samples
    count = 0
    total_chars = 0

    for i, law_dir in enumerate(law_dirs):
        if count >= sample_size:
            break

        print(f"\nProcessing {i+1}: {law_dir[:60]}...", file=sys.stderr)

        raw = fetch_law(session, law_dir)
        if not raw:
            print("  Skipping (no valid PDF)", file=sys.stderr)
            continue

        normalized = normalize(raw)

        # Validate
        text_len = len(normalized.get("text", ""))
        if text_len < 100:
            print(f"  Skipping (text too short: {text_len} chars)", file=sys.stderr)
            continue

        # Save to file
        filename = f"{normalized['_id']}.json"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        print(f"  Saved: {filename}", file=sys.stderr)
        print(f"  Title: {normalized['title'][:60]}", file=sys.stderr)
        print(f"  Text: {text_len:,} chars", file=sys.stderr)
        print(f"  Consolidated: {normalized.get('is_consolidated', False)}", file=sys.stderr)

        total_chars += text_len
        count += 1

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Sample complete:", file=sys.stderr)
    print(f"  Documents: {count}", file=sys.stderr)
    print(f"  Total chars: {total_chars:,}", file=sys.stderr)
    print(f"  Avg chars/doc: {total_chars // count if count else 0:,}", file=sys.stderr)
    print(f"  Output: {output_dir}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Brčko District (BiH) Legislation Fetcher"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help="Fetch sample documents"
    )
    bootstrap_parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample data only"
    )
    bootstrap_parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "sample",
        help="Output directory for samples"
    )
    bootstrap_parser.add_argument(
        "--count",
        type=int,
        default=12,
        help="Number of samples to fetch"
    )

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List available laws"
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum laws to list"
    )

    args = parser.parse_args()

    if args.command == "bootstrap" and args.sample:
        bootstrap_sample(args.output, args.count)
    elif args.command == "list":
        session = get_session()
        laws = list_law_directories(session)
        for law in laws[:args.limit]:
            title = parse_law_title(law)
            print(f"  {title}")
        print(f"\nTotal: {len(laws)} laws")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
