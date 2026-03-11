#!/usr/bin/env python3
"""
Montenegro Official Gazette (Službeni list Crne Gore) fetcher.

Fetches legislation from the official gazette at sluzbenilist.me.
Documents are downloaded as PDFs with full text extraction using pdfplumber.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import gc

import requests
from bs4 import BeautifulSoup

# PDF extraction - use pypdf (lighter memory footprint than pdfplumber)
try:
    from pypdf import PdfReader
    HAS_PDF_SUPPORT = True
except ImportError:
    try:
        # Fallback to older PyPDF2
        from PyPDF2 import PdfReader
        HAS_PDF_SUPPORT = True
    except ImportError:
        HAS_PDF_SUPPORT = False
        print("Warning: pypdf not installed. PDF text extraction disabled.", file=sys.stderr)
        print("Install with: pip install pypdf", file=sys.stderr)

# Configuration
BASE_URL = "https://www.sluzbenilist.me"
REGISTRY_URL = f"{BASE_URL}/registri"
PROPISI_URL = f"{BASE_URL}/propisi"
REQUEST_DELAY = 2.0  # seconds between requests


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WorldWideLaw/1.0 (legal research project)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sr,hr,bs,en;q=0.5",
    })
    return session


def list_gazette_issues(session: requests.Session, gazette_type: int = 0,
                        year: int = 2025, max_pages: int = 3) -> list[dict]:
    """
    List gazette issues from the registry.

    Args:
        session: requests session
        gazette_type: 0 = Službeni list CG, 1 = Municipal, etc.
        year: year to fetch
        max_pages: maximum pages to fetch

    Returns:
        List of issue dicts with uuid, date, number
    """
    issues = []

    for page in range(1, max_pages + 1):
        url = f"{REGISTRY_URL}?type={gazette_type}&year={year}&page={page}"

        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch registry page {page}: {e}", file=sys.stderr)
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # Find issue links - support both relative and absolute URLs
        for link in soup.select('a[href*="/registri/"]'):
            href = link.get("href", "")
            # Extract UUID from href (works for both /registri/UUID and https://...sluzbenilist.me/registri/UUID)
            if "/registri/" in href:
                parts = href.split("/registri/")
                if len(parts) > 1:
                    uuid = parts[1].split("/")[0].split("?")[0]  # Get UUID, strip any trailing path or params
                    if not uuid or len(uuid) < 10:
                        continue

                # Extract text for date/number info
                text = link.get_text(strip=True)

                # Parse date (format: dd.mm.yyyy)
                date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text)
                date = None
                if date_match:
                    date = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"

                # Parse gazette number (format: number/year)
                number_match = re.search(r'(\d+)/(\d{4})', text)
                number = number_match.group(0) if number_match else None

                issues.append({
                    "uuid": uuid,
                    "date": date,
                    "number": number,
                    "year": year,
                })

        # Check if there are more pages
        if f"page={page + 1}" not in response.text:
            break

        time.sleep(REQUEST_DELAY)

    return issues


def list_documents_in_issue(session: requests.Session, issue_uuid: str) -> list[dict]:
    """
    List documents (propisi) in a gazette issue.

    Args:
        session: requests session
        issue_uuid: UUID of the gazette issue

    Returns:
        List of document dicts with uuid, title
    """
    url = f"{REGISTRY_URL}/{issue_uuid}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch issue {issue_uuid}: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    documents = []

    # Find document links - support both relative and absolute URLs
    for link in soup.select('a[href*="/propisi/"]'):
        href = link.get("href", "")
        if "/propisi/" in href and "/download" not in href:
            # Extract UUID from href
            parts = href.split("/propisi/")
            if len(parts) > 1:
                uuid = parts[1].split("/")[0].split("?")[0]
                if not uuid or len(uuid) < 10:
                    continue

            # Get title from link text or parent
            title = link.get_text(strip=True)
            if not title:
                parent = link.find_parent("div")
                if parent:
                    title = parent.get_text(strip=True)

            documents.append({
                "uuid": uuid,
                "title": title[:200] if title else None,
            })

    return documents


def download_pdf(session: requests.Session, doc_uuid: str) -> Optional[bytes]:
    """
    Download a document as PDF.

    Args:
        session: requests session
        doc_uuid: document UUID

    Returns:
        PDF bytes or None on error
    """
    url = f"{PROPISI_URL}/{doc_uuid}/download"

    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()

        # Verify it's a PDF
        if response.headers.get("Content-Type", "").startswith("application/pdf") or \
           response.content[:4] == b'%PDF':
            return response.content

    except requests.RequestException as e:
        print(f"Failed to download PDF {doc_uuid}: {e}", file=sys.stderr)

    return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Extract text from PDF bytes using pypdf with memory-efficient streaming.

    Uses pypdf (lighter than pdfplumber) and processes pages one at a time,
    discarding each page after extraction to minimize memory usage.

    Args:
        pdf_bytes: PDF content

    Returns:
        Extracted text
    """
    if not HAS_PDF_SUPPORT:
        return ""

    import io
    pdf_file = None
    reader = None

    try:
        pdf_file = io.BytesIO(pdf_bytes)
        reader = PdfReader(pdf_file)

        text_parts = []
        num_pages = len(reader.pages)

        # Process pages one at a time to minimize memory
        for i in range(num_pages):
            try:
                page = reader.pages[i]
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                # Explicitly dereference page to help GC
                del page
            except Exception as page_err:
                print(f"Failed to extract page {i}: {page_err}", file=sys.stderr)
                continue

        text = "\n\n".join(text_parts)

        # Clean up text
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.strip()

        return text

    except Exception as e:
        print(f"PDF extraction error: {e}", file=sys.stderr)
        return ""
    finally:
        # Explicit cleanup to free memory
        if reader is not None:
            del reader
        if pdf_file is not None:
            pdf_file.close()
            del pdf_file
        # Force garbage collection after each PDF
        gc.collect()


def extract_metadata(text: str, doc_uuid: str, issue_info: dict) -> dict:
    """
    Extract metadata from document text.

    Args:
        text: extracted document text
        doc_uuid: document UUID
        issue_info: gazette issue metadata

    Returns:
        Metadata dict
    """
    # Extract title (usually first centered/bold line)
    title = None
    lines = text.split('\n')
    for line in lines[:20]:
        line = line.strip()
        # Look for document type headers
        if re.match(r'^(ZAKON|ODLUKA|UREDBA|PRAVILNIK|NAREDBA|UKAZ|RJEŠENJE|UPUTSTVO)', line, re.IGNORECASE):
            title = line[:200]
            break

    # Extract issuer
    issuer = None
    issuer_patterns = [
        r'(VLADA CRNE GORE)',
        r'(SKUPŠTINA CRNE GORE)',
        r'(PREDSJEDNIK CRNE GORE)',
        r'(MINISTARSTVO[^\n,]+)',
        r'(AGENCIJA[^\n,]+)',
        r'(UPRAVA[^\n,]+)',
    ]
    for pattern in issuer_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            issuer = match.group(1).strip()[:100]
            break

    # Extract date from text if not in issue info
    date = issue_info.get("date")
    if not date:
        date_match = re.search(r'(\d{1,2})\.\s*(\w+)\s*(\d{4})\.\s*godine', text)
        if date_match:
            day = date_match.group(1).zfill(2)
            month_text = date_match.group(2).lower()
            year = date_match.group(3)

            month_map = {
                'januar': '01', 'januara': '01',
                'februar': '02', 'februara': '02',
                'mart': '03', 'marta': '03',
                'april': '04', 'aprila': '04',
                'maj': '05', 'maja': '05',
                'jun': '06', 'juna': '06', 'juni': '06',
                'jul': '07', 'jula': '07', 'juli': '07',
                'avgust': '08', 'avgusta': '08', 'august': '08',
                'septembar': '09', 'septembra': '09',
                'oktobar': '10', 'oktobra': '10',
                'novembar': '11', 'novembra': '11',
                'decembar': '12', 'decembra': '12',
            }
            month = month_map.get(month_text, '01')
            date = f"{year}-{month}-{day}"

    return {
        "title": title,
        "issuer": issuer,
        "date": date,
    }


def normalize(raw: dict) -> dict:
    """
    Normalize raw document data to standard schema.

    Args:
        raw: raw document dict

    Returns:
        Normalized document dict
    """
    doc_uuid = raw.get("uuid", "")
    gazette_number = raw.get("gazette_number", "")
    gazette_year = raw.get("gazette_year")

    # Generate unique ID
    short_uuid = doc_uuid[:8] if doc_uuid else "unknown"
    _id = f"ME-SL-{gazette_number.replace('/', '-')}-{short_uuid}" if gazette_number else f"ME-SL-{short_uuid}"

    return {
        "_id": _id,
        "_source": "ME/SluzbenList",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title") or f"Document {gazette_number}",
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": f"{PROPISI_URL}/{doc_uuid}",
        "gazette_number": gazette_number,
        "gazette_year": gazette_year,
        "issuer": raw.get("issuer"),
        "language": "sr-Latn-ME",  # Montenegrin (Latin script)
    }


def fetch_document(session: requests.Session, doc_uuid: str, issue_info: dict) -> Optional[dict]:
    """
    Fetch a single document by UUID.

    Memory-optimized: explicitly frees PDF bytes after extraction.

    Args:
        session: requests session
        doc_uuid: document UUID
        issue_info: gazette issue metadata

    Returns:
        Normalized document dict or None on error
    """
    print(f"  Downloading PDF...", file=sys.stderr)
    pdf_bytes = download_pdf(session, doc_uuid)

    if not pdf_bytes:
        print(f"  Failed to download PDF", file=sys.stderr)
        return None

    pdf_size = len(pdf_bytes)
    print(f"  Extracting text ({pdf_size:,} bytes)...", file=sys.stderr)
    text = extract_text_from_pdf(pdf_bytes)

    # Free PDF bytes immediately after extraction
    del pdf_bytes
    gc.collect()

    if not text or len(text) < 100:
        print(f"  Text extraction failed or too short", file=sys.stderr)
        return None

    # Extract metadata from text
    metadata = extract_metadata(text, doc_uuid, issue_info)

    raw = {
        "uuid": doc_uuid,
        "title": metadata.get("title") or issue_info.get("title"),
        "text": text,
        "date": metadata.get("date"),
        "issuer": metadata.get("issuer"),
        "gazette_number": issue_info.get("number"),
        "gazette_year": issue_info.get("year"),
    }

    return normalize(raw)


def fetch_all(session: requests.Session, max_docs: int = 100) -> Iterator[dict]:
    """
    Fetch all documents.

    Args:
        session: requests session
        max_docs: maximum documents to fetch

    Yields:
        Normalized document dicts
    """
    count = 0
    current_year = datetime.now().year

    for year in range(current_year, current_year - 3, -1):
        print(f"Fetching gazette issues for {year}...", file=sys.stderr)
        issues = list_gazette_issues(session, gazette_type=0, year=year, max_pages=5)
        print(f"Found {len(issues)} issues", file=sys.stderr)

        for issue in issues:
            if count >= max_docs:
                return

            print(f"Processing issue {issue.get('number')}...", file=sys.stderr)
            docs = list_documents_in_issue(session, issue["uuid"])
            print(f"  Found {len(docs)} documents", file=sys.stderr)

            for doc in docs[:3]:  # Limit docs per issue
                if count >= max_docs:
                    return

                result = fetch_document(session, doc["uuid"], {
                    "number": issue.get("number"),
                    "year": year,
                    "date": issue.get("date"),
                    "title": doc.get("title"),
                })

                if result and result.get("text"):
                    yield result
                    count += 1

                time.sleep(REQUEST_DELAY)

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
    # Fetch recent documents
    yield from fetch_all(session, max_docs=50)


def bootstrap_sample(output_dir: Path, sample_size: int = 12):
    """
    Fetch sample documents and save to output directory.

    Args:
        output_dir: directory to save samples
        sample_size: number of samples to fetch
    """
    if not HAS_PDF_SUPPORT:
        print("Error: pypdf is required for PDF text extraction", file=sys.stderr)
        print("Install with: pip install pypdf", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    session = get_session()

    print(f"Fetching {sample_size} sample documents...", file=sys.stderr)

    # Get recent gazette issues
    current_year = datetime.now().year
    all_issues = []

    for year in [current_year, current_year - 1]:
        print(f"Listing issues for {year}...", file=sys.stderr)
        issues = list_gazette_issues(session, gazette_type=0, year=year, max_pages=2)
        all_issues.extend([(issue, year) for issue in issues])
        time.sleep(REQUEST_DELAY)

    print(f"Found {len(all_issues)} gazette issues", file=sys.stderr)

    count = 0
    total_chars = 0

    for issue, year in all_issues:
        if count >= sample_size:
            break

        issue_number = issue.get("number", "unknown")
        print(f"\nProcessing issue {issue_number}...", file=sys.stderr)

        # Get documents in this issue
        docs = list_documents_in_issue(session, issue["uuid"])
        print(f"  Found {len(docs)} documents", file=sys.stderr)
        time.sleep(REQUEST_DELAY)

        for doc in docs[:2]:  # Max 2 per issue
            if count >= sample_size:
                break

            doc_title = doc.get("title", "")[:50] or doc["uuid"][:20]
            print(f"\n  Fetching: {doc_title}...", file=sys.stderr)

            result = fetch_document(session, doc["uuid"], {
                "number": issue.get("number"),
                "year": year,
                "date": issue.get("date"),
                "title": doc.get("title"),
            })

            if result and result.get("text"):
                text_len = len(result["text"])

                if text_len < 200:
                    print(f"    Skipping (text too short: {text_len} chars)", file=sys.stderr)
                    continue

                # Save to file
                filename = f"{result['_id']}.json"
                filepath = output_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)

                print(f"    Saved: {filename} ({text_len:,} chars)", file=sys.stderr)
                total_chars += text_len
                count += 1

            time.sleep(REQUEST_DELAY)

    print(f"\n{'='*50}", file=sys.stderr)
    print(f"Sample complete:", file=sys.stderr)
    print(f"  Documents: {count}", file=sys.stderr)
    print(f"  Total chars: {total_chars:,}", file=sys.stderr)
    print(f"  Avg chars/doc: {total_chars // count if count else 0:,}", file=sys.stderr)
    print(f"  Output: {output_dir}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Montenegro Official Gazette fetcher"
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
        help="List gazette issues"
    )
    list_parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Year to list"
    )

    args = parser.parse_args()

    if args.command == "bootstrap" and args.sample:
        bootstrap_sample(args.output, args.count)
    elif args.command == "list":
        session = get_session()
        issues = list_gazette_issues(session, year=args.year, max_pages=3)
        for issue in issues[:20]:
            print(f"{issue.get('number', 'N/A'):10} | {issue.get('date', 'N/A'):12} | {issue['uuid'][:20]}...")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
