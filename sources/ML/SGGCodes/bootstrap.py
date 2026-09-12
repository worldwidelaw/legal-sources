#!/usr/bin/env python3
"""
ML/SGGCodes - Mali Consolidated Legal Codes (Secrétariat Général du Gouvernement)

Data source: https://sgg-mali.ml/fr/journal-officiel/les-codes-consolides.html
Format: HTML page with direct PDF download links
License: Public Domain (official legislation)
Records: 21 consolidated legal codes (Penal, Civil, Labor, Mining, etc.)

The SGG publishes Mali's consolidated legal codes as downloadable PDFs.
Full text is extracted from PDFs using pdfplumber.
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    PDF_EXTRACTOR = "pdfplumber"
except ImportError:
    try:
        import PyPDF2
        PDF_EXTRACTOR = "PyPDF2"
    except ImportError:
        PDF_EXTRACTOR = None

# Configuration
SOURCE_ID = "ML/SGGCodes"
BASE_URL = "https://sgg-mali.ml"
CODES_PAGE = f"{BASE_URL}/fr/journal-officiel/les-codes-consolides.html"
REQUEST_DELAY = 1.5


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7",
    })
    return session


def discover_codes(session: requests.Session) -> List[Tuple[str, str]]:
    """Scrape the codes page and return list of (title, pdf_url) tuples."""
    resp = session.get(CODES_PAGE, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    codes = []
    # Find all links to PDF files in the codes directory
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/codes/" in href and href.lower().endswith(".pdf"):
            pdf_url = urljoin(BASE_URL, href)
            title = a_tag.get_text(strip=True)
            # Remove "subject" prefix from hidden span elements
            if title.startswith("subject"):
                title = title[len("subject"):]
            title = title.strip()
            if not title:
                # Try parent element text
                parent = a_tag.parent
                if parent:
                    title = parent.get_text(strip=True)
            if not title:
                # Extract from filename
                title = href.split("/")[-1].replace(".pdf", "").replace("-", " ").title()
            codes.append((title, pdf_url))

    # Deduplicate by URL
    seen = set()
    unique_codes = []
    for title, url in codes:
        if url not in seen:
            seen.add(url)
            unique_codes.append((title, url))

    return unique_codes


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    if PDF_EXTRACTOR == "pdfplumber":
        return _extract_with_pdfplumber(pdf_bytes)
    elif PDF_EXTRACTOR == "PyPDF2":
        return _extract_with_pypdf2(pdf_bytes)
    return ""


def _extract_with_pdfplumber(pdf_bytes: bytes) -> str:
    """Extract text using pdfplumber."""
    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        print(f"  pdfplumber error: {e}", file=sys.stderr)
    return "\n\n".join(text_parts)


def _extract_with_pypdf2(pdf_bytes: bytes) -> str:
    """Extract text using PyPDF2."""
    text_parts = []
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    except Exception as e:
        print(f"  PyPDF2 error: {e}", file=sys.stderr)
    return "\n\n".join(text_parts)


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    """Download a PDF, return bytes or None."""
    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        if len(resp.content) < 100:
            return None
        return resp.content
    except Exception as e:
        print(f"  PDF download error for {url}: {e}", file=sys.stderr)
        return None


def extract_year_from_title(title: str) -> Optional[str]:
    """Extract the primary year from a code title."""
    # Match patterns like "2012", "1999", etc.
    years = re.findall(r"\b(19\d{2}|20[0-2]\d)\b", title)
    if years:
        return years[0]
    return None


def make_code_id(title: str, pdf_url: str) -> str:
    """Generate a stable ID from the PDF filename."""
    filename = pdf_url.split("/")[-1].replace(".pdf", "")
    # Clean up for use as ID
    clean = re.sub(r"[^a-zA-Z0-9_-]", "-", filename)
    clean = re.sub(r"-+", "-", clean).strip("-")
    return f"ML-SGG-{clean}"


def normalize(title: str, pdf_url: str, full_text: str) -> Dict:
    """Transform code metadata + extracted text into standard schema."""
    code_id = make_code_id(title, pdf_url)
    year = extract_year_from_title(title)

    # Clean text
    clean_text = re.sub(r"[ \t]+", " ", full_text)
    clean_text = re.sub(r"\n\s*\n+", "\n\n", clean_text)
    clean_text = clean_text.strip()

    return {
        "_id": code_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": clean_text,
        "date": f"{year}-01-01" if year else None,
        "url": pdf_url,
        "pdf_url": pdf_url,
    }


def fetch_all() -> Generator[Dict, None, None]:
    """Fetch all consolidated codes with full text."""
    session = get_session()

    print("Discovering codes from SGG page...")
    codes = discover_codes(session)
    print(f"  Found {len(codes)} codes")

    total_yielded = 0
    total_skipped = 0

    for i, (title, pdf_url) in enumerate(codes):
        print(f"\n  [{i+1}/{len(codes)}] {title}")
        print(f"    URL: {pdf_url}")

        time.sleep(REQUEST_DELAY)
        pdf_bytes = download_pdf(session, pdf_url)
        if not pdf_bytes:
            print(f"    SKIP: download failed")
            total_skipped += 1
            continue

        print(f"    PDF size: {len(pdf_bytes):,} bytes")
        full_text = extract_text_from_pdf(pdf_bytes)

        if len(full_text) < 100:
            print(f"    SKIP: text too short ({len(full_text)} chars)")
            total_skipped += 1
            continue

        record = normalize(title, pdf_url, full_text)
        yield record
        total_yielded += 1
        print(f"    OK: {len(full_text):,} chars")

    print(f"\n  Total records yielded: {total_yielded}")
    print(f"  Total skipped: {total_skipped}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch all codes (static collection, no incremental support)."""
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 21):
    """Fetch sample records for validation."""
    if not PDF_EXTRACTOR:
        print("ERROR: No PDF extraction library available (need pdfplumber or PyPDF2)")
        sys.exit(1)

    session = get_session()
    print(f"Using PDF extractor: {PDF_EXTRACTOR}")
    print("Discovering codes from SGG page...")

    codes = discover_codes(session)
    print(f"Found {len(codes)} codes")

    if not codes:
        print("ERROR: No codes found!")
        return

    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    records_attempted = 0
    total_text_chars = 0

    for title, pdf_url in codes:
        if records_saved >= count:
            break

        records_attempted += 1
        print(f"\n  [{records_attempted}] {title}")
        print(f"    Downloading: {pdf_url.split('/')[-1]}")

        time.sleep(REQUEST_DELAY)
        pdf_bytes = download_pdf(session, pdf_url)
        if not pdf_bytes:
            print(f"    SKIP: download failed")
            continue

        print(f"    PDF size: {len(pdf_bytes):,} bytes")
        full_text = extract_text_from_pdf(pdf_bytes)
        text_len = len(full_text)

        if text_len < 100:
            print(f"    SKIP: text too short ({text_len} chars, may be scanned)")
            continue

        record = normalize(title, pdf_url, full_text)
        total_text_chars += text_len
        records_saved += 1

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    [{records_saved}/{count}] Saved: {filename}")
        print(f"    Text: {text_len:,} chars | Date: {record.get('date', '?')}")

    # Summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records attempted: {records_attempted}")
    print(f"Records saved: {records_saved}")
    if records_saved > 0:
        avg_chars = total_text_chars // records_saved
        print(f"Total text chars: {total_text_chars:,}")
        print(f"Average text length: {avg_chars:,} chars/doc")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="Mali Consolidated Legal Codes Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=21,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Run full bootstrap (all records)")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            sample_dir.mkdir(parents=True, exist_ok=True)
            records_saved = 0
            for record in fetch_all():
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
                filename = f"{safe_name}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                records_saved += 1

            print(f"\nFull bootstrap complete: {records_saved} records saved")

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
