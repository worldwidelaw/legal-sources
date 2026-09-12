#!/usr/bin/env python3
"""
SL/SierraLeoneWeb - Sierra Leone Web Laws Archive

Data source: https://www.sierra-leone.org/laws.html
Format: Static HTML index pages + PDF documents
License: Public Domain (government legislation)
Records: ~600+ Acts (1960-2021) and colonial Ordinances (1856-1960)

The Sierra Leone Web archive provides a comprehensive collection of
Sierra Leonean legislation as downloadable PDF documents.
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
from urllib.parse import urljoin, quote

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
SOURCE_ID = "SL/SierraLeoneWeb"
BASE_URL = "https://www.sierra-leone.org"
LAWS_PAGE = f"{BASE_URL}/laws.html"
ORDINANCES_PAGE = f"{BASE_URL}/ordinances.html"
REQUEST_DELAY = 1.0
PDF_DOWNLOAD_DELAY = 0.5


def get_session() -> requests.Session:
    """Create a requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


def discover_laws(session: requests.Session) -> List[Tuple[str, str, str]]:
    """Scrape laws.html for (title, pdf_url, doc_type) tuples."""
    resp = session.get(LAWS_PAGE, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    laws = []
    seen_urls = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        # Only collect links to Laws/ directory (PDFs and any HTML law pages)
        if not href.startswith("Laws/"):
            continue
        # Skip index pages and non-document links
        if href.endswith(".html") and "PublicNotices" in href:
            continue

        title = a_tag.get_text(strip=True)
        if not title:
            continue

        # Build full URL, handle spaces in filenames
        pdf_url = urljoin(BASE_URL + "/", href)

        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        # Classify document type
        if "NPRC" in href or "AFRC" in href:
            doc_type = "decree"
        elif "constitution" in href.lower():
            doc_type = "constitution"
        else:
            doc_type = "act"

        laws.append((title, pdf_url, doc_type))

    return laws


def discover_ordinances(session: requests.Session) -> List[Tuple[str, str, str]]:
    """Scrape ordinances.html for (title, pdf_url, doc_type) tuples."""
    resp = session.get(ORDINANCES_PAGE, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    ordinances = []
    seen_urls = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if not href.startswith("Laws/"):
            continue

        title = a_tag.get_text(strip=True)
        if not title:
            continue

        pdf_url = urljoin(BASE_URL + "/", href)
        if pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        ordinances.append((title, pdf_url, "ordinance"))

    return ordinances


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    if PDF_EXTRACTOR == "pdfplumber":
        return _extract_with_pdfplumber(pdf_bytes)
    elif PDF_EXTRACTOR == "PyPDF2":
        return _extract_with_pypdf2(pdf_bytes)
    return ""


def _extract_with_pdfplumber(pdf_bytes: bytes) -> str:
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


def download_document(session: requests.Session, url: str) -> Optional[bytes]:
    """Download a document, return bytes or None."""
    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        if len(resp.content) < 50:
            return None
        return resp.content
    except Exception as e:
        print(f"  Download error for {url}: {e}", file=sys.stderr)
        return None


def extract_year_from_url(url: str) -> Optional[str]:
    """Extract year from URL patterns like Laws/YYYY-##.pdf or Laws/Cap ##.pdf."""
    # Pattern: Laws/YYYY-...
    match = re.search(r"/Laws/(\d{4})-", url)
    if match:
        return match.group(1)
    return None


def extract_year_from_title(title: str) -> Optional[str]:
    """Extract year from title text."""
    # Look for 4-digit year
    years = re.findall(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
    if years:
        return years[-1]  # Use last year mentioned (usually the act year)
    return None


def make_doc_id(url: str) -> str:
    """Generate a stable ID from the document URL."""
    # Extract the filename part
    path = url.split("/Laws/")[-1] if "/Laws/" in url else url.split("/")[-1]
    # Remove extension
    path = re.sub(r"\.(pdf|html)$", "", path, flags=re.IGNORECASE)
    # Clean for ID use
    clean = re.sub(r"[^a-zA-Z0-9_-]", "-", path)
    clean = re.sub(r"-+", "-", clean).strip("-")[:80]
    return f"SL-LAW-{clean}"


def is_pdf(content: bytes) -> bool:
    """Check if content starts with PDF magic bytes."""
    return content[:5] == b"%PDF-"


def normalize(title: str, url: str, full_text: str, doc_type: str) -> Dict:
    """Transform document metadata + extracted text into standard schema."""
    doc_id = make_doc_id(url)
    year = extract_year_from_url(url) or extract_year_from_title(title)

    # Clean text
    clean_text = re.sub(r"[ \t]+", " ", full_text)
    clean_text = re.sub(r"\n\s*\n+", "\n\n", clean_text)
    clean_text = clean_text.strip()

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": clean_text,
        "date": f"{year}-01-01" if year else None,
        "url": url,
        "document_type": doc_type,
    }


def fetch_all() -> Generator[Dict, None, None]:
    """Fetch all legislation with full text."""
    session = get_session()

    print("Discovering laws from laws.html...")
    laws = discover_laws(session)
    print(f"  Found {len(laws)} laws")

    time.sleep(REQUEST_DELAY)
    print("Discovering ordinances from ordinances.html...")
    ordinances = discover_ordinances(session)
    print(f"  Found {len(ordinances)} ordinances")

    all_docs = laws + ordinances
    total = len(all_docs)
    total_yielded = 0
    total_skipped = 0

    for i, (title, url, doc_type) in enumerate(all_docs):
        time.sleep(PDF_DOWNLOAD_DELAY)
        doc_bytes = download_document(session, url)

        if not doc_bytes:
            total_skipped += 1
            continue

        # Check if it's a PDF
        if is_pdf(doc_bytes):
            full_text = extract_text_from_pdf(doc_bytes)
        elif url.endswith(".html"):
            soup = BeautifulSoup(doc_bytes, "html.parser")
            full_text = soup.get_text(separator="\n")
        else:
            # Try as PDF anyway (extensionless files)
            full_text = extract_text_from_pdf(doc_bytes)

        if len(full_text) < 100:
            total_skipped += 1
            continue

        record = normalize(title, url, full_text, doc_type)
        yield record
        total_yielded += 1

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{total} — yielded: {total_yielded}, skipped: {total_skipped}")

    print(f"\n  Total records yielded: {total_yielded}")
    print(f"  Total skipped: {total_skipped}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch all documents (static archive, no incremental support)."""
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    if not PDF_EXTRACTOR:
        print("ERROR: No PDF extraction library available")
        sys.exit(1)

    session = get_session()
    print(f"Using PDF extractor: {PDF_EXTRACTOR}")

    print("Discovering laws...")
    laws = discover_laws(session)
    print(f"Found {len(laws)} laws on laws.html")

    time.sleep(REQUEST_DELAY)
    print("Discovering ordinances...")
    ordinances = discover_ordinances(session)
    print(f"Found {len(ordinances)} ordinances on ordinances.html")

    # Sample from both: take from laws first, then ordinances
    all_docs = laws + ordinances

    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    records_attempted = 0
    total_text_chars = 0

    # Sample spread: first 10 from laws, last 5 from ordinances
    sample_indices = list(range(min(12, len(laws))))
    if len(ordinances) > 0:
        # Add some ordinances
        ord_start = 0
        for j in range(min(5, len(ordinances))):
            sample_indices.append(len(laws) + j)

    for idx in sample_indices:
        if records_saved >= count:
            break

        title, url, doc_type = all_docs[idx]
        records_attempted += 1
        filename_part = url.split("/")[-1]
        print(f"\n  [{records_attempted}] {title}")
        print(f"    Downloading: {filename_part}")

        time.sleep(PDF_DOWNLOAD_DELAY)
        doc_bytes = download_document(session, url)
        if not doc_bytes:
            print(f"    SKIP: download failed")
            continue

        if is_pdf(doc_bytes):
            full_text = extract_text_from_pdf(doc_bytes)
        elif url.endswith(".html"):
            soup = BeautifulSoup(doc_bytes, "html.parser")
            full_text = soup.get_text(separator="\n")
        else:
            full_text = extract_text_from_pdf(doc_bytes)

        text_len = len(full_text)
        if text_len < 100:
            print(f"    SKIP: text too short ({text_len} chars)")
            continue

        record = normalize(title, url, full_text, doc_type)
        total_text_chars += text_len
        records_saved += 1

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    [{records_saved}/{count}] Saved: {filename}")
        print(f"    Text: {text_len:,} chars | Date: {record.get('date', '?')} | Type: {doc_type}")

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
    parser = argparse.ArgumentParser(description="Sierra Leone Web Laws Archive Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
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
                if records_saved % 50 == 0:
                    print(f"  Saved {records_saved} records...")

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
