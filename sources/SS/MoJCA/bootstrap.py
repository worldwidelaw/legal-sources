#!/usr/bin/env python3
"""
SS/MoJCA - South Sudan Ministry of Justice Laws

Data source: https://mojca.gov.ss/laws-of-the-republic-of-south-sudan/
Format: WordPress page with PDF downloads
License: Public Domain (government legislation)
Records: ~77 Acts (2005-2017)
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

SOURCE_ID = "SS/MoJCA"
LAWS_URL = "https://mojca.gov.ss/laws-of-the-republic-of-south-sudan/"
REQUEST_DELAY = 1.5
PDF_DOWNLOAD_DELAY = 1.0


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


def discover_laws(session: requests.Session) -> List[Tuple[str, str]]:
    """Scrape the laws page for (title, pdf_url) tuples."""
    resp = session.get(LAWS_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    laws = []
    seen_urls = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if not href.lower().endswith(".pdf"):
            continue

        title = a_tag.get_text(strip=True)
        if not title:
            continue

        # Normalize URL
        pdf_url = href.strip()

        # Skip duplicates (some PDFs appear under different titles)
        url_key = pdf_url.split("/")[-1].lower()
        if url_key in seen_urls:
            continue
        seen_urls.add(url_key)

        laws.append((title, pdf_url))

    return laws


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
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


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        if len(resp.content) < 100:
            return None
        if resp.content[:5] != b"%PDF-":
            print(f"  Not a PDF: {url}", file=sys.stderr)
            return None
        return resp.content
    except Exception as e:
        print(f"  Download error for {url}: {e}", file=sys.stderr)
        return None


def extract_act_number(title: str) -> Optional[str]:
    """Extract act number from title like 'Advocates Act (55 of 2013)'."""
    match = re.search(r"\((\d+)\s+of\s+\d{4}\)", title)
    if match:
        return match.group(0).strip("()")
    return None


def extract_year(title: str) -> Optional[str]:
    """Extract year from title text."""
    years = re.findall(r"\b(19\d{2}|20[0-2]\d)\b", title)
    if years:
        return years[-1]
    return None


def make_doc_id(title: str, url: str) -> str:
    """Generate a stable ID from the PDF filename."""
    filename = url.split("/")[-1]
    name = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    clean = re.sub(r"[^a-zA-Z0-9_-]", "-", name)
    clean = re.sub(r"-+", "-", clean).strip("-")[:80]
    return f"SS-{clean}"


def normalize(title: str, url: str, full_text: str) -> Dict:
    doc_id = make_doc_id(title, url)
    year = extract_year(title)
    act_number = extract_act_number(title)

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
        "act_number": act_number,
    }


def fetch_all() -> Generator[Dict, None, None]:
    session = get_session()

    print("Discovering laws from MoJCA...")
    laws = discover_laws(session)
    print(f"  Found {len(laws)} laws")

    total = len(laws)
    yielded = 0
    skipped = 0

    for i, (title, url) in enumerate(laws):
        time.sleep(PDF_DOWNLOAD_DELAY)
        pdf_bytes = download_pdf(session, url)

        if not pdf_bytes:
            skipped += 1
            continue

        full_text = extract_text_from_pdf(pdf_bytes)
        if len(full_text) < 100:
            print(f"  SKIP (too short): {title} ({len(full_text)} chars)")
            skipped += 1
            continue

        record = normalize(title, url, full_text)
        yield record
        yielded += 1

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{total} — yielded: {yielded}, skipped: {skipped}")

    print(f"\n  Total records yielded: {yielded}")
    print(f"  Total skipped: {skipped}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    yield from fetch_all()


def bootstrap_sample(sample_dir: Path, count: int = 15):
    if not PDF_EXTRACTOR:
        print("ERROR: No PDF extraction library available (need pdfplumber or PyPDF2)")
        sys.exit(1)

    session = get_session()
    print(f"Using PDF extractor: {PDF_EXTRACTOR}")

    print("Discovering laws...")
    laws = discover_laws(session)
    print(f"Found {len(laws)} laws")

    sample_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    attempted = 0
    total_chars = 0

    # Spread sample across the list
    step = max(1, len(laws) // count)
    indices = [i * step for i in range(count) if i * step < len(laws)]
    # Fill remaining from start if needed
    if len(indices) < count:
        for i in range(len(laws)):
            if i not in indices:
                indices.append(i)
            if len(indices) >= count:
                break

    for idx in indices:
        if saved >= count:
            break

        title, url = laws[idx]
        attempted += 1
        filename_part = url.split("/")[-1]
        print(f"\n  [{attempted}] {title}")
        print(f"    Downloading: {filename_part}")

        time.sleep(PDF_DOWNLOAD_DELAY)
        pdf_bytes = download_pdf(session, url)
        if not pdf_bytes:
            print(f"    SKIP: download failed")
            continue

        full_text = extract_text_from_pdf(pdf_bytes)
        text_len = len(full_text)
        if text_len < 100:
            print(f"    SKIP: text too short ({text_len} chars)")
            continue

        record = normalize(title, url, full_text)
        total_chars += text_len
        saved += 1

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    [{saved}/{count}] Saved: {filename}")
        print(f"    Text: {text_len:,} chars | Date: {record.get('date', '?')} | Act: {record.get('act_number', '?')}")

    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records attempted: {attempted}")
    print(f"Records saved: {saved}")
    if saved > 0:
        avg = total_chars // saved
        print(f"Total text chars: {total_chars:,}")
        print(f"Average text length: {avg:,} chars/doc")
    print(f"Sample directory: {sample_dir}")

    if saved >= 10:
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="South Sudan MoJCA Laws Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--count", type=int, default=15)
    parser.add_argument("--since", type=str)
    parser.add_argument("--full", action="store_true")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            sample_dir.mkdir(parents=True, exist_ok=True)
            saved = 0
            for record in fetch_all():
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
                filepath = sample_dir / f"{safe_name}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                saved += 1
            print(f"\nFull bootstrap complete: {saved} records saved")

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
