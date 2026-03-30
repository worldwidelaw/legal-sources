#!/usr/bin/env python3
"""
TT/DigitalLegislativeLibrary - Trinidad & Tobago Legislation

Fetches full-text legislation from the Trinidad & Tobago Digital Legislative
Library (laws.gov.tt). ~545 revised Acts from 1838-present.

Strategy:
  - Paginate HTML listing at /ttdll-web/revision/list?offset=N
  - For each act, fetch detail page to get PDF download link
  - Download PDF, decrypt with empty password, extract text via PyPDF2

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
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
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False

SOURCE_ID = "TT/DigitalLegislativeLibrary"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://laws.gov.tt/ttdll-web"
LIST_URL = BASE_URL + "/revision/list"


def http_get(url: str, retries: int = 3, binary: bool = False):
    """Fetch URL with retries. Returns str or bytes depending on binary flag."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={
                "User-Agent": "LegalDataHunter/1.0",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
            })
            with urlopen(req, timeout=120) as resp:
                data = resp.read()
                if binary:
                    return data
                return data.decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Failed to fetch {url}: {e}")


def get_act_ids_from_listing(offset: int = 0) -> list[dict]:
    """Parse a listing page to extract act IDs and names."""
    url = f"{LIST_URL}?offset={offset}"
    html = http_get(url)

    results = []
    # Look for links with currentid parameter
    # Pattern: href="...?...currentid=NNNN..."
    pattern = r'currentid=(\d+)'
    ids_found = re.findall(pattern, html)

    # Also extract act names from the listing
    # The listing has table rows with act names and chapter references
    # Extract act name + id pairs
    # Look for links like: <a href="...currentid=12345...">Act Name</a>
    link_pattern = r'currentid=(\d+)[^"]*"[^>]*>([^<]+)</a>'
    link_matches = re.findall(link_pattern, html)

    seen = set()
    for act_id, name in link_matches:
        if act_id not in seen:
            seen.add(act_id)
            results.append({"id": act_id, "name": name.strip()})

    # If link_pattern didn't find names, use just IDs
    if not results:
        seen = set()
        for act_id in ids_found:
            if act_id not in seen:
                seen.add(act_id)
                results.append({"id": act_id, "name": ""})

    return results


def get_download_info(act_id: str) -> Optional[dict]:
    """Get the PDF download URL and metadata for an act."""
    url = f"{LIST_URL}?offset=0&q=&currentid={act_id}"
    try:
        html = http_get(url)
    except Exception:
        return None

    # Extract download link: /ttdll-web/revision/download/{docId}?type=act
    dl_match = re.search(r'/ttdll-web/revision/download/(\d+)\?type=act', html)
    if not dl_match:
        return None

    doc_id = dl_match.group(1)

    # Extract title from detail panel
    title_match = re.search(r'<h[23][^>]*>([^<]+)</h[23]>', html)
    title = title_match.group(1).strip() if title_match else ""

    # Extract chapter reference
    chapter_match = re.search(r'Chapter\s+([\d:.]+)', html, re.IGNORECASE)
    chapter = chapter_match.group(1) if chapter_match else None

    # Try to extract date/year
    year_match = re.search(r'Act\s+(?:\d+\s+of\s+)?(\d{4})', html)
    year = year_match.group(1) if year_match else None

    return {
        "doc_id": doc_id,
        "title": title,
        "chapter": chapter,
        "year": year,
        "download_url": f"{BASE_URL}/revision/download/{doc_id}?type=act",
    }


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from a (possibly encrypted) PDF."""
    if not HAS_PYPDF2:
        return None

    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        if reader.is_encrypted:
            try:
                reader.decrypt("")
            except Exception:
                return None

        text_parts = []
        for page in reader.pages:
            try:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            except Exception:
                continue

        text = "\n".join(text_parts).strip()
        return text if len(text) > 50 else None
    except Exception:
        return None


def extract_act_name(text: str) -> Optional[str]:
    """Extract the act name from the first lines of PDF text."""
    # The first line typically contains the act name in uppercase
    # e.g. "ABSCONDING  DEBTORS  ACT" or "COMPANIES  ACT"
    for line in text.split("\n")[:5]:
        line = line.strip()
        # Collapse multiple spaces
        line = re.sub(r'\s+', ' ', line)
        # Skip lines that are just page numbers, L.R.O. references, etc.
        if re.match(r'^[\d\s.\-–]+$', line):
            continue
        if line.startswith("L.R.O.") or line.startswith("LAWS OF"):
            continue
        # Act names are uppercase and contain "ACT" or "ORDINANCE"
        if re.search(r'\bACT\b|\bORDINANCE\b', line, re.IGNORECASE) and len(line) > 5:
            # Clean up: remove leading artifacts like "(Deoxyribonucleic" -> keep as-is
            return line.title()
    return None


def normalize(act_id: str, text: str, info: dict) -> dict:
    """Normalize an act into standard schema."""
    date = f"{info['year']}-01-01" if info.get("year") else None

    # Prefer act name extracted from PDF text over generic page title
    title = extract_act_name(text) or info.get("name") or info.get("title") or f"TT Act {act_id}"

    return {
        "_id": f"TT_DLL_{act_id}_{info.get('doc_id', 'unknown')}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title[:300],
        "date": date,
        "chapter": info.get("chapter"),
        "text": text,
        "url": f"https://laws.gov.tt/ttdll-web/revision/list?offset=0&currentid={act_id}",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all acts from the Digital Legislative Library."""
    if not HAS_PYPDF2:
        print("ERROR: PyPDF2 is required. Install with: pip install PyPDF2")
        return

    total_yielded = 0
    offset = 0
    max_offset = 20 if sample else 600  # ~545 acts total
    empty_pages = 0

    while offset <= max_offset:
        print(f"  Listing offset {offset}...")
        acts = get_act_ids_from_listing(offset)
        if not acts:
            empty_pages += 1
            if empty_pages >= 2:
                break
            offset += 10
            continue

        empty_pages = 0
        print(f"    Found {len(acts)} acts")

        for act in acts:
            act_id = act["id"]
            print(f"    Processing act {act_id}: {act.get('name', '')[:50]}...")
            time.sleep(1)

            info = get_download_info(act_id)
            if not info:
                print(f"      No download info found")
                continue

            if not info.get("title") and act.get("name"):
                info["title"] = act["name"]

            print(f"      Downloading PDF {info['doc_id']}...")
            time.sleep(1)

            try:
                pdf_bytes = http_get(info["download_url"], binary=True)
            except Exception as e:
                print(f"      Download failed: {e}")
                continue

            text = extract_text_from_pdf(pdf_bytes)
            if not text:
                print(f"      No text extracted from PDF")
                continue

            record = normalize(act_id, text, info)
            yield record
            total_yielded += 1
            print(f"      OK: {len(text)} chars")

            if sample and total_yielded >= 15:
                return

        offset += 10

    print(f"  Total acts with text: {total_yielded}")


def test_connection():
    """Test connectivity to laws.gov.tt."""
    print("Testing TT/DigitalLegislativeLibrary connectivity...")

    if not HAS_PYPDF2:
        print("  FAIL: PyPDF2 not installed")
        return False

    print("\n1. Checking listing page...")
    try:
        acts = get_act_ids_from_listing(0)
        print(f"   OK: Found {len(acts)} acts on first page")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    if not acts:
        print("   FAIL: No acts found")
        return False

    print(f"   First act: {acts[0]}")

    print("\n2. Getting download info...")
    act_id = acts[0]["id"]
    info = get_download_info(act_id)
    if info:
        print(f"   OK: doc_id={info['doc_id']}, title={info.get('title', '')[:60]}")
    else:
        print("   FAIL: No download info found")
        return False

    print("\n3. Downloading PDF...")
    try:
        pdf_bytes = http_get(info["download_url"], binary=True)
        print(f"   OK: {len(pdf_bytes)} bytes")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n4. Extracting text...")
    text = extract_text_from_pdf(pdf_bytes)
    if text:
        print(f"   OK: {len(text)} chars")
        print(f"   Preview: {text[:200]}...")
    else:
        print("   FAIL: No text extracted")
        return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="TT Digital Legislative Library Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name} ({len(record['text'])} chars)")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
