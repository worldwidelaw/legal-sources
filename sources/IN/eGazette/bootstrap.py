#!/usr/bin/env python3
"""
IN/eGazette — Gazette of India

Fetches Indian central government gazette notifications from the Internet
Archive's "gazetteofindia" collection. Uses the Archive.org Advanced Search
API for metadata and downloads pre-extracted OCR text (_djvu.txt files).

Data coverage: ~736,000 gazette items (central + state), historical to present.
This scraper focuses on central extraordinary gazettes (most legally significant).
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests

# ── API configuration ──────────────────────────────────────────────────
SEARCH_URL = "https://archive.org/advancedsearch.php"
METADATA_URL = "https://archive.org/metadata/{identifier}"
DOWNLOAD_URL = "https://archive.org/download/{identifier}/{filename}"
RATE_LIMIT_DELAY = 1.0
PAGE_SIZE = 100

# Central gazette identifiers: in.gazette.central.e.YYYY-MM-DD.NNNNNN (extraordinary)
# and in.gazette.central.w.YYYY-MM-DD.NNNNNN (weekly)
CENTRAL_QUERY = "collection:gazetteofindia AND identifier:in.gazette.central.*"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "LegalDataHunter/1.0 (legal data research)"})


def search_gazettes(
    query: str = CENTRAL_QUERY,
    page: int = 0,
    rows: int = PAGE_SIZE,
    sort: str = "date desc",
) -> Dict[str, Any]:
    """Search Internet Archive for gazette items."""
    params = {
        "q": query,
        "fl[]": ["identifier", "title", "date", "subject", "description", "downloads"],
        "sort[]": sort,
        "rows": rows,
        "page": page + 1,  # IA uses 1-based pages
        "output": "json",
    }
    resp = SESSION.get(SEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_item_metadata(identifier: str) -> Optional[Dict[str, Any]]:
    """Fetch full metadata for an Archive.org item."""
    url = METADATA_URL.format(identifier=identifier)
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Warning: Failed to fetch metadata for {identifier}: {e}")
        return None


def download_text(identifier: str, filename: str) -> Optional[str]:
    """Download a text file from Archive.org."""
    url = DOWNLOAD_URL.format(identifier=identifier, filename=filename)
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        text = resp.text
        return text.strip() if text.strip() else None
    except Exception as e:
        print(f"  Warning: Failed to download text {filename}: {e}")
        return None


def _find_text_file(files: list) -> Optional[str]:
    """Find the best text file from an item's file list."""
    # Prefer _djvu.txt (OCR full text), then .txt, then _chocr.html.gz
    djvu_txt = None
    plain_txt = None
    for f in files:
        name = f.get("name", "")
        if name.endswith("_djvu.txt"):
            djvu_txt = name
        elif name.endswith(".txt") and not name.endswith("_files.xml"):
            plain_txt = name
    return djvu_txt or plain_txt


def _find_pdf_file(files: list) -> Optional[str]:
    """Find the PDF file from an item's file list."""
    for f in files:
        name = f.get("name", "")
        if name.endswith(".pdf") and "thumb" not in name.lower():
            return name
    return None


def _clean_text(text: str) -> str:
    """Clean OCR text: normalize whitespace, remove artifacts."""
    # Remove common OCR artifacts
    text = re.sub(r"\f", "\n\n", text)  # form feeds → double newline
    text = re.sub(r"[ \t]+", " ", text)  # collapse horizontal whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)  # max 2 consecutive newlines
    return text.strip()


def _parse_date(date_val) -> Optional[str]:
    """Parse Archive.org date to ISO 8601."""
    if not date_val:
        return None
    if isinstance(date_val, list):
        date_val = date_val[0]
    date_str = str(date_val)
    # Try YYYY-MM-DDT... or YYYY-MM-DD
    match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if match:
        return match.group(1)
    # Try YYYY
    match = re.match(r"(\d{4})", date_str)
    if match:
        return f"{match.group(1)}-01-01"
    return None


def _extract_gazette_type(identifier: str) -> str:
    """Extract gazette type from identifier."""
    if ".central.e." in identifier:
        return "extraordinary"
    elif ".central.w." in identifier:
        return "weekly"
    elif ".central." in identifier:
        return "central"
    else:
        # State gazette
        parts = identifier.split(".")
        if len(parts) >= 3:
            return f"state-{parts[2]}"
        return "unknown"


def normalize(meta: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Normalize a gazette item to the standard schema."""
    md = meta.get("metadata", {})
    identifier = md.get("identifier", "")

    title = md.get("title", "")
    if isinstance(title, list):
        title = title[0]

    subject = md.get("subject", [])
    if isinstance(subject, str):
        subject = [subject]

    description = md.get("description", "")
    if isinstance(description, list):
        description = " ".join(description)

    gazette_type = _extract_gazette_type(identifier)
    date = _parse_date(md.get("date"))

    # Extract gazette number from identifier (last segment)
    parts = identifier.split(".")
    gazette_number = parts[-1] if parts else ""

    return {
        "_id": identifier,
        "_source": "IN/eGazette",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": f"https://archive.org/details/{identifier}",
        "gazette_type": gazette_type,
        "gazette_number": gazette_number,
        "subjects": subject,
        "description": description,
        "language": md.get("language", ""),
    }


def fetch_all(
    max_records: Optional[int] = None,
    query: str = CENTRAL_QUERY,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch gazette items with full text."""
    page = 0
    fetched = 0
    total = None

    while True:
        if max_records and fetched >= max_records:
            return

        time.sleep(RATE_LIMIT_DELAY)
        result = search_gazettes(query=query, page=page)
        response = result.get("response", {})

        if total is None:
            total = response.get("numFound", 0)
            print(f"Found {total:,} gazette items")

        docs = response.get("docs", [])
        if not docs:
            break

        for doc in docs:
            if max_records and fetched >= max_records:
                return

            identifier = doc.get("identifier", "")
            if not identifier:
                continue

            print(f"  Processing {identifier}...")
            time.sleep(RATE_LIMIT_DELAY)

            # Get full metadata to find text files
            meta = get_item_metadata(identifier)
            if not meta:
                continue

            files = meta.get("files", [])

            # Try to get pre-extracted text
            text = None
            txt_file = _find_text_file(files)
            if txt_file:
                time.sleep(0.5)
                text = download_text(identifier, txt_file)
                if text:
                    text = _clean_text(text)

            # Fall back to PDF extraction if no text file
            if not text:
                pdf_file = _find_pdf_file(files)
                if pdf_file:
                    pdf_url = DOWNLOAD_URL.format(identifier=identifier, filename=pdf_file)
                    try:
                        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
                        from common.pdf_extract import extract_pdf_markdown
                        time.sleep(0.5)
                        text = extract_pdf_markdown(
                            source="IN/eGazette",
                            source_id=identifier,
                            pdf_url=pdf_url,
                            table="legislation",
                        )
                    except ImportError:
                        pass

                    if not text:
                        try:
                            import pdfplumber
                            import io
                            resp = SESSION.get(pdf_url, timeout=60)
                            resp.raise_for_status()
                            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                                pages = [p.extract_text() or "" for p in pdf.pages]
                            text = "\n\n".join(p for p in pages if p.strip())
                        except Exception as e:
                            print(f"    PDF extraction failed: {e}")

            if not text or len(text) < 50:
                print(f"    Skipping {identifier}: no usable text")
                continue

            record = normalize(meta, text)
            yield record
            fetched += 1

        page += 1


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """Fetch gazettes added since a given date."""
    since_str = since.strftime("%Y-%m-%d")
    query = f"{CENTRAL_QUERY} AND date:[{since_str} TO *]"
    yield from fetch_all(query=query)


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample gazette records."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    for record in fetch_all(max_records=count):
        doc_id = record["_id"].replace("/", "_").replace(":", "_")
        filepath = sample_dir / f"{doc_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        print(f"  Saved {filepath.name} (date={record.get('date')}, {text_len:,} chars)")
        saved += 1

    return saved


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records."""
    samples = list(sample_dir.glob("*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 100:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        for field in ["_id", "_source", "_type", "title", "date"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="IN/eGazette fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--count", type=int, default=15, help="Number of records")
    parser.add_argument("--since", type=str, help="Fetch since date (YYYY-MM-DD)")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count)
            print(f"\nSaved {saved} sample records to {sample_dir}")
            valid = validate_samples(sample_dir)
            sys.exit(0 if saved >= 10 and valid else 1)
        else:
            print("Use --sample for bootstrap mode")
            sys.exit(1)

    elif args.command == "validate":
        valid = validate_samples(sample_dir)
        sys.exit(0 if valid else 1)

    elif args.command == "fetch":
        if args.since:
            since = datetime.fromisoformat(args.since)
            for record in fetch_updates(since):
                print(json.dumps(record, ensure_ascii=False))
        else:
            for record in fetch_all(max_records=args.count):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
