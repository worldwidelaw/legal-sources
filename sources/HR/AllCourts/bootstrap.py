#!/usr/bin/env python3
"""
HR/AllCourts - Croatian Court Decisions (odluke.sudovi.hr)

Official database of Croatian court decisions covering 900K+ decisions from all courts.
Server-side rendered HTML with full text, metadata, and EuroVoc classification.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
from html import unescape
import urllib.parse

import requests
from bs4 import BeautifulSoup

# Configuration
SOURCE_ID = "HR/AllCourts"
BASE_URL = "https://odluke.sudovi.hr"
RATE_LIMIT = 1.0  # seconds between requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Session with retry
session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
})


def get_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    """Fetch URL with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            time.sleep(RATE_LIMIT)
            response = session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait = (2 ** attempt) + 1
                print(f"  Retry {attempt + 1}/{max_retries} after {wait}s: {e}", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"  Failed after {max_retries} attempts: {e}", file=sys.stderr)
                return None
    return None


def parse_date(date_str: str) -> Optional[str]:
    """Parse Croatian date format (DD.MM.YYYY.) to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip('.')
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def clean_text(html_content: str) -> str:
    """Extract and clean text from HTML decision content."""
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for element in soup(['script', 'style', 'head']):
        element.decompose()

    # Get text and clean up whitespace
    text = soup.get_text(separator='\n')

    # Clean up multiple newlines and spaces
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    text = text.strip()

    return text


def get_document_ids_from_page(
    page: int,
    dd_from: str = "",
    dd_to: str = "",
) -> list[str]:
    """
    Extract document IDs from a search results page.

    Args:
        page: Page number.
        dd_from: Start date filter in Croatian format ``D.M.YYYY.`` (trailing dot).
        dd_to: End date filter in Croatian format ``D.M.YYYY.`` (trailing dot).
    """
    params = {"page": str(page)}
    if dd_from:
        params["dd_from"] = dd_from
    if dd_to:
        params["dd_to"] = dd_to

    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/Document/DisplayList?{qs}"
    response = get_with_retry(url)
    if not response:
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    doc_ids = []

    # Find all document links
    for link in soup.find_all('a', class_='search-result'):
        href = link.get('href', '')
        match = re.search(r'id=([a-f0-9-]+)', href)
        if match:
            doc_ids.append(match.group(1))

    return doc_ids


def fetch_document(doc_id: str) -> Optional[dict]:
    """Fetch and parse a single document."""
    url = f"{BASE_URL}/Document/View?id={doc_id}"
    response = get_with_retry(url)
    if not response:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Get decision number from title
    title_tag = soup.find('title')
    title = title_tag.get_text().strip() if title_tag else ""

    # Get decision text from decision-text div
    decision_div = soup.find('div', class_='decision-text')
    full_text = clean_text(str(decision_div)) if decision_div else ""

    if not full_text:
        print(f"  Warning: No text found for {doc_id}", file=sys.stderr)
        return None

    # Parse metadata from metadata items
    metadata = {}
    for item in soup.find_all('div', class_='metadata-item'):
        data_type = item.get('data-metadata-type', '')
        title_elem = item.find('p', class_='metadata-title')
        content_elem = item.find('p', class_='metadata-content')

        if title_elem and content_elem:
            metadata[data_type] = content_elem.get_text().strip()
        elif title_elem:
            # Handle list-based content (like thesaurus)
            content_list = item.find('ul', class_='metadata-content')
            if content_list:
                items = [li.get_text().strip() for li in content_list.find_all('li')]
                metadata[data_type] = items

    # Extract structured fields
    decision_number = metadata.get('decision-number', '')
    court = metadata.get('court', '')
    decision_date = parse_date(metadata.get('decision-date', ''))
    publication_date = parse_date(metadata.get('publication-date', ''))
    decision_type = metadata.get('decision-type', '')
    registry_type = metadata.get('court-registry-type', '')
    finality = metadata.get('decision-finality', '')

    # Get legal field from stvarno-kazalo-index
    legal_field = None
    sk_index = metadata.get('stvarno-kazalo-index')
    if isinstance(sk_index, list) and sk_index:
        legal_field = sk_index[0]  # Top-level category

    # Look for ECLI in the page
    ecli = None
    ecli_match = re.search(r'ECLI:[A-Z]{2}:[A-Z0-9]+:\d{4}:\d+', response.text)
    if ecli_match:
        ecli = ecli_match.group(0)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.replace(", ", " - ") if title else f"{decision_number} ({court})",
        "text": full_text,
        "date": decision_date,
        "url": url,
        "decision_number": decision_number,
        "court": court,
        "decision_type": decision_type,
        "publication_date": publication_date,
        "registry_type": registry_type,
        "finality": finality,
        "legal_field": legal_field,
        "ecli": ecli,
    }


# ---------------------------------------------------------------------------
# Checkpoint / resume
#
# Issue #1104: the fleet relaunches the (100h) full bootstrap on crash/timeout.
# Without a checkpoint every relaunch re-walks from 1990 and re-appends the same
# early-year records to data/records.jsonl, so the loader saw ~43 copies of the
# first ~5,329 docs (5,329 written of 229,900 "fetched").  The checkpoint records
# which calendar years are fully done and how far into the in-progress year we
# got, so a relaunch skips completed years (no network calls) and resumes the
# current year at the right page.  Kept next to the module so it survives the
# fleet's temp CWD.
# ---------------------------------------------------------------------------
CHECKPOINT_PATH = Path(__file__).parent / "hr_allcourts_checkpoint.json"


def _load_checkpoint() -> dict:
    try:
        with open(CHECKPOINT_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return {
            "completed_years": set(data.get("completed_years", [])),
            "current_year": data.get("current_year"),
            "current_page": data.get("current_page", 1),
        }
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return {"completed_years": set(), "current_year": None, "current_page": 1}


def _save_checkpoint(state: dict) -> None:
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            {
                "completed_years": sorted(state["completed_years"]),
                "current_year": state["current_year"],
                "current_page": state["current_page"],
            },
            f,
        )
    tmp.replace(CHECKPOINT_PATH)


def _fetch_year(
    year: int,
    start_page: int = 1,
    on_page: Optional[Any] = None,
) -> Generator[dict, None, None]:
    """Fetch all documents for a single calendar year.

    Resumes at ``start_page`` and, after each page is fully yielded, invokes
    ``on_page(next_page)`` so the caller can persist a checkpoint.
    """
    dd_from = f"1.1.{year}."
    dd_to = f"31.12.{year}."

    page = start_page
    empty_pages = 0
    year_count = 0

    while empty_pages < 3:
        doc_ids = get_document_ids_from_page(page, dd_from=dd_from, dd_to=dd_to)

        if not doc_ids:
            empty_pages += 1
            page += 1
            continue

        empty_pages = 0

        for doc_id in doc_ids:
            doc = fetch_document(doc_id)
            if doc:
                yield doc
                year_count += 1

        page += 1
        if on_page is not None:
            on_page(page)

    if year_count:
        print(f"  Year {year}: {year_count} documents", file=sys.stderr)


def fetch_all() -> Generator[dict, None, None]:
    """
    Yield all documents from the database using year-based partitioning.

    The search endpoint caps results at ~10K per query.  By filtering
    each request to a single calendar year (``dd_from``/``dd_to`` in
    ``D.M.YYYY.`` format) every partition stays well under the cap.

    Resume-safe: years are walked oldest-first (append-at-tail) and a
    checkpoint (see ``CHECKPOINT_PATH``) records completed years plus the
    in-progress page so a relaunch does not re-walk finished years.
    """
    current_year = datetime.now().year
    # Croatian court decisions available from ~1990 onwards
    years = list(range(1990, current_year + 1))

    ckpt = _load_checkpoint()
    completed = ckpt["completed_years"]
    if completed:
        print(
            f"Resuming: {len(completed)} year(s) already complete, "
            f"in-progress year={ckpt['current_year']} page={ckpt['current_page']}",
            file=sys.stderr,
        )

    total = 0
    for year in years:
        if year in completed:
            continue

        # Resume mid-year only for the year we were last working on.
        if year == ckpt["current_year"]:
            start_page = ckpt["current_page"]
        else:
            start_page = 1

        print(f"Fetching year {year} (from page {start_page})...", file=sys.stderr)
        before = total

        def _on_page(next_page: int, _year: int = year) -> None:
            ckpt["current_year"] = _year
            ckpt["current_page"] = next_page
            _save_checkpoint(
                {
                    "completed_years": completed,
                    "current_year": _year,
                    "current_page": next_page,
                }
            )

        for doc in _fetch_year(year, start_page=start_page, on_page=_on_page):
            yield doc
            total += 1

        # Year finished — mark complete and reset the in-progress pointer.
        completed.add(year)
        _save_checkpoint(
            {"completed_years": completed, "current_year": None, "current_page": 1}
        )

        year_docs = total - before
        if year_docs == 0:
            print(f"  Year {year}: 0 documents (skipping)", file=sys.stderr)

    print(f"\nTotal fetched: {total} documents across {len(years)} years", file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    # The search interface doesn't easily support date filtering
    # For now, yield from recent pages until we hit older documents
    since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))

    page = 1
    found_older = False

    while not found_older and page <= 100:  # Limit to 100 pages for updates
        print(f"Fetching page {page} for updates...", file=sys.stderr)
        doc_ids = get_document_ids_from_page(page)

        if not doc_ids:
            break

        for doc_id in doc_ids:
            doc = fetch_document(doc_id)
            if doc:
                # Check publication date
                pub_date = doc.get('publication_date')
                if pub_date:
                    doc_dt = datetime.fromisoformat(pub_date)
                    if doc_dt.replace(tzinfo=timezone.utc) < since_dt:
                        found_older = True
                        break
                yield doc

        page += 1


def normalize(raw: dict) -> dict:
    """Normalize raw data to standard schema."""
    # Already normalized in fetch_document
    return raw


def test_connection() -> bool:
    """Test connectivity to the data source."""
    try:
        response = session.get(BASE_URL, timeout=10)
        response.raise_for_status()

        # Check for expected content
        if 'odluke' in response.text.lower() or 'sudovi' in response.text.lower():
            print("Connection successful: Croatian Court Decisions database accessible")

            # Try to get count
            match = re.search(r'(\d[\d\s.,]*)\s*objavljenih?\s*odluk', response.text)
            if match:
                count = match.group(1).replace(' ', '').replace('.', '').replace(',', '')
                print(f"Database contains approximately {count} decisions")

            return True
        else:
            print("Connection failed: Unexpected content", file=sys.stderr)
            return False
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return False


def bootstrap_sample(count: int = 12) -> list[dict]:
    """Fetch a sample of documents for testing."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    documents = []
    page = 1

    while len(documents) < count:
        print(f"Fetching page {page}...", file=sys.stderr)
        doc_ids = get_document_ids_from_page(page)

        if not doc_ids:
            break

        for doc_id in doc_ids:
            if len(documents) >= count:
                break

            print(f"  Fetching document {len(documents) + 1}/{count}: {doc_id}", file=sys.stderr)
            doc = fetch_document(doc_id)

            if doc and doc.get('text'):
                documents.append(doc)

                # Save to sample directory
                filepath = sample_dir / f"{doc['_id']}.json"
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)

                print(f"    Saved: {doc['title'][:60]}... ({len(doc['text'])} chars)")

        page += 1

    # Print summary
    if documents:
        total_chars = sum(len(d['text']) for d in documents)
        avg_chars = total_chars // len(documents)
        print(f"\nSample complete: {len(documents)} documents, avg {avg_chars} chars/doc")

        # List courts represented
        courts = set(d.get('court', 'Unknown') for d in documents)
        print(f"Courts: {', '.join(sorted(courts))}")

    return documents


def _records_jsonl_path() -> Path:
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "records.jsonl"


def bootstrap_full() -> int:
    """Stream the whole corpus to ``data/records.jsonl``.

    Issue #1487: this used to print records to stdout only, so the fleet's
    2.5-day crawl died mid-run (~162.5K records) with nothing on disk.  Every
    record is written and flushed immediately, and the file is appended to when
    the checkpoint says we are resuming a partially-walked corpus, so a crash
    costs at most the page that was in flight rather than the whole run.
    """
    jsonl_path = _records_jsonl_path()

    ckpt = _load_checkpoint()
    resuming = bool(ckpt["completed_years"]) or ckpt["current_year"] is not None
    mode = "a" if (resuming and jsonl_path.exists()) else "w"
    if mode == "a":
        print(f"Resuming: appending to existing {jsonl_path}", file=sys.stderr)

    count = 0
    with open(jsonl_path, mode, encoding="utf-8") as f:
        for doc in fetch_all():
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            # Flush per record: the fleet kills long crawls (OOM/timeout) and a
            # buffered write would take the whole run's output with it.
            f.flush()
            count += 1
            if count % 100 == 0:
                print(f"Progress: {count} records written", file=sys.stderr)

    print(f"Full bootstrap complete: {count} records -> {jsonl_path}", file=sys.stderr)
    return count


def main():
    import argparse

    parser = argparse.ArgumentParser(description='HR/AllCourts - Croatian Court Decisions')
    parser.add_argument('command', choices=['test', 'bootstrap', 'bootstrap-fast', 'update'],
                        help='Command to run (bootstrap-fast is the fleet pipeline alias)')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for testing)')
    parser.add_argument('--since', type=str,
                        help='ISO date for incremental updates')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'test':
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command in ('bootstrap', 'bootstrap-fast'):
        if args.sample:
            bootstrap_sample(12)
        else:
            bootstrap_full()

    elif args.command == 'update':
        since = args.since or datetime.now(timezone.utc).replace(day=1).isoformat()
        for doc in fetch_updates(since):
            print(json.dumps(doc, ensure_ascii=False))


if __name__ == '__main__':
    main()
