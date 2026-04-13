#!/usr/bin/env python3
"""
UN/UPRDocuments - OHCHR Universal Periodic Review Documents

Fetches UPR documents (national reports, UN compilations, stakeholder summaries)
for each country review across all UPR cycles.

Data flow:
  1. UPR Info Uwazi API → review metadata (country, session, cycle)
  2. UN ODS → PDF download via document symbol
  3. common/pdf_extract → full text extraction

~676 reviews × 3 document types = ~2000 PDF documents.

License: UN public domain
Auth: None

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UN/UPRDocuments"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "application/json",
}

RATE_LIMIT_DELAY = 2.0

UWAZI_BASE = "https://upr-info-database.uwazi.io/api"
UWAZI_DOC_TEMPLATE = "5e57cdd6f54e0a1304c0d50d"
UWAZI_STATE_TEMPLATE = "5d8cdec361cde0408222d3ec"
ODS_SYMBOL_URL = "https://documents.un.org/api/symbol/access"

# Document types and their symbol suffix
DOC_TYPES = {
    "national_report": {"suffix": "1", "label": "National Report"},
    "un_compilation": {"suffix": "2", "label": "UN Compilation"},
    "stakeholder_summary": {"suffix": "3", "label": "Stakeholder Summary"},
}


def fetch_url(url: str, timeout: int = 60, **kwargs) -> Optional[requests.Response]:
    """Fetch a URL with error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        print(f"  Error fetching {url[:120]}: {e}")
        return None


def fetch_state_map() -> dict:
    """Fetch State entities from Uwazi to build sharedId → (name, ISO-3) map."""
    url = (
        f"{UWAZI_BASE}/search"
        f"?limit=200&types=%5B%22{UWAZI_STATE_TEMPLATE}%22%5D"
    )
    resp = fetch_url(url, timeout=30)
    if not resp:
        return {}
    data = resp.json()
    mapping = {}
    for r in data.get("rows", []):
        sid = r.get("sharedId", "")
        name = r.get("title", "")
        icon = r.get("icon", {}) or {}
        code = icon.get("_id", "")
        if sid and code:
            mapping[sid] = (name, code)
    print(f"  State map: {len(mapping)} countries")
    return mapping


def fetch_reviews(limit: int = 0) -> list[dict]:
    """Fetch all UPR review metadata from Uwazi API."""
    print("  Loading state → ISO-3 mapping...")
    state_map = fetch_state_map()

    reviews = []
    page_size = 100
    offset = 0

    while True:
        url = (
            f"{UWAZI_BASE}/search"
            f"?limit={page_size}&from={offset}"
            f"&types=%5B%22{UWAZI_DOC_TEMPLATE}%22%5D"
            f"&order=asc&sort=creationDate"
        )
        resp = fetch_url(url, timeout=30)
        if not resp:
            break

        data = resp.json()
        rows = data.get("rows", [])
        total = data.get("totalRows", 0)

        for row in rows:
            md = row.get("metadata", {})

            # Resolve country via state_map
            state = md.get("state_reviewed", [{}])
            if isinstance(state, list):
                state = state[0] if state else {}
            state_id = state.get("value", "")
            country_name, country_code = state_map.get(state_id, (state.get("label", "Unknown"), ""))

            # Extract session number
            session_data = md.get("session_of_the_document", [{}])
            if isinstance(session_data, list):
                session_data = session_data[0] if session_data else {}
            session_label = session_data.get("label", "")
            session_match = re.match(r"(\d+)", session_label)
            session_num = int(session_match.group(1)) if session_match else None

            # Extract cycle
            cycle_data = md.get("cycle_of_the_document", [{}])
            if isinstance(cycle_data, list):
                cycle_data = cycle_data[0] if cycle_data else {}
            cycle_label = cycle_data.get("label", "")
            cycle_match = re.search(r"Cycle\s+(\d+)", cycle_label)
            cycle_num = int(cycle_match.group(1)) if cycle_match else None

            # Extract date
            date_data = md.get("document_date", [{}])
            if isinstance(date_data, list):
                date_data = date_data[0] if date_data else {}
            date_ts = date_data.get("value")
            date_str = None
            if date_ts and isinstance(date_ts, (int, float)):
                date_str = datetime.fromtimestamp(date_ts, tz=timezone.utc).strftime("%Y-%m-%d")

            if country_code and session_num:
                reviews.append({
                    "country_name": country_name,
                    "country_code": country_code,
                    "session": session_num,
                    "cycle": cycle_num,
                    "date": date_str,
                    "title": row.get("title", ""),
                })

        print(f"  Fetched {len(reviews)}/{total} reviews...")
        offset += page_size

        if offset >= total or len(rows) == 0:
            break
        if limit > 0 and len(reviews) >= limit:
            reviews = reviews[:limit]
            break

        time.sleep(1.0)

    print(f"  Total: {len(reviews)} reviews with valid metadata")
    return reviews


def fetch_upr_pdf(symbol: str) -> Optional[bytes]:
    """Fetch a UPR document PDF from UN ODS by document symbol."""
    url = f"{ODS_SYMBOL_URL}?s={requests.utils.quote(symbol)}&l=en&t=pdf"
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=120, allow_redirects=True
        )
        if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
            return resp.content
        print(f"    Not a PDF or error ({resp.status_code}) for {symbol}")
        return None
    except requests.RequestException as e:
        print(f"    Error downloading {symbol}: {e}")
        return None


def make_symbol(session: int, country_code: str, suffix: str) -> str:
    """Construct a UN document symbol for a UPR document."""
    return f"A/HRC/WG.6/{session}/{country_code}/{suffix}"


def normalize(
    review: dict, doc_type_key: str, text: str, symbol: str
) -> dict:
    """Normalize a UPR document into standard schema."""
    doc_info = DOC_TYPES[doc_type_key]
    cc = review["country_code"]
    sess = review["session"]
    title = f"{review['country_name']} - {doc_info['label']} (Cycle {review.get('cycle', '?')}, Session {sess})"

    return {
        "_id": f"UPR-{cc}-C{review.get('cycle', 0)}-S{sess}-{doc_info['suffix']}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": review.get("date"),
        "url": f"https://documents.un.org/api/symbol/access?s={requests.utils.quote(symbol)}&l=en&t=pdf",
        "country_reviewed": review["country_name"],
        "country_code": cc,
        "session": sess,
        "cycle": review.get("cycle"),
        "doc_type": doc_type_key,
        "symbol": symbol,
    }


def fetch_documents(reviews: list[dict], sample: bool = False) -> Generator[dict, None, None]:
    """Fetch UPR documents for each review."""
    total_docs = 0
    total_text = 0

    for i, review in enumerate(reviews):
        cc = review["country_code"]
        sess = review["session"]
        print(f"\n  [{i+1}/{len(reviews)}] {review['country_name']} - Session {sess}, Cycle {review.get('cycle')}")

        for doc_key, doc_info in DOC_TYPES.items():
            symbol = make_symbol(sess, cc, doc_info["suffix"])
            print(f"    {doc_info['label']}: {symbol}")

            pdf_bytes = fetch_upr_pdf(symbol)
            if not pdf_bytes:
                time.sleep(RATE_LIMIT_DELAY)
                continue

            print(f"      PDF: {len(pdf_bytes)} bytes")

            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=f"{cc}-S{sess}-{doc_info['suffix']}",
                pdf_bytes=pdf_bytes,
                table="doctrine",
            ) or ""

            print(f"      Text: {len(text)} chars")

            record = normalize(review, doc_key, text, symbol)
            yield record
            total_docs += 1
            if text:
                total_text += 1

            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  Total: {total_docs} documents, {total_text} with text")


def bootstrap_sample():
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Use a small set of well-known reviews for sample (verified session numbers)
    sample_reviews = [
        {"country_name": "France", "country_code": "FRA", "session": 43, "cycle": 4, "date": "2023-05-03"},
        {"country_name": "Brazil", "country_code": "BRA", "session": 41, "cycle": 4, "date": "2022-11-07"},
        {"country_name": "Costa Rica", "country_code": "CRI", "session": 47, "cycle": 4, "date": "2024-05-06"},
        {"country_name": "Japan", "country_code": "JPN", "session": 43, "cycle": 4, "date": "2023-01-31"},
        {"country_name": "Ukraine", "country_code": "UKR", "session": 28, "cycle": 3, "date": "2017-11-15"},
    ]

    total = 0
    for record in fetch_documents(sample_reviews, sample=True):
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        total += 1
        print(f"      Saved {fname.name}")

    print(f"\nSample complete: {total} records saved to {SAMPLE_DIR}")
    validate_sample()


def bootstrap_full():
    """Fetch all UPR documents."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching review metadata from Uwazi API...")
    reviews = fetch_reviews()

    total = 0
    for record in fetch_documents(reviews):
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        total += 1

    print(f"\nFull bootstrap complete: {total} records saved.")


def validate_sample():
    """Validate sample records."""
    files = list(SAMPLE_DIR.glob("*.json"))
    if not files:
        print("FAIL: No sample files found")
        return False

    print(f"\nValidating {len(files)} sample records...")
    issues = []
    text_present = 0
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            rec = json.load(fh)
        name = f.name
        if not rec.get("text"):
            issues.append(f"{name}: missing or empty 'text' field")
        elif len(rec["text"]) < 100:
            issues.append(f"{name}: text too short ({len(rec['text'])} chars)")
        else:
            text_present += 1
        if not rec.get("title"):
            issues.append(f"{name}: missing title")
        if not rec.get("_id"):
            issues.append(f"{name}: missing _id")

    if issues:
        print("ISSUES FOUND:")
        for i in issues:
            print(f"  - {i}")

    print(f"\n{text_present}/{len(files)} records have full text")
    if text_present >= len(files) * 0.7:
        print("VALIDATION PASSED (>=70% have text)")
        return True
    else:
        print("VALIDATION FAILED (<70% have text)")
        return False


def test_connectivity():
    """Test connectivity to required APIs."""
    print("Testing UPR Documents connectivity...\n")

    # Test Uwazi API
    url = f"{UWAZI_BASE}/search?limit=1&types=%5B%22{UWAZI_DOC_TEMPLATE}%22%5D"
    resp = fetch_url(url, timeout=30)
    if resp:
        data = resp.json()
        total = data.get("totalRows", 0)
        print(f"  Uwazi API: OK ({total} review documents)")
    else:
        print("  Uwazi API: FAILED")
        return False

    # Test UN ODS symbol access
    symbol = "A/HRC/WG.6/43/FRA/1"
    pdf_bytes = fetch_upr_pdf(symbol)
    if pdf_bytes:
        print(f"  UN ODS PDF: OK ({len(pdf_bytes)} bytes for {symbol})")
    else:
        print("  UN ODS PDF: FAILED")
        return False

    # Test PDF extraction
    text = extract_pdf_markdown(
        source=SOURCE_ID,
        source_id="test-FRA-S44-1",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""
    if len(text) > 100:
        print(f"  PDF extraction: OK ({len(text)} chars)")
    else:
        print(f"  PDF extraction: WEAK ({len(text)} chars)")

    print("\nConnectivity test complete.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UN/UPRDocuments data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            bootstrap_full()
