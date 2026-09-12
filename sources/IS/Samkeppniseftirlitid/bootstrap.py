#!/usr/bin/env python3
"""
IS/Samkeppniseftirlitid - Icelandic Competition Authority Decisions Fetcher

Fetches decisions, rulings, opinions, and reports from Samkeppniseftirlitið
via the Algolia search index (metadata) + PDF download (full text).

Data source: https://www.samkeppni.is/urlausnir/
License: Public Domain (official government decisions)
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import pdfplumber
import requests

ALGOLIA_APP_ID = "0A53LHL1EC"
ALGOLIA_API_KEY = "46bb93437537769d958f44b7e3b82ff1"
ALGOLIA_INDEX = "wp_algolia_posts_case"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Samkeppniseftirlitid"
REQUEST_DELAY = 1.0
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
}

ALGOLIA_HEADERS = {
    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
    "X-Algolia-API-Key": ALGOLIA_API_KEY,
    "Content-Type": "application/json",
}


def search_algolia(session: requests.Session, page: int = 0,
                   hits_per_page: int = 100, year_filter: str = None) -> dict:
    """Query Algolia for decision listings."""
    params = f"hitsPerPage={hits_per_page}&page={page}&query="
    if year_filter:
        params += f"&facetFilters=[[\"meta.case_number_year:{year_filter}\"]]"

    resp = session.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS,
                        json={"params": params}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_all_years(session: requests.Session) -> list[str]:
    """Get all available years from Algolia facets."""
    resp = session.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS, json={
        "params": 'hitsPerPage=0&query=&facets=["meta.case_number_year"]'
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    years = data.get("facets", {}).get("meta.case_number_year", {})
    return sorted(years.keys())


def extract_text_from_pdf(session: requests.Session, pdf_url: str) -> Optional[str]:
    """Download PDF and extract text using pdfplumber."""
    try:
        resp = session.get(pdf_url, timeout=60, stream=True)
        resp.raise_for_status()

        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_PDF_SIZE:
            print(f"    Skipping oversized PDF: {int(content_length) / 1024 / 1024:.1f}MB")
            return None

        pdf_bytes = resp.content
        if len(pdf_bytes) > MAX_PDF_SIZE:
            return None

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(text_parts).strip()

    except Exception as e:
        print(f"    PDF extraction error: {e}")
        return None


def normalize_date(hit: dict) -> Optional[str]:
    """Extract and normalize date from Algolia hit."""
    case_date_unix = hit.get("meta", {}).get("case_date_unix")
    if case_date_unix:
        try:
            return datetime.utcfromtimestamp(int(case_date_unix)).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass

    post_date = hit.get("post_date")
    if post_date:
        try:
            return datetime.utcfromtimestamp(int(post_date)).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass

    return None


def process_hit(session: requests.Session, hit: dict) -> Optional[dict]:
    """Process a single Algolia hit into a normalized record."""
    meta = hit.get("meta", {})
    pdf_url = meta.get("avista_acf_file_url") or meta.get("avista_acf_file_external_url")

    if not pdf_url:
        return None

    text = extract_text_from_pdf(session, pdf_url)
    if not text or len(text) < 50:
        return None

    # Clean text
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    case_id = meta.get("case_id", "")
    title = hit.get("post_title", "")
    permalink = hit.get("permalink", "")
    date = normalize_date(hit)

    taxonomies = hit.get("taxonomies", {})
    case_type = (taxonomies.get("type_cases") or [None])[0]
    companies = taxonomies.get("company", [])
    policy_area = (taxonomies.get("policy-area") or [None])[0]
    sector = (taxonomies.get("sector") or [None])[0]

    record_id = case_id if case_id else str(hit.get("post_id", ""))

    return {
        "_id": record_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": title,
        "text": text,
        "date": date,
        "url": permalink,
        "language": "isl",
        "case_number": case_id,
        "case_type": case_type,
        "companies": companies if companies else None,
        "policy_area": policy_area,
        "sector": sector,
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all decisions with full text from PDFs."""
    session = requests.Session()
    session.headers.update(HEADERS)

    years = get_all_years(session)
    print(f"  Found {len(years)} years: {years[0]}-{years[-1]}")

    count = 0
    for year in years:
        if max_records and count >= max_records:
            break

        page = 0
        while True:
            if max_records and count >= max_records:
                break

            print(f"  Fetching year {year}, page {page + 1}...")
            data = search_algolia(session, page=page, year_filter=year)
            hits = data.get("hits", [])

            if not hits:
                break

            for hit in hits:
                if max_records and count >= max_records:
                    break

                time.sleep(REQUEST_DELAY)
                record = process_hit(session, hit)

                if record and len(record.get("text", "")) >= 50:
                    yield record
                    count += 1
                    if count % 5 == 0:
                        print(f"    Fetched {count} records...")

            page += 1
            time.sleep(REQUEST_DELAY)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    for record in fetch_all():
        if record.get("date"):
            try:
                doc_date = datetime.fromisoformat(record["date"])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def normalize(raw: dict) -> dict:
    """Validate and normalize the record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "date", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get("text") or len(raw["text"]) < 50:
        raise ValueError("Document has insufficient text content")

    return raw


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    for i, record in enumerate(fetch_all(max_records=sample_count)):
        try:
            normalized = normalize(record)
            records.append(normalized)

            filename = SAMPLE_DIR / f"record_{i+1:03d}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get("text", ""))
            print(f"  [{i+1:02d}] {normalized['title'][:60]} ({text_len:,} chars)")

        except ValueError as e:
            print(f"  Skipping record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="IS/Samkeppniseftirlitid competition decisions fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "info"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "info":
        print(f"{SOURCE_ID} - Icelandic Competition Authority Decisions")
        print(f"Source URL: https://www.samkeppni.is/urlausnir/")
        print(f"API: Algolia ({ALGOLIA_APP_ID})")
        print(f"Index: {ALGOLIA_INDEX}")

    elif args.command == "bootstrap":
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
