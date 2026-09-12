#!/usr/bin/env python3
"""BB/ParliamentLaws — Barbados Parliament Laws & Statutory Instruments.

Fetches consolidated statutes from barbadosparliament-laws.com via AJAX
listing endpoints + direct PDF downloads.
"""

import argparse
import json
import io
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    from pdfminer.high_level import extract_text as _pdf_extract
except ImportError:
    _pdf_extract = None

SOURCE_ID = "BB/ParliamentLaws"
BASE_URL = "https://www.barbadosparliament-laws.com"
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/html, */*",
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    if _pdf_extract is None:
        raise ImportError("pdfminer.six is required: pip install pdfminer.six")
    return _pdf_extract(io.BytesIO(pdf_bytes))


def list_documents(doc_type: str = "BrowseTitle", max_pages: int = 20):
    """Yield (doc_id, doc_path, title_hint) from paginated AJAX listing."""
    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}/en/{doc_type}"
        params = {"page": page}
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [WARN] Page {page} of {doc_type} failed: {e}", file=sys.stderr)
            break

        html = data.get("view", "")
        if not html:
            break

        # Extract showdoc links: /en/showdoc/{type}/{id}
        links = re.findall(r'/en/showdoc/(\w+)/(\d+)', html)
        if not links:
            break

        seen = set()
        for dtype, did in links:
            key = f"{dtype}/{did}"
            if key not in seen:
                seen.add(key)
                yield did, f"/en/showdoc/{dtype}/{did}", dtype

        time.sleep(1)


def fetch_document(doc_path: str):
    """Fetch a single document page and extract PDF URL + metadata."""
    url = f"{BASE_URL}{doc_path}"
    resp = session.get(url, timeout=30, headers={
        **HEADERS,
        "X-Requested-With": "",  # Regular page request
    })
    resp.raise_for_status()
    html = resp.text

    # Extract title
    title_match = re.search(r'<title>([^<]+)', html)
    title = title_match.group(1).strip() if title_match else "Unknown"
    title = title.replace(" | Barbados Parliament", "").strip()

    # Extract PDF URL from uploads path
    pdf_patterns = [
        r'(https?://www\.barbadosparliament-laws\.com/uploads/[^"\'<>\s]+\.pdf)',
        r'(uploads/[^"\'<>\s]+\.pdf)',
    ]
    pdf_url = None
    for pat in pdf_patterns:
        match = re.search(pat, html)
        if match:
            pdf_url = match.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = f"{BASE_URL}/{pdf_url}"
            break

    # Extract date if available
    date_match = re.search(r'(\w+ \d{1,2},?\s*\d{4})', html)
    date_str = None
    if date_match:
        try:
            from dateutil.parser import parse as dateparse
            dt = dateparse(date_match.group(1))
            date_str = dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return {
        "title": title,
        "pdf_url": pdf_url,
        "date": date_str,
        "page_url": url,
    }


def download_and_extract_pdf(pdf_url: str) -> str:
    """Download PDF and extract text."""
    resp = session.get(pdf_url, timeout=60, headers={
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
    })
    resp.raise_for_status()
    return extract_text_from_pdf(resp.content)


def normalize(raw: dict) -> dict:
    """Normalize a raw document into the standard schema."""
    now = datetime.now(timezone.utc).isoformat()
    doc_id = f"bb-parl-{raw['doc_type']}-{raw['doc_id']}"
    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": now,
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("page_url", ""),
        "doc_id": doc_id,
        "doc_type": raw.get("doc_type", "cs"),
        "pdf_url": raw.get("pdf_url", ""),
    }


def fetch_all():
    """Yield all normalized documents."""
    for doc_id, doc_path, doc_type in list_documents("BrowseTitle"):
        try:
            meta = fetch_document(doc_path)
            if not meta.get("pdf_url"):
                print(f"  [SKIP] No PDF for {doc_path}", file=sys.stderr)
                continue

            text = download_and_extract_pdf(meta["pdf_url"])
            if len(text.strip()) < 50:
                print(f"  [SKIP] Insufficient text for {doc_path}", file=sys.stderr)
                continue

            raw = {
                "doc_id": doc_id,
                "doc_type": doc_type,
                "title": meta["title"],
                "text": text,
                "date": meta.get("date"),
                "page_url": meta["page_url"],
                "pdf_url": meta["pdf_url"],
            }
            yield normalize(raw)
            time.sleep(1.5)
        except Exception as e:
            print(f"  [ERROR] {doc_path}: {e}", file=sys.stderr)
            time.sleep(2)


def bootstrap_sample(limit: int = 15):
    """Fetch sample documents and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetch_all():
        if count >= limit:
            break
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        text_len = len(record.get("text", ""))
        print(f"  [{count+1}/{limit}] {record['title'][:60]} ({text_len} chars)")
        count += 1

    print(f"\nSaved {count} samples to {SAMPLE_DIR}")
    return count


def main():
    parser = argparse.ArgumentParser(description="BB/ParliamentLaws bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run bootstrapper")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--limit", type=int, default=15, help="Sample limit")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            count = bootstrap_sample(args.limit)
            if count < 10:
                print(f"WARNING: Only {count} samples collected", file=sys.stderr)
                sys.exit(1)
        else:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
