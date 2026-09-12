#!/usr/bin/env python3
"""TL/ATTL-TaxRulings — Timor-Leste Tax Authority Public Rulings.

Fetches public rulings from attl.gov.tl/public-ruling/.
PDFs are downloaded and text extracted via pdfminer.
"""

import argparse
import html as html_mod
import io
import json
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

SOURCE_ID = "TL/ATTL-TaxRulings"
BASE_URL = "https://attl.gov.tl"
RULINGS_PAGE = f"{BASE_URL}/public-ruling/"
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    if _pdf_extract is None:
        raise ImportError("pdfminer.six is required: pip install pdfminer.six")
    return _pdf_extract(io.BytesIO(pdf_bytes))


def discover_rulings():
    """Scrape the public-ruling page and yield document metadata."""
    resp = session.get(RULINGS_PAGE, timeout=30)
    resp.raise_for_status()
    page_html = resp.text

    # Find all PDF links
    pdf_pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    # Find section/row context — table rows with ruling info
    # The page uses tables with ruling number, title, and language links
    seen_urls = set()
    for match in pdf_pattern.finditer(page_html):
        pdf_url = match.group(1).strip()
        link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

        if not pdf_url or pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        # Make URL absolute
        if not pdf_url.startswith("http"):
            pdf_url = f"{BASE_URL}/{pdf_url.lstrip('/')}"

        # Detect language from URL or link text
        lang = "en"
        url_lower = pdf_url.lower()
        if "_por" in url_lower or "_pt" in url_lower or "portuguese" in url_lower:
            lang = "pt"
        elif "_indo" in url_lower or "_ind" in url_lower or "indonesian" in url_lower:
            lang = "id"
        elif "tetun" in url_lower or "-tt" in url_lower.split("/")[-1]:
            lang = "tet"

        # Extract ruling number from filename
        filename = Path(pdf_url).stem
        ruling_num = None
        num_match = re.search(r'(\d{4})[_-](\d{1,2})', filename)
        if num_match:
            ruling_num = f"{num_match.group(1)}/{num_match.group(2)}"

        # Derive title from link text or nearby context
        title = html_mod.unescape(link_text) if link_text else filename.replace("-", " ").replace("_", " ")

        # Generate doc_id
        doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)

        yield {
            "doc_id": doc_id,
            "title": title,
            "pdf_url": pdf_url,
            "language": lang,
            "ruling_number": ruling_num,
        }


def normalize(raw: dict) -> dict:
    """Normalize a raw document into the standard schema."""
    now = datetime.now(timezone.utc).isoformat()
    doc_id = f"tl-attl-{raw['doc_id']}"

    # Try to extract a year from ruling number or URL
    date = None
    if raw.get("ruling_number"):
        year_match = re.match(r'(\d{4})/', raw["ruling_number"])
        if year_match:
            date = f"{year_match.group(1)}-01-01"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": now,
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("pdf_url", RULINGS_PAGE),
        "doc_id": doc_id,
        "language": raw.get("language", "en"),
        "ruling_number": raw.get("ruling_number"),
        "pdf_url": raw.get("pdf_url", ""),
    }


def fetch_all():
    """Yield all normalized documents."""
    for doc in discover_rulings():
        try:
            print(f"  Fetching: {doc['title'][:60]}...", file=sys.stderr)
            resp = session.get(doc["pdf_url"], timeout=60)
            resp.raise_for_status()

            text = extract_text_from_pdf(resp.content)
            if len(text.strip()) < 50:
                print(f"  [SKIP] Insufficient text for {doc['title']}", file=sys.stderr)
                continue

            doc["text"] = text.strip()
            yield normalize(doc)
            time.sleep(1.5)
        except Exception as e:
            print(f"  [ERROR] {doc['title']}: {e}", file=sys.stderr)
            time.sleep(2)


def bootstrap_sample(limit: int = 15):
    """Fetch sample documents and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetch_all():
        if count >= limit:
            break
        fname = SAMPLE_DIR / f"record_{count:04d}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        text_len = len(record.get("text", ""))
        print(f"  [{count+1}/{limit}] {record['title'][:60]} ({text_len} chars)")
        count += 1

    print(f"\nSaved {count} samples to {SAMPLE_DIR}")
    return count


def main():
    parser = argparse.ArgumentParser(description="TL/ATTL-TaxRulings bootstrap")
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
