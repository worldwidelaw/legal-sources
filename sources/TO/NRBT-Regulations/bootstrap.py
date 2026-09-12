#!/usr/bin/env python3
"""TO/NRBT-Regulations — National Reserve Bank of Tonga.

Fetches legislation, regulations, and prudential statements from
reservebank.to. PDFs downloaded and text extracted via pdfminer.
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

try:
    from pdfminer.high_level import extract_text as _pdf_extract
except ImportError:
    _pdf_extract = None

SOURCE_ID = "TO/NRBT-Regulations"
BASE_URL = "https://www.reservebank.to"
PAGES = [
    f"{BASE_URL}/index.php/about-us/our-regulatory-frameworks",
    f"{BASE_URL}/index.php/about-us/prudential-statements",
]
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    if _pdf_extract is None:
        raise ImportError("pdfminer.six is required: pip install pdfminer.six")
    return _pdf_extract(io.BytesIO(pdf_bytes))


def discover_documents():
    """Scrape regulatory framework and prudential statements pages for PDFs."""
    seen_urls = set()
    pdf_pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    for page_url in PAGES:
        try:
            resp = session.get(page_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [ERROR] Failed to fetch {page_url}: {e}", file=sys.stderr)
            continue

        page_html = resp.text

        for match in pdf_pattern.finditer(page_html):
            pdf_url = match.group(1).strip()
            link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip()

            if not pdf_url:
                continue

            # Make URL absolute
            if pdf_url.startswith("/"):
                pdf_url = f"{BASE_URL}{pdf_url}"
            elif not pdf_url.startswith("http"):
                pdf_url = urljoin(page_url, pdf_url)

            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            url_lower = pdf_url.lower()
            filename = Path(pdf_url).stem

            # Skip Tongan translations — keep English only
            if filename.endswith("x") and ("Lao" in pdf_url or "Ngaahi" in pdf_url):
                continue
            if any(tok in url_lower for tok in [
                "fakafonua", "laoki", "ngaahi", "tuutuuni",
                "fetongi", "paanga", "faihi",
            ]):
                continue

            # Skip non-legislation PDFs (reports, data sheets, currency info)
            skip_dirs = ["economics/", "currency/", "fsystems/"]
            if any(d in url_lower for d in skip_dirs):
                continue

            lang = "en"

            # Derive title from filename if link text is generic
            generic_texts = ["read the english version", "read the tongan version",
                             "english", "tongan", ""]
            if link_text.lower().strip() in generic_texts:
                # Derive from filename: "BankingAct2020_1" -> "Banking Act 2020"
                raw_name = filename.split("_")[0] if "_" in filename else filename
                # CamelCase to spaced
                title = re.sub(r'([a-z])([A-Z])', r'\1 \2', raw_name)
                title = re.sub(r'(\d{4})', r' \1', title).strip()
                # Fix common preposition run-ons
                for prep in ["of", "and", "on", "in", "for", "the", "to"]:
                    title = title.replace(f"{prep.title()} ", f"{prep} ")
                    title = re.sub(rf'(\w){prep}\b', rf'\1 {prep}', title)
            else:
                title = link_text

            # Extract year from URL or filename
            year = None
            year_match = re.search(r'/(\d{4})/', pdf_url)
            if year_match:
                year = year_match.group(1)
            else:
                year_match2 = re.search(r'(\d{4})', filename)
                if year_match2:
                    year = year_match2.group(1)

            doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)

            yield {
                "doc_id": doc_id,
                "title": title,
                "pdf_url": pdf_url,
                "language": lang,
                "year": year,
            }


def normalize(raw: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    doc_id = f"to-nrbt-{raw['doc_id']}"
    date = f"{raw['year']}-01-01" if raw.get("year") else None

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": now,
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("pdf_url", ""),
        "doc_id": doc_id,
        "language": raw.get("language", "en"),
        "pdf_url": raw.get("pdf_url", ""),
    }


def fetch_all():
    for doc in discover_documents():
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
    parser = argparse.ArgumentParser(description="TO/NRBT-Regulations bootstrap")
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
