#!/usr/bin/env python3
"""
LK/CBSL - Central Bank of Sri Lanka — Directions, Circulars & Guidelines

Fetches regulatory documents (directions, circulars, guidelines, determinations)
from all CBSL category pages. Each document is a PDF hosted on cbsl.gov.lk.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import hashlib
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
import requests
from bs4 import BeautifulSoup

SOURCE_ID = "LK/CBSL"
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"

BASE_URL = "https://www.cbsl.gov.lk"

# All category pages for directions/circulars/guidelines
CATEGORIES = [
    {
        "slug": "directions-circulars-guidelines-for-banks",
        "label": "Banking",
        "view_class": "view-directions-circulars-and-guidelines-for-banks",
    },
    {
        "slug": "directions-circulars-guidelines-for-non-banks",
        "label": "Non-Banking",
        "view_class": "view-directions-circulars-and-guidelines-for-non-banks",
    },
    {
        "slug": "directions-circulars-guidelines-on-foreign-exchange",
        "label": "Foreign Exchange",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-exchange-control",
        "label": "Exchange Control",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-international-operations",
        "label": "International Operations",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-payments-and-settlements",
        "label": "Payments and Settlements",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-domestic-operations",
        "label": "Market Operations",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-resolution-and-enforcement",
        "label": "Deposit Insurance and Resolution",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-for-micro-fianace-institutions",
        "label": "Micro Finance",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-macroprudential-surveillance",
        "label": "Macroprudential Surveillance",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-public-debt",
        "label": "Public Debt",
        "view_class": None,
    },
    {
        "slug": "directions-circulars-guidelines-on-financial-consumer-relations",
        "label": "Financial Consumer Protection",
        "view_class": None,
    },
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "LegalDataHunter/1.0 (research)"})


def fetch_page(url: str) -> str:
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                print(f"  Failed to fetch {url}: {e}")
                return None


def fetch_pdf_bytes(url: str) -> bytes:
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=60)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            if len(resp.content) > 50_000_000:  # Skip files > 50MB
                print(f"  Skipping oversized PDF: {len(resp.content)} bytes")
                return None
            return resp.content
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                print(f"  Failed to download PDF {url}: {e}")
                return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages)
    except Exception as e:
        print(f"  PDF extraction error: {e}")
        return ""


def parse_date(date_str: str) -> str:
    """Parse DD.MM.YYYY date to ISO format."""
    date_str = date_str.strip()
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_listing_page(html: str, category_label: str) -> list:
    """Parse a listing page and extract document metadata."""
    soup = BeautifulSoup(html, "html.parser")
    docs = []

    # Find all views-row divs that contain /laws/cdg/ PDF links
    all_rows = soup.find_all(
        "div", class_=lambda c: c and "views-row" in c
    )

    for row in all_rows:
        # Must contain a laws/cdg PDF link
        pdf_link = row.find("a", href=lambda h: h and "/laws/cdg/" in h)
        if not pdf_link:
            continue

        pdf_url = pdf_link.get("href", "")
        if not pdf_url.startswith("http"):
            pdf_url = BASE_URL + pdf_url

        # Extract fields from span elements
        date_span = row.find(
            "span",
            class_=lambda c: c and "views-field-field-published-date" in c,
        )
        ref_span = row.find(
            "span",
            class_=lambda c: c
            and "views-field-field-circular-direction-number" in c,
        )
        title_span = row.find(
            "span",
            class_=lambda c: c and "views-field-field-file-title" in c,
        )

        date_raw = date_span.get_text(strip=True) if date_span else ""
        ref_num = ref_span.get_text(strip=True) if ref_span else ""
        title = title_span.get_text(strip=True) if title_span else ""

        if not title:
            title = pdf_link.get_text(strip=True)

        date_iso = parse_date(date_raw) if date_raw else None

        if title and pdf_url:
            docs.append(
                {
                    "title": title,
                    "reference_number": ref_num,
                    "date": date_iso,
                    "date_raw": date_raw,
                    "pdf_url": pdf_url,
                    "category": category_label,
                }
            )

    return docs


def get_max_page(html: str) -> int:
    """Extract the max page number from pagination."""
    soup = BeautifulSoup(html, "html.parser")
    pager = soup.find(class_="pager")
    if not pager:
        return 0

    last_link = pager.find("a", title="Go to last page")
    if last_link:
        href = last_link.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            return int(match.group(1))

    # Fallback: find highest page number in pager links
    max_page = 0
    for link in pager.find_all("a"):
        href = link.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            max_page = max(max_page, int(match.group(1)))
    return max_page


def fetch_all_docs(sample: bool = False) -> list:
    """Fetch document listings from all categories and pages."""
    all_docs = []
    sample_limit = 20 if sample else None

    for cat in CATEGORIES:
        if sample_limit and len(all_docs) >= sample_limit:
            break

        cat_url = f"{BASE_URL}/en/laws/{cat['slug']}"
        print(f"Fetching category: {cat['label']} ({cat_url})")

        html = fetch_page(cat_url)
        if not html:
            print(f"  Skipping category (no response)")
            continue

        docs = parse_listing_page(html, cat["label"])
        max_page = get_max_page(html)
        print(f"  Page 0: {len(docs)} docs, max_page={max_page}")
        all_docs.extend(docs)

        if sample_limit and len(all_docs) >= sample_limit:
            break

        # Fetch remaining pages
        for page_num in range(1, max_page + 1):
            if sample_limit and len(all_docs) >= sample_limit:
                break

            page_url = f"{cat_url}?page={page_num}"
            time.sleep(1)
            html = fetch_page(page_url)
            if not html:
                break

            docs = parse_listing_page(html, cat["label"])
            if not docs:
                break

            print(f"  Page {page_num}: {len(docs)} docs")
            all_docs.extend(docs)

    # Deduplicate by PDF URL
    seen = set()
    unique = []
    for doc in all_docs:
        if doc["pdf_url"] not in seen:
            seen.add(doc["pdf_url"])
            unique.append(doc)

    print(f"\nTotal unique documents found: {len(unique)}")
    return unique


def normalize(raw: dict, text: str) -> dict:
    """Normalize a document record."""
    doc_id = hashlib.sha256(raw["pdf_url"].encode()).hexdigest()[:16]
    ref = raw.get("reference_number", "")
    title = raw["title"]
    if ref and ref not in title:
        full_title = f"{ref} — {title}"
    else:
        full_title = title

    doc_type = "legislation"
    ref_lower = ref.lower() if ref else ""
    if any(
        kw in ref_lower
        for kw in ["circular", "guideline", "operating instruction"]
    ):
        doc_type = "doctrine"

    return {
        "_id": f"LK-CBSL-{doc_id}",
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": text,
        "date": raw.get("date"),
        "url": raw["pdf_url"],
        "reference_number": ref,
        "category": raw.get("category", ""),
        "jurisdiction": "LK",
    }


def bootstrap(sample: bool = False):
    """Main bootstrap: fetch listings, download PDFs, extract text."""
    out_dir = SAMPLE_DIR if sample else DATA_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    docs = fetch_all_docs(sample=sample)
    limit = 15 if sample else len(docs)

    success = 0
    for i, doc in enumerate(docs[:limit]):
        print(
            f"\n[{i+1}/{min(limit, len(docs))}] {doc['title'][:70]}..."
        )
        print(f"  PDF: {doc['pdf_url']}")

        time.sleep(1)
        pdf_bytes = fetch_pdf_bytes(doc["pdf_url"])
        if not pdf_bytes:
            print("  SKIP: could not download PDF")
            continue

        text = extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            print(f"  SKIP: insufficient text ({len(text)} chars)")
            continue

        record = normalize(doc, text)
        fname = f"{record['_id']}.json"
        with open(out_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  OK: {len(text)} chars, saved as {fname}")
        success += 1

    print(f"\nDone: {success}/{min(limit, len(docs))} records saved to {out_dir}")
    return success


def test():
    """Test connectivity and basic parsing."""
    url = f"{BASE_URL}/en/laws/directions-circulars-guidelines-for-banks"
    print(f"Testing: {url}")
    html = fetch_page(url)
    if not html:
        print("FAIL: could not fetch page")
        return False

    docs = parse_listing_page(html, "Banking")
    print(f"Found {len(docs)} documents on page 1")

    if docs:
        doc = docs[0]
        print(f"First doc: {doc['title']}")
        print(f"  Date: {doc['date']}")
        print(f"  Ref: {doc['reference_number']}")
        print(f"  PDF: {doc['pdf_url']}")

        pdf_bytes = fetch_pdf_bytes(doc["pdf_url"])
        if pdf_bytes:
            text = extract_text_from_pdf(pdf_bytes)
            print(f"  Text length: {len(text)} chars")
            print(f"  Preview: {text[:200]}...")
            return len(text) > 50
        else:
            print("  FAIL: could not download PDF")
            return False

    return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="LK/CBSL bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "test"], help="Command to run"
    )
    parser.add_argument(
        "--sample", action="store_true", help="Fetch sample records only"
    )
    args = parser.parse_args()

    if args.command == "test":
        ok = test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)
