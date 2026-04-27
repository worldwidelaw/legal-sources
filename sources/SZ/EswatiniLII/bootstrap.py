#!/usr/bin/env python3
"""
SZ/EswatiniLII — Eswatini Legislation Fetcher

Fetches Eswatini legislation from EswatiniLII (eswatinilii.org), a Laws.Africa
PeachJam platform operated by AfricanLII.

Strategy:
  - Paginated listing at /legislation/?page=N (50 per page, ~7 pages)
  - Extract AKN URLs from listing pages
  - Fetch full text from document pages (Akoma Ntoso HTML embedded)
  - Skip PDF-only documents (no text extraction available)
  - English language

Source: https://eswatinilii.org/
Rate limit: 5 sec crawl delay (per robots.txt)

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "SZ/EswatiniLII"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SZ.EswatiniLII")

BASE_URL = "https://eswatinilii.org"
MAX_PAGES = 15

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def list_legislation() -> list:
    """Fetch all legislation URLs from paginated listing pages."""
    all_docs = []
    seen_urls = set()

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}/legislation/?page={page}"
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch listing page {page}: {e}")
            break

        html = resp.text
        links = re.findall(
            r'href="(/akn/sz/act/[^"]+)"[^>]*>\s*(.*?)\s*</a>',
            html,
            re.DOTALL,
        )

        page_docs = []
        for doc_url, title_html in links:
            if doc_url in seen_urls:
                continue
            seen_urls.add(doc_url)
            title = re.sub(r'<[^>]+>', '', title_html).strip()
            if title and len(title) > 3:
                page_docs.append({"url": doc_url, "title": title})

        if not page_docs:
            logger.info(f"Page {page}: no new documents, stopping")
            break

        all_docs.extend(page_docs)
        logger.info(f"Page {page}: {len(page_docs)} documents (total: {len(all_docs)})")
        time.sleep(5.0)

    return all_docs


def parse_akn_url(url: str) -> dict:
    """Parse metadata from an AKN URL like /akn/sz/act/2005/6/eng@2005-09-01."""
    parts = url.strip("/").split("/")
    meta = {"doc_subtype": "", "year": "", "number": "", "date": ""}
    if len(parts) >= 5:
        meta["doc_subtype"] = parts[3] if len(parts) > 3 else ""
    if len(parts) >= 6:
        meta["year"] = parts[4] if len(parts) > 4 else ""

    for i, p in enumerate(parts):
        if "@" in p:
            meta["number"] = parts[i - 1] if i > 0 else ""
            date_part = p.split("@")[1] if "@" in p else ""
            meta["date"] = date_part
            break

    return meta


def fetch_document_text(doc_url: str) -> str:
    """Fetch full text from a document page."""
    full_url = f"{BASE_URL}{doc_url}"
    try:
        resp = SESSION.get(full_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch document {doc_url}: {e}")
        return ""

    html = resp.text

    # Skip PDF-only documents
    if "Loading PDF" in html and "akn-body" not in html:
        return ""

    # Try to find akn-body content (main law text)
    m = re.search(r'class="akn-body">(.*?)(?=</section>\s*(?:<div|<footer)|$)', html, re.DOTALL)
    if m:
        return clean_html(m.group(1))

    # Try preamble + body combined
    text_parts = []
    for cls in ["akn-preamble", "akn-body"]:
        pm = re.search(rf'class="{cls}">(.*?)(?=</section>|</div>\s*<section)', html, re.DOTALL)
        if pm:
            text_parts.append(clean_html(pm.group(1)))
    if text_parts:
        return "\n\n".join(text_parts)

    # Try akn-akomaNtoso content (full document)
    m2 = re.search(r'class="akn-akomaNtoso">(.*?)(?=</la-akoma-ntoso)', html, re.DOTALL)
    if m2:
        return clean_html(m2.group(1))

    return ""


def normalize(doc_info: dict, full_text: str) -> dict:
    """Normalize a document record to standard schema."""
    url = doc_info["url"]
    meta = parse_akn_url(url)

    doc_id = url.replace("/", "_").replace("@", "_").strip("_")
    doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_id)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc_info["title"],
        "text": full_text,
        "date": meta.get("date", ""),
        "url": f"{BASE_URL}{url}",
        "doc_subtype": meta.get("doc_subtype", ""),
        "number": meta.get("number", ""),
        "year": meta.get("year", ""),
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all legislation records."""
    logger.info("Listing all legislation...")
    docs = list_legislation()
    logger.info(f"Found {len(docs)} legislation documents")

    count = 0
    errors = 0
    limit = 15 if sample else None
    skipped_pdf = 0

    for doc_info in docs:
        if limit and count >= limit:
            break

        try:
            full_text = fetch_document_text(doc_info["url"])
        except Exception as e:
            logger.error(f"Error fetching {doc_info['url']}: {e}")
            errors += 1
            if errors > 20:
                logger.error("Too many errors, stopping")
                break
            continue

        if not full_text:
            skipped_pdf += 1
            continue

        record = normalize(doc_info, full_text)
        count += 1
        logger.info(f"[{count}] {record['title'][:60]}... ({len(full_text)} chars)")
        yield record
        time.sleep(5.0)

    logger.info(f"Done. Fetched {count} documents, skipped {skipped_pdf} PDF-only.")


def save_sample(record: dict) -> None:
    """Save a record to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{record['_id'][:80]}.json"
    path = SAMPLE_DIR / fname
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def cmd_test_api():
    """Test connectivity."""
    print("Testing EswatiniLII connectivity...")
    resp = SESSION.get(f"{BASE_URL}/legislation/", timeout=15)
    print(f"Status: {resp.status_code}")
    links = re.findall(r'href="(/akn/sz/act/[^"]+)"', resp.text)
    unique = len(set(links))
    print(f"OK: Found {unique} legislation links on first page")

    if links:
        text = fetch_document_text(links[0])
        print(f"Document text: {len(text)} chars")
        if text:
            print(f"Preview: {text[:200]}...")
        else:
            print("First doc is PDF-only, trying next...")
            for link in links[1:5]:
                text = fetch_document_text(link)
                if text:
                    print(f"Document text: {len(text)} chars")
                    print(f"Preview: {text[:200]}...")
                    break
                time.sleep(5.0)


def cmd_bootstrap(sample: bool = False):
    """Run the bootstrap."""
    count = 0
    for record in fetch_all(sample=sample):
        if sample:
            save_sample(record)
        count += 1

    if sample:
        print(f"\nSaved {count} sample records to {SAMPLE_DIR}/")
    else:
        print(f"\nFetched {count} legislation records.")

    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SZ/EswatiniLII Legislation Fetcher")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test connectivity")
    boot = sub.add_parser("bootstrap", help="Fetch legislation")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command == "bootstrap":
        cmd_bootstrap(sample=args.sample)
    else:
        parser.print_help()
        sys.exit(1)
