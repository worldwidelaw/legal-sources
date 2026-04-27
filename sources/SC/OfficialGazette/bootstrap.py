#!/usr/bin/env python3
"""
SC/OfficialGazette — Seychelles Official Gazette Fetcher

Fetches Acts, Statutory Instruments, and Bills from gazette.sc.
Documents are PDF files listed on Drupal Views pages by type and year.

Strategy:
  - Browse listing pages: /v/{type}/{year} for act, si, bill
  - Paginate with Drupal Views AJAX: POST /views/ajax
  - Download each PDF and extract text via common/pdf_extract
  - Years: 2020 to present

Source: https://www.gazette.sc/
Rate limit: 1.5 sec between requests

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
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from common.pdf_extract import extract_pdf_markdown
except ImportError:
    extract_pdf_markdown = None

SOURCE_ID = "SC/OfficialGazette"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SC.OfficialGazette")

BASE_URL = "https://www.gazette.sc"

DOC_TYPES = [
    {"slug": "act", "label": "Act", "data_type": "legislation"},
    {"slug": "si", "label": "Statutory Instrument", "data_type": "legislation"},
    {"slug": "bill", "label": "Bill", "data_type": "legislation"},
]

MIN_YEAR = 2020
MAX_YEAR = datetime.now().year

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def extract_pdf_text_fallback(pdf_bytes: bytes) -> str:
    """Fallback PDF text extraction using pdfplumber or pypdf."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except ImportError:
        pass

    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)
    except ImportError:
        pass

    try:
        from PyPDF2 import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)
    except ImportError:
        pass

    return ""


def download_and_extract_pdf(pdf_url: str, source_id: str = "") -> str:
    """Download PDF and extract text. Uses common extractor if available, else fallback."""
    if extract_pdf_markdown:
        try:
            md = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=source_id,
                pdf_url=pdf_url,
                table="legislation",
            )
            if md:
                return md
        except Exception as e:
            logger.debug(f"common.pdf_extract failed, using fallback: {e}")

    try:
        resp = SESSION.get(pdf_url, timeout=120)
        resp.raise_for_status()
        return extract_pdf_text_fallback(resp.content)
    except Exception as e:
        logger.warning(f"Failed to download PDF {pdf_url}: {e}")
        return ""


def scrape_listing_page(doc_type_slug: str, year: int, page: int = 0) -> list:
    """Scrape a single listing page and return list of (title, pdf_url, doc_number) tuples."""
    if page == 0:
        url = f"{BASE_URL}/v/{doc_type_slug}/{year}"
    else:
        url = f"{BASE_URL}/v/{doc_type_slug}/{year}?page={page}"

    try:
        resp = SESSION.get(url, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch listing {url}: {e}")
        return []

    html = resp.text
    results = []

    # Each document is in a views-row div. The title link is inside
    # views-field-nothing, not the number-only link in views-field-field-number.
    # Pattern: <div class="views-field views-field-nothing">...<a href="PDF_URL">TITLE</a>...
    row_pattern = re.compile(
        r'views-field-nothing.*?<a\s+href="([^"]*?/sites/default/files/[^"]*?\.pdf)"[^>]*>\s*(.*?)\s*</a>',
        re.DOTALL | re.IGNORECASE,
    )

    for match in row_pattern.finditer(html):
        raw_url = match.group(1)
        title_html = match.group(2)
        title = re.sub(r'<[^>]+>', '', title_html).strip()
        if not title or len(title) < 5:
            continue

        pdf_url = urljoin(BASE_URL, raw_url)
        results.append((title, pdf_url))

    return results


def list_documents_for_type_year(doc_type_slug: str, year: int, max_pages: int = 20) -> list:
    """List all documents for a given type and year, paginating as needed."""
    all_docs = []
    seen_urls = set()

    for page in range(0, max_pages):
        items = scrape_listing_page(doc_type_slug, year, page)
        new_count = 0
        for title, pdf_url in items:
            if pdf_url not in seen_urls:
                seen_urls.add(pdf_url)
                all_docs.append((title, pdf_url))
                new_count += 1

        if new_count == 0:
            break

        time.sleep(1.5)

    return all_docs


def parse_doc_number(title: str, doc_type_slug: str) -> str:
    """Extract document number from title like 'Act 23 - 2024 - Title'."""
    prefixes = {"act": r"Act", "si": r"SI", "bill": r"Bill"}
    prefix = prefixes.get(doc_type_slug, doc_type_slug.upper())
    m = re.match(rf'{prefix}\s+(\d+)', title, re.IGNORECASE)
    if m:
        return m.group(1)
    return ""


def parse_year_from_title(title: str) -> str:
    """Extract year from title."""
    m = re.search(r'(\d{4})', title)
    return m.group(1) if m else ""


def clean_title(title: str) -> str:
    """Remove duplicate prefix like 'Act 6 2026 - Act 6 2026 - Title' → 'Act 6 2026 - Title'."""
    m = re.match(r'^(.+?\d{4})\s*-\s*\1\s*-\s*(.+)$', title)
    if m:
        return f"{m.group(1)} - {m.group(2)}"
    return title


def normalize(title: str, text: str, pdf_url: str, doc_type: dict, year: int) -> dict:
    """Normalize a document record to standard schema."""
    title = clean_title(title)
    doc_number = parse_doc_number(title, doc_type["slug"])
    doc_year = parse_year_from_title(title) or str(year)

    slug = doc_type["slug"]
    num_part = f"-{doc_number}" if doc_number else ""
    doc_id = f"sc-gazette-{slug}{num_part}-{doc_year}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": doc_type["data_type"],
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.strip(),
        "text": text,
        "date": f"{doc_year}-01-01" if doc_year else None,
        "url": pdf_url,
        "doc_type": doc_type["label"],
        "doc_number": doc_number,
        "year": int(doc_year) if doc_year else year,
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all gazette documents."""
    count = 0
    errors = 0
    empty = 0
    limit = 15 if sample else None

    for doc_type in DOC_TYPES:
        if limit and count >= limit:
            break

        slug = doc_type["slug"]
        label = doc_type["label"]
        logger.info(f"Processing {label}s ({slug})...")

        years = range(MAX_YEAR, MIN_YEAR - 1, -1)

        for year in years:
            if limit and count >= limit:
                break

            docs = list_documents_for_type_year(slug, year, max_pages=2 if sample else 20)
            if not docs:
                continue

            logger.info(f"  {slug}/{year}: {len(docs)} documents found")

            for title, pdf_url in docs:
                if limit and count >= limit:
                    break

                doc_id = f"sc-gazette-{slug}-{parse_doc_number(title, slug) or 'x'}-{year}"
                logger.info(f"  Fetching: {title[:80]}...")

                text = download_and_extract_pdf(pdf_url, source_id=doc_id)

                if not text or len(text.strip()) < 50:
                    logger.warning(f"  Empty/short text for: {title}")
                    empty += 1
                    continue

                record = normalize(title, text, pdf_url, doc_type, year)
                count += 1
                yield record

                time.sleep(1.5)

    logger.info(f"Done. {count} records, {errors} errors, {empty} empty.")


def test_api():
    """Test connectivity and list available documents."""
    print(f"Testing {BASE_URL}...")

    for doc_type in DOC_TYPES:
        slug = doc_type["slug"]
        url = f"{BASE_URL}/v/{slug}/{MAX_YEAR}"
        try:
            resp = SESSION.get(url, timeout=15)
            print(f"  {slug}/{MAX_YEAR}: HTTP {resp.status_code} ({len(resp.text)} bytes)")

            items = scrape_listing_page(slug, MAX_YEAR)
            print(f"    Found {len(items)} documents")
            for title, pdf_url in items[:3]:
                print(f"      - {title[:70]}")
                print(f"        {pdf_url}")
        except Exception as e:
            print(f"  {slug}/{MAX_YEAR}: FAILED — {e}")

    # Test PDF download
    print("\nTesting PDF download...")
    test_items = scrape_listing_page("act", MAX_YEAR)
    if test_items:
        title, pdf_url = test_items[0]
        print(f"  Downloading: {title[:60]}...")
        text = download_and_extract_pdf(pdf_url, "test")
        print(f"  Text length: {len(text)} chars")
        if text:
            print(f"  First 300 chars:\n{text[:300]}")
    else:
        print("  No test items found")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        count += 1
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"  Saved: {fname.name} ({len(record.get('text', ''))} chars)")

    logger.info(f"Bootstrap complete. {count} records saved to {SAMPLE_DIR}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SC/OfficialGazette bootstrap")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test API connectivity")

    bp = sub.add_parser("bootstrap", help="Fetch and save documents")
    bp.add_argument("--sample", action="store_true", help="Fetch only ~15 sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample or not args.full)
    else:
        parser.print_help()
