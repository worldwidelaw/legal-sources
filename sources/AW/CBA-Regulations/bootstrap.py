#!/usr/bin/env python3
"""
AW/CBA-Regulations -- Central Bank of Aruba Regulatory Documents Data Fetcher

Fetches regulatory documents (state ordinances, directives, policy papers,
guidelines, AML/CFT docs) from cbaruba.org.

Strategy:
  - Scrape /document/{slug} pages for PDF links and titles
  - Download PDFs via readBlob.do endpoint
  - Extract text using common/pdf_extract or pdfplumber fallback
  - Also scrape HTML content from regulatory description pages

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap --full     # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AW/CBA-Regulations"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AW.CBA-Regulations")

BASE_URL = "https://www.cbaruba.org"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

# Document index pages to scrape
DOCUMENT_PAGES = [
    ("/document/supervision-legislation", "Supervision Legislation"),
    ("/document/supervisory-directives-credit-institutions", "Supervisory Directives - Credit Institutions"),
    ("/document/policy-papers", "Policy Papers"),
    ("/document/guidelines", "Guidelines"),
    ("/document/aml-cft-state-ordinance-downloads", "AML/CFT"),
    ("/document/captive-insurance-companies", "Captive Insurance"),
    ("/document/regulation-ultimate-beneficial-owner-aml-cft-state-ordinance", "UBO Regulation"),
]

# HTML content pages with inline regulatory text
HTML_PAGES = [
    ("/integrity-supervision-enforcement/", "Enforcement Policy"),
    ("/the-aml-cft-framework/", "AML/CFT Framework"),
    ("/financial-sanctions-regulations/", "Financial Sanctions"),
    ("/foreign-exchange-transactions/", "Foreign Exchange"),
]

session = requests.Session()
session.headers.update(HEADERS)


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'</h[1-6]>', '\n\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using available tools."""
    # Try common/pdf_extract first
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from common.pdf_extract import extract_pdf_markdown
        return extract_pdf_markdown(pdf_bytes)
    except Exception:
        pass

    # Fallback to pdfplumber
    try:
        import pdfplumber
        text_parts = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        return "\n\n".join(text_parts)
    except Exception:
        pass

    # Fallback to pypdf
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
        return "\n\n".join(text_parts)
    except Exception:
        pass

    logger.warning("No PDF extraction library available")
    return ""


def scrape_document_page(path: str) -> list[dict]:
    """Scrape a /document/{slug} page for PDF download links and titles."""
    url = f"{BASE_URL}{path}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        documents = []

        # Pattern: <span class="text">Title.pdf</span> ... <a class="dl-link" href="/readBlob.do?id=123">
        # More flexible: find all readBlob.do links and their surrounding context
        pattern = r'<span[^>]*class="text"[^>]*>(.*?)</span>.*?href="(/readBlob\.do\?id=\d+)"'
        matches = re.findall(pattern, html, re.DOTALL)

        if not matches:
            # Alternative pattern: any anchor linking to readBlob.do
            pattern2 = r'<a[^>]*href="(/readBlob\.do\?id=(\d+))"[^>]*>([^<]*)</a>'
            for href, blob_id, text in re.findall(pattern2, html):
                title = text.strip() or f"Document {blob_id}"
                title = re.sub(r'\.pdf$', '', title, flags=re.IGNORECASE).strip()
                documents.append({"title": unescape(title), "blob_path": href, "blob_id": blob_id})
        else:
            for title_raw, href in matches:
                title = clean_html(title_raw).strip()
                title = re.sub(r'\.pdf$', '', title, flags=re.IGNORECASE).strip()
                blob_id_match = re.search(r'id=(\d+)', href)
                blob_id = blob_id_match.group(1) if blob_id_match else ""
                documents.append({"title": unescape(title), "blob_path": href, "blob_id": blob_id})

        return documents
    except requests.RequestException as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return []


def fetch_pdf_document(blob_path: str) -> Optional[str]:
    """Download a PDF via readBlob.do and extract text."""
    url = f"{BASE_URL}{blob_path}"
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()

        if b'%PDF' not in resp.content[:10]:
            logger.warning(f"Not a PDF: {url}")
            return None

        text = extract_pdf_text(resp.content)
        return text if text and len(text) > 20 else None
    except requests.RequestException as e:
        logger.warning(f"Failed to download PDF {url}: {e}")
        return None


def scrape_html_page(path: str) -> Optional[str]:
    """Scrape regulatory description from an HTML content page."""
    url = f"{BASE_URL}{path}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()

        # Extract main content area
        # Look for article or main content div
        content = ""

        # Try common content containers
        for pattern in [
            r'<article[^>]*>(.*?)</article>',
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>\s*</div>',
            r'<div[^>]*class="[^"]*entry[^"]*"[^>]*>(.*?)</div>',
        ]:
            match = re.search(pattern, resp.text, re.DOTALL)
            if match:
                content = match.group(1)
                break

        if not content:
            body_match = re.search(r'<body[^>]*>(.*?)</body>', resp.text, re.DOTALL | re.IGNORECASE)
            if body_match:
                content = body_match.group(1)

        text = clean_html(content)
        return text if text and len(text) > 50 else None
    except requests.RequestException as e:
        logger.warning(f"Failed to scrape HTML page {url}: {e}")
        return None


def normalize(title: str, text: str, url: str, category: str, doc_type: str = "doctrine") -> dict:
    """Transform data to standard schema."""
    doc_id = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:100]
    doc_id = f"aw-cba-{doc_id}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": None,
        "url": url,
        "category": category,
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents. If sample=True, fetch only ~15 records."""
    count = 0
    sample_limit = 15

    # Phase 1: PDF documents from /document/ pages
    for path, category in DOCUMENT_PAGES:
        if sample and count >= sample_limit:
            return

        logger.info(f"Scraping document page: {path}")
        time.sleep(2.0)
        docs = scrape_document_page(path)
        logger.info(f"  Found {len(docs)} PDF documents")

        for doc in docs:
            if sample and count >= sample_limit:
                return

            time.sleep(2.0)
            text = fetch_pdf_document(doc["blob_path"])
            if text:
                url = f"{BASE_URL}{doc['blob_path']}"
                record = normalize(doc["title"], text, url, category)
                count += 1
                yield record
                logger.info(f"  [{count}] {doc['title'][:60]} ({len(text)} chars)")
            else:
                logger.warning(f"  No text extracted: {doc['title'][:60]}")

    # Phase 2: HTML content pages
    if not sample or count < sample_limit:
        for path, category in HTML_PAGES:
            if sample and count >= sample_limit:
                return

            logger.info(f"Scraping HTML page: {path}")
            time.sleep(2.0)
            text = scrape_html_page(path)
            if text:
                url = f"{BASE_URL}{path}"
                record = normalize(category, text, url, category, "doctrine")
                count += 1
                yield record
                logger.info(f"  [{count}] {category} ({len(text)} chars)")

    logger.info(f"Total documents fetched: {count}")


def save_sample(records: list[dict]) -> None:
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for old in SAMPLE_DIR.glob("record_*.json"):
        old.unlink()
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_api() -> bool:
    """Test connectivity to cbaruba.org."""
    try:
        resp = session.get(BASE_URL, timeout=15)
        resp.raise_for_status()
        logger.info(f"Homepage: HTTP {resp.status_code}, {len(resp.text)} bytes")

        # Test a document page
        time.sleep(2.0)
        docs = scrape_document_page("/document/supervision-legislation")
        logger.info(f"Supervision legislation: {len(docs)} documents found")

        if docs:
            time.sleep(2.0)
            text = fetch_pdf_document(docs[0]["blob_path"])
            if text:
                logger.info(f"PDF test: '{docs[0]['title'][:50]}' — {len(text)} chars")
                return True
            else:
                logger.warning("PDF text extraction failed")
                return False

        return len(docs) > 0
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AW/CBA-Regulations data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        is_sample = args.sample or not args.full
        records = []
        for doc in fetch_all(sample=is_sample):
            records.append(doc)

        if records:
            save_sample(records)
            logger.info(f"Bootstrap complete: {len(records)} records")

            texts = [r for r in records if r.get("text") and len(r["text"]) > 50]
            logger.info(f"Records with full text: {len(texts)}/{len(records)}")
            if texts:
                avg_len = sum(len(r["text"]) for r in texts) // len(texts)
                logger.info(f"Average text length: {avg_len} chars")
        else:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
