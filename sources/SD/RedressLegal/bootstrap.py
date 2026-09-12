#!/usr/bin/env python3
"""
SD/RedressLegal — Sudan Legal Resources (REDRESS — Bilingual Laws)

Fetches ~80+ key Sudanese laws in full text from redress.org/sudan-legal-resources/.
PDFs are bilingual (Arabic/English). Text extracted via pdfplumber.

Strategy:
  - Parse the Sudan Legal Resources page for tables of law links
  - Each row: law name, English PDF link, Arabic PDF link
  - Download English PDFs (or Arabic if no English), extract text
  - Derive metadata from law name + filename

Source: https://redress.org/sudan-legal-resources/
Rate limit: 1 req/sec

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "SD/RedressLegal"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SD.RedressLegal")

PAGE_URL = "https://redress.org/sudan-legal-resources/"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/pdf",
    "Accept-Language": "en,ar",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

RATE_LIMIT = 1.5
_last_request = 0.0


def _throttle():
    global _last_request
    now = time.time()
    wait = RATE_LIMIT - (now - _last_request)
    if wait > 0:
        time.sleep(wait)
    _last_request = time.time()


def _get(url, **kwargs):
    _throttle()
    resp = SESSION.get(url, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp


def extract_year(text):
    """Extract a 4-digit year from text like 'Criminal Act (1991)' or filename."""
    match = re.search(r"((?:19|20)\d{2})", text)
    return match.group(1) if match else None


def slug_from_url(url):
    """Create a slug from a PDF URL."""
    filename = unquote(url.split("/")[-1])
    # Remove .pdf extension
    slug = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    # Replace non-alphanum with hyphens
    slug = re.sub(r"[^\w\-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:120]


def extract_pdf_text(pdf_bytes, doc_id):
    """Extract text from PDF bytes via the shared helper.

    This used to call pdfplumber directly. pdfplumber emits glyphs in the order
    the content stream lists them, which for the Arabic half of these bilingual
    PDFs is *visual* order, so those lines were stored character-reversed —
    ``قانون`` as ``نوناق`` (issue #1560). Keyword search then matches nothing,
    and silently: the semantic half of a hybrid index still returns something,
    so the source looks healthy while ranking against gibberish.

    ``common.pdf_extract`` routes RTL-heavy output back through
    ``common.arabic_pdf``, which orders glyph clusters by x-geometry rather than
    trusting the emitted sequence. The English text in these same documents is
    not RTL, so it passes through unchanged.
    """
    try:
        return extract_pdf_markdown(
            SOURCE_ID,
            doc_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
            force=True,
        ) or ""
    except Exception as e:
        logger.error(f"PDF extraction error: {e}")
        return ""


def get_all_law_entries():
    """Parse the Sudan Legal Resources page and return law entries."""
    logger.info(f"Fetching listing page: {PAGE_URL}")
    resp = _get(PAGE_URL)
    soup = BeautifulSoup(resp.text, "html.parser")

    entries = []
    current_category = "Unknown"

    # Find all tables and section headers
    # The page has h2/h3 category headers followed by tables
    for el in soup.find_all(["h2", "h3", "h4", "table"]):
        if el.name in ("h2", "h3", "h4"):
            text = el.get_text(strip=True)
            if text and len(text) < 100:
                current_category = text

        elif el.name == "table":
            rows = el.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue

                # First cell: law name
                law_name = cells[0].get_text(strip=True)
                if not law_name or law_name.lower() in ("law", "legislation", "document", ""):
                    continue

                # Find PDF links in remaining cells
                en_url = None
                ar_url = None
                for i, cell in enumerate(cells[1:], 1):
                    link = cell.find("a", href=True)
                    if link:
                        href = link["href"]
                        if href.endswith(".pdf"):
                            link_text = link.get_text(strip=True).lower()
                            # Second cell is typically English, third is Arabic
                            if i == 1:
                                en_url = href
                            elif i == 2:
                                ar_url = href
                            # Also detect by link text
                            elif "link" == link_text or "english" in link_text:
                                en_url = href
                            elif "اللينك" in link_text or "arabic" in link_text:
                                ar_url = href

                if en_url or ar_url:
                    entries.append({
                        "name": law_name,
                        "category": current_category,
                        "en_url": en_url,
                        "ar_url": ar_url,
                    })

    # Also find standalone PDF links not in tables
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".pdf") and "redress.org/storage/" in href:
            # Check if already captured
            if not any(e.get("en_url") == href or e.get("ar_url") == href for e in entries):
                text = a.get_text(strip=True)
                if text and len(text) > 3:
                    entries.append({
                        "name": text,
                        "category": "Uncategorized",
                        "en_url": href,
                        "ar_url": None,
                    })

    logger.info(f"Found {len(entries)} law entries")
    return entries


def fetch_all(sample=False):
    """Fetch all Sudanese laws."""
    entries = get_all_law_entries()
    if not entries:
        logger.error("No law entries found on listing page")
        return

    seen_urls = set()
    count = 0
    for entry in entries:
        # Prefer English PDF, fallback to Arabic
        pdf_url = entry.get("en_url") or entry.get("ar_url")
        if not pdf_url or pdf_url in seen_urls:
            continue
        seen_urls.add(pdf_url)

        language = "en" if pdf_url == entry.get("en_url") else "ar"

        try:
            logger.info(f"Downloading: {entry['name'][:60]} ({language})")
            resp = _get(pdf_url)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {pdf_url}")
                continue

            doc_id = slug_from_url(pdf_url)

            text = extract_pdf_text(resp.content, doc_id)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text from {pdf_url}: {len(text)} chars")
                continue

            year = extract_year(entry["name"]) or extract_year(pdf_url)
            date = f"{year}-01-01" if year else None

            doc = {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": "legislation",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": entry["name"],
                "text": text,
                "date": date,
                "url": pdf_url,
                "language": language,
                "category": entry["category"],
            }

            yield doc
            count += 1
            logger.info(f"[{count}] {entry['name'][:60]} — {len(text)} chars")

        except Exception as e:
            logger.error(f"Error fetching {pdf_url}: {e}")
            continue

    logger.info(f"Total documents fetched: {count}")


def save_sample(records, output_dir):
    """Save sample records as JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        fname = re.sub(r"[^\w\-]", "_", rec["_id"])[:80] + ".json"
        path = output_dir / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved: {path.name}")


def test_api():
    """Quick connectivity test."""
    print(f"Testing {PAGE_URL} ...")
    resp = _get(PAGE_URL)
    print(f"Status: {resp.status_code}")
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    print(f"Tables found: {len(tables)}")
    pdf_links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".pdf")]
    print(f"PDF links found: {len(pdf_links)}")
    for link in pdf_links[:5]:
        print(f"  - {link.split('/')[-1]}")
    print("API test passed." if pdf_links else "WARNING: No PDF links found!")


def main():
    parser = argparse.ArgumentParser(description="SD/RedressLegal bootstrapper")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Save sample records only")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        records = list(fetch_all(sample=args.sample))
        if records:
            save_sample(records, SAMPLE_DIR)
            print(f"\nBootstrap complete: {len(records)} records saved to {SAMPLE_DIR}")
            text_lens = [len(r.get("text", "")) for r in records]
            print(f"Text lengths: min={min(text_lens)}, max={max(text_lens)}, avg={sum(text_lens)//len(text_lens)}")
        else:
            print("ERROR: No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
