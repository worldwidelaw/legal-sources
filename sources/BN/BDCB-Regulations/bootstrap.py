#!/usr/bin/env python3
"""
BN/BDCB-Regulations -- Brunei Darussalam Central Bank Regulations & Guidelines

Fetches regulatory documents (guidelines, notices, legislation, directives) from
the BDCB website. Documents are PDFs hosted on cms.bdcb.gov.bn.

Strategy:
  1. Paginate through HTML listing at bdcb.gov.bn/regulatory/regulations
  2. Extract document metadata (title, date, URL, topic)
  3. Download PDFs and extract text via pdfplumber
  4. Skip non-PDF files (.xlsx forms)

Usage:
  python bootstrap.py bootstrap          # Full pull (~342 documents)
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.BDCB-Regulations")

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"
LISTING_URL = "https://www.bdcb.gov.bn/regulatory/regulations?page={page}"
REQUEST_DELAY = 1.5
MAX_PAGES = 40  # safety cap; actual ~35 pages

# Regex patterns for parsing listing pages
DATE_RE = re.compile(r"Published Date:\s*(\d{1,2}\s+\w+\s+\d{4})")
LINK_RE = re.compile(
    r'<a\s+href="(https://cms\.bdcb\.gov\.bn/storage/uploads/regulatories/[^"]+)"'
    r'[^>]*>([^<]+)</a>'
)
TAG_RE = re.compile(r"<[^>]+>")
TOTAL_RE = re.compile(r"Showing \d+-\d+ of (\d+) results")

# Infer document type from title
DOC_TYPE_KEYWORDS = {
    "guidelines": "guidelines",
    "guideline": "guidelines",
    "notice": "notice",
    "directive": "directive",
    "order": "legislation",
    "act": "legislation",
    "regulation": "legislation",
}


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    import html
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _infer_doc_type(title: str) -> str:
    """Infer document type from title keywords."""
    title_lower = title.lower()
    for keyword, dtype in DOC_TYPE_KEYWORDS.items():
        if keyword in title_lower:
            return dtype
    return "doctrine"


def _parse_date(date_str: str) -> str:
    """Parse date string like '21 May 2026' to ISO format."""
    try:
        dt = datetime.strptime(date_str.strip(), "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        try:
            dt = datetime.strptime(date_str.strip(), "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None


def _fetch_url(url: str, timeout: int = 30) -> bytes:
    """Fetch URL content with proper headers."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _fetch_listing_page(page: int) -> list:
    """Fetch a listing page and extract document metadata.

    Returns list of dicts: {url, title, date, doc_type}
    """
    url = LISTING_URL.format(page=page)
    try:
        html_bytes = _fetch_url(url)
        html_text = html_bytes.decode("utf-8", errors="replace")
    except (HTTPError, URLError) as e:
        logger.warning(f"Failed to fetch page {page}: {e}")
        return []

    dates = DATE_RE.findall(html_text)
    links = LINK_RE.findall(html_text)

    docs = []
    for i, (doc_url, title) in enumerate(links):
        title = _clean_html(title)
        # Skip non-PDF files (Excel forms, etc.)
        if not doc_url.lower().endswith(".pdf"):
            logger.debug(f"Skipping non-PDF: {doc_url}")
            continue

        date_str = dates[i] if i < len(dates) else None
        iso_date = _parse_date(date_str) if date_str else None

        docs.append({
            "url": doc_url,
            "title": title,
            "date": iso_date,
            "doc_type": _infer_doc_type(title),
        })

    return docs


def _extract_pdf_text(url: str, doc_id: str) -> str:
    """Download PDF and extract text via the shared pdf_extract helper."""
    try:
        pdf_bytes = _fetch_url(url, timeout=60)
    except (HTTPError, URLError) as e:
        logger.warning(f"Failed to download PDF {url}: {e}")
        return ""

    if not pdf_bytes[:5].startswith(b"%PDF"):
        logger.warning(f"Not a valid PDF (header {pdf_bytes[:5]!r}): {url}")
        return ""

    try:
        text = extract_pdf_markdown(
            source="BN/BDCB-Regulations",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
        )
        return text if text else ""
    except Exception as e:
        logger.warning(f"Failed to extract text from {url}: {e}")
        return ""


class BDCBRegulationsScraper(BaseScraper):
    """Scraper for Brunei Darussalam Central Bank regulations."""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Iterate through all listing pages and yield document metadata."""
        page = 1
        total_docs = 0

        while page <= MAX_PAGES:
            logger.info(f"Fetching listing page {page}...")
            docs = _fetch_listing_page(page)

            if not docs:
                logger.info(f"No documents on page {page}, stopping pagination.")
                break

            for doc in docs:
                total_docs += 1
                yield doc

            time.sleep(REQUEST_DELAY)
            page += 1

        logger.info(f"Total documents enumerated: {total_docs}")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch updates since a given date (re-runs full listing, filters by date)."""
        for doc in self.fetch_all():
            if doc.get("date") and doc["date"] >= since.strftime("%Y-%m-%d"):
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Download PDF and normalize to standard schema."""
        url = raw["url"]
        title = raw["title"]

        # Generate stable ID from URL
        file_id = url.split("/")[-1].replace(".pdf", "")
        doc_id = f"bdcb-{file_id}"

        logger.info(f"Processing: {title}")

        # Download and extract text
        text = _extract_pdf_text(url, doc_id)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text ({len(text)} chars) for: {title}")
            return None

        time.sleep(REQUEST_DELAY)

        return {
            "_id": doc_id,
            "_source": "BN/BDCB-Regulations",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "doc_type": raw.get("doc_type", "doctrine"),
            "country": "BN",
            "language": "en",
            "issuing_body": "Brunei Darussalam Central Bank (BDCB)",
        }


def main():
    source_dir = Path(__file__).parent
    scraper = BDCBRegulationsScraper(source_dir)

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to BDCB...")
        try:
            docs = _fetch_listing_page(1)
            logger.info(f"Page 1: {len(docs)} documents found")
            if docs:
                logger.info(f"First doc: {docs[0]['title']}")
            print("OK")
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        logger.info(f"Bootstrap complete: {json.dumps(result, indent=2)}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
