#!/usr/bin/env python3
"""
YE/CBY-Regulations — Central Bank of Yemen Circulars & Banking Laws

Fetches banking circulars and major banking laws from the Central Bank of Yemen
(Aden branch at english.cby-ye.com).

Strategy:
  1. Scrape /publicationsandcirculars (paginated) → extract publication IDs
  2. For each circular: GET /publications/{id} → parse title, date, full text from HTML
  3. Scrape /rulesandregulations → extract PDF URLs for major banking laws
  4. Download PDFs → extract full text via pypdf/PyMuPDF

Data:
  - ~15 banking supervision circulars (full text in HTML)
  - 7 foundational banking laws (PDF)
  - Content primarily in Arabic

Usage:
  python3 bootstrap.py bootstrap          # Full initial pull
  python3 bootstrap.py bootstrap --sample # Fetch 15 sample records
"""

import io
import json
import logging
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://english.cby-ye.com"
# PDFs are served from the Arabic domain (English domain returns 500 for files)
FILES_BASE_URL = "https://cby-ye.com"
DELAY = 2.0
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5,ar;q=0.3",
}

# Known law PDFs from /rulesandregulations
LAW_PDFS = [
    {"title": "قانون البنك المركزي اليمني المعدل", "title_en": "Central Bank Law (Amended)", "path": "/files/615db6c864235.pdf"},
    {"title": "قانون المصارف", "title_en": "Banking Law", "path": "/files/615db717308c2.pdf"},
    {"title": "قانون الصرافة", "title_en": "Exchange Law", "path": "/files/615db756b7a15.pdf"},
    {"title": "قانون الدين العام", "title_en": "Public Debt Law", "path": "/files/615db7a57d9e2.pdf"},
    {"title": "قانون المصارف الاسلامية", "title_en": "Islamic Banks Law", "path": "/files/615db806e2b95.pdf"},
    {"title": "قانون مكافحة غسل الأموال وتمويل الارهاب", "title_en": "Anti-Money Laundering and Terrorist Financing Law", "path": "/files/615db8658c433.pdf"},
]

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
    try:
        from pypdf import PdfReader
    except ImportError:
        PdfReader = None


class _HTMLTextExtractor(HTMLParser):
    """Extract visible text from HTML, stripping tags."""

    def __init__(self):
        super().__init__()
        self._parts: List[str] = []
        self._skip = False
        self._skip_tags = {"script", "style", "head", "nav", "footer", "header"}

    def handle_starttag(self, tag, attrs):
        if tag in self._skip_tags:
            self._skip = True
        if tag in ("br", "p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr"):
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._skip_tags:
            self._skip = False
        if tag in ("p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._parts)
        # Collapse multiple blank lines
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def _html_to_text(html: str) -> str:
    """Convert HTML to plain text."""
    parser = _HTMLTextExtractor()
    parser.feed(html)
    return parser.get_text()


def _get(url: str, timeout: int = 30) -> Optional[str]:
    """GET a URL and return response text."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"GET {url} failed: {e}")
        return None


def _download_bytes(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download binary content from URL."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        logger.warning(f"Download {url} failed: {e}")
        return None


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            return text.strip()
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
            return ""
    elif PdfReader is not None:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            text = ""
            for page in reader.pages:
                text += (page.extract_text() or "")
            return text.strip()
        except Exception as e:
            logger.warning(f"pypdf extraction failed: {e}")
            return ""
    logger.warning("No PDF library available (need PyMuPDF or pypdf)")
    return ""


def _extract_article_body(html: str) -> str:
    """Extract the main article body text from a publication page."""
    # Try to find the main content area — typically after the header/date section
    # Look for content between the date/header and the footer/sidebar
    body_match = re.search(
        r'<div[^>]*class="[^"]*(?:content|article|post-body|entry)[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if body_match:
        return _html_to_text(body_match.group(1))

    # Fallback: extract everything from the main container
    # Remove nav, header, footer, sidebar, and script sections first
    cleaned = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<header[^>]*>.*?</header>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<footer[^>]*>.*?</footer>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<script[^>]*>.*?</script>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<style[^>]*>.*?</style>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)

    text = _html_to_text(cleaned)

    # Try to isolate just the article text by finding the title and extracting after it
    # The title appears in h1, then date in h5, then the content follows
    lines = text.split("\n")
    # Find where the actual content starts (skip navigation, menus, etc.)
    content_lines = []
    started = False
    for line in lines:
        stripped = line.strip()
        if not started:
            # Look for date-like pattern or circular reference to mark start of content
            if re.search(r'\d{4}[-/]\d{2}[-/]\d{2}', stripped) or re.search(r'منشور\s+دوري', stripped):
                started = True
                content_lines.append(stripped)
        else:
            # Stop at common footer markers
            if re.search(r'(?:Share|Facebook|Twitter|LinkedIn|©|Copyright|Related News)', stripped, re.IGNORECASE):
                break
            content_lines.append(stripped)

    if len(content_lines) > 3:
        return "\n".join(content_lines).strip()

    return text


def get_circular_ids_and_titles() -> List[Tuple[int, str]]:
    """Get all circular (id, title) pairs from the listing pages."""
    all_items: List[Tuple[int, str]] = []
    seen_ids = set()
    for page_num in range(1, 10):  # Max 10 pages
        if page_num == 1:
            url = f"{BASE_URL}/publicationsandcirculars"
        else:
            url = f"{BASE_URL}/publicationsandcirculars?page={page_num}"

        logger.info(f"Fetching circular list page {page_num}: {url}")
        html = _get(url)
        if not html:
            break

        # Extract (id, title) pairs — links like <a href="/publications/157">TITLE</a>
        pairs = re.findall(r'<a[^>]*href="/publications/(\d+)"[^>]*>(.*?)</a>', html, re.DOTALL)
        if not pairs:
            break

        for pid_str, raw_title in pairs:
            pid = int(pid_str)
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            title = _html_to_text(raw_title).strip()
            if title:
                all_items.append((pid, title))

        logger.info(f"  Found {len(pairs)} circulars on page {page_num}")

        # Check if there's a next page
        if f"page={page_num + 1}" not in html:
            break

        time.sleep(DELAY)

    return all_items


def fetch_circular(pub_id: int, listing_title: str = "") -> Optional[Dict[str, Any]]:
    """Fetch a single circular publication and extract metadata + text."""
    url = f"{BASE_URL}/publications/{pub_id}"
    html = _get(url)
    if not html:
        return None

    # Extract title — prefer listing title over page <h1> (which is often just section name)
    title = listing_title
    if not title:
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        title = _html_to_text(title_match.group(1)).strip() if title_match else ""

    # Extract date from <h5> or meta
    date_str = None
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', html)
    if date_match:
        date_str = date_match.group(1)

    # Extract circular number if present (handles both لعام and لسنة)
    circ_match = re.search(r'منشور\s+دوري\s+رقم\s*\((\d+)\)\s*(?:لعام|لسنة)\s*(\d{4})', html)
    circ_number = None
    if circ_match:
        circ_number = f"Circular No. {circ_match.group(1)} of {circ_match.group(2)}"

    # Extract article body text
    text = _extract_article_body(html)

    if not title and not text:
        return None

    return {
        "pub_id": pub_id,
        "title": title,
        "date": date_str,
        "circular_number": circ_number,
        "text": text,
        "url": url,
        "doc_type": "circular",
    }


def fetch_law_pdfs() -> Iterator[Dict[str, Any]]:
    """Fetch and extract text from banking law PDFs."""
    for i, law in enumerate(LAW_PDFS):
        pdf_url = f"{FILES_BASE_URL}{law['path']}"
        logger.info(f"Downloading law PDF: {law['title_en']} from {pdf_url}")

        pdf_bytes = _download_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"  Failed to download {law['title_en']}")
            continue

        text = _extract_text_from_pdf(pdf_bytes)
        if not text:
            logger.warning(f"  No text extracted from {law['title_en']}")
            continue

        logger.info(f"  Extracted {len(text)} chars from {law['title_en']}")

        yield {
            "doc_id": f"law-{i+1}",
            "title": law["title"],
            "title_en": law["title_en"],
            "text": text,
            "date": None,
            "url": pdf_url,
            "pdf_url": pdf_url,
            "doc_type": "law",
            "circular_number": None,
        }

        time.sleep(DELAY)


def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a raw record into the standard schema."""
    if raw["doc_type"] == "circular":
        doc_id = f"YE-CBY-circular-{raw['pub_id']}"
    else:
        doc_id = f"YE-CBY-{raw['doc_id']}"

    return {
        "_id": doc_id,
        "_source": "YE/CBY-Regulations",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "doc_type": raw.get("doc_type", ""),
        "circular_number": raw.get("circular_number"),
        "pdf_url": raw.get("pdf_url"),
    }


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Yield all normalized records with full text."""
    count = 0

    # 1. Fetch circulars
    logger.info("=== Fetching circulars ===")
    circulars = get_circular_ids_and_titles()
    logger.info(f"Found {len(circulars)} circulars to fetch")

    if sample:
        circulars = circulars[:12]
        logger.info("Sample mode: limiting circulars to 12")

    for pub_id, listing_title in circulars:
        logger.info(f"Fetching circular {pub_id}: {listing_title[:50]}")
        raw = fetch_circular(pub_id, listing_title)
        if not raw:
            logger.warning(f"  Skipping circular {pub_id}: no data")
            continue

        if not raw.get("text") or len(raw["text"]) < 50:
            logger.warning(f"  Skipping circular {pub_id}: insufficient text ({len(raw.get('text', ''))} chars)")
            continue

        record = normalize(raw)
        yield record
        count += 1
        time.sleep(DELAY)

    # 2. Fetch law PDFs
    if not sample or count < 10:
        logger.info("=== Fetching law PDFs ===")
        for raw in fetch_law_pdfs():
            record = normalize(raw)
            yield record
            count += 1

            if sample and count >= 15:
                break

    logger.info(f"Total records yielded: {count}")


def bootstrap(sample: bool = False):
    """Run bootstrap: fetch records and save to sample/."""
    src_dir = Path(__file__).parent
    sample_dir = src_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    all_records = []
    for record in fetch_all(sample=sample):
        count += 1
        fname = sample_dir / f"record_{count:04d}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        all_records.append(record)
        logger.info(f"  Saved {fname.name}: {record['title'][:60]}")

    if all_records:
        combined = sample_dir / "all_samples.json"
        with open(combined, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    # Validation summary
    texts = [r.get("text", "") for r in all_records]
    non_empty = sum(1 for t in texts if len(t) > 100)
    avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
    logger.info(f"Validation: {non_empty}/{count} records have substantial text (>100 chars)")
    logger.info(f"Average text length: {avg_len} chars")

    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    args = sys.argv[1:]
    if not args or args[0] == "bootstrap":
        sample = "--sample" in args
        bootstrap(sample=sample)
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")
        sys.exit(1)
