#!/usr/bin/env python3
"""
KH/HuskyPartners -- Husky & Partners Cambodian Legal Library

Fetches Cambodian laws and regulations from the Husky & Partners legal library.

Strategy:
  - Download the single library page (all documents in one 1.4MB HTML page)
  - Parse document metadata (title, year, type, category, language)
  - Download English PDFs where available, Khmer as fallback
  - Extract full text using PyMuPDF (fitz)

Endpoints:
  - Library: https://huskyandpartners.com/huskypublication/legal-library
  - PDFs: https://huskyandpartners.com/images///Law Library/{category}/{filename}.pdf

Data:
  - ~640 documents across 30+ legal categories
  - ~243 English translations, ~400 Khmer-only
  - Document types: Royal Decrees, Laws, Sub-Decrees, Prakas, Circulars
  - Coverage: 1920-2024

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote, unquote

import requests
try:
    import fitz  # PyMuPDF — optional; falls back to shared extractor if absent (issue #816)
except ImportError:
    fitz = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KH.HuskyPartners")

BASE_URL = "https://huskyandpartners.com"
LIBRARY_URL = BASE_URL + "/huskypublication/legal-library"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Document type headers as they appear in the HTML
DOC_TYPE_HEADERS = [
    "Royal Decree", "Law", "Sub Decree", "Sub-Decree", "Prakas",
    "Circular", "Notification", "Decision", "Regulation",
    "Circular / Notification", "Others",
]


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF.

    Prefers PyMuPDF (fitz) when installed; otherwise falls back to the shared
    pdfplumber/pypdf extractor in common.pdf_extract, so the scraper still runs
    on hosts without PyMuPDF (issue #816).
    """
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            parts = []
            for page in doc:
                text = page.get_text()
                if text:
                    parts.append(text.strip())
            doc.close()
            joined = "\n\n".join(parts)
            if joined.strip():
                return joined
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")
    # Fallback: shared extractor (pdfplumber/pypdf — always in requirements.txt)
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source="KH/HuskyPartners", source_id="_pdf", pdf_bytes=pdf_bytes, force=True
        )
        return text or ""
    except Exception as e:
        logger.warning(f"pdf_extract fallback failed: {e}")
        return ""


def _parse_library_page(html_content: str) -> List[Dict[str, Any]]:
    """Parse the library page and extract all document entries."""
    entries = []

    # Track current document type by scanning for category headers
    # Headers are: <div class="list-group-item" style="background: #eee;font-size:15px">TYPE</div>
    doc_type_positions = []
    for m in re.finditer(
        r'<div class="list-group-item" style="background: #eee;font-size:15px">\s*(.*?)\s*</div>',
        html_content, re.DOTALL
    ):
        doc_type_positions.append((m.start(), re.sub(r"<[^>]+>", "", m.group(1)).strip()))

    # Track current category from collapse panel headers
    # Category names are in: <a ... data-toggle="collapse" ...>CATEGORY</a>
    cat_positions = []
    for m in re.finditer(
        r'<a[^>]*data-toggle="collapse"[^>]*>\s*(.*?)\s*</a>',
        html_content, re.DOTALL
    ):
        cat_name = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        if cat_name and len(cat_name) > 2:
            cat_positions.append((m.start(), cat_name))

    # Parse each document entry
    doc_pattern = re.compile(
        r'<a href="(/images/[^"]+\.pdf[^"]*)"[^>]*target="_blank"[^>]*>\s*'
        r'<i class="fa fa-file-pdf-o text-danger"></i>\s*&nbsp;\s*(.*?)\s*</a>'
        r'.*?<span style="font-size:14px">(\d{4})</span>'
        r'(.*?)</div>\s*</div>\s*</div>',
        re.DOTALL
    )

    for m in doc_pattern.finditer(html_content):
        pos = m.start()
        pdf_path = m.group(1)
        title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m.group(2))).strip()
        year = m.group(3)
        download_section = m.group(4)

        # Determine document type from nearest preceding header
        doc_type = "Unknown"
        for dt_pos, dt_name in reversed(doc_type_positions):
            if dt_pos < pos:
                doc_type = dt_name
                break

        # Determine category from nearest preceding category header
        category = "Unknown"
        for cat_pos, cat_name in reversed(cat_positions):
            if cat_pos < pos:
                category = cat_name
                break

        # Extract download links with language labels
        downloads = re.findall(
            r'data-href="([^"]+)">\s*<i class="fa fa-download"></i>\s*(\w+)',
            download_section
        )
        en_download = next((d[0] for d in downloads if d[1] == "En"), None)
        kh_download = next((d[0] for d in downloads if d[1] == "Kh"), None)

        # Prefer English download URL, then direct PDF link
        if en_download:
            preferred_url = en_download
            language = "en"
        else:
            preferred_url = pdf_path
            language = "km"

        # Build stable doc_id from title + year
        id_str = f"{title}:{year}"
        doc_id = hashlib.md5(id_str.encode()).hexdigest()[:12]

        entries.append({
            "doc_id": f"husky-{doc_id}",
            "title": title,
            "year": int(year),
            "doc_type": doc_type,
            "category": category,
            "language": language,
            "direct_pdf_path": pdf_path,
            "en_download": en_download,
            "kh_download": kh_download,
            "preferred_url": preferred_url,
        })

    return entries


class KHHuskyPartnersScraper(BaseScraper):
    """
    Scraper for KH/HuskyPartners -- Husky & Partners Cambodian Legal Library.
    Country: KH
    URL: https://huskyandpartners.com/huskypublication/legal-library
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _download_pdf_text(self, entry: Dict[str, Any]) -> Optional[str]:
        """Download a PDF and extract text. Try English first, then Khmer."""
        urls_to_try = []

        # Build full URLs
        if entry.get("en_download"):
            urls_to_try.append(f"{BASE_URL}{entry['en_download']}")
        if entry.get("direct_pdf_path"):
            urls_to_try.append(f"{BASE_URL}{entry['direct_pdf_path']}")
        if entry.get("kh_download"):
            urls_to_try.append(f"{BASE_URL}{entry['kh_download']}")

        for url in urls_to_try:
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=60)
                if resp.status_code != 200:
                    continue
                if resp.content[:5] != b"%PDF-":
                    continue

                text = extract_pdf_text(resp.content)
                if text and len(text.strip()) > 100:
                    return text.strip()

            except requests.exceptions.RequestException as e:
                logger.debug(f"Download failed for {url}: {e}")
                continue

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from the Husky Partners library."""
        logger.info(f"Fetching library page: {LIBRARY_URL}")
        resp = self.session.get(LIBRARY_URL, timeout=60)
        resp.raise_for_status()

        entries = _parse_library_page(resp.text)
        logger.info(f"Parsed {len(entries)} document entries")

        # Prioritize English documents
        en_entries = [e for e in entries if e["language"] == "en"]
        km_entries = [e for e in entries if e["language"] == "km"]
        ordered = en_entries + km_entries
        logger.info(f"  English: {len(en_entries)}, Khmer: {len(km_entries)}")

        for entry in ordered:
            logger.info(f"  Downloading: {entry['title'][:60]} ({entry['year']})")
            text = self._download_pdf_text(entry)
            if text:
                entry["text"] = text
                yield entry
            else:
                logger.warning(f"  Skipping {entry['doc_id']} — no text extracted")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents from the current year."""
        current_year = datetime.now().year
        resp = self.session.get(LIBRARY_URL, timeout=60)
        resp.raise_for_status()
        entries = _parse_library_page(resp.text)
        for entry in entries:
            if entry["year"] >= current_year - 1:
                text = self._download_pdf_text(entry)
                if text:
                    entry["text"] = text
                    yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        title = raw.get("title", "").strip()
        if not title:
            title = f"Cambodia Law — {raw.get('doc_type', 'Document')} ({raw.get('year', '')})"

        date_str = f"{raw['year']}-01-01" if raw.get("year") else None

        url = ""
        if raw.get("en_download"):
            url = f"{BASE_URL}{raw['en_download']}"
        elif raw.get("direct_pdf_path"):
            url = f"{BASE_URL}{raw['direct_pdf_path']}"

        return {
            "_id": raw["doc_id"],
            "_source": "KH/HuskyPartners",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "title": title,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": url,
            "doc_type": raw.get("doc_type", ""),
            "category": raw.get("category", ""),
            "language": raw.get("language", ""),
            "year": raw.get("year"),
            "country": "KH",
            "court": "Kingdom of Cambodia",
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="KH/HuskyPartners scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10+ records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = KHHuskyPartnersScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        resp = scraper.session.get(LIBRARY_URL, timeout=30)
        entries = _parse_library_page(resp.text)
        en_count = sum(1 for e in entries if e["language"] == "en")
        logger.info(f"Test OK: {len(entries)} documents ({en_count} English)")
        if entries:
            text = scraper._download_pdf_text(entries[1])  # Skip first (may be scanned)
            logger.info(f"PDF test: {len(text) if text else 0} chars")
        sys.exit(0)

    if args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        logger.info(f"Bootstrap result: {result}")

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {result}")
