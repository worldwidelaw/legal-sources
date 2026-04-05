#!/usr/bin/env python3
"""
AU/WA-Legislation -- Western Australia Legislation Fetcher

Fetches WA Acts and subsidiary legislation from legislation.wa.gov.au.

Strategy:
  - Enumerate alphabetical index pages (actsif_a..z, subsif_a..z)
  - Parse HTML tables to extract act IDs, titles, numbers, and mrdoc IDs
  - Download DOCX via RedirectURL agent and extract text with python-docx
  - Fall back to PDF if DOCX fails
  - No auth required

Data:
  - ~1,000+ Acts and subsidiary legislation in force
  - Full text in DOCX format
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all (no date filter available)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import json
import logging
import re
import string
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests as _requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.WA-Legislation")

BASE_URL = "https://www.legislation.wa.gov.au/legislation/statutes.nsf"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "text/html, application/xhtml+xml",
}

# Index page types: acts and subsidiary legislation
INDEX_TYPES = [
    ("actsif", "act"),
    ("subsif", "statutory_rule"),
]

# Regex to parse index pages:
# law_aXXX.html...view=consolidated>Title</a></td><td>NNN of YYYY</td>...mrdoc_XXX.docx
ROW_PATTERN = re.compile(
    r'law_a(\d+)\.html[^>]*view=consolidated[^>]*>([^<]+)</a></td>\s*<td[^>]*>([^<]*)</td>',
    re.DOTALL,
)
MRDOC_PATTERN = re.compile(r'law_a(\d+)\.html.*?mrdoc_(\d+)\.docx', re.DOTALL)


def _fetch_page(url: str) -> Optional[str]:
    """Fetch an HTML page with retries."""
    for attempt in range(3):
        try:
            resp = _requests.get(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            return resp.text
        except _requests.RequestException as e:
            logger.warning(f"Fetch attempt {attempt+1} failed for {url[:80]}: {e}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return None


def _fetch_binary(url: str) -> Optional[bytes]:
    """Fetch a binary file with retries."""
    for attempt in range(3):
        try:
            resp = _requests.get(
                url, headers={**HEADERS, "Accept": "*/*"},
                timeout=120, allow_redirects=True,
            )
            resp.raise_for_status()
            return resp.content
        except _requests.RequestException as e:
            logger.warning(f"Binary fetch attempt {attempt+1} failed for {url[:80]}: {e}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return None


def _extract_text_docx(content: bytes) -> Optional[str]:
    """Extract text from a DOCX file."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(content))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n".join(paragraphs)
        return text if len(text) > 50 else None
    except Exception as e:
        logger.debug(f"DOCX extraction failed: {e}")
        return None


def _extract_text_pdf(content: bytes) -> Optional[str]:
    """Extract text from a PDF file."""
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(content))
        parts = []
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                parts.append(page_text)
        text = "\n".join(parts)
        return text if len(text) > 50 else None
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
        return None


def _parse_act_number(number_str: str) -> tuple:
    """Parse act number like '024 of 1972' into (number, year)."""
    match = re.search(r'(\d+)\s+of\s+(\d{4})', number_str)
    if match:
        return match.group(1).lstrip("0") or "0", match.group(2)
    return None, None


def _parse_index_page(html: str) -> List[Dict]:
    """Parse an index page to extract act entries."""
    # First get title and number for each law_a ID
    rows = ROW_PATTERN.findall(html)
    info_by_id = {}
    for law_id, title, number in rows:
        info_by_id[law_id] = {
            "title": title.strip(),
            "number_str": number.strip(),
        }

    # Then get mrdoc mapping
    mrdoc_map = dict(MRDOC_PATTERN.findall(html))

    entries = []
    for law_id, info in info_by_id.items():
        mrdoc_id = mrdoc_map.get(law_id)
        if not mrdoc_id:
            continue
        number, year = _parse_act_number(info["number_str"])
        entries.append({
            "law_id": law_id,
            "mrdoc_id": mrdoc_id,
            "title": info["title"],
            "number": number,
            "year": year,
            "number_str": info["number_str"],
        })

    return entries


class Scraper(BaseScraper):
    SOURCE_ID = "AU/WA-Legislation"

    def __init__(self):
        super().__init__(Path(__file__).parent)

    def _enumerate_all(self, doc_type_filter: str = None) -> Generator[Dict, None, None]:
        """Enumerate all legislation entries from index pages."""
        seen = set()
        for index_type, doc_type in INDEX_TYPES:
            if doc_type_filter and doc_type != doc_type_filter:
                continue
            for letter in string.ascii_lowercase:
                url = f"{BASE_URL}/{index_type}_{letter}.html"
                logger.info(f"Fetching index: {index_type}_{letter}")
                html = _fetch_page(url)
                if not html:
                    continue

                entries = _parse_index_page(html)
                for entry in entries:
                    key = entry["law_id"]
                    if key in seen:
                        continue
                    seen.add(key)
                    entry["doc_type"] = doc_type
                    yield entry

                time.sleep(1)

    def _fetch_document(self, entry: Dict) -> Optional[Dict]:
        """Fetch full text for a single legislation entry."""
        mrdoc_id = entry["mrdoc_id"]
        title = entry["title"]

        # Try DOCX first
        docx_url = f"{BASE_URL}/RedirectURL?OpenAgent&query=mrdoc_{mrdoc_id}.docx"
        content = _fetch_binary(docx_url)
        text = None
        if content:
            text = _extract_text_docx(content)

        # Fall back to PDF
        if not text or len(text) < 100:
            pdf_url = f"{BASE_URL}/RedirectURL?OpenAgent&query=mrdoc_{mrdoc_id}.pdf"
            content = _fetch_binary(pdf_url)
            if content:
                text = _extract_text_pdf(content)

        if not text or len(text) < 100:
            logger.warning(f"No text extracted for {title} (mrdoc_{mrdoc_id})")
            return None

        year = entry.get("year")
        number = entry.get("number")
        doc_type = entry.get("doc_type", "act")
        doc_id = f"{doc_type}-{year}-{number}" if year and number else f"law-{entry['law_id']}"

        return {
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": f"{year}-01-01" if year else None,
            "url": f"{BASE_URL}/law_a{entry['law_id']}.html",
            "year": year,
            "number": number,
            "doc_type": doc_type,
            "law_id": entry["law_id"],
            "mrdoc_id": mrdoc_id,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all WA legislation."""
        for entry in self._enumerate_all():
            doc = self._fetch_document(entry)
            if doc:
                yield doc
            time.sleep(2)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents (no date-based filtering available)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to the standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", ""),
            "year": raw.get("year"),
            "number": raw.get("number"),
            "law_id": raw.get("law_id"),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/WA-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = Scraper()

    if args.command == "test":
        logger.info("Testing index page access...")
        html = _fetch_page(f"{BASE_URL}/actsif_a.html")
        if html:
            entries = _parse_index_page(html)
            logger.info(f"OK — found {len(entries)} acts starting with 'A'")
            if entries:
                logger.info(f"First: {entries[0]['title']}")
        else:
            logger.error("FAILED — could not access index page")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
