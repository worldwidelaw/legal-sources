#!/usr/bin/env python3
"""
MX/Jalisco-Legislation -- Jalisco State Legislation Fetcher

Fetches consolidated laws, codes, and the constitution from the Congreso de
Jalisco Biblioteca Virtual catalog page. ~185 laws/codes as PDF documents
with full text extraction via PyPDF2.

Strategy:
  - Scrape the Biblioteca Virtual listing page for all PDF/DOC links
  - Filter to Constitución, Códigos, and Leyes categories (skip Ingresos)
  - Download PDFs (preferred) and extract text with PyPDF2
  - Parse date from filename suffix (DDMMYY format)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (small dataset)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import quote, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.Jalisco-Legislation")

CATALOG_URL = "https://congresoweb.congresojal.gob.mx/BibliotecaVirtual/busquedasleyes/Listado%272.cfm"
BASE_URL = "https://congresoweb.congresojal.gob.mx/bibliotecavirtual/legislacion/"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "MX/Jalisco-Legislation"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

REQUEST_DELAY = 2  # seconds between PDF downloads

# Categories to include (skip Ingresos = municipal revenue laws)
INCLUDE_CATEGORIES = {"Constitución", "Códigos", "Leyes"}


class JaliscoLegislationScraper(BaseScraper):
    """Scraper for MX/Jalisco-Legislation -- Jalisco state laws and codes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _parse_filename_date(self, filename: str) -> str:
        """Extract DDMMYY date from filename suffix and convert to ISO 8601."""
        match = re.search(r'-(\d{6})\.\w+$', filename)
        if not match:
            return ""
        date_str = match.group(1)
        try:
            day = int(date_str[0:2])
            month = int(date_str[2:4])
            year = int(date_str[4:6])
            # Two-digit year: 00-50 → 2000s, 51-99 → 1900s
            year = 2000 + year if year <= 50 else 1900 + year
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
        except (ValueError, IndexError):
            pass
        return ""

    def _resolve_url(self, href: str) -> str:
        """Resolve relative href from catalog page to absolute URL.

        Links on the page use ../legislacion/ relative to busquedasleyes/,
        so the absolute base is BASE_URL.
        """
        # Strip leading ../legislacion/ or ../legislacion
        cleaned = href
        if cleaned.startswith('../legislacion/'):
            cleaned = cleaned[len('../legislacion/'):]
        elif cleaned.startswith('../legislacion'):
            cleaned = cleaned[len('../legislacion'):]

        # The server uses Latin-1 encoding for filenames. We need to
        # properly encode non-ASCII characters in the URL path.
        # Split into path segments and encode each one.
        parts = cleaned.split('/')
        encoded_parts = []
        for part in parts:
            # Decode any existing percent-encoding first
            decoded = unquote(part, encoding='latin-1')
            # Re-encode with proper percent-encoding
            encoded = quote(decoded, safe='.-_~')
            encoded_parts.append(encoded)

        return BASE_URL + '/'.join(encoded_parts)

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Scrape the catalog page for law titles and PDF/DOC URLs."""
        logger.info(f"Fetching catalog from {CATALOG_URL}")
        resp = self.session.get(CATALOG_URL, timeout=30)
        resp.raise_for_status()
        # Page uses Latin-1 encoding (ColdFusion on IIS)
        resp.encoding = 'latin-1'
        soup = BeautifulSoup(resp.text, 'html.parser')

        laws = []
        seen_titles = set()

        # Find all links to legislation files
        for link in soup.find_all('a', href=True):
            href = link['href']

            # Only process links to legislation files
            if '../legislacion/' not in href:
                continue

            # Skip ZIP files (exposiciones de motivos)
            if href.lower().endswith('.zip'):
                continue

            # Determine category from path
            category = None
            for cat in INCLUDE_CATEGORIES:
                if f'/{cat}/' in href or f'/{cat.lower()}/' in href.lower():
                    category = cat
                    break
            # Also check for Codigos without accent
            if not category and '/C%F3digos/' in href:
                category = "Códigos"
            if not category and '/Constituci%F3n/' in href:
                category = "Constitución"

            if not category:
                continue

            # Skip PDF subdirectory links if we already have the DOC or vice versa
            # Prefer PDF over DOC
            is_pdf = href.lower().endswith('.pdf')
            is_doc = href.lower().endswith('.doc')
            if not (is_pdf or is_doc):
                continue

            # Extract title from filename
            filename = href.split('/')[-1]
            # Decode percent-encoded filename
            decoded_filename = unquote(filename, encoding='latin-1')
            # Remove date suffix and extension
            title = re.sub(r'-\d{6}\.\w+$', '', decoded_filename)
            title = title.replace('-', ' ').strip()

            # Deduplicate: prefer PDF over DOC
            title_key = title.lower()
            if title_key in seen_titles and not is_pdf:
                continue

            date = self._parse_filename_date(decoded_filename)
            abs_url = self._resolve_url(href)

            # If we already have a DOC version and this is PDF, replace it
            if title_key in seen_titles and is_pdf:
                laws = [l for l in laws if l['title'].lower() != title_key]

            seen_titles.add(title_key)
            laws.append({
                'title': title,
                'url': abs_url,
                'date': date,
                'category': category,
                'filename': decoded_filename,
                'is_pdf': is_pdf,
            })

        logger.info(f"Found {len(laws)} laws ({sum(1 for l in laws if l['is_pdf'])} PDF, "
                     f"{sum(1 for l in laws if not l['is_pdf'])} DOC)")
        return laws

    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MX/Jalisco-Legislation",
            source_id="",
            pdf_bytes=content,
            table="legislation",
        ) or ""

    def _extract_doc_text(self, content: bytes) -> str:
        """Extract text from DOC file using antiword or textract fallback."""
        import subprocess
        import tempfile
        try:
            with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            result = subprocess.run(
                ['textutil', '-convert', 'txt', '-stdout', tmp_path],
                capture_output=True, timeout=30
            )
            Path(tmp_path).unlink(missing_ok=True)
            if result.returncode == 0:
                text = result.stdout.decode('utf-8', errors='replace')
                text = re.sub(r' +', ' ', text)
                text = re.sub(r'\n{3,}', '\n\n', text)
                return text.strip()
        except Exception as e:
            logger.warning(f"DOC extraction failed: {e}")
        return ""

    def _fetch_law(self, law: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download document and extract full text for a single law."""
        time.sleep(REQUEST_DELAY)
        try:
            resp = self.session.get(law['url'], timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to download {law['title'][:50]}: {e}")
            return None

        if law['is_pdf']:
            text = self._extract_pdf_text(resp.content)
            page_count = 0
            try:
                page_count = len(PdfReader(io.BytesIO(resp.content)).pages)
            except Exception:
                pass
        else:
            text = self._extract_doc_text(resp.content)
            page_count = 0

        if len(text) < 100:
            logger.warning(f"Very short text for {law['title'][:50]}: {len(text)} chars")
            return None

        content_hash = hashlib.md5(resp.content).hexdigest()[:12]

        return {
            'title': law['title'],
            'text': text,
            'url': law['url'],
            'date': law['date'],
            'category': law['category'],
            'filename': law['filename'],
            'content_hash': content_hash,
            'file_size': len(resp.content),
            'page_count': page_count,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw law data into standard schema."""
        slug = re.sub(r'[^a-z0-9]+', '-', raw['title'].lower())[:60].strip('-')
        doc_id = f"MX-JAL-{raw['content_hash']}"

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw['title'],
            'text': raw['text'],
            'date': raw.get('date', ''),
            'category': raw.get('category', ''),
            'page_count': raw.get('page_count', 0),
            'url': raw['url'],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all Jalisco laws with full text."""
        laws = self._get_law_list()

        if sample:
            step = max(1, len(laws) // 15)
            laws = laws[::step][:15]

        total = 0
        for i, law in enumerate(laws):
            logger.info(f"[{i+1}/{len(laws)}] Downloading: {law['title'][:60]}...")
            raw = self._fetch_law(law)
            if raw:
                record = self.normalize(raw)
                yield record
                total += 1
                logger.info(f"  OK: {len(raw['text'])} chars, {raw['page_count']} pages")
            else:
                logger.warning(f"  SKIP: {law['title'][:60]}")

        logger.info(f"\nTotal laws fetched: {total}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Fetch all laws (small dataset, always full refresh)."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    scraper = JaliscoLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to congresojal.gob.mx...")
        laws = scraper._get_law_list()
        if laws:
            logger.info(f"Found {len(laws)} laws. Testing first PDF...")
            raw = scraper._fetch_law(laws[0])
            if raw:
                logger.info(f"SUCCESS: {raw['title'][:60]} - {len(raw['text'])} chars, {raw['page_count']} pages")
            else:
                logger.error("FAILED: Could not extract text from first PDF")
                sys.exit(1)
        else:
            logger.error("FAILED: Could not fetch law listing")
            sys.exit(1)

    elif command == "bootstrap":
        SAMPLE_DIR.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            if sample:
                fname = SAMPLE_DIR / f"{record['_id']}.json"
                with open(fname, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Bootstrap complete: {count} records")

    elif command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
