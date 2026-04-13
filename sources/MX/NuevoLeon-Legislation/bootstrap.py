#!/usr/bin/env python3
"""
MX/NuevoLeon-Legislation -- Nuevo León State Legislation Fetcher

Fetches consolidated laws, codes, and the constitution from the Secretaría
General de Gobierno (SGG) transparency portal. ~220 laws across 23 paginated
ASP.NET pages, each with PDF download links. Full text via PyPDF2.

Strategy:
  - Navigate ASP.NET paginated listing (POST with ViewState)
  - Parse table rows for title, pub date, reform date, PDF URL
  - Download each PDF and extract text with PyPDF2
  - Parse dates from Spanish format (DD/mmm/YYYY)

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
logger = logging.getLogger("legal-data-hunter.MX.NuevoLeon-Legislation")

PAGE_URL = "https://sistec.nl.gob.mx/Transparencia_2015_LYPOE/Acciones/Legislacion.aspx"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "MX/NuevoLeon-Legislation"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

REQUEST_DELAY = 2  # seconds between requests

SPANISH_MONTHS = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
}


class NuevoLeonLegislationScraper(BaseScraper):
    """Scraper for MX/NuevoLeon-Legislation -- Nuevo León state laws and codes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _parse_spanish_date(self, date_str: str) -> str:
        """Parse DD/mmm/YYYY (e.g. '16/dic/1917') to ISO 8601."""
        match = re.match(r'(\d{1,2})/(\w{3})/(\d{4})', date_str.strip())
        if not match:
            return ""
        day, month_str, year = int(match.group(1)), match.group(2).lower(), int(match.group(3))
        month = SPANISH_MONTHS.get(month_str, 0)
        if month and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"
        return ""

    def _get_form_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract ASP.NET hidden form fields."""
        fields = {}
        for inp in soup.find_all('input', {'type': 'hidden'}):
            name = inp.get('name', '')
            if name:
                fields[name] = inp.get('value', '')
        return fields

    def _parse_page_laws(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Parse law entries from a single page's HTML table."""
        laws = []
        for tr in soup.find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) < 4:
                continue
            title = cells[0].get_text(strip=True)
            if not title or len(title) < 10 or title == 'Nombre del documento':
                continue

            pub_date = cells[1].get_text(strip=True)
            reform_date = cells[2].get_text(strip=True)

            # Find PDF link (prefer PDF over DOC)
            pdf_link = cells[3].find('a', href=lambda h: h and '.pdf' in h.lower())
            if not pdf_link:
                continue

            pdf_url = pdf_link['href']
            # Resolve relative URLs
            if not pdf_url.startswith('http'):
                if pdf_url.startswith('/'):
                    pdf_url = 'https://sistec.nl.gob.mx' + pdf_url
                else:
                    pdf_url = 'https://sistec.nl.gob.mx/Transparencia_2015_LYPOE/Acciones/' + pdf_url

            laws.append({
                'title': title,
                'pub_date': pub_date,
                'reform_date': reform_date,
                'pdf_url': pdf_url,
            })
        return laws

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Scrape all pages of the legislation listing."""
        logger.info(f"Fetching legislation listing from SGG portal")

        # Get initial page
        resp = self.session.get(PAGE_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Determine total pages from dropdown
        select = soup.find('select', {'name': 'ddlPagina'})
        total_pages = len(select.find_all('option')) if select else 1
        logger.info(f"Found {total_pages} pages to scrape")

        # Parse first page
        all_laws = self._parse_page_laws(soup)
        logger.info(f"Page 1/{total_pages}: {len(all_laws)} laws")

        # Navigate through remaining pages
        for page_idx in range(1, total_pages):
            time.sleep(REQUEST_DELAY)
            fields = self._get_form_fields(soup)
            fields['ddlPagina'] = str(page_idx)
            fields['__EVENTTARGET'] = 'ddlPagina'
            fields['__EVENTARGUMENT'] = ''
            # Remove button fields
            for key in ['btnLeyesEstatales', 'btnReglamentosEstatales', 'btnReglamentosMunicipales']:
                fields.pop(key, None)

            try:
                resp = self.session.post(PAGE_URL, data=fields, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')
                page_laws = self._parse_page_laws(soup)
                all_laws.extend(page_laws)
                logger.info(f"Page {page_idx+1}/{total_pages}: {len(page_laws)} laws (total: {len(all_laws)})")
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch page {page_idx+1}: {e}")

        # Deduplicate by title
        seen = set()
        unique_laws = []
        for law in all_laws:
            key = law['title'].lower()
            if key not in seen:
                seen.add(key)
                unique_laws.append(law)

        logger.info(f"Total unique laws: {len(unique_laws)}")
        return unique_laws

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MX/NuevoLeon-Legislation",
            source_id="",
            pdf_bytes=pdf_content,
            table="legislation",
        ) or ""

    def _fetch_law(self, law: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download PDF and extract full text for a single law."""
        time.sleep(REQUEST_DELAY)
        try:
            resp = self.session.get(law['pdf_url'], timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to download {law['title'][:50]}: {e}")
            return None

        text = self._extract_pdf_text(resp.content)
        if len(text) < 100:
            logger.warning(f"Very short text for {law['title'][:50]}: {len(text)} chars")
            return None

        content_hash = hashlib.md5(resp.content).hexdigest()[:12]
        page_count = 0
        try:
            page_count = len(PdfReader(io.BytesIO(resp.content)).pages)
        except Exception:
            pass

        return {
            'title': law['title'],
            'text': text,
            'pdf_url': law['pdf_url'],
            'pub_date': law['pub_date'],
            'reform_date': law['reform_date'],
            'content_hash': content_hash,
            'file_size': len(resp.content),
            'page_count': page_count,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw law data into standard schema."""
        iso_date = self._parse_spanish_date(raw.get('pub_date', ''))
        reform_date = self._parse_spanish_date(raw.get('reform_date', ''))

        return {
            '_id': f"MX-NL-{raw['content_hash']}",
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw['title'],
            'text': raw['text'],
            'date': iso_date,
            'reform_date': reform_date,
            'page_count': raw.get('page_count', 0),
            'url': raw['pdf_url'],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all Nuevo León laws with full text."""
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
    scraper = NuevoLeonLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to SGG portal...")
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
