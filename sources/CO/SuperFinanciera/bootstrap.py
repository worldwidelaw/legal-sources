#!/usr/bin/env python3
"""
CO/SuperFinanciera -- Colombia Superintendencia Financiera Circulars & Resolutions

Fetches Circulares Externas, Cartas Circulares, and Resoluciones from the
Superintendencia Financiera de Colombia (financial regulator) since 2005.
Documents are PDF files downloaded via loader.php endpoints.

Source: https://www.superfinanciera.gov.co/publicaciones/20149/
  - Year index pages list documents in HTML tables (Número, Fecha, Descripción)
  - Each document links to a PDF via loader.php?...&idFile=XXXXX
  - PDFs are text-based (not scanned), extractable with pdfminer

Usage:
  python bootstrap.py bootstrap          # Full scan and fetch
  python bootstrap.py bootstrap --sample  # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent 2 years
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from pdfminer.high_level import extract_text as pdf_extract_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.SuperFinanciera")

BASE_URL = "https://www.superfinanciera.gov.co"
SOURCE_ID = "CO/SuperFinanciera"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DELAY = 1.5  # seconds between requests
PDF_DELAY = 2.0  # seconds between PDF downloads

# Index page listing all year pages
INDEX_PAGE_ID = "20149"

# Spanish month names for date parsing
MONTHS_ES = {
    'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
    'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12',
}

# Document types
DOC_TYPES = {
    "circulares-externas": "Circular Externa",
    "cartas-circulares": "Carta Circular",
    "resoluciones": "Resolución",
}


def parse_date(date_text: str, year: int) -> Optional[str]:
    """Parse Spanish date like 'Diciembre 13' with a known year to ISO format."""
    date_text = date_text.strip()
    if not date_text:
        return None

    # Try "Mes DD" pattern
    match = re.match(r'(\w+)\s+(\d{1,2})', date_text)
    if match:
        month_name = match.group(1).lower()
        day = match.group(2).zfill(2)
        month = MONTHS_ES.get(month_name)
        if month:
            return f"{year}-{month}-{day}"

    # Try "DD de Mes" pattern
    match = re.match(r'(\d{1,2})\s+de\s+(\w+)', date_text)
    if match:
        day = match.group(1).zfill(2)
        month_name = match.group(2).lower()
        month = MONTHS_ES.get(month_name)
        if month:
            return f"{year}-{month}-{day}"

    return None


def extract_year_from_url(url: str) -> Optional[int]:
    """Extract year from URL path like 'circulares-externas-2023'."""
    match = re.search(r'(\d{4})/?$', url.rstrip('/'))
    if match:
        return int(match.group(1))
    return None


def extract_doc_type_from_url(url: str) -> str:
    """Determine document type from URL path."""
    url_lower = url.lower()
    if 'cartas-circulares' in url_lower or 'carta-circular' in url_lower:
        return "Carta Circular"
    if 'resolucio' in url_lower:
        return "Resolución"
    return "Circular Externa"


def classify_year_page(url: str) -> tuple:
    """Returns (doc_type_key, doc_type_label) based on URL."""
    url_lower = url.lower()
    if 'carta' in url_lower:
        return ("cartas-circulares", "Carta Circular")
    if 'resolucion' in url_lower:
        return ("resoluciones", "Resolución")
    return ("circulares-externas", "Circular Externa")


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    try:
        text = pdf_extract_text(io.BytesIO(pdf_bytes))
        # Clean up
        text = re.sub(r'\x0c', '\n', text)  # form feeds
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


class SuperFinancieraScraper(BaseScraper):
    """Scraper for CO/SuperFinanciera -- Colombian financial regulator circulars."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page with retries."""
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                time.sleep(3)
        return None

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        for attempt in range(3):
            try:
                time.sleep(PDF_DELAY)
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 200:
                    content_type = resp.headers.get('content-type', '')
                    disp = resp.headers.get('content-disposition', '')
                    if '.pdf' in disp.lower() or 'pdf' in content_type.lower() or 'octet' in content_type.lower():
                        return resp.content
                    logger.warning(f"Non-PDF response for {url}: {content_type}")
                    return resp.content  # try anyway
                logger.warning(f"HTTP {resp.status_code} for PDF {url}")
            except requests.RequestException as e:
                logger.warning(f"PDF download attempt {attempt+1} failed: {e}")
                time.sleep(5)
        return None

    def _get_year_pages(self) -> list:
        """Fetch the index page and extract all year page URLs with types."""
        index_url = f"{BASE_URL}/publicaciones/{INDEX_PAGE_ID}/"
        html = self._fetch_page(index_url)
        if not html:
            logger.error("Failed to fetch index page")
            return []

        year_pages = []
        # Find all internal links to year pages
        for match in re.finditer(
            r'href="(/publicaciones/(\d+)/[^"]*(?:circular|carta|resolucion)[^"]*)"',
            html, re.IGNORECASE
        ):
            path = match.group(1)
            page_id = match.group(2)
            full_url = f"{BASE_URL}{path}"

            # Determine year from URL
            year_match = re.search(r'(\d{4})', path.split('/')[-2] if '/' in path else path)
            if not year_match:
                # Try to extract from page title by fetching
                continue

            year = int(year_match.group(1))
            if year < 2005 or year > 2030:
                continue

            doc_type_key, doc_type_label = classify_year_page(path)

            year_pages.append({
                "url": full_url,
                "page_id": page_id,
                "year": year,
                "doc_type_key": doc_type_key,
                "doc_type_label": doc_type_label,
            })

        # Also check for loader.php style links
        for match in re.finditer(
            r'href="([^"]*loader\.php[^"]*lServicio=Publicaciones[^"]*id=(\d+)[^"]*)"[^>]*>\s*([^<]+)',
            html, re.IGNORECASE
        ):
            href = match.group(1).replace('&amp;', '&')
            page_id = match.group(2)
            text = match.group(3).strip()

            year_match = re.search(r'(\d{4})', text)
            if not year_match:
                continue
            year = int(year_match.group(1))
            if year < 2005 or year > 2030:
                continue

            doc_type_key, doc_type_label = classify_year_page(text)
            full_url = f"{BASE_URL}{href}" if href.startswith('/') else href

            # Avoid duplicates
            if any(yp['page_id'] == page_id for yp in year_pages):
                continue

            year_pages.append({
                "url": full_url,
                "page_id": page_id,
                "year": year,
                "doc_type_key": doc_type_key,
                "doc_type_label": doc_type_label,
            })

        # Deduplicate by page_id
        seen = set()
        deduped = []
        for yp in year_pages:
            if yp['page_id'] not in seen:
                seen.add(yp['page_id'])
                deduped.append(yp)

        logger.info(f"Found {len(deduped)} year pages")
        return sorted(deduped, key=lambda x: (x['year'], x['doc_type_key']), reverse=True)

    def _parse_year_page(self, year_page: dict) -> list:
        """Parse a year page to extract document metadata and download links."""
        # Use /publicaciones/ URL form
        url = f"{BASE_URL}/publicaciones/{year_page['page_id']}/"
        html = self._fetch_page(url)
        if not html:
            logger.warning(f"Failed to fetch year page {year_page['page_id']}")
            return []

        documents = []
        year = year_page['year']
        doc_type = year_page['doc_type_label']

        # Find the pgel content section
        pgel_idx = html.find('class="pgel"')
        if pgel_idx < 0:
            pgel_idx = 0
        content = html[pgel_idx:]

        # Find the table
        table_match = re.search(r'<table[^>]*>(.*?)</table>', content, re.DOTALL)
        if not table_match:
            logger.warning(f"No table found on year page {year_page['page_id']}")
            return []

        table_html = table_match.group(1)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

        for row in rows:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL)
            if len(cells) < 3:
                continue

            # First cell: number with download link
            num_cell = cells[0]
            num_text = re.sub(r'<[^>]+>', '', num_cell).strip()

            # Skip header row
            if num_text.lower() in ('número', 'numero', 'n°', 'nro', 'no.', ''):
                continue

            # Extract download link
            link_match = re.search(r'href="([^"]*loader\.php[^"]*idFile=\d+[^"]*)"', num_cell)
            if not link_match:
                continue

            download_path = link_match.group(1).replace('&amp;', '&')
            download_url = f"{BASE_URL}{download_path}" if download_path.startswith('/') else download_path

            # Second cell: date
            date_text = re.sub(r'<[^>]+>', '', cells[1]).strip()
            date_iso = parse_date(date_text, year)

            # Third cell: description
            description = re.sub(r'<[^>]+>', '', cells[2]).strip()
            # Clean up whitespace
            description = re.sub(r'\s+', ' ', description).strip()

            # Build document ID
            doc_id = f"{doc_type}-{num_text.zfill(3)}-{year}"

            # Build title
            title = f"{doc_type} {num_text} de {year}"
            if description:
                title = f"{title} — {description}"

            documents.append({
                "number": num_text,
                "year": year,
                "doc_type": doc_type,
                "date": date_iso or f"{year}-01-01",
                "description": description,
                "download_url": download_url,
                "doc_id": doc_id,
                "title": title,
            })

        logger.info(f"Found {len(documents)} documents on {doc_type} {year} page")
        return documents

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw document record."""
        return {
            "_id": raw["doc_id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw["date"],
            "url": raw["download_url"],
            "number": raw["number"],
            "year": raw["year"],
            "doc_type": raw["doc_type"],
            "description": raw.get("description", ""),
            "issuer": "Superintendencia Financiera de Colombia",
            "country": "CO",
            "language": "es",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all year pages."""
        year_pages = self._get_year_pages()
        total_docs = 0

        for yp in year_pages:
            docs = self._parse_year_page(yp)
            for doc in docs:
                # Download PDF and extract text
                pdf_bytes = self._fetch_pdf(doc["download_url"])
                if pdf_bytes:
                    text = extract_pdf_text(pdf_bytes)
                    if len(text) > 100:
                        doc["text"] = text
                        yield self.normalize(doc)
                        total_docs += 1
                    else:
                        logger.warning(f"Insufficient text ({len(text)} chars) for {doc['doc_id']}")
                else:
                    logger.warning(f"Failed to download PDF for {doc['doc_id']}")

        logger.info(f"Total documents fetched: {total_docs}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Yield documents from recent years only."""
        current_year = datetime.now().year
        year_pages = self._get_year_pages()
        # Filter to last 2 years
        recent = [yp for yp in year_pages if yp['year'] >= current_year - 1]

        for yp in recent:
            docs = self._parse_year_page(yp)
            for doc in docs:
                pdf_bytes = self._fetch_pdf(doc["download_url"])
                if pdf_bytes:
                    text = extract_pdf_text(pdf_bytes)
                    if len(text) > 100:
                        doc["text"] = text
                        yield self.normalize(doc)

    def fetch_sample(self, n: int = 15) -> list:
        """Fetch a sample of documents for testing."""
        year_pages = self._get_year_pages()
        if not year_pages:
            logger.error("No year pages found")
            return []

        samples = []
        # Get documents from the most recent year pages (one of each type)
        types_seen = set()
        pages_to_try = []
        for yp in year_pages:
            key = yp['doc_type_key']
            if key not in types_seen:
                types_seen.add(key)
                pages_to_try.append(yp)
            if len(pages_to_try) >= 3:
                break

        # If we don't have all 3 types, add more pages
        if len(pages_to_try) < 3:
            for yp in year_pages:
                if yp not in pages_to_try:
                    pages_to_try.append(yp)
                if len(pages_to_try) >= 4:
                    break

        for yp in pages_to_try:
            docs = self._parse_year_page(yp)
            # Take up to 5 from each page
            for doc in docs[:5]:
                if len(samples) >= n:
                    break
                pdf_bytes = self._fetch_pdf(doc["download_url"])
                if pdf_bytes:
                    text = extract_pdf_text(pdf_bytes)
                    if len(text) > 100:
                        doc["text"] = text
                        samples.append(self.normalize(doc))
                        logger.info(f"Sample {len(samples)}/{n}: {doc['doc_id']} ({len(text)} chars)")
                    else:
                        logger.warning(f"Skipping {doc['doc_id']}: only {len(text)} chars")
            if len(samples) >= n:
                break

        return samples

    def run_bootstrap(self, sample: bool = False):
        """Main entry point for bootstrap mode."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        if sample:
            records = self.fetch_sample(15)
            for i, record in enumerate(records):
                sample_path = SAMPLE_DIR / f"sample_{i+1:03d}.json"
                with open(sample_path, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
            return records
        else:
            count = 0
            for record in self.fetch_all():
                count += 1
                if count % 50 == 0:
                    logger.info(f"Fetched {count} documents so far")
            logger.info(f"Bootstrap complete: {count} documents")
            return count

    def test_connectivity(self):
        """Quick test that the source is reachable."""
        try:
            resp = self.session.get(
                f"{BASE_URL}/publicaciones/{INDEX_PAGE_ID}/",
                timeout=15
            )
            if resp.status_code == 200 and 'Circulares' in resp.text:
                print(f"OK: Index page reachable ({len(resp.text)} bytes)")
                return True
            print(f"FAIL: HTTP {resp.status_code}")
            return False
        except Exception as e:
            print(f"FAIL: {e}")
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CO/SuperFinanciera bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records")
    args = parser.parse_args()

    scraper = SuperFinancieraScraper()

    if args.command == "test":
        scraper.test_connectivity()
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} documents")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
