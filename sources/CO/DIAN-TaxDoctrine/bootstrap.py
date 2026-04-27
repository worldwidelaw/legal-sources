#!/usr/bin/env python3
"""
CO/DIAN-TaxDoctrine -- Colombian Tax Authority Doctrine (Conceptos y Oficios)

Fetches tax doctrine documents from DIAN's Normograma legal compilation.
5,000+ conceptos and oficios covering income tax, VAT, withholding, transfer pricing,
customs, and exchange controls. Documents from 1987 to present.

Source: https://normograma.dian.gov.co/dian/compilacion/t_2_doctrina_tributaria.html
  - Individual documents at: /dian/compilacion/docs/oficio_dian_{NUMBER}_{YEAR}.htm
  - Numbers < 1000 are zero-padded to 4 digits (e.g., 0207)
  - Numbers >= 1000 are unpadded (e.g., 5035, 13272)
  - Full text in HTML, extracted by stripping tags from panel-documento div

Usage:
  python bootstrap.py bootstrap          # Full scan and fetch
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records (last 2 years)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as htmlmod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.DIAN-TaxDoctrine")

BASE_URL = "https://normograma.dian.gov.co/dian/compilacion/docs/"
SOURCE_ID = "CO/DIAN-TaxDoctrine"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html",
}

DELAY = 1.5  # seconds between requests
HEAD_DELAY = 0.3  # faster for HEAD requests (discovery)

# Known valid document numbers for sample mode (verified to exist)
SAMPLE_DOCS = [
    (207, 2025), (991, 2025), (3524, 2025), (4510, 2025),
    (5035, 2025), (11861, 2025), (13272, 2025), (18226, 2025),
    (1513, 2024), (3028, 2024), (3093, 2024), (4772, 2024),
    (5917, 2024), (7058, 2024), (8660, 2024), (9485, 2024),
]


def format_doc_number(number: int) -> str:
    """Format document number: zero-pad to 4 digits if < 1000."""
    if number < 1000:
        return f"{number:04d}"
    return str(number)


def build_url(number: int, year: int) -> str:
    """Build the URL for a DIAN doctrine document."""
    return f"{BASE_URL}oficio_dian_{format_doc_number(number)}_{year}.htm"


def extract_text(html_content: str) -> str:
    """Extract clean text from DIAN normograma HTML page."""
    # Find the panel-documento div
    start = html_content.find('class="panel-documento"')
    if start < 0:
        return ""

    # Find end boundary
    end = len(html_content)
    for marker in ['class="ir-arriba"', 'class="contenedor-barra-creditos"',
                   'class="contenedor-footer"']:
        idx = html_content.find(marker, start + 100)
        if 0 < idx < end:
            end = idx

    chunk = html_content[start:end]

    # Remove script/style blocks
    chunk = re.sub(r'<style[^>]*>.*?</style>', '', chunk, flags=re.DOTALL)
    chunk = re.sub(r'<script[^>]*>.*?</script>', '', chunk, flags=re.DOTALL)

    # Replace block elements with newlines
    chunk = re.sub(r'<br\s*/?>', '\n', chunk, flags=re.IGNORECASE)
    chunk = re.sub(r'</(?:p|div|h[1-6]|li|tr|td)>', '\n', chunk, flags=re.IGNORECASE)

    # Strip remaining tags
    chunk = re.sub(r'<[^>]+>', ' ', chunk)

    # Decode HTML entities
    text = htmlmod.unescape(chunk)

    # Clean whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    # Remove leading div class noise
    text = re.sub(r'^panel-documento">\s*', '', text)

    return text


def extract_title(html_content: str) -> str:
    """Extract document title from HTML title tag."""
    match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.DOTALL | re.IGNORECASE)
    if match:
        title = htmlmod.unescape(match.group(1).strip())
        # Remove prefix
        title = re.sub(r'^Compilación Jurídica de la DIAN\s*-\s*', '', title)
        return title
    return ""


def extract_date(text: str) -> Optional[str]:
    """Extract date from document text. Returns ISO format or None."""
    # Pattern: "(mes DD)" or "(DD de mes de YYYY)" near the top
    months_es = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12',
    }

    # Try "DE YYYY (mes DD)" pattern at the beginning
    match = re.search(r'DE\s+(\d{4})\s*\n?\s*\((\w+)\s+(\d{1,2})\)', text[:500])
    if match:
        year = match.group(1)
        month = months_es.get(match.group(2).lower())
        day = match.group(3).zfill(2)
        if month:
            return f"{year}-{month}-{day}"

    # Try "(DD de mes de YYYY)"
    match = re.search(r'\((\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})\)', text[:500])
    if match:
        day = match.group(1).zfill(2)
        month = months_es.get(match.group(2).lower())
        year = match.group(3)
        if month:
            return f"{year}-{month}-{day}"

    return None


def extract_subject(text: str) -> str:
    """Extract subject/descriptors from the document text."""
    match = re.search(r'Descriptores?\s+(.*?)(?:\n|Fuentes)', text[:1000])
    if match:
        return match.group(1).strip()
    return ""


class DIANTaxDoctrineScraper(BaseScraper):
    """Scraper for CO/DIAN-TaxDoctrine -- Colombian tax doctrine documents."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {"last_year": None, "last_number": 0}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _fetch_document(self, number: int, year: int) -> Optional[dict]:
        """Fetch and parse a single DIAN doctrine document."""
        url = build_url(number, year)

        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(url, timeout=20)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return {"html": resp.text, "number": number, "year": year, "url": url}
            except requests.exceptions.ConnectionError as e:
                wait = 3 * (attempt + 1)
                logger.warning("Attempt %d failed for %s: %s. Retrying in %ds...",
                               attempt + 1, url, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Failed to fetch %s: %s", url, e)
                return None

        return None

    def _check_exists(self, number: int, year: int) -> bool:
        """Check if a document exists using HEAD request (fast discovery)."""
        url = build_url(number, year)
        try:
            time.sleep(HEAD_DELAY)
            resp = self.session.head(url, timeout=10)
            return resp.status_code == 200
        except Exception:
            return False

    def normalize(self, doc: dict) -> dict:
        """Transform a fetched document into the standard schema."""
        html_content = doc["html"]
        number = doc["number"]
        year = doc["year"]
        url = doc["url"]

        text = extract_text(html_content)
        title = extract_title(html_content)
        date = extract_date(text)
        subject = extract_subject(text)

        if not title:
            title = f"Concepto {number} de {year} DIAN"

        doc_id = f"CO-DIAN-{number}-{year}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date or f"{year}-01-01",
            "url": url,
            "language": "es",
            "concept_number": str(number),
            "year": year,
            "subject": subject,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch DIAN tax doctrine documents."""
        if sample:
            yield from self._fetch_sample()
            return

        # Full bootstrap: scan number ranges for recent years
        checkpoint = self._load_checkpoint()
        current_year = datetime.now().year
        years = list(range(current_year, current_year - 5, -1))  # Last 5 years
        total_yielded = 0

        for year in years:
            logger.info("Scanning year %d...", year)
            found_in_year = 0

            # Scan ranges: 1-20000 using HEAD to discover, then GET to fetch
            for number in range(1, 20001):
                if self._check_exists(number, year):
                    raw = self._fetch_document(number, year)
                    if raw:
                        record = self.normalize(raw)
                        if record["text"] and len(record["text"]) >= 100:
                            yield record
                            total_yielded += 1
                            found_in_year += 1
                            if total_yielded % 50 == 0:
                                logger.info("Progress: %d docs fetched (%d in %d)",
                                            total_yielded, found_in_year, year)

                checkpoint["last_year"] = year
                checkpoint["last_number"] = number
                if number % 500 == 0:
                    self._save_checkpoint(checkpoint)

            logger.info("Year %d complete: %d documents found", year, found_in_year)

        logger.info("Fetch complete. Total: %d documents", total_yielded)

    def _fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a sample of known valid documents."""
        count = 0
        for number, year in SAMPLE_DOCS:
            if count >= 15:
                break
            raw = self._fetch_document(number, year)
            if not raw:
                logger.warning("Sample doc %d/%d not found", number, year)
                continue
            record = self.normalize(raw)
            if not record["text"] or len(record["text"]) < 100:
                logger.warning("Sample doc %d/%d has insufficient text (%d chars)",
                               number, year, len(record["text"]))
                continue
            yield record
            count += 1
            logger.info("  [%d] %s | %s | text=%d chars",
                        count, record["date"], record["title"][:60], len(record["text"]))

        logger.info("Sample complete: %d documents fetched", count)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch documents from recent years."""
        current_year = datetime.now().year
        years = [current_year, current_year - 1]
        total = 0

        for year in years:
            logger.info("Scanning year %d for updates...", year)
            for number in range(1, 20001):
                if self._check_exists(number, year):
                    raw = self._fetch_document(number, year)
                    if raw:
                        record = self.normalize(raw)
                        if record["text"] and len(record["text"]) >= 100:
                            yield record
                            total += 1

        logger.info("Update complete: %d new records", total)

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing connectivity to DIAN Normograma...")
        try:
            raw = self._fetch_document(207, 2025)
            if not raw:
                logger.error("Test failed: could not fetch known document")
                return False
            record = self.normalize(raw)
            logger.info("OK: '%s' (%d chars text)", record["title"][:60], len(record["text"]))
            logger.info("Test PASSED")
            return True
        except Exception as e:
            logger.error("Test FAILED: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description='CO/DIAN-TaxDoctrine fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'])
    parser.add_argument('--sample', action='store_true', help='Fetch 15 sample records')
    parser.add_argument('--since', type=str, help='Date for update (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DIANTaxDoctrineScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])
            out_file = SAMPLE_DIR / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf-8')
            count += 1
        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    elif args.command == 'update':
        count = 0
        for record in scraper.fetch_updates(since=args.since):
            count += 1
            logger.info("  [%d] %s: %s", count, record["date"], record["title"][:60])
        logger.info("Update complete: %d new records", count)


if __name__ == '__main__':
    main()
