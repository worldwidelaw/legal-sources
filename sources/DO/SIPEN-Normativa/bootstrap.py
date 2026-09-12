#!/usr/bin/env python3
"""
DO/SIPEN-Normativa -- Superintendencia de Pensiones (Dominican Republic)

Fetches the full text of SIPEN's regulatory corpus:
  * Circulares — technical specifications and operational instructions
  * Resoluciones de la SIPEN — binding decisions of the Superintendencia
  * Resoluciones de la CCRLI — disability/occupational risk pension decisions

Strategy:
  sipen.gob.do is a Laravel/Bootstrap site. Three paginated listing pages
  enumerate the corpus as HTML tables with direct links to /documentos/*.pdf:
    /normativas/circulares              (~150 circulars)
    /normativas/resoluciones-de-la-sipen (~520 resolutions)
    /normativas/resoluciones-de-la-ccrli (~300 resolutions)
  Each row has: number, description, date, status, PDF download link.
  PDFs are downloaded and text-extracted with pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import io
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict
from html.parser import HTMLParser

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.SIPEN-Normativa")

BASE_URL = "https://sipen.gob.do"
MIN_TEXT_CHARS = 200

LISTING_PAGES = [
    ("/normativas/circulares", "circular"),
    ("/normativas/resoluciones-de-la-sipen", "resolucion_sipen"),
    ("/normativas/resoluciones-de-la-ccrli", "resolucion_ccrli"),
]

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def extract_pdf_text(content: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(
        r"(?:\b[A-ZÁÉÍÓÚÑ]\s){3,}[A-ZÁÉÍÓÚÑ]\b",
        lambda m: m.group(0).replace(" ", ""),
        text,
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO 8601."""
    date_str = date_str.strip()
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)),
                            int(m.group(1))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


class TableParser(HTMLParser):
    """Parse the SIPEN normativa HTML table into rows."""

    def __init__(self):
        super().__init__()
        self.rows: List[Dict] = []
        self._in_tbody = False
        self._in_tr = False
        self._in_td = False
        self._in_a = False
        self._current_row: List[str] = []
        self._current_cell = ""
        self._current_href = ""
        self._pdf_href = ""
        self._td_count = 0

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "tbody":
            self._in_tbody = True
        elif tag == "tr" and self._in_tbody:
            self._in_tr = True
            self._current_row = []
            self._pdf_href = ""
            self._td_count = 0
        elif tag == "td" and self._in_tr:
            self._in_td = True
            self._current_cell = ""
            self._td_count += 1
        elif tag == "a" and self._in_td:
            href = attrs_dict.get("href", "")
            if href.endswith(".pdf"):
                self._pdf_href = href
            self._in_a = True
        elif tag == "time" and self._in_td:
            dt = attrs_dict.get("datetime", "")
            if dt:
                self._current_cell += dt

    def handle_data(self, data):
        if self._in_td and not self._in_a:
            self._current_cell += data
        elif self._in_a and not self._current_cell.strip():
            # Only capture link text if cell is otherwise empty
            pass

    def handle_endtag(self, tag):
        if tag == "tbody":
            self._in_tbody = False
        elif tag == "tr" and self._in_tr:
            self._in_tr = False
            if len(self._current_row) >= 4 and self._pdf_href:
                self.rows.append({
                    "number": self._current_row[0].strip(),
                    "description": self._current_row[1].strip(),
                    "date_str": self._current_row[2].strip(),
                    "status": self._current_row[3].strip(),
                    "pdf_url": self._pdf_href,
                })
        elif tag == "td" and self._in_td:
            self._in_td = False
            self._current_row.append(self._current_cell)
        elif tag == "a":
            self._in_a = False


def parse_page(html: str) -> List[Dict]:
    parser = TableParser()
    parser.feed(html)
    return parser.rows


def get_max_page(html: str) -> int:
    pages = re.findall(r'\?page=(\d+)', html)
    if not pages:
        return 1
    return max(int(p) for p in pages)


class SIPENNormativaScraper(BaseScraper):
    """
    Scraper for DO/SIPEN-Normativa — Superintendencia de Pensiones
    (Dominican Republic).
    Country: DO
    URL: https://sipen.gob.do/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) LegalDataHunter/1.0 "
                          "(research; https://github.com/ZachLaik/LegalDataHunter)",
        })

    def _discover_category(self, path: str, category: str,
                           max_pages: Optional[int] = None) -> List[Dict]:
        """Discover all documents from a paginated listing page."""
        items = []
        url = BASE_URL + path
        try:
            r = self.session.get(url, timeout=30)
            if r.status_code != 200:
                logger.warning(f"{path}: HTTP {r.status_code}")
                return items
        except Exception as e:
            logger.warning(f"{path} failed: {e}")
            return items

        total_pages = get_max_page(r.text)
        if max_pages:
            total_pages = min(total_pages, max_pages)
        logger.info(f"{path}: {total_pages} pages to scrape")

        rows = parse_page(r.text)
        for row in rows:
            row["category"] = category
        items.extend(rows)

        for page_num in range(2, total_pages + 1):
            time.sleep(1.0)
            try:
                r = self.session.get(f"{url}?page={page_num}", timeout=30)
                if r.status_code != 200:
                    logger.warning(f"{path}?page={page_num}: HTTP {r.status_code}")
                    continue
            except Exception as e:
                logger.warning(f"{path}?page={page_num} failed: {e}")
                continue
            rows = parse_page(r.text)
            for row in rows:
                row["category"] = category
            items.extend(rows)
            if page_num % 10 == 0:
                logger.info(f"  {path}: scraped {page_num}/{total_pages} pages ({len(items)} items)")

        logger.info(f"{path}: {len(items)} documents discovered")
        return items

    def _discover(self, sample: bool = False) -> List[Dict]:
        """Discover all documents across all categories."""
        all_items = []
        for path, category in LISTING_PAGES:
            max_pages = 2 if sample else None
            items = self._discover_category(path, category, max_pages=max_pages)
            all_items.extend(items)
            time.sleep(1.0)
        logger.info(f"Total discovered: {len(all_items)} documents")
        return all_items

    def _download_and_extract(self, item: Dict) -> Optional[dict]:
        """Download a PDF and extract full text."""
        pdf_url = item["pdf_url"]
        try:
            r = self.session.get(pdf_url, timeout=90)
            if r.status_code != 200 or r.content[:4] != b"%PDF":
                logger.debug(f"Not a live PDF ({r.status_code}): {pdf_url}")
                return None
        except Exception as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

        text = clean_text(extract_pdf_text(r.content))
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text ({len(text)} chars): {pdf_url}")
            return None

        date = parse_date(item.get("date_str", ""))
        title = item["description"] or item["number"]

        return {
            "number": item["number"],
            "title": title,
            "text": text,
            "date": date,
            "date_str": item.get("date_str", ""),
            "status": item.get("status", ""),
            "category": item["category"],
            "pdf_url": pdf_url,
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["pdf_url"],
            "_source": "DO/SIPEN-Normativa",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "doc_number": raw.get("number"),
            "category": raw.get("category"),
            "status": raw.get("status"),
            "issuer": "Superintendencia de Pensiones (SIPEN)",
            "jurisdiction": "DO",
            "language": "es",
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        items = self._discover(sample=False)
        yielded = 0
        for item in items:
            result = self._download_and_extract(item)
            if result:
                yield result
                yielded += 1
                if yielded % 25 == 0:
                    logger.info(f"Extracted {yielded} documents...")
            time.sleep(1.0)
        logger.info(f"fetch_all complete: {yielded} documents with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        items = self._discover(sample=False)
        yielded = 0
        for item in items:
            result = self._download_and_extract(item)
            if result:
                if result.get("date") and result["date"] < since:
                    continue
                yield result
                yielded += 1
            time.sleep(1.0)
        logger.info(f"fetch_updates complete: {yielded} documents")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="DO/SIPEN-Normativa — Superintendencia de Pensiones"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    bf = subparsers.add_parser("bootstrap-fast", help="Alias for bootstrap")
    bf.add_argument("--sample", action="store_true", help="Sample mode")
    bf.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bf.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = SIPENNormativaScraper()

    if args.command == "test":
        logger.info("Testing SIPEN connectivity...")
        items = scraper._discover(sample=True)
        if not items:
            logger.error("No documents discovered")
            sys.exit(1)
        logger.info(f"First candidate: {items[0]['pdf_url']}")
        result = scraper._download_and_extract(items[0])
        if result:
            logger.info(f"Title: {result['title'][:100]}")
            logger.info(f"Number: {result['number']} | Category: {result['category']}")
            logger.info(f"Date: {result['date']} | Text: {len(result['text'])} chars")
            logger.info(f"Preview: {result['text'][:200]}")
            logger.info("Connectivity test passed!")
        else:
            logger.error("Failed to extract text from first candidate")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
