#!/usr/bin/env python3
"""
IM/Legislation -- Isle of Man Legislation (Acts of Tynwald)

Fetches consolidated legislation from legislation.gov.im via open directory
listings at /cms/images/LEGISLATION/. Both principal (Acts) and subordinate
(Regulations, Orders) legislation are stored as PDFs with selectable text.

For each item directory (e.g., PRINCIPAL/2019/2019-0003/), we pick the
latest version PDF (highest _N suffix) and extract full text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IM.Legislation")

BASE_URL = "https://legislation.gov.im"
HEADERS = {
    "User-Agent": "legal-data-hunter/1.0 (research)",
    "Accept": "*/*",
}

LEG_TYPES = [
    ("PRINCIPAL", "principal"),
    ("SUBORDINATE", "subordinate"),
]
LEG_BASE = "/cms/images/LEGISLATION/"


class LinkParser(HTMLParser):
    """Extract href links from an HTML directory listing."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)


def parse_links(html: str) -> list[str]:
    """Parse directory listing HTML and return hrefs."""
    parser = LinkParser()
    parser.feed(html)
    return parser.links


def pick_latest_pdf(pdf_files: list[str]) -> Optional[str]:
    """From a list of PDF filenames, pick the latest version.

    Naming convention: 2019-0003.pdf (original), 2019-0003_1.pdf (v1),
    2019-0003_2.pdf (v2), etc. Also _R.pdf for repealed versions.
    We prefer the highest numbered version, falling back to the base file.
    """
    if not pdf_files:
        return None

    numbered = []
    base = None
    for f in pdf_files:
        # Match _N.pdf where N is a number
        m = re.search(r'_(\d+)\.pdf$', f, re.IGNORECASE)
        if m:
            numbered.append((int(m.group(1)), f))
        elif re.search(r'\.pdf$', f, re.IGNORECASE) and '_R.' not in f.upper():
            base = f

    if numbered:
        numbered.sort(key=lambda x: x[0], reverse=True)
        return numbered[0][1]
    return base


def extract_title_from_text(text: str) -> str:
    """Extract a title from the first meaningful lines of PDF text."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    title_lines = []
    for line in lines[:20]:
        # Skip page headers, numbers, dates
        if re.match(r'^(page\s+\d+|p\.\s*\d+|\d+$)', line, re.IGNORECASE):
            continue
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}', line):
            continue
        if len(line) < 4:
            continue
        title_lines.append(line)
        # If we have a decent title, stop
        if len(" ".join(title_lines)) > 30 or len(title_lines) >= 2:
            break

    title = " ".join(title_lines).strip()
    # Remove markdown headers
    title = re.sub(r'^#+\s*', '', title)
    if len(title) > 250:
        title = title[:247] + "..."
    return title or "Untitled"


class LegislationScraper(BaseScraper):
    """Scraper for Isle of Man legislation from directory listings."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            max_retries=3,
            backoff_factor=1.0,
            timeout=30,
        )

    def _get_page(self, path: str) -> Optional[str]:
        """Fetch an HTML page."""
        try:
            resp = self.client.get(path, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {path}: {e}")
            return None

    def _list_dirs(self, path: str) -> list[str]:
        """List subdirectories from a directory listing page."""
        html = self._get_page(path)
        if not html:
            return []
        links = parse_links(html)
        dirs = []
        for link in links:
            # Directory links end with / and are not parent (..)
            if link.endswith("/") and not link.startswith("..") and not link.startswith("/"):
                dirs.append(link.rstrip("/"))
        return dirs

    def _list_pdfs(self, path: str) -> list[str]:
        """List PDF files from a directory listing page."""
        html = self._get_page(path)
        if not html:
            return []
        links = parse_links(html)
        return [l for l in links if l.lower().endswith(".pdf")]

    def _download_pdf(self, url_path: str) -> Optional[bytes]:
        """Download a PDF and return bytes."""
        try:
            resp = self.client.get(url_path, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 200:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url_path}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"PDF download failed for {url_path}: {e}")
            return None

    def _enumerate_items(self, leg_type_dir: str, leg_type_label: str,
                         recent_years_only: bool = False) -> Generator[dict, None, None]:
        """Enumerate all legislation items for a given type (PRINCIPAL or SUBORDINATE).

        Yields dicts with: dir_path, leg_type, year, reference
        """
        base_path = f"{LEG_BASE}{leg_type_dir}/"
        years = self._list_dirs(base_path)
        time.sleep(0.5)

        if recent_years_only:
            # For sample mode, just use last 3 years
            years = sorted(years, reverse=True)[:3]

        for year_dir in sorted(years, reverse=True):
            year_path = f"{base_path}{year_dir}/"
            items = self._list_dirs(year_path)
            time.sleep(0.5)

            for item_dir in items:
                item_path = f"{year_path}{item_dir}/"
                yield {
                    "dir_path": item_path,
                    "leg_type": leg_type_label,
                    "year": year_dir,
                    "reference": item_dir,
                }

    def _process_item(self, item: dict) -> Optional[dict]:
        """Download the latest PDF for an item and extract text."""
        dir_path = item["dir_path"]
        pdfs = self._list_pdfs(dir_path)
        time.sleep(0.5)

        latest = pick_latest_pdf(pdfs)
        if not latest:
            logger.warning(f"No PDF found in {dir_path}")
            return None

        pdf_url = f"{dir_path}{latest}"
        pdf_bytes = self._download_pdf(pdf_url)
        time.sleep(1)

        if not pdf_bytes:
            return None

        doc_id = f"{item['leg_type'].upper()}/{item['reference']}"
        text = extract_pdf_markdown("IM/Legislation", doc_id, pdf_bytes=pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text from {pdf_url} ({len(text) if text else 0} chars)")
            return None

        return {
            "doc_id": doc_id,
            "reference": item["reference"],
            "year": item["year"],
            "leg_type": item["leg_type"],
            "text": text,
            "pdf_url": pdf_url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation items with full text."""
        for leg_dir, leg_label in LEG_TYPES:
            logger.info(f"Enumerating {leg_label} legislation...")
            count = 0
            for item in self._enumerate_items(leg_dir, leg_label):
                result = self._process_item(item)
                if result:
                    count += 1
                    yield result
                    if count % 25 == 0:
                        logger.info(f"Processed {count} {leg_label} items")
            logger.info(f"Total {leg_label} items: {count}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent legislation (last 3 years of each type)."""
        for leg_dir, leg_label in LEG_TYPES:
            logger.info(f"Fetching recent {leg_label} legislation...")
            for item in self._enumerate_items(leg_dir, leg_label, recent_years_only=True):
                result = self._process_item(item)
                if result:
                    yield result

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        doc_id = raw["doc_id"]
        title = extract_title_from_text(text)
        year = raw.get("year", "")
        pdf_url = raw.get("pdf_url", "")

        return {
            "_id": doc_id,
            "_source": "IM/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": f"{year}-01-01" if year and year.isdigit() else None,
            "url": BASE_URL + pdf_url if pdf_url else "",
            "leg_type": raw.get("leg_type", ""),
            "year": year,
            "reference": raw.get("reference", ""),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to legislation.gov.im."""
        try:
            resp = self.client.get(f"{LEG_BASE}PRINCIPAL/", timeout=15)
            if resp.status_code == 200:
                logger.info("Connection test passed")
                return True
            logger.error(f"Unexpected response: status={resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = LegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        count = 15 if sample_mode else 0
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=count or 10)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
