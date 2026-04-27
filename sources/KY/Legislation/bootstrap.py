#!/usr/bin/env python3
"""
KY/Legislation -- Cayman Islands Legislation (legislation.gov.ky)

Fetches consolidated laws from the official Cayman Islands legislation portal.
The site stores PDFs in a browsable directory structure under
/cms/images/LEGISLATION/PRINCIPAL/{year}/{id}/ with multiple revision files.

Strategy:
  1. Crawl /cms/images/LEGISLATION/PRINCIPAL/ for year directories
  2. For each year, list legislation item directories
  3. For each item, find the latest revision PDF (prefer "Revision" over "Act")
  4. Download PDF and extract full text via pdfplumber/pypdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.KY.Legislation")

BASE_URL = "https://legislation.gov.ky"
PRINCIPAL_DIR = "/cms/images/LEGISLATION/PRINCIPAL/"
SUBORDINATE_DIR = "/cms/images/LEGISLATION/SUBORDINATE/"


class _LinkParser(HTMLParser):
    """Extract href links from directory listing HTML."""

    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)


def _parse_directory_links(html: str) -> List[str]:
    """Parse directory listing HTML and return href links."""
    parser = _LinkParser()
    parser.feed(html)
    return parser.links


def _extract_year_dirs(links: List[str]) -> List[str]:
    """Filter links to year directories (4-digit years)."""
    year_dirs = []
    for link in links:
        clean = link.strip("/").split("/")[-1]
        if re.match(r"^\d{4}$", clean):
            year_dirs.append(clean)
    return sorted(year_dirs)


def _extract_item_dirs(links: List[str]) -> List[str]:
    """Filter links to legislation item directories (e.g., 2020-0001)."""
    items = []
    for link in links:
        clean = link.strip("/").split("/")[-1]
        if re.match(r"^\d{4}-\w+$", clean):
            items.append(clean)
    return items


def _pick_best_pdf(links: List[str], item_id: str) -> Optional[str]:
    """Pick the best PDF from a list: prefer latest Revision, then Act."""
    pdfs = [l for l in links if l.lower().endswith(".pdf")]
    if not pdfs:
        return None

    # Prefer "Revision" PDFs (consolidated text)
    revision_pdfs = [p for p in pdfs if "revision" in p.lower()]
    if revision_pdfs:
        # Pick the latest revision by year in filename
        def extract_rev_year(name):
            m = re.search(r"(\d{4})\s*Revision", name, re.IGNORECASE)
            return int(m.group(1)) if m else 0
        revision_pdfs.sort(key=extract_rev_year, reverse=True)
        return revision_pdfs[0]

    # Fall back to "Act" PDFs
    act_pdfs = [p for p in pdfs if "act" in p.lower()]
    if act_pdfs:
        return act_pdfs[0]

    # Last resort: any PDF
    return pdfs[0]


def _title_from_pdf_text(text: str) -> str:
    """Extract title from the first lines of extracted PDF text."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    # Often the title is in the first few lines, sometimes after a header
    title_lines = []
    for line in lines[:10]:
        # Skip common headers
        if re.match(r"^(Cayman Islands|CAYMAN ISLANDS|Page \d|Supplement)", line, re.IGNORECASE):
            continue
        if re.match(r"^\d{4}\s+Revision$", line, re.IGNORECASE):
            continue
        if re.match(r"^(Published by|Printed and|Under the authority)", line, re.IGNORECASE):
            continue
        title_lines.append(line)
        # Stop after 2 meaningful lines
        if len(title_lines) >= 2:
            break
    return " ".join(title_lines) if title_lines else "Untitled"


class CaymanLegislationScraper(BaseScraper):
    """Scraper for KY/Legislation -- Cayman Islands Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _fetch_directory(self, path: str) -> List[str]:
        """Fetch a directory listing and return links."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return _parse_directory_links(resp.text)
        except Exception as e:
            logger.warning(f"Failed to fetch directory {path}: {e}")
            return []

    def _download_pdf(self, pdf_path: str) -> Optional[bytes]:
        """Download a PDF file and return bytes."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_path)
            resp.raise_for_status()
            if resp.content and resp.content[:5] == b"%PDF-":
                return resp.content
            logger.warning(f"Not a valid PDF: {pdf_path}")
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_path}: {e}")
            return None

    def _crawl_items(self, base_dir: str, leg_type: str) -> Generator[Dict[str, Any], None, None]:
        """Crawl a legislation directory (PRINCIPAL or SUBORDINATE) and yield items."""
        logger.info(f"Crawling {leg_type} legislation: {base_dir}")

        # Get year directories
        year_links = self._fetch_directory(base_dir)
        years = _extract_year_dirs(year_links)
        logger.info(f"Found {len(years)} year directories for {leg_type}")

        for year in years:
            year_path = f"{base_dir}{year}/"
            item_links = self._fetch_directory(year_path)
            items = _extract_item_dirs(item_links)

            if not items:
                continue

            logger.info(f"Year {year}: {len(items)} items")

            for item_id in items:
                item_path = f"{year_path}{item_id}/"
                file_links = self._fetch_directory(item_path)

                pdf_file = _pick_best_pdf(file_links, item_id)
                if not pdf_file:
                    logger.warning(f"No PDF found for {item_id}")
                    continue

                # Build full PDF path
                # Links may be relative or absolute
                if pdf_file.startswith("/"):
                    pdf_path = pdf_file
                elif pdf_file.startswith("http"):
                    pdf_path = pdf_file.replace(BASE_URL, "")
                else:
                    pdf_path = f"{item_path}{pdf_file}"

                pdf_url = f"{BASE_URL}{pdf_path}"

                yield {
                    "legislation_id": item_id,
                    "year": year,
                    "type": leg_type,
                    "pdf_filename": pdf_file,
                    "pdf_path": pdf_path,
                    "pdf_url": pdf_url,
                }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        leg_id = raw.get("legislation_id", "")
        leg_type = raw.get("type", "principal")
        title = raw.get("title", "")
        if not title:
            title = _title_from_pdf_text(raw.get("text", ""))

        return {
            "_id": f"KY/Legislation/{leg_id}",
            "_source": "KY/Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("year", ""),
            "url": raw.get("pdf_url", ""),
            "legislation_id": leg_id,
            "legislation_type": leg_type,
            "pdf_filename": raw.get("pdf_filename", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        errors = 0

        # Process principal acts
        for item in self._crawl_items(PRINCIPAL_DIR, "principal"):
            pdf_bytes = self._download_pdf(item["pdf_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="KY/Legislation",
                source_id=item["legislation_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(f"Insufficient text for {item['legislation_id']}: {len(text)} chars")
                errors += 1
                continue

            item["text"] = text
            item["title"] = _title_from_pdf_text(text)
            yield item
            count += 1

            if count % 50 == 0:
                logger.info(f"Progress: {count} records, {errors} errors")

        # Process subordinate legislation
        for item in self._crawl_items(SUBORDINATE_DIR, "subordinate"):
            pdf_bytes = self._download_pdf(item["pdf_path"])
            if not pdf_bytes:
                errors += 1
                continue

            text = extract_pdf_markdown(
                source="KY/Legislation",
                source_id=item["legislation_id"],
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if not text or len(text.strip()) < 50:
                logger.warning(f"Insufficient text for {item['legislation_id']}: {len(text)} chars")
                errors += 1
                continue

            item["text"] = text
            item["title"] = _title_from_pdf_text(text)
            yield item
            count += 1

            if count % 50 == 0:
                logger.info(f"Progress: {count} records, {errors} errors")

        logger.info(f"Completed: {count} records, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = CaymanLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing directory listing...")
        links = scraper._fetch_directory(PRINCIPAL_DIR)
        years = _extract_year_dirs(links)
        if not years:
            logger.error("FAILED — no year directories found")
            sys.exit(1)
        logger.info(f"OK — {len(years)} year directories found")

        # Test one item
        year = years[-1]  # Most recent year
        item_links = scraper._fetch_directory(f"{PRINCIPAL_DIR}{year}/")
        items = _extract_item_dirs(item_links)
        if not items:
            logger.error(f"FAILED — no items in year {year}")
            sys.exit(1)
        logger.info(f"OK — {len(items)} items in {year}")

        # Test PDF download
        item_id = items[0]
        file_links = scraper._fetch_directory(f"{PRINCIPAL_DIR}{year}/{item_id}/")
        pdf_file = _pick_best_pdf(file_links, item_id)
        if not pdf_file:
            logger.error(f"FAILED — no PDF in {item_id}")
            sys.exit(1)

        pdf_path = f"{PRINCIPAL_DIR}{year}/{item_id}/{pdf_file}"
        pdf_bytes = scraper._download_pdf(pdf_path)
        if pdf_bytes:
            text = extract_pdf_markdown(
                source="KY/Legislation",
                source_id=item_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""
            logger.info(f"OK — PDF extracted, {len(text)} chars from {pdf_file}")
        else:
            logger.error("FAILED — PDF download failed")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
