#!/usr/bin/env python3
"""
IT/AGCOM -- Italian Communications Authority Data Fetcher

Fetches regulatory decisions (delibere, determine) from AGCOM website.

Strategy:
  - Scrapes paginated listing at https://www.agcom.it/provvedimenti?page=N
  - Extracts metadata (title, category, sector, date, subtitle) from listing cards
  - Downloads individual delibera pages to get PDF links
  - Extracts full text from PDFs using pdfplumber
  - 651 pages × 30 items = ~19,500 decisions total

License: Italian Open Data (public domain)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (last 30 days)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import io
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.AGCOM")

BASE_URL = "https://www.agcom.it"
LISTING_URL = "https://www.agcom.it/provvedimenti"


class AGCOMScraper(BaseScraper):
    """
    Scraper for IT/AGCOM -- Italian Communications Authority.
    Country: IT
    URL: https://www.agcom.it

    Fetches regulatory decisions from paginated listing + PDF full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _curl_get(self, url: str, timeout: int = 60) -> Optional[bytes]:
        """Download URL using curl."""
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", str(timeout), url],
                capture_output=True,
                timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
            return None
        except Exception as e:
            logger.warning(f"curl failed for {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IT/AGCOM",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def _parse_listing_page(self, html: str) -> list:
        """Parse listing page HTML to extract delibera metadata."""
        items = []

        # Find all bookmark links (each is one delibera)
        links = list(re.finditer(
            r'href="(/provvedimenti/[^"]+)"[^>]*rel="bookmark"[^>]*>\s*<span>([^<]+)</span>',
            html
        ))

        for link_match in links:
            path = link_match.group(1)
            title = link_match.group(2).strip()
            pos = link_match.start()

            # Look backward for sector and category
            context_before = html[max(0, pos - 600):pos]

            sector_match = re.search(
                r'flag-icon[^>]*>.*?</i>\s*([^<\n]+)',
                context_before, re.S
            )
            sector = sector_match.group(1).strip() if sector_match else None

            cat_match = re.search(
                r'class="category">([^<]+)',
                context_before
            )
            category = cat_match.group(1).strip() if cat_match else None

            # Date may appear as "Roma,&nbsp; 20/03/2026" or just "19/03/2026"
            date_match = re.search(
                r'class="data">(.*?)</span>',
                context_before, re.S
            )
            date_str = None
            if date_match:
                date_text = re.sub(r'<[^>]+>', '', date_match.group(1))
                date_text = date_text.replace('&nbsp;', ' ').strip()
                d = re.search(r'(\d{2}/\d{2}/\d{4})', date_text)
                if d:
                    date_str = d.group(1)

            # Look forward for subtitle
            context_after = html[link_match.end():link_match.end() + 500]
            subtitle_match = re.search(
                r'card-subtitle">\s*([^<]+)',
                context_after
            )
            subtitle = subtitle_match.group(1).strip() if subtitle_match else None

            items.append({
                "path": path,
                "title": title,
                "category": category,
                "sector": sector,
                "date_str": date_str,
                "subtitle": subtitle,
            })

        return items

    def _get_pdf_url_from_detail(self, path: str) -> Optional[str]:
        """Fetch the detail page and extract the main PDF link."""
        url = BASE_URL + path
        data = self._curl_get(url)
        if not data:
            return None
        html = data.decode("utf-8", errors="ignore")

        # Find PDF link in article - look for "Documento Principale" link
        pdf_match = re.search(
            r'href="(/sites/default/files/[^"]+\.pdf)"',
            html
        )
        if pdf_match:
            return BASE_URL + pdf_match.group(1)

        return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse Italian date DD/MM/YYYY to ISO format."""
        if not date_str:
            return None
        try:
            parts = date_str.strip().split("/")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except Exception:
            pass
        return None

    def _make_id(self, title: str) -> str:
        """Create a stable ID from the delibera title."""
        # e.g. "Delibera 57/26/CONS" -> "delibera-57-26-cons"
        return re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')

    def normalize(self, raw: Dict) -> Dict:
        """Normalize raw data into standard schema."""
        title = raw.get("title", "")
        doc_id = self._make_id(title)
        date = self._parse_date(raw.get("date_str"))

        subtitle = raw.get("subtitle", "") or ""
        full_title = f"{title}: {subtitle}" if subtitle else title

        return {
            "_id": doc_id,
            "_source": "IT/AGCOM",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": raw.get("text", ""),
            "date": date,
            "url": BASE_URL + raw.get("path", ""),
            "delibera_number": title.split(maxsplit=1)[1] if " " in title else title,
            "category": raw.get("category"),
            "sector": raw.get("sector"),
            "pdf_url": raw.get("pdf_url"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch all AGCOM decisions."""
        max_pages = 3 if sample else 651
        total = 0
        sample_limit = 15 if sample else float("inf")

        for page_num in range(max_pages):
            if total >= sample_limit:
                break

            url = f"{LISTING_URL}?page={page_num}"
            logger.info(f"Fetching listing page {page_num}...")
            data = self._curl_get(url)
            if not data:
                logger.warning(f"Failed to fetch page {page_num}")
                continue

            html = data.decode("utf-8", errors="ignore")
            items = self._parse_listing_page(html)

            if not items:
                logger.info(f"No items on page {page_num}, stopping.")
                break

            for item in items:
                if total >= sample_limit:
                    break

                # Get PDF URL from detail page
                logger.info(f"Fetching detail: {item['title']}")
                pdf_url = self._get_pdf_url_from_detail(item["path"])
                time.sleep(1)

                text = ""
                if pdf_url:
                    item["pdf_url"] = pdf_url
                    logger.info(f"Downloading PDF: {pdf_url}")
                    pdf_bytes = self._curl_get(pdf_url, timeout=120)
                    if pdf_bytes and len(pdf_bytes) > 500:
                        text = self._extract_pdf_text(pdf_bytes) or ""
                        logger.info(f"Extracted {len(text)} chars from PDF")
                    time.sleep(1)
                else:
                    logger.warning(f"No PDF found for {item['title']}")

                item["text"] = text
                record = self.normalize(item)

                if record.get("text"):
                    yield record
                    total += 1
                    logger.info(f"[{total}] {record['title'][:80]}")
                else:
                    logger.warning(f"Skipping {item['title']} - no text extracted")

            time.sleep(2)

        logger.info(f"Total records fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[Dict, None, None]:
        """Fetch decisions published since a given date."""
        since_date = datetime.fromisoformat(since).date()

        for page_num in range(50):  # Check recent pages
            url = f"{LISTING_URL}?page={page_num}"
            data = self._curl_get(url)
            if not data:
                break

            html = data.decode("utf-8", errors="ignore")
            items = self._parse_listing_page(html)

            if not items:
                break

            page_has_old = False
            for item in items:
                date = self._parse_date(item.get("date_str"))
                if date and date < since_date.isoformat():
                    page_has_old = True
                    continue

                pdf_url = self._get_pdf_url_from_detail(item["path"])
                time.sleep(1)

                text = ""
                if pdf_url:
                    item["pdf_url"] = pdf_url
                    pdf_bytes = self._curl_get(pdf_url, timeout=120)
                    if pdf_bytes and len(pdf_bytes) > 500:
                        text = self._extract_pdf_text(pdf_bytes) or ""
                    time.sleep(1)

                item["text"] = text
                record = self.normalize(item)
                if record.get("text"):
                    yield record

            if page_has_old:
                break
            time.sleep(2)

    def test_api(self) -> bool:
        """Test connectivity to AGCOM website."""
        logger.info("Testing AGCOM website connectivity...")

        # Test listing page
        data = self._curl_get(f"{LISTING_URL}?page=0")
        if not data:
            logger.error("Failed to fetch listing page")
            return False

        html = data.decode("utf-8", errors="ignore")
        items = self._parse_listing_page(html)
        logger.info(f"Listing page returned {len(items)} items")

        if not items:
            logger.error("No items found on listing page")
            return False

        # Test detail page
        first = items[0]
        pdf_url = self._get_pdf_url_from_detail(first["path"])
        logger.info(f"First item: {first['title']}, PDF: {pdf_url}")

        if pdf_url:
            pdf_bytes = self._curl_get(pdf_url, timeout=30)
            if pdf_bytes:
                text = self._extract_pdf_text(pdf_bytes)
                if text:
                    logger.info(f"PDF text extraction OK ({len(text)} chars)")
                    return True

        logger.error("PDF download/extraction failed")
        return False


def main():
    scraper = AGCOMScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=sample):
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else (
            datetime.now() - timedelta(days=30)
        ).isoformat()
        for record in scraper.fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
