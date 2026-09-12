#!/usr/bin/env python3
"""
VC/NTRC -- SVG National Telecommunications Regulatory Commission

Fetches telecom legislation and regulations from the NTRC website.
All documents are PDFs hosted at ntrc.vc/docs/legislations/.

Strategy:
  1. Scrape the legislation page for PDF links
  2. Download each PDF and extract text via common/pdf_extract
  3. Normalize into standard schema

Endpoint:
  - https://www.ntrc.vc/providers/legislation/

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VC.NTRC")

BASE_URL = "https://www.ntrc.vc"
LEGISLATION_URL = f"{BASE_URL}/providers/legislation/"


class VCNTRCScraper(BaseScraper):
    """Scraper for VC/NTRC -- SVG National Telecommunications Regulatory Commission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "text/html,application/pdf",
            },
            timeout=120,
        )

    def _get_html(self, url: str) -> Optional[str]:
        """GET HTML page with retry."""
        for attempt in range(3):
            try:
                resp = self.http.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _extract_pdf_text(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            text = extract_pdf_markdown(
                "VC/NTRC",
                doc_id,
                pdf_url=url,
                table="legislation",
                force=True,
            )
            if text and len(text.strip()) > 50:
                return text.strip()
            return None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def _extract_year(self, title: str) -> str:
        """Extract year from title like 'Regulations 2007' or 'Act 2020'."""
        match = re.search(r'\b(19\d{2}|20\d{2})\b', title)
        return match.group(1) if match else ""

    def _scrape_legislation_page(self) -> Generator[dict, None, None]:
        """Parse the legislation page for PDF links."""
        html = self._get_html(LEGISLATION_URL)
        if not html:
            logger.error("Failed to fetch legislation page")
            return

        # Find all links to PDF files
        # Pattern: <a href="URL">Title text</a>
        links = re.findall(
            r'<a\s+[^>]*href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE,
        )

        seen = set()
        for href, link_text in links:
            # Clean up the title
            title = re.sub(r'<[^>]+>', '', link_text).strip()
            title = re.sub(r'\s+', ' ', title)
            if not title or len(title) < 5:
                continue

            # Normalize URL
            if href.startswith("http://"):
                href = href.replace("http://", "https://", 1)
            if not href.startswith("http"):
                href = urljoin(BASE_URL + "/", href)

            if href in seen:
                continue
            seen.add(href)

            # Create stable ID from PDF filename
            filename = href.split("/")[-1].split("?")[0]
            doc_id = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
            doc_id = re.sub(r'%20', '_', doc_id)

            year = self._extract_year(title)

            yield {
                "id": doc_id,
                "title": title,
                "url": href,
                "date": year,
            }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents with full text."""
        total = 0
        failed = 0

        logger.info("Scraping NTRC legislation page for PDF links...")

        for item in self._scrape_legislation_page():
            doc_id = item["id"]
            url = item["url"]
            title = item["title"]

            logger.info(f"Downloading: {title}")
            text = self._extract_pdf_text(url, doc_id)

            if not text:
                logger.warning(f"No text extracted for: {title} ({url})")
                failed += 1
                continue

            total += 1
            yield {
                "id": doc_id,
                "title": title,
                "text": text,
                "date": item.get("date", ""),
                "url": url,
                "doc_type": "legislation",
            }
            time.sleep(2)

        logger.info(f"Total: {total} documents with full text, {failed} failed")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date.

        Since these are static PDFs, updates are rare. Re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_str = raw.get("date", "")
        if date_str and len(date_str) == 4:
            date_str = f"{date_str}-01-01"

        return {
            "_id": raw.get("id", ""),
            "_source": "VC/NTRC",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = VCNTRCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to NTRC legislation page...")
        html = scraper._get_html(LEGISLATION_URL)
        if html and ".pdf" in html.lower():
            pdf_count = len(re.findall(r'\.pdf', html, re.IGNORECASE))
            logger.info(f"OK — found {pdf_count} PDF references")
            print(f"Test passed: legislation page accessible, {pdf_count} PDF refs")
        else:
            logger.error("Failed to reach legislation page or no PDFs found")
            sys.exit(1)

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
