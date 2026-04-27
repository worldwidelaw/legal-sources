#!/usr/bin/env python3
"""
BS/Legislation -- Bahamas Consolidated Legislation

Fetches consolidated laws from laws.bahamas.gov.bs. The site runs Joomla with
legislation stored as PDFs in a directory tree under /cms/images/LEGISLATION/.

Strategy:
  - Parse the HTML alphabetical index pages to get act titles + PDF URLs
  - Download PDFs and extract full text
  - Cover PRINCIPAL acts and SUBORDINATE instruments

Index pages:
  - Acts: /cms/legislation/acts_only/by-alphabetical-order.html
  - Subsidiary: /cms/legislation/subsidiary/by-alphabetical-order.html

PDF pattern:
  /cms/images/LEGISLATION/{TYPE}/{YEAR}/{YEAR-NNNN}/{YEAR-NNNN}[_VER].pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin
import html as html_mod

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.Legislation")

BASE_URL = "https://laws.bahamas.gov.bs"

INDEX_PAGES = [
    ("/cms/legislation/acts_only/by-alphabetical-order.html", "principal"),
    ("/cms/legislation/subsidiary/by-alphabetical-order.html", "subordinate"),
]

# Match PDF links in the HTML index pages
PDF_LINK_RE = re.compile(
    r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

# Extract year from PDF path like /PRINCIPAL/2014/2014-0047/2014-0047_2.pdf
YEAR_RE = re.compile(r'/(\d{4})/')

# Extract legislation number from path like 2014-0047
LEG_NUM_RE = re.compile(r'(\d{4}-\d{4})')


class BSLegislationScraper(BaseScraper):
    """
    Scraper for BS/Legislation -- Bahamas Consolidated Legislation.
    """

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

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="BS/Legislation",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def _parse_index_page(self, path: str, leg_type: str) -> List[Dict[str, Any]]:
        """Parse an alphabetical index page to extract legislation entries."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch index {path}: {e}")
            return []

        results = []
        seen_urls = set()

        for match in PDF_LINK_RE.finditer(resp.text):
            href = match.group(1)
            link_text = match.group(2)

            # Clean up the link text
            title = html_mod.unescape(re.sub(r'<[^>]+>', '', link_text)).strip()
            title = re.sub(r'\s+', ' ', title)
            if not title or len(title) < 3:
                continue

            # Make URL absolute
            if href.startswith('/'):
                pdf_url = href
            elif href.startswith('http'):
                pdf_url = href.replace(BASE_URL, '')
            else:
                pdf_url = f"/cms/legislation/{href}"

            # Skip duplicates
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            # Extract year from path
            year_match = YEAR_RE.search(pdf_url)
            year = year_match.group(1) if year_match else None

            # Extract legislation number
            num_match = LEG_NUM_RE.search(pdf_url)
            leg_num = num_match.group(1) if num_match else None

            results.append({
                "title": title,
                "pdf_path": pdf_url,
                "legislation_type": leg_type,
                "year": year,
                "leg_num": leg_num,
            })

        return results

    def _make_doc_id(self, pdf_path: str) -> str:
        return hashlib.sha256(pdf_path.encode()).hexdigest()[:16]

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        pdf_path = raw.get("pdf_path", "")
        doc_id = self._make_doc_id(pdf_path)
        full_url = f"{BASE_URL}{pdf_path}" if pdf_path.startswith('/') else pdf_path

        return {
            "_id": f"BS/Legislation/{doc_id}",
            "_source": "BS/Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("year"),
            "url": full_url,
            "doc_id": doc_id,
            "legislation_type": raw.get("legislation_type", "principal"),
            "year": raw.get("year"),
            "file_url": full_url,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for index_path, leg_type in INDEX_PAGES:
            if limit and count >= limit:
                break

            logger.info(f"Parsing index: {index_path} ({leg_type})")
            entries = self._parse_index_page(index_path, leg_type)
            logger.info(f"  Found {len(entries)} entries")

            for entry in entries:
                if limit and count >= limit:
                    break

                pdf_path = entry["pdf_path"]
                title = entry.get("title", "?")
                logger.info(f"  [{count + 1}] Downloading: {title[:60]}")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_path)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download {pdf_path}: {e}")
                    continue

                if resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a PDF: {pdf_path}")
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  No meaningful text from {title[:40]}")
                    continue

                entry["text"] = text
                yield entry
                count += 1

        logger.info(f"Fetched {count} legislation documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation — just re-parse current indexes."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    scraper = BSLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
