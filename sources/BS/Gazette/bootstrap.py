#!/usr/bin/env python3
"""
BS/Gazette -- Bahamas Official Gazette

Fetches official gazettes from laws.bahamas.gov.bs. The site organizes
gazettes by year via a Joomla form POST. Each gazette is a PDF with
extractable text.

Strategy:
  - POST to the gazettes-by-year page with year=YYYY (2021-present)
  - Parse HTML to extract PDF links and gazette titles
  - Download PDFs and extract full text

URL pattern:
  /cms/images/LEGISLATION/GAZETTES/{YEAR}/{YEAR-NNNN}/{YEAR-NNNN}.pdf

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
from typing import Generator, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.BS.Gazette")

BASE_URL = "https://laws.bahamas.gov.bs"
GAZETTE_PAGE = "/cms/gazettes/gazettes-by-year.html"

# Years with gazette data (site supports 1930-2026 but only 2021+ has content)
GAZETTE_YEARS = list(range(2026, 2020, -1))

# Match PDF links in the gazette page
PDF_LINK_RE = re.compile(
    r'<a[^>]*href="(/cms/images/LEGISLATION/GAZETTES/[^"]*\.pdf)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

# Extract gazette number from path like 2025-0001
GAZETTE_NUM_RE = re.compile(r'(\d{4}-\d{4})')


class BSGazetteScraper(BaseScraper):
    """
    Scraper for BS/Gazette -- Bahamas Official Gazette.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes, source_id: str) -> str:
        return extract_pdf_markdown(
            "BS/Gazette",
            source_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def _fetch_year_entries(self, year: int) -> List[Dict[str, Any]]:
        """Fetch gazette entries for a given year via form POST."""
        try:
            self.rate_limiter.wait()
            resp = self.client.post(
                GAZETTE_PAGE,
                data={"year": year},
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch year {year}: {e}")
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

            # Skip duplicates
            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Extract gazette number
            num_match = GAZETTE_NUM_RE.search(href)
            gazette_num = num_match.group(1) if num_match else None

            results.append({
                "title": title,
                "pdf_path": href,
                "year": str(year),
                "gazette_number": gazette_num,
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
            "_id": f"BS/Gazette/{doc_id}",
            "_source": "BS/Gazette",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("year"),
            "url": full_url,
            "doc_id": doc_id,
            "gazette_number": raw.get("gazette_number"),
            "year": raw.get("year"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for year in GAZETTE_YEARS:
            if limit and count >= limit:
                break

            logger.info(f"Fetching gazettes for year {year}")
            entries = self._fetch_year_entries(year)
            logger.info(f"  Found {len(entries)} entries for {year}")

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

                gazette_num = entry.get("gazette_number", "")
                text = self._extract_pdf_text(resp.content, gazette_num)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  No meaningful text from {title[:40]}")
                    continue

                entry["text"] = text
                yield entry
                count += 1

        logger.info(f"Fetched {count} gazette documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent gazettes — re-parse current year."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BSGazetteScraper()

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
