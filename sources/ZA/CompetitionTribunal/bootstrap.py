#!/usr/bin/env python3
"""
ZA/CompetitionTribunal -- South African Competition Tribunal Data Fetcher

Fetches decided cases from the Competition Tribunal of South Africa.

Strategy:
  - Scrape the decided-cases listing pages (paginated with ?page=N)
  - Parse HTML table rows for case metadata (number, title, status, date, PDF links)
  - Download Order/Reasons PDFs and extract full text via common/pdf_extract
  - Prefer "Reasons" PDFs (full decision text) over "Order" PDFs (short orders)

Endpoints:
  - Listing: https://www.comptrib.co.za/decided-cases?page=N
  - PDFs: https://www.comptrib.co.za/uploads/topics/CompTrib_Case_Files/...

Data:
  - Case types: Merger Review, Prohibited Practice, Consent Orders, Appeals, etc.
  - ~3000+ decided cases
  - Language: English
  - License: Open Government Data (public court decisions)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
import socket
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.CompetitionTribunal")

BASE_URL = "https://www.comptrib.co.za"
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html, application/xhtml+xml",
    "Accept-Language": "en",
}


class CompetitionTribunalScraper(BaseScraper):
    """
    Scraper for ZA/CompetitionTribunal -- South African Competition Tribunal.
    Country: ZA
    URL: https://www.comptrib.co.za

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            timeout=60,
        )

    def _parse_listing_page(self, page_html: str) -> List[Dict[str, Any]]:
        """
        Parse a decided-cases listing page HTML and extract case rows.

        Each row has:
          <td><a href="cases-case-files/...">CASE_NUMBER</a></td>
          <td><a href="cases-case-files/...">TITLE</a></td>
          <td>STATUS</td>
          <td>DATE</td>
          <td><a href="uploads/...pdf">Order Pdf</a> ...</td>
        """
        cases = []

        # Find all table rows with case data
        # Pattern: 5 <td> elements per row
        row_pattern = re.compile(
            r'<td>\s*<a\s+href="(cases-case-files/[^"]+)"[^>]*>([^<]+)</a>\s*</td>\s*'
            r'<td>\s*<a\s+href="cases-case-files/[^"]*"[^>]*>([^<]+)</a>\s*</td>\s*'
            r'<td>([^<]*)</td>\s*'
            r'<td>([^<]*)</td>\s*'
            r'<td>(.*?)</td>',
            re.DOTALL,
        )

        for match in row_pattern.finditer(page_html):
            detail_path = match.group(1)
            case_number = html_module.unescape(match.group(2).strip())
            title = html_module.unescape(match.group(3).strip())
            status = match.group(4).strip()
            date_str = match.group(5).strip()
            pdf_cell = match.group(6)

            # Extract PDF links from the last cell
            pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"[^>]*>([^<]+)<', pdf_cell)

            # Categorize PDFs
            reasons_pdf = None
            order_pdf = None
            other_pdfs = []

            for pdf_url, pdf_label in pdf_links:
                label_lower = pdf_label.strip().lower()
                full_url = f"{BASE_URL}/{pdf_url}" if not pdf_url.startswith("http") else pdf_url
                if "reason" in label_lower:
                    reasons_pdf = full_url
                elif "order" in label_lower:
                    order_pdf = full_url
                else:
                    other_pdfs.append(full_url)

            # Prefer reasons PDF (full decision text), fall back to order PDF
            primary_pdf = reasons_pdf or order_pdf or (other_pdfs[0] if other_pdfs else None)

            cases.append({
                "case_number": case_number,
                "title": title,
                "status": status,
                "date": date_str,
                "detail_url": f"{BASE_URL}/{detail_path}",
                "reasons_pdf": reasons_pdf,
                "order_pdf": order_pdf,
                "primary_pdf": primary_pdf,
            })

        return cases

    def _get_max_page(self, page_html: str) -> int:
        """Extract the maximum page number from pagination links."""
        # Pattern: ?page=N
        pages = re.findall(r'\?page=(\d+)', page_html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all decided cases with full text from PDFs."""
        # Preload existing IDs to skip already-ingested records
        existing = preload_existing_ids("ZA/CompetitionTribunal", table="case_law")

        # First, fetch page 1 to determine total pages
        logger.info("Fetching page 1 to determine pagination...")
        self.rate_limiter.wait()
        resp = self.client.get("/decided-cases")
        resp.raise_for_status()
        page_html = resp.text

        max_page = self._get_max_page(page_html)
        logger.info(f"Found {max_page} pages of decided cases")

        # Process page 1
        yield from self._process_page(page_html, 1, existing)

        # Process remaining pages
        for page_num in range(2, max_page + 1):
            logger.info(f"Fetching page {page_num}/{max_page}")
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/decided-cases?page={page_num}")
                resp.raise_for_status()
                yield from self._process_page(resp.text, page_num, existing)
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_num}: {e}")
                continue

    def _process_page(self, page_html: str, page_num: int, existing: set) -> Generator[Dict[str, Any], None, None]:
        """Process a single listing page and yield normalized records."""
        cases = self._parse_listing_page(page_html)
        logger.info(f"Page {page_num}: found {len(cases)} cases")

        for case in cases:
            case_id = case["case_number"]

            # Skip if already in Neon
            if case_id in existing:
                logger.debug(f"Skipping {case_id} — already in Neon")
                continue

            if not case.get("primary_pdf"):
                logger.warning(f"No PDF available for {case_id}")
                continue

            # Extract text from PDF
            self.rate_limiter.wait()
            text = extract_pdf_markdown(
                source="ZA/CompetitionTribunal",
                source_id=case_id,
                pdf_url=case["primary_pdf"],
                table="case_law",
            )

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {case_id} (PDF: {case['primary_pdf']})")
                continue

            yield self.normalize({**case, "text": text})

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent cases (first few pages only)."""
        existing = preload_existing_ids("ZA/CompetitionTribunal", table="case_law")

        # Only check first 5 pages for updates
        for page_num in range(1, 6):
            logger.info(f"Checking page {page_num} for updates")
            self.rate_limiter.wait()
            try:
                resp = self.client.get(f"/decided-cases?page={page_num}")
                resp.raise_for_status()
                yield from self._process_page(resp.text, page_num, existing)
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_num}: {e}")
                continue

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw case data into standard schema."""
        case_number = raw.get("case_number", "")
        title = raw.get("title", "")
        date_str = raw.get("date", "")
        text = raw.get("text", "")
        status = raw.get("status", "")

        # Parse date (already ISO format from the site: YYYY-MM-DD)
        date_iso = None
        if date_str:
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = date_str

        return {
            "_id": case_number,
            "_source": "ZA/CompetitionTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "title": title,
            "text": text,
            "date": date_iso,
            "status": status,
            "url": raw.get("detail_url", f"{BASE_URL}/decided-cases"),
            "reasons_pdf": raw.get("reasons_pdf"),
            "order_pdf": raw.get("order_pdf"),
        }

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.client.get("/decided-cases")
            resp.raise_for_status()
            cases = self._parse_listing_page(resp.text)
            logger.info(f"Connection OK — found {len(cases)} cases on page 1")
            return len(cases) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZA/CompetitionTribunal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only 10-15 sample records")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records (full bootstrap)")
    args = parser.parse_args()

    scraper = CompetitionTribunalScraper()

    if args.command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for record in scraper.fetch_all():
            if count >= max_records:
                break

            # Save to sample directory
            filename = re.sub(r'[^\w\-.]', '_', record["_id"]) + ".json"
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

            text_len = len(record.get("text", ""))
            logger.info(f"Saved {record['_id']}: {record['title'][:60]}... ({text_len} chars)")
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
            logger.info(f"Updated: {record['_id']}")
        logger.info(f"Update complete: {count} new/updated records")


if __name__ == "__main__":
    main()
