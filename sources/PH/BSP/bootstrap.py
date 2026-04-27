#!/usr/bin/env python3
"""
PH/BSP -- Bangko Sentral ng Pilipinas Circulars

Fetches BSP regulatory issuances (circulars, memoranda, circular letters) via
the SharePoint REST API exposed at bsp.gov.ph. Full text extracted from PDFs.

API:
  - SharePoint OData: /_api/Web/Lists(guid'68408d09-548e-40f5-b2fd-62d36e30fd3f')/Items
  - Returns JSON (Accept: application/json;odata=verbose)
  - Pagination via __next URL or $skip/$top
  - No auth required
  - PDFs at /Regulations/Issuances/{year}/{CircularNumber}.pdf

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.PH.BSP")

BASE_URL = "https://www.bsp.gov.ph"
LIST_GUID = "68408d09-548e-40f5-b2fd-62d36e30fd3f"
API_URL = f"/_api/Web/Lists(guid'{LIST_GUID}')/Items"
SELECT_FIELDS = "Title,IssuanceID,IssuanceType,DateIssued,CircularNumber,Content,Id"
PAGE_SIZE = 100


def extract_pdf_link_from_content(content_html: str) -> Optional[str]:
    """Extract the PDF download link from the Content HTML field."""
    if not content_html:
        return None
    match = re.search(r'href="(/Regulations/Issuances/[^"]+\.pdf)"', content_html)
    if match:
        return match.group(1)
    match = re.search(r'href="([^"]+\.pdf)"', content_html, re.IGNORECASE)
    if match:
        link = match.group(1)
        if link.startswith("/"):
            return link
        if link.startswith("http"):
            return link
    return None


def strip_html(html_content: str) -> str:
    """Strip HTML tags and clean text content."""
    if not html_content:
        return ""
    import html as html_module
    text = re.sub(r'</(p|div|li|tr|h[1-6]|blockquote)>', '\n', html_content)
    text = re.sub(r'<(br|hr)\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))
    return text.strip()


class BSPScraper(BaseScraper):
    """Scraper for PH/BSP -- Bangko Sentral ng Pilipinas issuances."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "application/json;odata=verbose",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _fetch_page(self, url: str) -> tuple[List[Dict], Optional[str]]:
        """Fetch one page of issuances. Returns (items, next_url)."""
        self.rate_limiter.wait()
        try:
            if url.startswith("http"):
                resp = self.client.session.get(url, timeout=60)
            else:
                resp = self.client.get(url)

            if not resp or resp.status_code != 200:
                logger.error(f"API error: {resp.status_code if resp else 'no response'}")
                return [], None

            data = resp.json()
            d = data.get("d", {})
            items = d.get("results", [])
            next_url = d.get("__next")
            return items, next_url

        except Exception as e:
            logger.error(f"Error fetching page: {e}")
            return [], None

    def _fetch_all_issuances(self) -> List[Dict]:
        """Paginate through all issuances from the SharePoint API."""
        all_items = []
        url = f"{API_URL}?$top={PAGE_SIZE}&$select={SELECT_FIELDS}&$orderby=DateIssued%20desc"

        page = 0
        while url:
            page += 1
            items, next_url = self._fetch_page(url)
            if not items:
                break
            all_items.extend(items)
            logger.info(f"Page {page}: fetched {len(items)} items (total: {len(all_items)})")
            url = next_url

        logger.info(f"Total issuances fetched: {len(all_items)}")
        return all_items

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download PDF and extract text."""
        if not pdf_url:
            return None
        full_url = f"{BASE_URL}{pdf_url}" if pdf_url.startswith("/") else pdf_url
        text = extract_pdf_markdown(
            source="PH/BSP",
            source_id=doc_id,
            pdf_url=full_url,
            table="doctrine",
        )
        return text

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all issuances from the BSP API."""
        issuances = self._fetch_all_issuances()
        for item in issuances:
            yield item

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch issuances issued since a given date."""
        since_str = since.strftime("%Y-%m-%dT00:00:00Z")
        url = (
            f"{API_URL}?$top={PAGE_SIZE}&$select={SELECT_FIELDS}"
            f"&$filter=DateIssued%20ge%20datetime'{since_str}'"
            f"&$orderby=DateIssued%20desc"
        )
        while url:
            items, next_url = self._fetch_page(url)
            if not items:
                break
            for item in items:
                yield item
            url = next_url

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw SharePoint record into standard schema, fetching PDF text."""
        title = (raw.get("Title") or "").strip()
        circular_number = (raw.get("CircularNumber") or "").strip()
        issuance_type = (raw.get("IssuanceType") or "").strip()
        date_issued = raw.get("DateIssued")
        content_html = raw.get("Content") or ""
        sp_id = raw.get("Id") or raw.get("ID") or ""

        if not title:
            return None

        # Parse date
        date_str = None
        if date_issued:
            try:
                dt = datetime.fromisoformat(date_issued.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass

        # Build document ID
        doc_id = circular_number if circular_number else f"BSP-{sp_id}"
        doc_id = re.sub(r'[^a-zA-Z0-9._-]', '_', doc_id)

        # Extract PDF link from Content HTML
        pdf_link = extract_pdf_link_from_content(content_html)

        # Try to construct PDF URL from circular number if not found in HTML
        if not pdf_link and circular_number and date_str:
            year = date_str[:4]
            encoded_num = circular_number.replace(" ", "%20")
            pdf_link = f"/Regulations/Issuances/{year}/{encoded_num}.pdf"

        # Extract text from PDF
        text = None
        if pdf_link:
            text = self._extract_pdf_text(pdf_link, doc_id)

        # Fall back to Content HTML text if PDF extraction fails
        if not text or len(text) < 50:
            content_text = strip_html(content_html)
            if content_text and len(content_text) >= 50:
                text = content_text

        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
            return None

        # Build source URL
        if pdf_link:
            source_url = f"{BASE_URL}{pdf_link}" if pdf_link.startswith("/") else pdf_link
        else:
            source_url = f"{BASE_URL}/SitePages/Regulations/RegulationsList.aspx"

        record = {
            "_id": doc_id,
            "_source": "PH/BSP",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": source_url,
            "circular_number": circular_number,
            "issuance_type": issuance_type,
            "jurisdiction": "PH",
            "language": "en",
        }

        return record

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing BSP SharePoint API...")

        url = f"{API_URL}?$top=3&$select={SELECT_FIELDS}&$orderby=DateIssued%20desc"
        items, next_url = self._fetch_page(url)

        if not items:
            print("FAILED: No issuances returned from API")
            return

        print(f"API accessible. Got {len(items)} items.")
        if next_url:
            print(f"Pagination: __next URL present (more data available)")

        for item in items:
            title = (item.get("Title") or "")[:80]
            circ_no = item.get("CircularNumber", "")
            date = item.get("DateIssued", "")
            itype = item.get("IssuanceType", "")
            print(f"\n  Type: {itype}")
            print(f"  Number: {circ_no}")
            print(f"  Title: {title}")
            print(f"  Date: {date}")

            pdf_link = extract_pdf_link_from_content(item.get("Content", ""))
            if pdf_link:
                print(f"  PDF: {pdf_link}")

        # Test PDF extraction on first item
        item = items[0]
        pdf_link = extract_pdf_link_from_content(item.get("Content", ""))
        if pdf_link:
            doc_id = f"BSP-test-{item.get('Id', 0)}"
            print(f"\nTesting PDF extraction: {pdf_link}")
            text = self._extract_pdf_text(pdf_link, doc_id)
            if text:
                print(f"  Extracted {len(text)} chars")
                print(f"  Sample: {text[:200]}...")
            else:
                print("  FAILED: No text extracted from PDF")

        print("\nTest complete!")


def main():
    scraper = BSPScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
