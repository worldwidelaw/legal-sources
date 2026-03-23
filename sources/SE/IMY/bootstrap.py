#!/usr/bin/env python3
"""
SE/IMY -- Swedish Data Protection Authority (IMY) Data Fetcher

Fetches GDPR supervision decisions from Integritetsskyddsmyndigheten.

Strategy:
  - Use JSON API at /sv/api/search/listsearch to list all supervision cases
  - For each decision, fetch the HTML page to extract the PDF link
  - Download PDF and extract full text using pypdf
  - Fall back to HTML page summary if no PDF available

Endpoints:
  - List API: https://www.imy.se/sv/api/search/listsearch?pageId=243&pageSize=200&page=1
  - Decision pages: https://www.imy.se/tillsyner/{slug}/
  - PDFs: https://www.imy.se/globalassets/dokument/beslut/...

Data:
  - ~120 supervision cases, ~80 with final decisions
  - Language: Swedish (SV)
  - Categories: Dataskydd, Kamerabevakning, Kreditupplysning
  - Rate limit: 1 request/second

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
import html
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, urlparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# PDF extraction
try:
    import pypdf
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SE.imy")

BASE_URL = "https://www.imy.se"
LIST_API = "/sv/api/search/listsearch"
PAGE_ID = "243"

# Swedish month names for date parsing
SWEDISH_MONTHS = {
    "januari": "01", "februari": "02", "mars": "03", "april": "04",
    "maj": "05", "juni": "06", "juli": "07", "augusti": "08",
    "september": "09", "oktober": "10", "november": "11", "december": "12",
}


class SwedishIMYScraper(BaseScraper):
    """
    Scraper for SE/IMY -- Swedish Data Protection Authority.
    Country: SE
    URL: https://www.imy.se

    Data types: doctrine
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json, text/html",
                "Accept-Language": "sv,en",
            },
            timeout=60,
        )

    def _clean_url(self, url: str) -> str:
        """Strip tracking query params from IMY URLs."""
        if not url:
            return ""
        parsed = urlparse(url)
        # Keep only the path, drop _t_tags etc.
        return parsed.path.rstrip("/")

    def _parse_swedish_date(self, date_str: str) -> str:
        """Parse Swedish date like '22 januari 2026' to ISO format."""
        if not date_str:
            return ""
        date_str = date_str.strip().lower()
        match = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2)
            year = match.group(3)
            month = SWEDISH_MONTHS.get(month_name, "")
            if month:
                return f"{year}-{month}-{day}"
        return ""

    def _fetch_listing(self, status_filter: str = "") -> List[Dict[str, Any]]:
        """
        Fetch all supervision cases from the JSON API.

        Returns list of dicts with: heading, preamble, date, status, categories, url.
        """
        params = {
            "query": "",
            "selectedSection": "",
            "pageSize": "200",
            "page": "1",
            "pageId": PAGE_ID,
        }
        if status_filter:
            params["selectedStatuses"] = status_filter

        try:
            self.rate_limiter.wait()
            resp = self.client.get(LIST_API, params=params)
            resp.raise_for_status()
            data = resp.json()

            hits = data.get("hits", [])
            logger.info(f"API returned {len(hits)} entries (filter={status_filter or 'all'})")
            return hits

        except Exception as e:
            logger.error(f"Failed to fetch listing: {e}")
            return []

    def _fetch_decision_page(self, page_url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a decision page and extract PDF URL and summary text.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(page_url)
            resp.raise_for_status()
            content = resp.text

            result = {}

            # Extract PDF link - look for links to /globalassets/dokument/*.pdf
            pdf_match = re.search(
                r'href="(/globalassets/dokument/[^"]*\.pdf)"',
                content,
                re.IGNORECASE,
            )
            if pdf_match:
                result["pdf_url"] = pdf_match.group(1)

            # Extract case reference number (e.g., IMY-2025-7801)
            ref_match = re.search(r'(IMY-\d{4}-\d+)', content)
            if ref_match:
                result["reference"] = ref_match.group(1)

            # Extract main content text from the page as fallback
            # Look for the main article content
            main_match = re.search(
                r'<main[^>]*>(.*?)</main>',
                content,
                re.DOTALL | re.IGNORECASE,
            )
            if main_match:
                main_html = main_match.group(1)
                # Remove script/style tags
                main_html = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', main_html, flags=re.DOTALL | re.IGNORECASE)
                # Strip HTML tags
                text = re.sub(r'<[^>]+>', ' ', main_html)
                text = html.unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                if len(text) > 100:
                    result["page_text"] = text

            return result

        except Exception as e:
            logger.warning(f"Failed to fetch decision page {page_url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download and extract text from a PDF."""
        if not HAS_PYPDF:
            logger.warning("pypdf not installed, skipping PDF extraction")
            return None

        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url)
            resp.raise_for_status()

            # Skip very large PDFs (>20MB)
            if len(resp.content) > 20 * 1024 * 1024:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {pdf_url}")
                return None

            pdf_file = io.BytesIO(resp.content)
            reader = pypdf.PdfReader(pdf_file)

            text_parts = []
            for page_num, page in enumerate(reader.pages):
                try:
                    page_text = page.extract_text() or ""
                    text_parts.append(page_text)
                except Exception as e:
                    logger.debug(f"Error extracting page {page_num}: {e}")
                    continue

            text = "\n\n".join(text_parts)
            text = re.sub(r'\s+', ' ', text).strip()

            if len(text) < 100:
                logger.warning(f"Very short PDF text ({len(text)} chars): {pdf_url}")
                return None

            return text

        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decision documents from IMY."""
        # Fetch all entries (not just decisions - include all statuses for completeness)
        entries = self._fetch_listing()

        for entry in entries:
            page_url = entry.get("url", "")
            if not page_url:
                continue

            heading = entry.get("heading", "")
            logger.info(f"Processing: {heading[:60]}...")

            # Fetch decision page for PDF URL and content
            page_data = self._fetch_decision_page(page_url)

            text = None
            pdf_url = ""

            # Try PDF extraction first
            if page_data and page_data.get("pdf_url"):
                pdf_url = page_data["pdf_url"]
                text = self._extract_pdf_text(pdf_url)

            # Fall back to page text if no PDF or extraction failed
            if not text and page_data and page_data.get("page_text"):
                text = page_data["page_text"]
                logger.info(f"Using page text fallback for: {heading[:40]}")

            if not text:
                logger.warning(f"No text available for: {heading[:60]}")
                continue

            yield {
                "heading": heading,
                "preamble": entry.get("preamble", ""),
                "date": entry.get("date", ""),
                "status": entry.get("status", ""),
                "categories": entry.get("categories", []),
                "url": page_url,
                "pdf_url": pdf_url,
                "reference": page_data.get("reference", "") if page_data else "",
                "text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents newer than the given date."""
        since_str = since.strftime("%Y-%m-%d")
        entries = self._fetch_listing()

        for entry in entries:
            raw_date = entry.get("date", "")
            iso_date = self._parse_swedish_date(raw_date)

            if iso_date and iso_date < since_str:
                continue

            page_url = entry.get("url", "")
            if not page_url:
                continue

            heading = entry.get("heading", "")
            page_data = self._fetch_decision_page(page_url)

            text = None
            pdf_url = ""

            if page_data and page_data.get("pdf_url"):
                pdf_url = page_data["pdf_url"]
                text = self._extract_pdf_text(pdf_url)

            if not text and page_data and page_data.get("page_text"):
                text = page_data["page_text"]

            if not text:
                continue

            yield {
                "heading": heading,
                "preamble": entry.get("preamble", ""),
                "date": raw_date,
                "status": entry.get("status", ""),
                "categories": entry.get("categories", []),
                "url": page_url,
                "pdf_url": pdf_url,
                "reference": page_data.get("reference", "") if page_data else "",
                "text": text,
            }

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        page_url = raw.get("url", "")
        heading = raw.get("heading", "")
        raw_date = raw.get("date", "")
        iso_date = self._parse_swedish_date(raw_date)

        # Clean URL and create ID from slug
        clean_path = self._clean_url(page_url)
        slug = clean_path.split("/")[-1] if clean_path else heading.lower().replace(" ", "-")[:50]
        doc_id = f"IMY/{slug}"

        full_url = f"{BASE_URL}{clean_path}" if clean_path else ""
        full_pdf = urljoin(BASE_URL, raw.get("pdf_url", "")) if raw.get("pdf_url") else ""

        return {
            "_id": doc_id,
            "_source": "SE/IMY",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": heading,
            "text": raw.get("text", ""),
            "date": iso_date,
            "url": full_url,
            "status": raw.get("status", ""),
            "categories": raw.get("categories", []),
            "preamble": raw.get("preamble", ""),
            "reference": raw.get("reference", ""),
            "pdf_url": full_pdf,
            "language": "sv",
            "authority": "Integritetsskyddsmyndigheten",
            "authority_en": "Swedish Authority for Privacy Protection",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing SE/IMY endpoints...")

        print("\n1. Testing JSON API...")
        try:
            entries = self._fetch_listing("Decision")
            print(f"   Found {len(entries)} decision entries")
            if entries:
                e = entries[0]
                print(f"   Sample: {e.get('heading', '')[:50]} ({e.get('date', '')})")
                print(f"   URL: {e.get('url', '')}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n2. Testing decision page fetch...")
        try:
            if entries:
                page_data = self._fetch_decision_page(entries[0]["url"])
                if page_data:
                    print(f"   PDF URL: {page_data.get('pdf_url', 'NOT FOUND')}")
                    print(f"   Reference: {page_data.get('reference', 'N/A')}")
                    print(f"   Page text length: {len(page_data.get('page_text', ''))} chars")
                else:
                    print("   ERROR: No page data returned")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        print("\n3. Testing PDF extraction...")
        try:
            if page_data and page_data.get("pdf_url"):
                text = self._extract_pdf_text(page_data["pdf_url"])
                if text:
                    print(f"   Text length: {len(text)} chars")
                    print(f"   Sample: {text[:150]}...")
                else:
                    print("   WARNING: No text extracted from PDF")
            else:
                print("   SKIP: No PDF URL available")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = SwedishIMYScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
