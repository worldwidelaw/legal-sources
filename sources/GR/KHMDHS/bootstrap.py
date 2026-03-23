#!/usr/bin/env python3
"""
GR/KHMDHS -- Central Electronic Registry of Public Procurement (ΚΗΜΔΗΣ)

Fetches Greek public procurement documents from the KHMDHS OpenData REST API.

Strategy:
  - POST /khmdhs-opendata/request?page=N with date filters to search requests
  - GET /khmdhs-opendata/request/attachment/{refNo} to download PDF full text
  - Extract text from PDFs using pypdf

Endpoints:
  - Search: POST https://cerpp.eprocurement.gov.gr/khmdhs-opendata/request?page=N
  - PDF: GET https://cerpp.eprocurement.gov.gr/khmdhs-opendata/request/attachment/{refNo}

Data:
  - Thousands of procurement requests per day
  - Structured JSON metadata + PDF attachments with full text
  - Rate limit: 350 requests/minute
  - No authentication required for read endpoints

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any

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
logger = logging.getLogger("legal-data-hunter.GR.khmdhs")

BASE_URL = "https://cerpp.eprocurement.gov.gr"
SEARCH_ENDPOINT = "/khmdhs-opendata/request"
ATTACHMENT_ENDPOINT = "/khmdhs-opendata/request/attachment"
PAGE_SIZE = 20  # API default page size


class GreekProcurementScraper(BaseScraper):
    """
    Scraper for GR/KHMDHS -- Greek Public Procurement Registry.
    Country: GR
    URL: https://cerpp.eprocurement.gov.gr

    Data types: doctrine
    Auth: none (Open public API)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _search_requests(self, page: int = 0, date_from: str = None,
                         date_to: str = None) -> Optional[Dict]:
        """Search procurement requests via the OpenData API."""
        body = {}
        if date_from:
            body["dateFrom"] = date_from
        if date_to:
            body["dateTo"] = date_to

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                resp = self.client.post(
                    f"{SEARCH_ENDPOINT}?page={page}",
                    json_data=body,
                )
                resp.raise_for_status()
                data = resp.json()

                # Check for rate limit response
                if isinstance(data, dict) and data.get("status") == 429:
                    wait_time = 10 * (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                return data

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Search attempt {attempt+1} failed: {e}")
                    time.sleep(5)
                else:
                    logger.error(f"Search failed after {max_retries} attempts: {e}")
                    return None

        return None

    def _extract_pdf_text(self, ref_number: str) -> Optional[str]:
        """Download and extract text from a procurement PDF attachment."""
        if not HAS_PYPDF:
            logger.warning("pypdf not installed, cannot extract PDF text")
            return None

        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{ATTACHMENT_ENDPOINT}/{ref_number}")
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and len(resp.content) < 100:
                logger.debug(f"No PDF for {ref_number}: {content_type}")
                return None

            # Memory-bounded PDF extraction
            pdf_data = resp.content
            if len(pdf_data) > 50 * 1024 * 1024:  # Skip >50MB PDFs
                logger.warning(f"PDF too large for {ref_number}: {len(pdf_data)} bytes")
                return None

            reader = pypdf.PdfReader(io.BytesIO(pdf_data))
            text_parts = []
            max_pages = min(len(reader.pages), 100)  # Cap at 100 pages

            for i in range(max_pages):
                try:
                    page_text = reader.pages[i].extract_text()
                    if page_text:
                        text_parts.append(page_text)
                except Exception:
                    continue

            full_text = "\n\n".join(text_parts).strip()
            if full_text:
                return full_text
            return None

        except Exception as e:
            logger.debug(f"PDF extraction failed for {ref_number}: {e}")
            return None

    def _build_text_from_record(self, rec: Dict) -> str:
        """Build text content from record metadata as fallback."""
        parts = []
        if rec.get("title"):
            parts.append(rec["title"])

        for od in rec.get("objectDetails", []):
            desc = od.get("shortDescription", "")
            if desc:
                parts.append(desc)

        return "\n\n".join(parts).strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all procurement requests, paginated by date windows."""
        # Fetch recent data in monthly windows (last 6 months by default)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=180)

        current = start_date
        while current < end_date:
            window_end = min(current + timedelta(days=30), end_date)
            date_from = current.strftime("%Y-%m-%d")
            date_to = window_end.strftime("%Y-%m-%d")

            logger.info(f"Fetching requests from {date_from} to {date_to}")

            page = 0
            while True:
                data = self._search_requests(
                    page=page,
                    date_from=date_from,
                    date_to=date_to,
                )
                if not data:
                    break

                content = data.get("content", [])
                if not content:
                    break

                for rec in content:
                    yield rec

                total_pages = data.get("totalPages", 1)
                page += 1
                if page >= total_pages:
                    break

            current = window_end

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield procurement requests created since the given date."""
        date_from = since.strftime("%Y-%m-%d")
        date_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        logger.info(f"Fetching updates from {date_from} to {date_to}")

        page = 0
        while True:
            data = self._search_requests(
                page=page,
                date_from=date_from,
                date_to=date_to,
            )
            if not data:
                break

            content = data.get("content", [])
            if not content:
                break

            for rec in content:
                yield rec

            total_pages = data.get("totalPages", 1)
            page += 1
            if page >= total_pages:
                break

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw KHMDHS record into the standard schema."""
        ref = raw.get("referenceNumber")
        if not ref:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        # Extract date
        date_str = raw.get("signedDate") or raw.get("submissionDate", "")
        if "T" in str(date_str):
            date_str = date_str.split("T")[0]

        # Try to get full text from PDF
        pdf_text = self._extract_pdf_text(ref)

        # Build text: prefer PDF, fall back to metadata
        if pdf_text and len(pdf_text) > 50:
            text = pdf_text
        else:
            text = self._build_text_from_record(raw)

        if not text or len(text) < 20:
            return None

        # Extract organization
        org = raw.get("organization", {})
        org_name = org.get("value", "") if isinstance(org, dict) else str(org)

        # Extract NUTS code
        nuts = raw.get("nutsCode", {})
        nuts_code = nuts.get("key", "") if isinstance(nuts, dict) else ""

        # Extract contract type
        contract_types = raw.get("contractTypes", [])
        contract_type = ""
        if contract_types:
            ct = contract_types[0].get("contractType", {})
            contract_type = ct.get("value", "") if isinstance(ct, dict) else ""

        # Extract total cost
        total_cost = raw.get("totalCostWithVAT") or raw.get("totalCostWithoutVAT")

        # Clean HTML from text
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return {
            "_id": f"GR/KHMDHS/{ref}",
            "_source": "GR/KHMDHS",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str if date_str else None,
            "url": f"https://cerpp.eprocurement.gov.gr/khmdhs-opendata/request/attachment/{ref}",
            "reference_number": ref,
            "organization": org_name,
            "total_cost": total_cost,
            "contract_type": contract_type,
            "nuts_code": nuts_code,
        }


# ── CLI entry point ─────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="GR/KHMDHS scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode")
    args = parser.parse_args()

    scraper = GreekProcurementScraper()

    if args.command == "test":
        logger.info("Testing KHMDHS API connectivity...")
        data = scraper._search_requests(page=0, date_from="2026-03-20", date_to="2026-03-21")
        if data and data.get("content"):
            total = data.get("totalElements", "?")
            logger.info(f"API OK. Found {total} requests for 2026-03-20.")

            # Test PDF download
            ref = data["content"][0]["referenceNumber"]
            text = scraper._extract_pdf_text(ref)
            if text:
                logger.info(f"PDF extraction OK. Text length: {len(text)} chars")
            else:
                logger.warning("PDF extraction returned no text")
        else:
            logger.error("API test failed")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
