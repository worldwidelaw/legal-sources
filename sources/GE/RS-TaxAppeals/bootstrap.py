#!/usr/bin/env python3
"""
GE/RS-TaxAppeals -- Georgia Revenue Service Tax Dispute Decisions

Fetches tax dispute decisions from the RS Information and Methodological Hub API.

Strategy:
  - Discovery: Paginated search via infohubapi.rs.ge/api/documents/search
  - Full text: HTML in 'description' field from document details endpoint
  - Document types: 15 (RS internal disputes), 16 (MoF disputes),
    75 (City Court tax), 76 (Appeals Court tax), 77 (Supreme Court tax)

Endpoints:
  - Search: GET /api/documents/search?take=50&skip=N&types=X&searchInName=true&searchInText=true
  - Detail: GET /api/documents/{uniqueKey}/details-by-key

Data:
  - ~10,600 tax dispute decisions (2019-present)
  - Full text in Georgian (HTML format, cleaned to plain text)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GE.RS-TaxAppeals")

# API base URL
API_BASE = "https://infohubapi.rs.ge/api"

# Document type IDs for tax disputes
DOC_TYPES = {
    15: "RS Internal Dispute Decision",
    16: "Ministry of Finance Dispute Resolution Decision",
    75: "City Court Decision (tax)",
    76: "Appeals Court Decision (tax)",
    77: "Supreme Court Decision (tax)",
}

# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
    "languageCode": "ka",
}


class RSTaxAppealsScraper(BaseScraper):
    """
    Scraper for GE/RS-TaxAppeals -- Georgia Revenue Service Tax Dispute Decisions.
    Country: GE
    URL: https://infohub.rs.ge

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, params: Optional[Dict] = None, timeout: int = 60) -> requests.Response:
        """Make HTTP GET request with rate limiting."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed for {url}: {e}")
            raise

    def _clean_html(self, html_content: str) -> str:
        """Strip HTML tags and clean text from description field."""
        if not html_content:
            return ""
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator="\n")
        # Decode HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)
        return text.strip()

    def _search_documents(self, doc_type: int, skip: int = 0, take: int = 50) -> Dict[str, Any]:
        """Search for documents of a given type with pagination."""
        url = f"{API_BASE}/documents/search"
        params = {
            "take": take,
            "skip": skip,
            "searchInName": "true",
            "searchInText": "true",
            "types": doc_type,
        }
        resp = self._get(url, params=params)
        return resp.json()

    def _get_document_detail(self, unique_key: str) -> Optional[Dict[str, Any]]:
        """Fetch full document details by unique key."""
        url = f"{API_BASE}/documents/{unique_key}/details-by-key"
        try:
            resp = self._get(url)
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch detail for {unique_key}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw API response into standard schema."""
        unique_key = raw.get("uniqueKey", "")
        doc_number = raw.get("documentNumber", "")
        title = raw.get("name", "")
        receipt_date = raw.get("receiptDate", "")
        description = raw.get("description", "")
        decision_content = raw.get("documentDecisionContent", "")
        doc_type_name = raw.get("type", {}).get("name", "") if isinstance(raw.get("type"), dict) else ""
        tags = raw.get("tags", [])

        # Clean full text from HTML
        text = self._clean_html(description)

        # Parse date
        date_str = ""
        if receipt_date:
            try:
                dt = datetime.fromisoformat(receipt_date.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_str = receipt_date[:10] if len(receipt_date) >= 10 else receipt_date

        # Extract tag names
        tag_names = []
        if tags and isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, dict) and tag.get("name"):
                    tag_names.append(tag["name"])

        # Decision outcome
        outcome = ""
        if isinstance(decision_content, dict):
            outcome = decision_content.get("name", "")
        elif isinstance(decision_content, str):
            outcome = decision_content

        return {
            "_id": unique_key,
            "_source": "GE/RS-TaxAppeals",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": f"https://infohub.rs.ge/ka/workspace/document/{unique_key}",
            "unique_key": unique_key,
            "document_number": doc_number,
            "decision_outcome": outcome,
            "document_type": doc_type_name,
            "tags": tag_names,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax dispute decisions."""
        total_fetched = 0
        sample_limit = 15 if sample else None

        for doc_type_id, doc_type_name in DOC_TYPES.items():
            if sample_limit and total_fetched >= sample_limit:
                break

            logger.info(f"Fetching type {doc_type_id}: {doc_type_name}")
            skip = 0
            take = 50

            while True:
                if sample_limit and total_fetched >= sample_limit:
                    break

                try:
                    result = self._search_documents(doc_type_id, skip=skip, take=take)
                except Exception as e:
                    logger.error(f"Search failed for type {doc_type_id} skip={skip}: {e}")
                    break

                # Extract documents from search result
                documents = result.get("data", [])
                if not documents:
                    break
                meta = result.get("meta", {})

                for doc_summary in documents:
                    if sample_limit and total_fetched >= sample_limit:
                        break

                    unique_key = doc_summary.get("uniqueKey", "")
                    if not unique_key:
                        continue

                    # Fetch full details
                    detail = self._get_document_detail(unique_key)
                    if not detail:
                        continue

                    record = self.normalize(detail)
                    if record.get("text"):
                        yield record
                        total_fetched += 1
                        if total_fetched % 50 == 0:
                            logger.info(f"Fetched {total_fetched} records so far...")
                    else:
                        logger.warning(f"No text for document {unique_key}")

                skip += take

                # Check if we've fetched all (total is in meta.total)
                total_count = meta.get("total", 0)
                if total_count and skip >= total_count:
                    break

        logger.info(f"Total fetched: {total_fetched}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents updated since a given date."""
        total_fetched = 0

        for doc_type_id, doc_type_name in DOC_TYPES.items():
            logger.info(f"Fetching updates for type {doc_type_id}: {doc_type_name} since {since}")
            skip = 0
            take = 50

            while True:
                url = f"{API_BASE}/documents/search"
                params = {
                    "take": take,
                    "skip": skip,
                    "searchInName": "true",
                    "searchInText": "true",
                    "types": doc_type_id,
                    "receiptDateFrom": since,
                }
                try:
                    resp = self._get(url, params=params)
                    result = resp.json()
                except Exception as e:
                    logger.error(f"Update search failed: {e}")
                    break

                documents = result.get("data", [])
                if not documents:
                    break
                meta = result.get("meta", {})

                for doc_summary in documents:
                    unique_key = doc_summary.get("uniqueKey", "")
                    if not unique_key:
                        continue

                    detail = self._get_document_detail(unique_key)
                    if not detail:
                        continue

                    record = self.normalize(detail)
                    if record.get("text"):
                        yield record
                        total_fetched += 1

                skip += take
                total_count = meta.get("total", 0)
                if total_count and skip >= total_count:
                    break

        logger.info(f"Total updates fetched: {total_fetched}")

    def test(self) -> bool:
        """Quick connectivity and data availability test."""
        logger.info("Testing GE/RS-TaxAppeals API connectivity...")
        try:
            result = self._search_documents(doc_type=15, skip=0, take=1)
            meta = result.get("meta", {})
            total = meta.get("total", 0)
            data = result.get("data", [])
            logger.info(f"RS internal dispute decisions available: {total}")

            if data:
                unique_key = data[0].get("uniqueKey", "")
                if unique_key:
                    detail = self._get_document_detail(unique_key)
                    if detail and detail.get("description"):
                        text = self._clean_html(detail["description"])
                        logger.info(f"Sample document text length: {len(text)} chars")
                        logger.info("Test PASSED: API accessible with full text")
                        return True

            logger.error("Test FAILED: Could not retrieve full text")
            return False
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GE/RS-TaxAppeals bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records (for validation)")
    parser.add_argument("--since", type=str, default=None,
                        help="Date for incremental update (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = RSTaxAppealsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            # Save to sample directory
            filename = f"{record['_id']}.json"
            # Sanitize filename
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        since = args.since or "2026-01-01"
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_updates(since=since):
            filename = re.sub(r'[^\w\-.]', '_', f"{record['_id']}.json")
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Update complete: {count} records saved")


if __name__ == "__main__":
    main()
