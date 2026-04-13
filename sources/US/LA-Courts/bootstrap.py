#!/usr/bin/env python3
"""
US/LA-Courts -- Louisiana Supreme Court & Courts of Appeal Opinions

Fetches case law via CourtListener's public search API (no auth needed) for
metadata discovery, then downloads opinion PDFs from storage.courtlistener.com
and extracts full text using pdfplumber.

Courts covered:
  - Supreme Court of Louisiana (la) — ~199K opinions from 1813
  - Louisiana Court of Appeal (lactapp) — ~123K opinions from 1926

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (paginated)
  python bootstrap.py update --since YYYY-MM-DD  # Incremental updates
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# PDF extraction
import requests

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.LA-Courts")

# CourtListener search API (no auth required)
SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_BASE = "https://storage.courtlistener.com/"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Louisiana courts on CourtListener
COURTS = "la,lactapp"

COURT_NAMES = {
    "la": "Supreme Court of Louisiana",
    "lactapp": "Louisiana Court of Appeal",
}


class LACourtsScraper(BaseScraper):
    """
    Scraper for US/LA-Courts — Louisiana Supreme Court & Courts of Appeal.
    Uses CourtListener public search API + PDF download + text extraction.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _search_opinions(self, court: str = COURTS, page_size: int = 20,
                         filed_after: str = None, filed_before: str = None,
                         page: int = 1) -> Dict[str, Any]:
        """Query CourtListener search API for Louisiana opinions."""
        params = {
            "format": "json",
            "type": "o",
            "court": court,
            "page_size": min(page_size, 20),
            "order_by": "dateFiled desc",
            "page": page,
        }
        if filed_after:
            params["filed_after"] = filed_after
        if filed_before:
            params["filed_before"] = filed_before

        for attempt in range(3):
            try:
                resp = self.session.get(SEARCH_URL, params=params, timeout=60)
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                if attempt < 2:
                    logger.warning("Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
            except Exception as e:
                logger.error(f"Search API error: {e}")
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise
        return {"count": 0, "results": []}

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file from CourtListener storage or official source."""
        try:
            resp = self.session.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" in content_type or url.endswith(".pdf") or len(resp.content) > 1000:
                return resp.content
            logger.warning(f"Unexpected content type for {url}: {content_type}")
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_data: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="US/LA-Courts",
            source_id="",
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def _get_pdf_url(self, opinion: Dict) -> Optional[str]:
        """Get the best PDF URL for an opinion result."""
        local_path = opinion.get("local_path")
        if local_path:
            return STORAGE_BASE + local_path

        download_url = opinion.get("download_url")
        if download_url:
            return download_url

        return None

    def _process_search_result(self, result: Dict) -> Optional[Dict[str, Any]]:
        """Process a single search result: download PDF and extract text."""
        opinions = result.get("opinions", [])
        if not opinions:
            return None

        opinion = opinions[0]
        pdf_url = self._get_pdf_url(opinion)
        if not pdf_url:
            logger.warning(f"No PDF URL for {result.get('caseName', 'unknown')}")
            return None

        pdf_data = self._download_pdf(pdf_url)
        if not pdf_data:
            return None

        text = self._extract_pdf_text(pdf_data)
        if not text or len(text) < 100:
            logger.warning(
                f"Insufficient text for {result.get('caseName', 'unknown')}: "
                f"{len(text)} chars"
            )
            return None

        citations = result.get("citation", [])
        citation_str = citations[0] if citations else ""

        return {
            "cluster_id": result.get("cluster_id"),
            "case_name": result.get("caseName", ""),
            "case_name_full": result.get("caseNameFull", ""),
            "docket_number": result.get("docketNumber", ""),
            "court_id": result.get("court_id", ""),
            "court": result.get("court", ""),
            "date_filed": result.get("dateFiled"),
            "citation": citation_str,
            "status": result.get("status", ""),
            "pdf_url": pdf_url,
            "cl_url": f"https://www.courtlistener.com{result.get('absolute_url', '')}",
            "text": text,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Louisiana opinions via CourtListener search API."""
        page = 1
        total_fetched = 0

        while True:
            logger.info(f"Fetching search results page {page}...")
            data = self._search_opinions(page=page)

            results = data.get("results", [])
            if not results:
                logger.info("No more results.")
                break

            for result in results:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    total_fetched += 1
                    yield raw

            if not data.get("next"):
                break
            page += 1

        logger.info(f"Total fetched: {total_fetched}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions filed since a given date."""
        if not since:
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        page = 1
        while True:
            logger.info(f"Fetching updates since {since}, page {page}...")
            data = self._search_opinions(filed_after=since, page=page)

            results = data.get("results", [])
            if not results:
                break

            for result in results:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    yield raw

            if not data.get("next"):
                break
            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record into the standard schema."""
        court_id = raw.get("court_id", "")
        cluster_id = raw.get("cluster_id", "")
        court_abbr = "LASC" if court_id == "la" else "LACA"
        doc_id = f"US-LA-{court_abbr}-{cluster_id}"

        court_name = COURT_NAMES.get(court_id, raw.get("court", "Louisiana Court"))

        return {
            "_id": doc_id,
            "_source": "US/LA-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date_filed"),
            "url": raw.get("cl_url", ""),
            "case_number": raw.get("docket_number", ""),
            "court": court_name,
            "citation": raw.get("citation", ""),
            "status": raw.get("status", ""),
            "jurisdiction": "US-LA",
            "pdf_url": raw.get("pdf_url", ""),
        }

    def test_connection(self) -> bool:
        """Test that the CourtListener search API is accessible for LA courts."""
        try:
            data = self._search_opinions(page_size=5)
            count = data.get("count", 0)
            results = data.get("results", [])
            logger.info(f"Connection test: {count} total LA opinions, got {len(results)} results")
            return len(results) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/LA-Courts data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample for validation",
    )
    parser.add_argument(
        "--since",
        help="ISO date for incremental updates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (all pages)",
    )
    args = parser.parse_args()

    scraper = LACourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        gen = scraper.fetch_all()
        for raw in gen:
            record = scraper.normalize(raw)

            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record['_id']}: {record['title'][:60]} "
                f"({text_len} chars)"
            )
            count += 1

            if count >= target:
                break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        gen = scraper.fetch_updates(since=args.since)
        for raw in gen:
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
