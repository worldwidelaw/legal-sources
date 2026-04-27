#!/usr/bin/env python3
"""
US/IL-Courts -- Illinois Supreme Court & Appellate Court Opinions

Fetches case law via CourtListener's public search API (no auth needed) for
metadata discovery, then downloads opinion PDFs from storage.courtlistener.com
and extracts full text using pdfplumber.

Courts covered:
  - Illinois Supreme Court (ill) — ~58K opinions from 1832
  - Illinois Appellate Court (illappct) — ~154K opinions from 1855

Strategy:
  1. Use CourtListener search API (no auth) to discover opinions with metadata
  2. Download PDFs from CourtListener's storage (cached copies of official PDFs)
  3. Extract full text via pdfplumber
  4. Normalize into standard schema

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
from typing import Generator, Optional, Dict, Any, List

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
logger = logging.getLogger("legal-data-hunter.US.IL-Courts")

# CourtListener search API (no auth required)
SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_BASE = "https://storage.courtlistener.com/"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Illinois courts on CourtListener
COURTS = "ill,illappct"

COURT_NAMES = {
    "ill": "Illinois Supreme Court",
    "illappct": "Illinois Appellate Court",
}


class ILCourtsScraper(BaseScraper):
    """
    Scraper for US/IL-Courts — Illinois Supreme & Appellate Court Opinions.
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
                         page: int = 1, url: str = None) -> Dict[str, Any]:
        """Query CourtListener search API for Illinois opinions.

        If `url` is provided (e.g., the `next` URL from a previous response),
        fetch it directly so cursor-based pagination state is preserved.
        Otherwise, build params from court/page/page_size/date filters.
        """
        if url:
            request_url = url
            params = None
        else:
            request_url = SEARCH_URL
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

        # CourtListener can return 503 during heavy load. Retry with exponential
        # backoff up to 12 attempts (~30 minutes total worst case).
        max_attempts = 12
        for attempt in range(max_attempts):
            try:
                resp = self.session.get(request_url, params=params, timeout=60)
                if resp.status_code in (429, 503, 502, 504):
                    wait = min(300, 5 * (2 ** min(attempt, 6)))
                    logger.warning(
                        f"Got {resp.status_code}, waiting {wait}s (attempt {attempt + 1}/{max_attempts})..."
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                if attempt < max_attempts - 1:
                    wait = min(120, 5 * (2 ** min(attempt, 5)))
                    logger.warning(f"Timeout, retrying in {wait}s...")
                    time.sleep(wait)
                    continue
                raise
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status in (429, 503, 502, 504) and attempt < max_attempts - 1:
                    wait = min(300, 5 * (2 ** min(attempt, 6)))
                    logger.warning(
                        f"HTTPError {status}, waiting {wait}s (attempt {attempt + 1}/{max_attempts})..."
                    )
                    time.sleep(wait)
                    continue
                logger.error(f"Search API HTTPError: {e}")
                raise
            except Exception as e:
                logger.error(f"Search API error: {e}")
                if attempt < max_attempts - 1:
                    wait = min(120, 5 * (2 ** min(attempt, 5)))
                    time.sleep(wait)
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
            source="US/IL-Courts",
            source_id="",
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def _get_pdf_url(self, opinion: Dict) -> Optional[str]:
        """Get the best PDF URL for an opinion result."""
        # Try CourtListener's cached copy first (more reliable)
        local_path = opinion.get("local_path")
        if local_path:
            return STORAGE_BASE + local_path

        # Fall back to original download URL
        download_url = opinion.get("download_url")
        if download_url:
            return download_url

        return None

    def _process_search_result(self, result: Dict) -> Optional[Dict[str, Any]]:
        """Process a single search result: download PDF and extract text."""
        opinions = result.get("opinions", [])
        if not opinions:
            return None

        # Use the first (main) opinion
        opinion = opinions[0]
        pdf_url = self._get_pdf_url(opinion)
        if not pdf_url:
            logger.warning(f"No PDF URL for {result.get('caseName', 'unknown')}")
            return None

        # Download PDF
        pdf_data = self._download_pdf(pdf_url)
        if not pdf_data:
            return None

        # Extract text
        text = self._extract_pdf_text(pdf_data)
        if not text or len(text) < 100:
            logger.warning(
                f"Insufficient text for {result.get('caseName', 'unknown')}: "
                f"{len(text)} chars"
            )
            return None

        # Build raw record
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
        """Fetch all Illinois opinions via CourtListener search API.

        Iterates each court separately to keep result sets manageable, and
        follows the `next` URL (cursor-based pagination) to avoid the deep
        page-number cap. If a single page fails after all retries, it is
        skipped so the bootstrap can still make progress.
        """
        total_fetched = 0
        for court in COURTS.split(","):
            logger.info(f"=== Fetching court={court} ===")
            page = 1
            next_url: Optional[str] = None
            consecutive_failures = 0

            while True:
                if next_url:
                    logger.info(f"[{court}] Fetching next cursor page (>= page {page})...")
                else:
                    logger.info(f"[{court}] Fetching page {page}...")
                try:
                    data = self._search_opinions(court=court, page=page, url=next_url)
                except Exception as e:
                    consecutive_failures += 1
                    logger.error(
                        f"[{court}] Page {page} failed after retries: {e} "
                        f"(consecutive_failures={consecutive_failures})"
                    )
                    if consecutive_failures >= 3:
                        logger.error(f"[{court}] Aborting after 3 consecutive page failures")
                        break
                    # Skip this page: bump page number and retry the next one.
                    next_url = None
                    page += 1
                    time.sleep(30)
                    continue
                consecutive_failures = 0

                results = data.get("results", [])
                if not results:
                    logger.info(f"[{court}] No more results.")
                    break

                for result in results:
                    time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                    raw = self._process_search_result(result)
                    if raw:
                        total_fetched += 1
                        yield raw

                next_url = data.get("next")
                if not next_url:
                    break
                page += 1

        logger.info(f"Total fetched across all courts: {total_fetched}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions filed since a given date."""
        if not since:
            # Default to last 30 days
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        for court in COURTS.split(","):
            page = 1
            next_url: Optional[str] = None
            consecutive_failures = 0
            while True:
                logger.info(f"[{court}] Fetching updates since {since}, page {page}...")
                try:
                    data = self._search_opinions(
                        court=court, filed_after=since, page=page, url=next_url
                    )
                except Exception as e:
                    consecutive_failures += 1
                    logger.error(f"[{court}] Update page {page} failed: {e}")
                    if consecutive_failures >= 3:
                        break
                    next_url = None
                    page += 1
                    time.sleep(30)
                    continue
                consecutive_failures = 0

                results = data.get("results", [])
                if not results:
                    break

                for result in results:
                    time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                    raw = self._process_search_result(result)
                    if raw:
                        yield raw

                next_url = data.get("next")
                if not next_url:
                    break
                page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record into the standard schema."""
        court_id = raw.get("court_id", "")
        cluster_id = raw.get("cluster_id", "")
        court_abbr = "ILSC" if court_id == "ill" else "ILAC"
        doc_id = f"US-IL-{court_abbr}-{cluster_id}"

        court_name = COURT_NAMES.get(court_id, raw.get("court", "Illinois Court"))

        return {
            "_id": doc_id,
            "_source": "US/IL-Courts",
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
            "jurisdiction": "US-IL",
            "pdf_url": raw.get("pdf_url", ""),
        }

    def test_connection(self) -> bool:
        """Test that the CourtListener search API is accessible for IL courts."""
        try:
            data = self._search_opinions(page_size=5)
            count = data.get("count", 0)
            results = data.get("results", [])
            logger.info(f"Connection test: {count} total IL opinions, got {len(results)} results")
            return len(results) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IL-Courts data fetcher")
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

    scraper = ILCourtsScraper()

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

            # Save to sample directory
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
