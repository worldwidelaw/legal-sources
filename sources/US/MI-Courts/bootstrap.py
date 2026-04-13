#!/usr/bin/env python3
"""
US/MI-Courts -- Michigan Supreme Court & Court of Appeals

Fetches case law via the CourtListener open search API (no auth required)
and downloads full-text PDFs from CourtListener's S3 storage.

Data Coverage:
  - Michigan Supreme Court: ~93,000 opinions (1805-present)
  - Michigan Court of Appeals: ~29,000 opinions (1965-present)
  - Language: English
  - Open access, no authentication required

Strategy:
  - CourtListener search API (v4) for metadata — no API key needed
  - PDF full text from storage.courtlistener.com (public S3, no auth)
  - Text extracted via pdfplumber/pypdf
  - Cursor-based pagination, 20 results per page

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import io
import time
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.US.MI-Courts")

# Configuration
API_BASE = "https://www.courtlistener.com/api/rest/v4/search/"
PDF_BASE = "https://storage.courtlistener.com/"
COURTS = {
    "mich": "Michigan Supreme Court",
    "michctapp": "Michigan Court of Appeals",
}
PAGE_SIZE = 20
REQUEST_DELAY = 1.5

# Session for connection reuse
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
})


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/MI-Courts",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def download_pdf(local_path: str) -> Optional[bytes]:
    """Download a PDF from CourtListener S3 storage."""
    url = f"{PDF_BASE}{local_path}"
    try:
        resp = SESSION.get(url, timeout=60)
        if resp.status_code == 200 and len(resp.content) > 100:
            return resp.content
        logger.debug(f"PDF download failed: {resp.status_code} for {url}")
    except requests.RequestException as e:
        logger.debug(f"PDF download error: {e}")
    return None


class MICourtsScraper(BaseScraper):

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Michigan court opinions from both courts."""
        for court_id, court_name in COURTS.items():
            logger.info(f"Fetching opinions from {court_name} (court={court_id})")
            yield from self._fetch_court(court_id, court_name)

    def _fetch_court(
        self,
        court_id: str,
        court_name: str,
        since: Optional[datetime] = None,
    ) -> Generator[dict, None, None]:
        """Paginate through all opinions for a court via CourtListener search API."""
        params = {
            "q": "*",
            "court": court_id,
            "type": "o",
            "order_by": "dateFiled desc",
            "page_size": PAGE_SIZE,
        }
        if since:
            params["filed_after"] = since.strftime("%Y-%m-%d")

        url = API_BASE
        page = 0
        total_yielded = 0

        while url:
            page += 1
            try:
                if page == 1:
                    resp = SESSION.get(url, params=params, timeout=30)
                else:
                    resp = SESSION.get(url, timeout=30)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited, sleeping {retry_after}s")
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()
                data = resp.json()

            except requests.RequestException as e:
                logger.error(f"API error on page {page}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            total = data.get("count", 0)
            if page == 1:
                logger.info(f"  Total results for {court_id}: {total}")

            for result in results:
                for opinion in result.get("opinions", []):
                    local_path = opinion.get("local_path")
                    if not local_path:
                        continue

                    record = {
                        "cluster_id": result.get("cluster_id"),
                        "opinion_id": opinion.get("id"),
                        "case_name": result.get("caseName", ""),
                        "case_name_full": result.get("caseNameFull", ""),
                        "date_filed": result.get("dateFiled", ""),
                        "docket_number": result.get("docketNumber", ""),
                        "citation": result.get("citation", []),
                        "court_id": court_id,
                        "court_name": court_name,
                        "judge": result.get("judge", ""),
                        "status": result.get("status", ""),
                        "local_path": local_path,
                        "download_url": opinion.get("download_url", ""),
                        "snippet": opinion.get("snippet", ""),
                        "per_curiam": opinion.get("per_curiam", False),
                        "opinion_type": opinion.get("type", ""),
                    }
                    yield record
                    total_yielded += 1

            url = data.get("next")
            if url:
                time.sleep(REQUEST_DELAY)

            if page % 50 == 0:
                logger.info(f"  Progress: page {page}, yielded {total_yielded} opinions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield opinions filed since the given date."""
        for court_id, court_name in COURTS.items():
            logger.info(f"Fetching updates from {court_name} since {since.date()}")
            yield from self._fetch_court(court_id, court_name, since=since)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download PDF and extract full text, then normalize to standard schema."""
        local_path = raw.get("local_path")
        if not local_path:
            return None

        pdf_bytes = download_pdf(local_path)
        if not pdf_bytes:
            logger.debug(f"Could not download PDF for opinion {raw.get('opinion_id')}")
            return None

        text = extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 100:
            logger.debug(f"Insufficient text extracted for opinion {raw.get('opinion_id')}")
            return None

        citations = raw.get("citation", [])
        citation_str = "; ".join(citations) if citations else ""

        opinion_id = raw.get("opinion_id", "")
        doc_id = f"mi-courts-{opinion_id}"

        return {
            "_id": doc_id,
            "_source": "US/MI-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", "") or raw.get("case_name_full", ""),
            "text": text,
            "date": raw.get("date_filed", ""),
            "url": raw.get("download_url", ""),
            "case_number": raw.get("docket_number", ""),
            "court": raw.get("court_name", ""),
            "court_id": raw.get("court_id", ""),
            "citation": citation_str,
            "judge": raw.get("judge", ""),
            "status": raw.get("status", ""),
            "per_curiam": raw.get("per_curiam", False),
            "opinion_type": raw.get("opinion_type", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────

def main():
    source_dir = Path(__file__).parent
    scraper = MICourtsScraper(str(source_dir))

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample] [--full]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing CourtListener search API for Michigan courts...")
        try:
            resp = SESSION.get(API_BASE, params={
                "q": "*", "court": "mich", "type": "o", "page_size": 1
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            print(f"  MI Supreme Court: {data['count']} opinions")

            resp2 = SESSION.get(API_BASE, params={
                "q": "*", "court": "michctapp", "type": "o", "page_size": 1
            }, timeout=15)
            resp2.raise_for_status()
            data2 = resp2.json()
            print(f"  MI Court of Appeals: {data2['count']} opinions")

            first = data["results"][0]["opinions"][0]
            lp = first.get("local_path")
            if lp:
                pdf = download_pdf(lp)
                if pdf:
                    text = extract_text_from_pdf(pdf)
                    print(f"  PDF text extraction: {len(text or '')} chars")
                else:
                    print("  PDF download: FAILED")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        since_days = 30
        since = datetime.now(timezone.utc) - timedelta(days=since_days)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
