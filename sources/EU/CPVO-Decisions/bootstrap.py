#!/usr/bin/env python3
"""
EU/CPVO-Decisions Data Fetcher
Community Plant Variety Office Board of Appeal decisions

Fetches decisions from the PVR Case Law database API:
1. Search for CPVO Board of Appeal cases (originsId=03)
2. Get case metadata via viewcase endpoint
3. Download decision PDFs via signed S3 URLs
4. Extract full text using common/pdf_extract

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import sys
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.CPVO-Decisions")

API_BASE = "https://online.plantvarieties.eu/api/caselaw"
ORIGIN_CPVO_BOA = "03"


class CPVODecisionsScraper(BaseScraper):
    """Scraper for EU/CPVO-Decisions - CPVO Board of Appeal decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0",
            "Accept": "application/json",
        })

    def _request(self, url: str, method: str = "GET", json_data: Optional[Dict] = None,
                 max_retries: int = 3, timeout: int = 60) -> requests.Response:
        """Make HTTP request with retry logic and rate-limit awareness."""
        for attempt in range(max_retries):
            try:
                if method == "POST":
                    resp = self.session.post(url, json=json_data, timeout=timeout)
                else:
                    resp = self.session.get(url, timeout=timeout)
                # CPVO API returns 404 with "TOO MANY REQUESTS" body for rate limits
                if resp.status_code in (404, 429):
                    body = resp.text
                    if "TOO MANY REQUESTS" in body.upper():
                        retry_after = int(resp.headers.get("Retry-After", 120))
                        wait = min(retry_after, 300)  # cap at 5 minutes
                        logger.warning(f"Rate-limited by CPVO API, waiting {wait}s (attempt {attempt + 1})")
                        time.sleep(wait)
                        continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (2 ** attempt))
                else:
                    raise

    def _search_cases(self, offset: int = 0, size: int = 20) -> list:
        """Search for CPVO Board of Appeal cases."""
        resp = self._request(
            f"{API_BASE}/casesSearchPublic",
            method="POST",
            json_data={
                "originsId": ORIGIN_CPVO_BOA,
                "from": str(offset),
                "size": str(size),
            }
        )
        data = resp.json().get("data", {})
        return data.get("cases", [])

    def _get_case_detail(self, case_id: int) -> Optional[Dict]:
        """Get full case metadata including file names."""
        try:
            resp = self._request(f"{API_BASE}/viewcase?caseId={case_id}")
            cases = resp.json().get("data", {}).get("case", [])
            return cases[0] if cases else None
        except Exception as e:
            logger.warning(f"Failed to get case {case_id}: {e}")
            return None

    def _download_pdf(self, filename: str) -> Optional[bytes]:
        """Download a decision PDF via the signed URL endpoint."""
        try:
            resp = self._request(f"{API_BASE}/downloadfile?uuid={filename}")
            signed_url = resp.json().get("signedUrl")
            if not signed_url:
                logger.warning(f"No signed URL for {filename}")
                return None
            pdf_resp = requests.get(signed_url, timeout=120)
            pdf_resp.raise_for_status()
            return pdf_resp.content
        except Exception as e:
            logger.warning(f"Failed to download {filename}: {e}")
            return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert YYYYMMDD to ISO date."""
        if not date_str or len(date_str) != 8:
            return None
        try:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except Exception:
            return None

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a raw case document to standard schema."""
        case_id = raw.get("caseId", "")
        decision_number = raw.get("decisionNumber", "")
        case_name = raw.get("caseName", "")
        text = raw.get("text", "")

        if not text or len(text) < 100:
            return None

        title = f"CPVO BoA {decision_number}"
        if case_name:
            title += f" - {case_name}"

        return {
            "_id": f"CPVO-BoA-{decision_number}" if decision_number else f"CPVO-{case_id}",
            "_source": "EU/CPVO-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_number": decision_number,
            "case_name": case_name,
            "parties": raw.get("parties", ""),
            "deciding_body": raw.get("decidingBody", "CPVO Board of Appeal"),
            "title": title,
            "text": text,
            "date": self._parse_date(raw.get("decisionDate")),
            "url": f"https://online.plantvarieties.eu/#/caselaw/{case_id}",
            "language": raw.get("languageName", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CPVO Board of Appeal decisions with full text."""
        offset = 0
        page_size = 20
        seen_ids = set()

        while True:
            self.rate_limiter.wait()
            cases = self._search_cases(offset=offset, size=page_size)
            if not cases:
                break

            for case_summary in cases:
                case_id = case_summary.get("caseId")
                if case_id in seen_ids:
                    continue
                seen_ids.add(case_id)

                self.rate_limiter.wait()
                detail = self._get_case_detail(case_id)
                if not detail:
                    continue

                decision_file = detail.get("decisionFile")
                if not decision_file:
                    logger.warning(f"No decision file for case {case_id}")
                    continue

                logger.info(f"Downloading case {case_id}: {detail.get('decisionNumber', 'N/A')}...")
                self.rate_limiter.wait()
                pdf_bytes = self._download_pdf(decision_file)
                if not pdf_bytes:
                    continue

                text = extract_pdf_markdown(
                    source="EU/CPVO-Decisions",
                    source_id=str(case_id),
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                ) or ""

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for case {case_id}: {len(text)} chars")
                    continue

                detail["text"] = text
                yield detail

            offset += page_size

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = CPVODecisionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing CPVO Decisions API...")
        try:
            cases = scraper._search_cases(offset=0, size=3)
            print(f"  API returned {len(cases)} cases")
            for c in cases:
                print(f"  - {c.get('decisionNumber')}: {c.get('caseName')} ({c.get('decisionDate')})")
            print("Test PASSED")
        except Exception as e:
            print(f"  FAIL: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
