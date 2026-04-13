#!/usr/bin/env python3
"""
EU/CPVO-Decisions Data Fetcher
Community Plant Variety Office Board of Appeal decisions

Fetches decisions from the PVR Case Law database API:
1. Search for CPVO Board of Appeal cases (originsId=03)
2. Get case metadata via viewcase endpoint
3. Download decision PDFs via signed S3 URLs
4. Extract full text using PyPDF2
"""

import io
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://online.plantvarieties.eu/api/caselaw"
ORIGIN_CPVO_BOA = "03"

class CPVODecisionsFetcher:
    """Fetcher for CPVO Board of Appeal decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LegalDataHunter/1.0',
            'Accept': 'application/json',
        })

    def _request(self, url: str, method: str = "GET", json_data: Optional[Dict] = None,
                 max_retries: int = 3, timeout: int = 60) -> requests.Response:
        """Make HTTP request with retry logic."""
        for attempt in range(max_retries):
            try:
                if method == "POST":
                    resp = self.session.post(url, json=json_data, timeout=timeout)
                else:
                    resp = self.session.get(url, timeout=timeout)
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

    def _extract_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="EU/CPVO-Decisions",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert YYYYMMDD to ISO date."""
        if not date_str or len(date_str) != 8:
            return None
        try:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except Exception:
            return None

    def fetch_all(self, max_docs: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Fetch all CPVO Board of Appeal decisions with full text.

        Enumerates cases via paginated search, downloads decision PDFs,
        and extracts text content.
        """
        offset = 0
        page_size = 20
        total_fetched = 0
        seen_ids = set()

        while True:
            cases = self._search_cases(offset=offset, size=page_size)
            if not cases:
                break

            for case_summary in cases:
                if max_docs is not None and total_fetched >= max_docs:
                    return

                case_id = case_summary.get("caseId")
                if case_id in seen_ids:
                    continue
                seen_ids.add(case_id)

                # Get full case detail
                detail = self._get_case_detail(case_id)
                if not detail:
                    continue

                decision_file = detail.get("decisionFile")
                if not decision_file:
                    logger.warning(f"No decision file for case {case_id}")
                    continue

                # Download and extract text
                logger.info(f"Downloading case {case_id}: {detail.get('decisionNumber', 'N/A')}...")
                pdf_bytes = self._download_pdf(decision_file)
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for case {case_id}: {len(text) if text else 0} chars")
                    continue

                detail["text"] = text
                detail["pdf_size"] = len(pdf_bytes)

                yield detail
                total_fetched += 1
                time.sleep(2)

            offset += page_size

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions newer than the given date."""
        since_str = since.strftime("%Y%m%d")
        for doc in self.fetch_all():
            date_str = doc.get("decisionDate", "")
            if date_str and date_str < since_str:
                break
            yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw case document to standard schema."""
        case_id = raw_doc.get("caseId", "")
        decision_number = raw_doc.get("decisionNumber", "")
        case_name = raw_doc.get("caseName", "")
        parties = raw_doc.get("parties", "")

        title = f"CPVO BoA {decision_number}"
        if case_name:
            title += f" - {case_name}"

        return {
            "_id": f"CPVO-BoA-{decision_number}" if decision_number else f"CPVO-{case_id}",
            "_source": "EU/CPVO-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now().isoformat(),
            "decision_number": decision_number,
            "case_name": case_name,
            "parties": parties,
            "deciding_body": raw_doc.get("decidingBody", "CPVO Board of Appeal"),
            "title": title,
            "text": raw_doc.get("text", ""),
            "date": self._parse_date(raw_doc.get("decisionDate")),
            "url": f"https://online.plantvarieties.eu/#/caselaw/{case_id}",
            "language": raw_doc.get("languageName", ""),
            "keywords": raw_doc.get("keywords", ""),
        }


def main():
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        fetcher = CPVODecisionsFetcher()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if not HAS_PYPDF2:
            logger.error("PyPDF2 is required. Install with: pip install PyPDF2")
            sys.exit(1)

        is_sample = "--sample" in sys.argv
        target = 15 if is_sample else 108

        logger.info(f"Fetching {'sample' if is_sample else 'all'} CPVO BoA decisions (target: {target})...")

        count = 0
        text_lengths = []

        for raw_doc in fetcher.fetch_all(max_docs=target):
            normalized = fetcher.normalize(raw_doc)

            text_len = len(normalized.get("text", ""))
            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len} chars)")
                continue

            safe_id = normalized["_id"].replace("/", "_").replace(":", "_").replace(" ", "_")
            filepath = sample_dir / f"{safe_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['_id']} ({text_len:,} chars)")
            text_lengths.append(text_len)
            count += 1

            if count >= target:
                break

        if text_lengths:
            avg_len = sum(text_lengths) // len(text_lengths)
            logger.info(f"\nBootstrap complete: {count} documents saved")
            logger.info(f"Average text length: {avg_len:,} chars")
            logger.info(f"Sample dir: {sample_dir}")
        else:
            logger.error("No documents with valid text found!")
            sys.exit(1)
    else:
        fetcher = CPVODecisionsFetcher()
        print("Testing CPVO Decisions fetcher...")
        cases = fetcher._search_cases(offset=0, size=3)
        print(f"API returned {len(cases)} cases")
        for c in cases:
            print(f"  - {c.get('decisionNumber')}: {c.get('caseName')} ({c.get('decisionDate')})")
        print("API test successful.")


if __name__ == "__main__":
    main()
