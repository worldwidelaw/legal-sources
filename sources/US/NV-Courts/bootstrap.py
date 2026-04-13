#!/usr/bin/env python3
"""
US/NV-Courts -- Nevada Supreme Court & Court of Appeals Opinions

Fetches case law from the Nevada Courts public API (WebSupplementalAPI).

Strategy:
  - Single API call returns all ~1100 advance opinions (2015-present)
  - Each entry has a base64-encoded docurl that decodes to PDF parameters
  - Download opinion PDFs and extract full text via pdfplumber
  - Court of Appeals opinions identified by "COA" in case number

Data Coverage:
  - Nevada Supreme Court opinions from 2015 to present
  - Nevada Court of Appeals opinions from 2015 to present
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent opinions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NV-Courts")

API_URL = "https://publicaccess.nvsupremecourt.us/WebSupplementalAPI/api/AdvanceOpinions"
PDF_BASE = "https://caseinfo.nvsupremecourt.us/document/view.do"
API_KEY = "080d4202-61b2-46c5-ad66-f479bf40be11"


def decode_pdf_url(docurl: str) -> Optional[str]:
    """Decode base64 docurl into a full PDF download URL."""
    try:
        decoded = base64.b64decode(docurl).decode("utf-8")
        parts = decoded.split("_")
        if len(parts) < 4:
            return None
        return (
            f"{PDF_BASE}?csNameID={parts[0]}&csIID={parts[1]}"
            f"&deLinkID={parts[2]}&onBaseDocumentNumber={parts[3]}"
        )
    except Exception as e:
        logger.warning(f"Failed to decode docurl {docurl}: {e}")
        return None


def determine_court(case_number: str) -> str:
    """Determine court from case number (COA prefix = Court of Appeals)."""
    if "COA" in case_number.upper():
        return "Nevada Court of Appeals"
    return "Nevada Supreme Court"


class NVCourtsScraper(BaseScraper):
    """
    Scraper for US/NV-Courts -- Nevada Supreme Court & Court of Appeals.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
            "XApiKey": API_KEY,
            "Referer": "https://nvcourts.gov/",
        })

    def _fetch_opinions_list(self) -> list:
        """Fetch the full list of advance opinions from the API."""
        resp = self.session.get(API_URL, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file (no API key needed for PDF endpoint)."""
        try:
            resp = requests.get(url, timeout=60, headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)"
            })
            resp.raise_for_status()
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_data: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="US/NV-Courts",
            source_id="",
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all advance opinions."""
        logger.info("Fetching opinions list from API...")
        opinions = self._fetch_opinions_list()
        logger.info(f"Found {len(opinions)} opinions")

        delay = self.config.get("fetch", {}).get("delay", 1.5)

        for i, op in enumerate(opinions):
            pdf_url = decode_pdf_url(op.get("docurl", ""))
            if not pdf_url:
                logger.warning(f"Cannot decode docurl for case {op.get('caseNumber')}")
                continue

            time.sleep(delay)

            pdf_data = self._download_pdf(pdf_url)
            if not pdf_data:
                continue

            text = self._extract_pdf_text(pdf_data)
            if not text or len(text) < 100:
                logger.warning(
                    f"Insufficient text for case {op.get('caseNumber')}: "
                    f"{len(text)} chars"
                )
                continue

            op["text"] = text
            op["pdf_url"] = pdf_url
            yield op

            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{len(opinions)}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions published since a given date."""
        logger.info("Fetching opinions list from API...")
        opinions = self._fetch_opinions_list()

        delay = self.config.get("fetch", {}).get("delay", 1.5)

        for op in opinions:
            op_date = op.get("date", "")[:10]  # "2026-04-09T00:00:00" -> "2026-04-09"
            if since and op_date < since:
                continue

            pdf_url = decode_pdf_url(op.get("docurl", ""))
            if not pdf_url:
                continue

            time.sleep(delay)

            pdf_data = self._download_pdf(pdf_url)
            if not pdf_data:
                continue

            text = self._extract_pdf_text(pdf_data)
            if not text or len(text) < 100:
                continue

            op["text"] = text
            op["pdf_url"] = pdf_url
            yield op

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record into the standard schema."""
        case_num = raw.get("caseNumber", "").strip()
        court = determine_court(case_num)
        title = raw.get("caseTitle", "").strip()
        date_str = raw.get("date", "")[:10] if raw.get("date") else None
        advance_num = raw.get("advanceNumber")
        citation_year = raw.get("officialCitationYear")

        court_abbr = "NVCOA" if "Appeals" in court else "NVSC"
        doc_id = f"US-{court_abbr}-{case_num}"

        return {
            "_id": doc_id,
            "_source": "US/NV-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{title} ({case_num})" if title else case_num,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("pdf_url", ""),
            "case_number": case_num,
            "court": court,
            "jurisdiction": "US-NV",
            "advance_number": advance_num,
            "citation_year": citation_year,
        }

    def test_connection(self) -> bool:
        """Test API connectivity."""
        try:
            opinions = self._fetch_opinions_list()
            logger.info(f"Connection test: found {len(opinions)} opinions")
            return len(opinions) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NV-Courts data fetcher")
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
    args = parser.parse_args()

    scraper = NVCourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
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
        for raw in scraper.fetch_updates(since=args.since):
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
