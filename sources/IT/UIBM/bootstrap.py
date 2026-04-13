#!/usr/bin/env python3
"""
IT/UIBM -- Italian Patent and Trademark Office (UIBM) Data Fetcher

Fetches trademark opposition, nullity, and cancellation decisions from UIBM.

Strategy:
  - JSON API at https://www.uibm.gov.it/bancadati/index.php/Decisioni/get_all_data
    returns all decisions (4700+) with metadata and PDF links
  - PDFs at https://files.uibm.gov.it/decisioni/{wpn}.pdf contain full text
  - Text extracted via PyMuPDF (fitz)

Decision types:
  - Opposizione: trademark opposition decisions (~4700)
  - Nullità: trademark nullity decisions (~24)
  - Decadenza: trademark cancellation/revocation decisions (~56)

Date range: 2015-present

License: Italian Open Data (public domain)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (last 30 days)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import subprocess
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


# Optional PDF extraction
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.UIBM")

# API endpoint for all decisions
API_URL = "https://www.uibm.gov.it/bancadati/index.php/Decisioni/get_all_data"

# Type mapping for normalized records
TYPE_MAP = {
    "Opposizione": "trademark_opposition",
    "Nullità": "trademark_nullity",
    "Decadenza": "trademark_cancellation",
}


class UIBMScraper(BaseScraper):
    """
    Scraper for IT/UIBM -- Italian Patent and Trademark Office.
    Country: IT
    URL: https://uibm.mise.gov.it

    Fetches trademark decisions via JSON API + PDF text extraction.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_all_decisions(self) -> list:
        """Fetch all decisions from the UIBM JSON API."""
        try:
            result = subprocess.run(
                [
                    "curl", "-sL", "--max-time", "60",
                    "-H", "Accept: application/json",
                    "-H", "X-Requested-With: XMLHttpRequest",
                    API_URL,
                ],
                capture_output=True,
                timeout=70,
            )
            if result.returncode != 0:
                logger.error("Failed to fetch decisions API")
                return []

            data = json.loads(result.stdout)
            if data.get("success") and "data" in data:
                logger.info(f"Fetched {len(data['data'])} decisions from API")
                return data["data"]
            else:
                logger.error(f"API returned unexpected format: {list(data.keys())}")
                return []
        except Exception as e:
            logger.error(f"Error fetching decisions: {e}")
            return []

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF using curl."""
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", "120", url],
                capture_output=True,
                timeout=130,
            )
            if result.returncode == 0 and len(result.stdout) > 500:
                if result.stdout[:4] == b'%PDF':
                    return result.stdout
                logger.warning(f"Not a valid PDF from {url}")
                return None
            return None
        except Exception as e:
            logger.warning(f"PDF download failed for {url}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IT/UIBM",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def normalize(self, raw: Dict) -> Dict:
        """Transform raw decision data into standard schema."""
        wpn = raw.get("wpn", "")
        decision_type = raw.get("type", "Unknown")

        return {
            "_id": f"IT_UIBM_{wpn}",
            "_source": "IT/UIBM",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"UIBM {decision_type} {wpn}",
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("media", "https://www.uibm.gov.it/bancadati/Decisioni/index"),
            "wpn": wpn,
            "wpn_linked": raw.get("wpn_linked", ""),
            "type": decision_type,
            "decision_subtype": TYPE_MAP.get(decision_type, decision_type),
            "result": raw.get("result", ""),
        }

    def fetch_all(self) -> Generator[Dict, None, None]:
        """Yield all decisions with full text (raw dicts before normalize)."""
        decisions = self._fetch_all_decisions()
        if not decisions:
            logger.error("No decisions retrieved from API")
            return

        # Sort by date descending (most recent first)
        decisions.sort(key=lambda x: x.get("date", ""), reverse=True)

        fetched = 0
        errors = 0

        for dec in decisions:
            wpn = dec.get("wpn", "unknown")
            pdf_url = dec.get("media", "")

            if not pdf_url:
                logger.warning(f"No PDF URL for decision {wpn}")
                errors += 1
                continue

            logger.info(f"Fetching decision {wpn} ({fetched+1}/{len(decisions)})")

            pdf_bytes = self._download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning(f"Failed to download PDF for {wpn}")
                errors += 1
                if errors > 10 and fetched == 0:
                    logger.error("Too many consecutive failures, aborting")
                    return
                continue

            text = self._extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"No/insufficient text extracted from {wpn} PDF")
                errors += 1
                continue

            dec["text"] = text
            fetched += 1
            errors = 0
            yield dec

            time.sleep(1.5)

        logger.info(f"Fetched {fetched} decisions total (errors: {errors})")

    def fetch_sample(self, count: int = 12) -> Generator[Dict, None, None]:
        """Fetch a sample of decisions for validation."""
        fetched = 0
        for raw in self.fetch_all():
            record = self.normalize(raw)
            yield record
            fetched += 1
            if fetched >= count:
                break

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        """Yield decisions modified since a given date."""
        since_str = since.strftime("%Y-%m-%d")

        decisions = self._fetch_all_decisions()
        recent = [
            d for d in decisions
            if d.get("date") and d["date"] >= since_str
        ]
        logger.info(f"Found {len(recent)} decisions since {since_str}")

        for dec in recent:
            wpn = dec.get("wpn", "unknown")
            pdf_url = dec.get("media", "")
            if not pdf_url:
                continue

            pdf_bytes = self._download_pdf(pdf_url)
            if not pdf_bytes:
                continue

            text = self._extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                continue

            dec["text"] = text
            yield self.normalize(dec)
            time.sleep(1.5)

    def test_api(self) -> bool:
        """Quick API connectivity test."""
        decisions = self._fetch_all_decisions()
        if not decisions:
            logger.error("API test FAILED: no decisions returned")
            return False

        logger.info(f"API test PASSED: {len(decisions)} decisions available")

        # Count by type
        types = {}
        for d in decisions:
            t = d.get("type", "unknown")
            types[t] = types.get(t, 0) + 1
        logger.info(f"Decision types: {types}")

        # Test PDF download on first record
        first = decisions[0]
        pdf_url = first.get("media", "")
        if pdf_url:
            pdf_bytes = self._download_pdf(pdf_url)
            if pdf_bytes:
                text = self._extract_text_from_pdf(pdf_bytes)
                if text:
                    logger.info(f"PDF test PASSED: {len(text)} chars extracted")
                    return True
                else:
                    logger.error("PDF test FAILED: no text extracted")
            else:
                logger.error("PDF test FAILED: download failed")
        return False


def main():
    parser = argparse.ArgumentParser(description="IT/UIBM data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch only sample records (for validation)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (all records)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=12,
        help="Number of sample records to fetch",
    )

    args = parser.parse_args()
    scraper = UIBMScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample and not args.full and args.command != "bootstrap-fast":
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
            logger.info(f"Sample bootstrap complete: {stats}")
        else:
            if args.command == "bootstrap-fast":
                stats = scraper.bootstrap_fast()
            else:
                stats = scraper.bootstrap(sample_mode=False)
            logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=30)

        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info(f"Update: {record.get('wpn')}")

        logger.info(f"Update complete: {count} new records")


if __name__ == "__main__":
    main()
