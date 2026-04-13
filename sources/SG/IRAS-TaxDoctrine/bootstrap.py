#!/usr/bin/env python3
"""
SG/IRAS-TaxDoctrine -- Singapore IRAS e-Tax Guides

Fetches tax doctrine from the Inland Revenue Authority of Singapore (IRAS).
196 e-Tax Guides covering GST, corporate/individual income tax, property tax,
international tax, stamp duty, trusts, and more.

Strategy:
  - Use the IRAS JSON search API to enumerate all guides (POST /Search/)
  - Download each PDF and extract full text with pdfplumber/PyPDF2
  - Normalize with title, date, tax type, and full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Re-fetch all (check for new guides)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import hashlib
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SG.IRAS-TaxDoctrine")

BASE_URL = "https://www.iras.gov.sg"
SEARCH_URL = f"{BASE_URL}/quick-links/e-tax-guides/Search/"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"


def _fetch_json(url: str, payload: dict, timeout: int = 30) -> Optional[dict]:
    """POST JSON and return parsed response."""
    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers=headers, method="POST")
    try:
        resp = urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError) as e:
        logger.error(f"API request failed: {e}")
        return None


def _fetch_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download PDF bytes from URL."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError) as e:
        logger.warning(f"PDF download failed for {url}: {e}")
        return None


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="SG/IRAS-TaxDoctrine",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def _parse_date(date_str: str) -> Optional[str]:
    """Parse IRAS date format (e.g., '18 Mar 2026') to ISO 8601."""
    for fmt in ["%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _make_guide_id(download_url: str) -> str:
    """Generate a stable ID from the download URL."""
    # Extract filename from URL for a readable ID
    path = download_url.split("?")[0]  # Remove query params
    filename = path.rsplit("/", 1)[-1] if "/" in path else path
    filename = filename.replace(".pdf", "").replace(".PDF", "")
    # Clean up for use as ID
    clean = re.sub(r'[^a-zA-Z0-9_-]', '-', filename)
    clean = re.sub(r'-+', '-', clean).strip('-')
    if len(clean) > 80:
        clean = clean[:80]
    return clean


class SingaporeIRASTaxDoctrineScraper(BaseScraper):
    """
    Scraper for SG/IRAS-TaxDoctrine.
    Country: SG
    URL: https://www.iras.gov.sg/quick-links/e-tax-guides

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_guide_list(self) -> list:
        """Fetch all guide metadata from the IRAS search API."""
        all_guides = []
        page = 1
        max_pages = 25  # Safety limit

        while page <= max_pages:
            self.rate_limiter.wait()
            result = _fetch_json(SEARCH_URL, {"page": page})
            if not result or not result.get("success"):
                logger.warning(f"API request failed for page {page}")
                break

            guides = result.get("data", [])
            if not guides:
                break

            all_guides.extend(guides)
            pagination = result.get("pagination", {})
            total_pages = pagination.get("noOfPages", 0)

            logger.info(f"Page {page}/{total_pages}: {len(guides)} guides (total so far: {len(all_guides)})")

            if page >= total_pages:
                break
            page += 1

        logger.info(f"Total guides found: {len(all_guides)}")
        return all_guides

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IRAS e-Tax Guides with full text."""
        guides = self._fetch_guide_list()

        for i, guide in enumerate(guides):
            download_url = guide.get("download", "")
            if not download_url:
                logger.warning(f"No download URL for: {guide.get('title', 'unknown')}")
                continue

            logger.info(f"[{i+1}/{len(guides)}] Downloading: {guide.get('title', '')[:60]}...")
            self.rate_limiter.wait()

            pdf_bytes = _fetch_pdf(download_url)
            if not pdf_bytes:
                logger.warning(f"Failed to download: {download_url}")
                continue

            text = _extract_pdf_text(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"PDF text extraction failed or too short for: {guide.get('title', '')}")
                continue

            yield {
                "title": guide.get("title", ""),
                "date": guide.get("date", ""),
                "download_url": download_url,
                "text": text,
                "size": guide.get("size", ""),
                "status": guide.get("status", ""),
                "amendments": guide.get("amendments", ""),
                "tax_type": guide.get("type", ""),
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all guides (check for new/updated ones)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw guide into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        download_url = raw.get("download_url", "")
        guide_id = _make_guide_id(download_url)
        date = _parse_date(raw.get("date", ""))
        title = raw.get("title", "").strip()

        return {
            "_id": guide_id,
            "_source": "SG/IRAS-TaxDoctrine",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "guide_id": guide_id,
            "title": title,
            "text": text,
            "date": date,
            "url": download_url,
            "tax_type": raw.get("tax_type", ""),
            "amendments": raw.get("amendments", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SG/IRAS-TaxDoctrine bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--sample-size", type=int, default=15, help="Sample size")
    args = parser.parse_args()

    scraper = SingaporeIRASTaxDoctrineScraper()

    if args.command == "test":
        logger.info("Testing connectivity to IRAS API...")
        result = _fetch_json(SEARCH_URL, {"page": 1})
        if result and result.get("success") and result.get("data"):
            count = result["pagination"]["noOfResults"]
            logger.info(f"SUCCESS: IRAS API accessible, {count} guides available")
        else:
            logger.error("FAILED: Could not access IRAS API")
            sys.exit(1)

    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2, default=str)}")

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {json.dumps(result, indent=2, default=str)}")


if __name__ == "__main__":
    main()
