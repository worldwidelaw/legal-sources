#!/usr/bin/env python3
"""
ET/FederalNegaritGazetta -- Ethiopian Federal Negarit Gazetta (Official Gazette)

Fetches proclamation PDFs from lawethiopia.com, a free-access Ethiopian legal
information portal. PDFs are stored at predictable URLs:
  https://lawethiopia.com/images/federal_proclamation/proclamations_by_number/{number}.pdf

The scraper enumerates proclamation numbers (1–1100), checks availability via
HEAD requests, downloads PDFs, and extracts full text using the centralized
pdf_extract utility.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no incremental API)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ET.FederalNegaritGazetta")

BASE_URL = "https://lawethiopia.com"
PDF_URL_TEMPLATE = BASE_URL + "/images/federal_proclamation/proclamations_by_number/{number}.pdf"

# Proclamation numbers range from 1 to ~1070 (with gaps)
MAX_PROCLAMATION_NUMBER = 1100


class EthiopiaNegaritGazettaScraper(BaseScraper):
    """
    Scraper for ET/FederalNegaritGazetta -- Ethiopian Federal Negarit Gazetta.
    Country: ET
    URL: https://lawethiopia.com/index.php/proclamations-by-number
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/pdf,*/*",
        })

    def _check_pdf_exists(self, number: int) -> bool:  # noqa: E501
        """HEAD request to check if a proclamation PDF exists."""
        url = PDF_URL_TEMPLATE.format(number=number)
        try:
            resp = self.session.head(url, timeout=15, allow_redirects=True)
            return (
                resp.status_code == 200
                and "pdf" in resp.headers.get("content-type", "").lower()
            )
        except Exception:
            return False

    def _download_pdf(self, number: int) -> Optional[bytes]:
        """Download a proclamation PDF."""
        url = PDF_URL_TEMPLATE.format(number=number)
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            if len(resp.content) < 500:
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download proclamation {number}: {e}")
            return None

    def _extract_title_from_text(self, text: str, number: int) -> str:
        """Try to extract a meaningful title from the PDF text."""
        # Look for "Proclamation No. XXX/YYYY" pattern
        match = re.search(
            r'[Pp]roclamation\s+(?:No\.?\s*)?\d+[/\-]\d{4}[^.\n]*',
            text[:2000]
        )
        if match:
            title = match.group(0).strip()
            if len(title) > 20:
                return title

        # Look for a prominent title in English in the first 1000 chars
        lines = text[:1500].split('\n')
        for line in lines:
            line = line.strip()
            # Skip short lines, Amharic-only lines, header boilerplate
            if len(line) < 15 or len(line) > 200:
                continue
            if 'NEGARIT' in line.upper() or 'GAZET' in line.upper():
                continue
            if 'FEDERAL DEMOCRATIC' in line.upper():
                continue
            # Prefer lines that look like proclamation titles
            if re.search(r'[Pp]roclamation|PROCLAMATION', line):
                return line
            if line.isupper() and len(line) > 20:
                return line

        return f"Proclamation No. {number}"

    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Try to extract a date from the PDF text."""
        # Look for common date patterns in Ethiopian gazettes
        # Pattern: "Month Day, Year" or "Day Month Year"
        date_match = re.search(
            r'(\d{1,2})\s*(?:st|nd|rd|th)?\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'[\s,]+(\d{4})',
            text[:3000]
        )
        if date_match:
            day, month, year = date_match.groups()
            months = {
                'January': '01', 'February': '02', 'March': '03',
                'April': '04', 'May': '05', 'June': '06',
                'July': '07', 'August': '08', 'September': '09',
                'October': '10', 'November': '11', 'December': '12',
            }
            return f"{year}-{months[month]}-{int(day):02d}"

        # Try "Month Day, Year" format
        date_match2 = re.search(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)'
            r'\s+(\d{1,2})[\s,]+(\d{4})',
            text[:3000]
        )
        if date_match2:
            month, day, year = date_match2.groups()
            months = {
                'January': '01', 'February': '02', 'March': '03',
                'April': '04', 'May': '05', 'June': '06',
                'July': '07', 'August': '08', 'September': '09',
                'October': '10', 'November': '11', 'December': '12',
            }
            return f"{year}-{months[month]}-{int(day):02d}"

        return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        number = raw.get("proclamation_number", 0)
        return {
            "_id": f"ET-FNG-{number}",
            "_source": "ET/FederalNegaritGazetta",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "proclamation_number": number,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents."""
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield all documents (no incremental API available)."""
        yield from self._fetch_documents(sample=False)

    def _fetch_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Core fetcher: enumerate proclamation numbers and download PDFs."""
        total_count = 0
        total_failures = 0

        if sample:
            # For sample, check a spread of known-good numbers
            candidates = [27, 89, 116, 200, 295, 400, 471, 600, 769, 900,
                          958, 979, 1000, 1020, 1035, 1051]
        else:
            candidates = list(range(1, MAX_PROCLAMATION_NUMBER + 1))

        logger.info(f"Checking {len(candidates)} proclamation numbers "
                    f"({'sample' if sample else 'full'} mode)")

        for number in candidates:
            # Quick HEAD check to avoid downloading non-existent PDFs
            self.rate_limiter.wait()
            if not self._check_pdf_exists(number):
                continue

            pdf_bytes = self._download_pdf(number)
            if not pdf_bytes:
                total_failures += 1
                continue

            text = extract_pdf_markdown(
                source="ET/FederalNegaritGazetta",
                source_id=str(number),
                pdf_bytes=pdf_bytes,
                table="legislation",
            ) or ""

            if len(text) < 50:
                logger.warning(f"Insufficient text ({len(text)} chars) for proclamation {number}")
                total_failures += 1
                continue

            title = self._extract_title_from_text(text, number)
            date = self._extract_date_from_text(text)
            url = PDF_URL_TEMPLATE.format(number=number)

            raw = {
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "proclamation_number": number,
            }

            record = self.normalize(raw)
            total_count += 1
            logger.info(f"[{total_count}] Proclamation {number}: "
                        f"{title[:50]} ({len(text)} chars)")
            yield record

            if sample and total_count >= 15:
                break

        logger.info(f"TOTAL: {total_count} records, {total_failures} failures")

    def test_api(self):
        """Quick connectivity and extraction test."""
        logger.info("Testing lawethiopia.com connectivity...")

        # Test a known proclamation
        number = 979
        url = PDF_URL_TEMPLATE.format(number=number)
        resp = self.session.head(url, timeout=15)
        logger.info(f"HEAD {url}: {resp.status_code}")

        if resp.status_code == 200:
            pdf_bytes = self._download_pdf(number)
            if pdf_bytes:
                text = extract_pdf_markdown(
                    source="ET/FederalNegaritGazetta",
                    source_id=str(number),
                    pdf_bytes=pdf_bytes,
                    table="legislation",
                )
                logger.info(f"OK: extracted {len(text)} chars from proclamation {number}")
                title = self._extract_title_from_text(text, number)
                date = self._extract_date_from_text(text)
                logger.info(f"Title: {title}")
                logger.info(f"Date: {date}")
            else:
                logger.error("Failed to download PDF")
        else:
            logger.error(f"HEAD request failed: {resp.status_code}")


def main():
    scraper = EthiopiaNegaritGazettaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper._fetch_documents(sample=sample):
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
