#!/usr/bin/env python3
"""
INTL/PCIJ -- Permanent Court of International Justice (CD-PCIJ via Zenodo)

Fetches PCIJ decisions (1922-1940) from Zenodo bulk CSV download.

Strategy:
  - Download CSV ZIP from Zenodo (~2MB)
  - Parse CSV with full text (259 documents)
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import csv
import io
import re
import sys
import json
import html
import time
import logging
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.PCIJ")

CSV_ZIP_URL = "https://zenodo.org/api/records/3840480/files/CD-PCIJ_1-0-0_EN_CSV_TESSERACT_FULL.zip/content"
CSV_FILENAME = "CD-PCIJ_1-0-0_EN_CSV_TESSERACT_FULL.csv"

# Map document type codes to human-readable names
DOCTYPE_MAP = {
    "JUD": "Judgment",
    "ADV": "Advisory Opinion",
    "ORD": "Order",
    "APP": "Application",
    "DEC": "Declaration",
    "ANX": "Annex",
}


def clean_ocr_text(text: str) -> str:
    """Clean OCR'd text: fix common artifacts, normalize whitespace."""
    if not text:
        return ""
    # Remove stray pipe characters common in OCR
    text = re.sub(r'\|', 'I', text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class PCIJScraper(BaseScraper):
    """
    Scraper for INTL/PCIJ -- Permanent Court of International Justice.
    Country: INTL
    URL: https://zenodo.org/records/3840480

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research project)",
            "Accept": "*/*",
        })

    def _download_csv(self) -> list:
        """Download and parse the CSV ZIP from Zenodo."""
        logger.info(f"Downloading CSV from Zenodo...")
        r = self.session.get(CSV_ZIP_URL, timeout=120)
        r.raise_for_status()
        logger.info(f"Downloaded {len(r.content)} bytes")

        csv.field_size_limit(2**24)  # 16MB field limit for large texts

        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            with zf.open(CSV_FILENAME) as csvfile:
                reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding='utf-8'))
                rows = list(reader)

        logger.info(f"Parsed {len(rows)} records from CSV")
        return rows

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw CSV row into standard schema."""
        text = clean_ocr_text(raw.get("text", ""))
        if not text or len(text) < 200:
            return None

        doc_id = raw.get("doc_id", "")
        shortname = raw.get("shortname", "")
        fullname = raw.get("fullname", "")
        date = raw.get("date", "")
        doctype = raw.get("doctype", "")
        court = raw.get("court", "PCIJ")
        series = raw.get("series", "")
        seriesno = raw.get("seriesno", "")

        # Build title
        doctype_label = DOCTYPE_MAP.get(doctype, doctype)
        title_name = fullname if fullname else shortname
        title = f"PCIJ - {title_name} - {doctype_label} ({date})" if title_name else f"PCIJ Series {series}/{seriesno} - {doctype_label} ({date})"

        return {
            "_id": f"PCIJ-{doc_id}" if doc_id else f"PCIJ-{series}-{seriesno}-{date}",
            "_source": "INTL/PCIJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date if date else None,
            "url": f"https://zenodo.org/records/3840480",
            "court": court,
            "case_name": fullname,
            "short_name": shortname,
            "series": f"{series}/{seriesno}" if series else None,
            "document_type": doctype_label,
            "applicant": raw.get("applicant"),
            "respondent": raw.get("respondent"),
            "year": raw.get("year"),
            "language": raw.get("language", "EN"),
            "license": raw.get("license", "CC0 1.0"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all PCIJ decisions from Zenodo CSV."""
        rows = self._download_csv()
        total = 0
        for row in rows:
            text = row.get("text", "")
            if len(text) < 200:
                continue
            yield row
            total += 1
        logger.info(f"Total records yielded: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Static dataset — re-fetch all."""
        yield from self.fetch_all()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/PCIJ data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = PCIJScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            r = scraper.session.head(CSV_ZIP_URL, timeout=30, allow_redirects=True)
            r.raise_for_status()
            logger.info(f"OK: Zenodo reachable, status {r.status_code}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
