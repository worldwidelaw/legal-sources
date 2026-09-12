#!/usr/bin/env python3
"""
MM/DICA -- Myanmar Directorate of Investment and Company Administration

Fetches Myanmar investment/company legislation from the DICA WordPress
JSON API. Downloads English-language PDF documents and extracts full text.

Strategy:
  - Hit /wp-json/wl/v1/wl_doc?category_name=rules-and-notifications&language=en
  - Filter for English-titled documents (skip Myanmar script, foreign translations)
  - Download PDFs and extract text via common/pdf_extract
  - Skip files >50MB to avoid huge scanned/image PDFs

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MM.DICA")

BASE_URL = "https://www.dica.gov.mm"
API_URL = f"{BASE_URL}/wp-json/wl/v1/wl_doc"

# Skip foreign-language translations (keep English originals only)
SKIP_LANG_PATTERNS = [
    r"translation in vietnam",
    r"translation in russian",
    r"translation in thai",
    r"translation in.*chinese",
    r"translation in japanese",
    r"translation in korean",
    r"unofficial.*chinese",
    r"unofficial.*japanese",
    r"unofficial.*korean",
    r"unofficial.*russian",
    r"unofficial.*thai",
    r"unofficial.*vietnam",
    r"myanmar version",
    r"\(japanese version\)",
]

MAX_PDF_SIZE_MB = 50


class DICAScraper(BaseScraper):
    """Scraper for MM/DICA -- Myanmar investment & company laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _fetch_document_list(self) -> List[Dict[str, Any]]:
        """Fetch the full document list from the DICA API."""
        for attempt in range(3):
            try:
                time.sleep(1)
                resp = self.session.get(
                    API_URL,
                    params={"category_name": "rules-and-notifications", "language": "en"},
                    timeout=30,
                )
                resp.raise_for_status()
                raw = resp.content.decode("utf-8-sig")
                data = json.loads(raw)
                logger.info(f"API returned {len(data)} documents")
                return data
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(5)
        return []

    def _is_english_law(self, doc: Dict[str, Any]) -> bool:
        """Filter for English-language original documents (not translations)."""
        title = doc.get("Title", "")

        # Skip docs with Myanmar/Burmese script
        if any(ord(c) > 0x0FFF for c in title):
            return False

        # Skip foreign-language translations
        title_lower = title.lower()
        for pattern in SKIP_LANG_PATTERNS:
            if re.search(pattern, title_lower):
                return False

        return True

    def _parse_file_size_mb(self, size_str: str) -> float:
        """Parse file size string like '3.21 MB' or '34.37 KB' to MB."""
        size_str = size_str.strip()
        if "MB" in size_str:
            return float(size_str.replace("MB", "").strip().replace(",", ""))
        elif "KB" in size_str:
            return float(size_str.replace("KB", "").strip().replace(",", "")) / 1024
        elif "GB" in size_str:
            return float(size_str.replace("GB", "").strip().replace(",", "")) * 1024
        return 0

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        wp_id = str(raw.get("ID", ""))
        doc_id = f"MM-DICA-{wp_id}"

        date = ""
        upload_date = raw.get("UploadDate", "")
        if upload_date:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", upload_date)
            if m:
                date = m.group(1)

        pdf_link = raw.get("Link", "")
        full_url = f"{BASE_URL}{pdf_link}" if pdf_link.startswith("/") else pdf_link

        return {
            "_id": doc_id,
            "_source": "MM/DICA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("Title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": full_url,
            "file_size": raw.get("FileSize", ""),
            "categories": raw.get("Categories", []),
            "language": "en",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all English-language DICA documents."""
        docs = self._fetch_document_list()
        if not docs:
            logger.error("No documents returned from API")
            return

        english_docs = [d for d in docs if self._is_english_law(d)]
        logger.info(f"Filtered to {len(english_docs)} English documents (from {len(docs)} total)")

        count = 0
        for doc in english_docs:
            title = doc.get("Title", "")
            link = doc.get("Link", "")
            file_size = doc.get("FileSize", "")
            wp_id = doc.get("ID", "")

            if not link:
                logger.warning(f"No link for: {title}")
                continue

            # Skip very large files
            size_mb = self._parse_file_size_mb(file_size)
            if size_mb > MAX_PDF_SIZE_MB:
                logger.warning(f"Skipping large file ({file_size}): {title}")
                continue

            pdf_url = f"{BASE_URL}{link}" if link.startswith("/") else link
            doc_id = f"MM-DICA-{wp_id}"

            logger.info(f"Extracting PDF for: {title[:80]} ({file_size})")
            time.sleep(1.5)

            try:
                text = extract_pdf_markdown(
                    source="MM/DICA",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for {title}: {e}")
                text = None

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {title}: {len(text) if text else 0} chars")
                continue

            raw = dict(doc)
            raw["text"] = text
            count += 1
            yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all (API has no date filter, re-fetch everything)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.session.get(
                API_URL,
                params={"category_name": "rules-and-notifications", "language": "en"},
                timeout=15,
            )
            resp.raise_for_status()
            raw = resp.content.decode("utf-8-sig")
            data = json.loads(raw)
            logger.info(f"API OK: {len(data)} documents available")

            if data:
                sample = data[0]
                logger.info(f"Sample: {sample.get('Title', 'N/A')[:80]}")

            return len(data) > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MM/DICA data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DICAScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
