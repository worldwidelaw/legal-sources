#!/usr/bin/env python3
"""
LA/NationalAssembly -- Lao National Assembly Legislation Fetcher

Fetches laws and resolutions from na.gov.la via WordPress REST API.

Strategy:
  - Query /wp-json/wp/v2/legals to get custom post type records
  - Each record has an ACF repeater field 'legal_file' with PDF entries
  - Each entry: name (Lao title), englist_name, file_url (PDF), categories
  - Download PDFs and extract full text using pdfplumber

Data:
  - ~216 documents: 176 laws + 40 resolutions
  - Categories: govern, defense, economic, culture, foreign
  - Full text in Lao from PDF files

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LA.NationalAssembly")

BASE_URL = "https://na.gov.la"
API_URL = f"{BASE_URL}/wp-json/wp/v2/legals"


class LaoNationalAssemblyScraper(BaseScraper):
    """
    Scraper for LA/NationalAssembly -- Lao National Assembly Legislation.
    Country: LA
    URL: https://na.gov.la

    Data types: legislation
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            },
            timeout=120,
        )

    def _fetch_legals(self) -> List[Dict]:
        """Fetch all legal post type records from WP REST API."""
        all_entries = []
        page = 1
        while True:
            self.rate_limiter.wait()
            resp = self.client.get(f"/wp-json/wp/v2/legals?per_page=100&page={page}")
            if resp.status_code == 400:
                break
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_entries.extend(data)
            page += 1
        return all_entries

    def _extract_pdf_entries(self, legals: List[Dict]) -> List[Dict]:
        """Extract individual PDF entries from ACF repeater fields."""
        entries = []
        for legal in legals:
            post_id = legal.get("id", 0)
            title_obj = legal.get("title", {})
            post_title = title_obj.get("rendered", "") if isinstance(title_obj, dict) else str(title_obj)

            # Determine post type from title
            post_type = "laws"
            if "ມະຕິ" in post_title:
                post_type = "resolutions"

            # Get ACF fields
            acf = legal.get("acf", {})
            if not acf:
                continue

            legal_files = acf.get("legal_file", [])
            if not legal_files:
                continue

            for idx, entry in enumerate(legal_files):
                if not isinstance(entry, dict):
                    continue

                file_url = entry.get("file_url", "")
                name = entry.get("name", "")
                english_name = entry.get("englist_name", "")
                category = entry.get("categories", "")

                if not file_url or not file_url.endswith(".pdf"):
                    continue

                doc_id = f"LA-NA-{post_id}-{idx:03d}"
                entries.append({
                    "doc_id": doc_id,
                    "post_id": post_id,
                    "index": idx,
                    "name": name.strip(),
                    "english_name": english_name.strip() if english_name else "",
                    "category": category.strip() if category else "",
                    "file_url": file_url.strip(),
                    "post_type": post_type,
                    "post_title": post_title,
                })

        return entries

    def _download_and_extract_pdf(self, file_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        text = extract_pdf_markdown(
            source="LA/NationalAssembly",
            source_id=doc_id,
            pdf_url=file_url,
            table="legislation",
        )
        if text and len(text.strip()) > 50:
            return text.strip()
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into the standard schema."""
        title = raw.get("name", "") or raw.get("english_name", "") or raw.get("doc_id", "")

        # Try to extract year from filename
        year_match = re.search(r'(\d{4})', raw.get("file_url", ""))
        date = ""
        if year_match:
            year = year_match.group(1)
            if 1975 <= int(year) <= 2030:
                date = f"{year}-01-01"

        return {
            "_id": raw["doc_id"],
            "_source": "LA/NationalAssembly",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "english_name": raw.get("english_name", ""),
            "text": raw.get("text", ""),
            "date": date,
            "category": raw.get("category", ""),
            "post_type": raw.get("post_type", ""),
            "pdf_url": raw.get("file_url", ""),
            "url": f"{BASE_URL}/legal/{raw.get('post_title', '')}",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all laws and resolutions."""
        logger.info("Fetching legal records from WP REST API...")
        legals = self._fetch_legals()
        logger.info(f"Found {len(legals)} legal post records")

        entries = self._extract_pdf_entries(legals)
        logger.info(f"Extracted {len(entries)} PDF entries total")

        for entry in entries:
            self.rate_limiter.wait()
            text = self._download_and_extract_pdf(entry["file_url"], entry["doc_id"])
            if not text:
                logger.warning(f"No text extracted for {entry['doc_id']}: {entry['name'][:60]}")
                continue

            entry["text"] = text
            yield self.normalize(entry)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch all records (small corpus, no date filtering needed)."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick test to verify the API is reachable."""
        try:
            resp = self.client.get("/wp-json/wp/v2/legals?per_page=1")
            return resp.status_code == 200
        except Exception:
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LA/NationalAssembly bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to execute")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a sample of records")
    parser.add_argument("--since", type=str, default=None,
                        help="ISO date for incremental updates")
    args = parser.parse_args()

    scraper = LaoNationalAssemblyScraper()

    if args.command == "test":
        if scraper.test_connection():
            print("Connection OK")
            sys.exit(0)
        else:
            print("Connection FAILED")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 99999

        logger.info("Fetching legal records from WP REST API...")
        legals = scraper._fetch_legals()
        logger.info(f"Found {len(legals)} legal post records")

        entries = scraper._extract_pdf_entries(legals)
        logger.info(f"Extracted {len(entries)} PDF entries total")

        if args.sample:
            entries = entries[:max_records]

        for entry in entries:
            if count >= max_records:
                break

            scraper.rate_limiter.wait()
            text = scraper._download_and_extract_pdf(entry["file_url"], entry["doc_id"])
            if not text:
                logger.warning(f"No text for {entry['doc_id']}: {entry['name'][:60]}")
                continue

            entry["text"] = text
            record = scraper.normalize(entry)

            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            count += 1
            logger.info(f"[{count}/{max_records}] Saved {record['_id']} ({len(text)} chars)")

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        since = args.since or "2024-01-01"
        count = 0
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        for record in scraper.fetch_updates(since):
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"[{count}] Updated {record['_id']}")

        print(f"\nUpdate complete: {count} records updated since {since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
