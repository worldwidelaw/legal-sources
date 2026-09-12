#!/usr/bin/env python3
"""
JM/Gazette -- Jamaica Official Gazette (Ministry of Justice)

Fetches gazette notices (proclamations, rules, regulations) from laws.moj.gov.jm.

Strategy:
  - DataTables server-side POST API at /library/gazettes/{year} for listings
  - Parse detail pages to extract PDF URLs from pdfjs viewer input
  - Download PDFs and extract full text via pdfplumber/pypdf
  - ~9,200+ entries across 2000-2024

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JM.Gazette")

BASE_URL = "https://laws.moj.gov.jm"
# Years 2000-2021 are scanned image PDFs (no text layer).
# Only 2022+ have digital/searchable PDFs.
YEARS = list(range(2022, 2025))


class JamaicaGazetteScraper(BaseScraper):
    """
    Scraper for JM/Gazette -- Jamaica Official Gazette.
    Country: JM
    URL: https://laws.moj.gov.jm/library/gazettes

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self.session.verify = False

    def _list_gazettes_for_year(self, year: int, max_records: int = 0) -> list[dict]:
        """Fetch all gazette entries for a year via DataTables server-side API."""
        entries = []
        start = 0
        page_size = 100

        while True:
            url = f"{BASE_URL}/library/gazettes/{year}"
            data = {
                "draw": str(start // page_size + 1),
                "start": str(start),
                "length": str(page_size),
                "_dt": "gazetteTable",
                "_init": "true",
            }

            try:
                resp = self.session.post(url, data=data, timeout=30)
                resp.raise_for_status()
                result = resp.json()
            except Exception as e:
                logger.error(f"Failed to fetch gazettes for {year} at start={start}: {e}")
                break

            records = result.get("data", [])
            if not records:
                break

            for row in records:
                if len(row) < 3:
                    continue
                title = row[0]
                ref_html = row[1]
                pub_year = row[2]

                # Extract slug from href in reference HTML
                slug_match = re.search(r'href="/library/gazette/([^"]+)"', ref_html)
                if not slug_match:
                    continue
                slug = slug_match.group(1)

                # Extract reference number text
                ref_text = re.sub(r'<[^>]+>', '', ref_html).strip()

                entries.append({
                    "title": html_mod.unescape(title),
                    "reference_number": ref_text,
                    "slug": slug,
                    "year": pub_year,
                })

            total_filtered = int(result.get("recordsFiltered", 0))
            start += page_size

            if max_records and len(entries) >= max_records:
                entries = entries[:max_records]
                break

            if start >= total_filtered:
                break

            time.sleep(1)

        logger.info(f"Year {year}: found {len(entries)} gazette entries")
        return entries

    def _get_pdf_url(self, slug: str) -> Optional[str]:
        """Fetch a gazette detail page and extract the PDF URL."""
        url = f"{BASE_URL}/library/gazette/{slug}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            html = resp.text

            # PDF path is in: <input data-pdfjs-target="pdf" value="/legislation/...pdf">
            pdf_match = re.search(
                r'data-pdfjs-target="pdf"\s+value="([^"]+\.pdf)"', html
            )
            if pdf_match:
                pdf_path = pdf_match.group(1)
                # URL-encode spaces in path (filenames have spaces)
                encoded_path = quote(pdf_path, safe="/:&=?#")
                return BASE_URL + encoded_path
            else:
                logger.warning(f"No PDF found on detail page: {slug}")
                return None
        except Exception as e:
            logger.error(f"Failed to fetch detail page for {slug}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette documents with full text."""
        for year in YEARS:
            entries = self._list_gazettes_for_year(year)

            for entry in entries:
                time.sleep(1.5)

                pdf_url = self._get_pdf_url(entry["slug"])
                if not pdf_url:
                    continue

                time.sleep(1)

                text = extract_pdf_markdown(
                    source="JM/Gazette",
                    source_id=f"jm-gazette-{entry['slug']}",
                    pdf_url=pdf_url,
                    table="legislation",
                )
                if not text:
                    logger.warning(f"No text from PDF: {entry['slug']}")
                    continue

                yield {
                    **entry,
                    "pdf_url": pdf_url,
                    "text": text,
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent gazettes (latest year only)."""
        current_year = datetime.now().year
        entries = self._list_gazettes_for_year(current_year)

        for entry in entries:
            time.sleep(1.5)
            pdf_url = self._get_pdf_url(entry["slug"])
            if not pdf_url:
                continue
            time.sleep(1)
            text = extract_pdf_markdown(
                source="JM/Gazette",
                source_id=f"jm-gazette-{entry['slug']}",
                pdf_url=pdf_url,
                table="legislation",
            )
            if not text:
                continue
            yield {**entry, "pdf_url": pdf_url, "text": text}

    def normalize(self, raw: dict) -> dict:
        """Transform raw gazette data into standard schema."""
        year = raw.get("year", "")
        date_str = f"{year}-01-01" if year else None

        slug = raw.get("slug", "")
        ref = raw.get("reference_number", slug)

        return {
            "_id": f"jm-gazette-{slug}",
            "_source": "JM/Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": f"{BASE_URL}/library/gazette/{slug}",
            "pdf_url": raw.get("pdf_url", ""),
            "reference_number": ref,
            "year": year,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    scraper = JamaicaGazetteScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing connectivity...")
        resp = scraper.session.post(
            f"{BASE_URL}/library/gazettes/2024",
            data={"draw": "1", "start": "0", "length": "5", "_dt": "gazetteTable", "_init": "true"},
            timeout=30,
        )
        print(f"Status: {resp.status_code}")
        data = resp.json()
        print(f"Total records: {data.get('recordsTotal')}")
        print(f"Year 2024 records: {data.get('recordsFiltered')}")
        print(f"Sample: {json.dumps(data.get('data', [])[:2], indent=2)}")
        sys.exit(0)

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample_mode)
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
