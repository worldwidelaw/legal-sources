#!/usr/bin/env python3
"""
LC/NPC-Acts -- Saint Lucia Acts of Parliament (National Printing Corporation)

Fetches Acts of Parliament (2003-2026) with full text from npc.govt.lc. PDFs
are downloaded via base64-encoded download URLs and text extracted via
pdfplumber.

Strategy:
  - Iterate year pages (/laws/acts/{year}) from 2026 down to 2001
  - Parse HTML table to extract download links and titles
  - Download each PDF and extract text with pdfplumber

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LC.NPC-Acts")

BASE_URL = "https://npc.govt.lc"
ACTS_URL = f"{BASE_URL}/laws/acts"
YEARS = list(range(2026, 2000, -1))  # 2026 down to 2001
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB

# Matches the various Act title formats seen on the site, e.g.
#   "Act No. 21 of 2024 - National Insurance Corporation (Amendment) Act"
#   "Act No1 of 2024 Tourism Development Act"
#   "Act No 4 of 2024 Appropriation Act"
ACT_TITLE_RE = re.compile(
    r"Act\s*No\.?\s*(\d+)\s+of\s+(\d{4})\s*[-–]?\s*(.*)", re.IGNORECASE
)
LINK_RE = re.compile(
    r'href="(https?://npc\.govt\.lc/laws/download/[^"]+)"[^>]*>([^<]+)</a>'
)


class LCNPCActsScraper(BaseScraper):
    """Scraper for LC/NPC-Acts -- Saint Lucia Acts of Parliament."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _request(self, url: str, timeout: int = 60, stream: bool = False) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, stream=stream)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_year_page(self, year: int) -> List[Dict[str, str]]:
        """Parse a year page and return list of Act dicts with title, download_url."""
        url = f"{ACTS_URL}/{year}"
        resp = self._request(url)
        if resp is None:
            return []

        documents = []
        seen = set()
        for match in LINK_RE.finditer(resp.text):
            download_url = match.group(1)
            title = re.sub(r"\s+", " ", match.group(2)).strip()
            if download_url in seen:
                continue
            seen.add(download_url)

            act_match = ACT_TITLE_RE.match(title)
            if act_match:
                act_number = int(act_match.group(1))
                act_year = act_match.group(2)
                act_title = act_match.group(3).strip() or title
            else:
                act_number = 0
                act_year = str(year)
                act_title = title

            documents.append({
                "title": title,
                "act_title": act_title,
                "act_number": act_number,
                "year": act_year,
                "download_url": download_url,
            })

        return documents

    def _extract_pdf_text(self, download_url: str) -> str:
        """Download PDF and extract text via pdfplumber."""
        resp = self._request(download_url, timeout=120, stream=True)
        if resp is None:
            return ""

        pdf_bytes = resp.content
        if len(pdf_bytes) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({len(pdf_bytes)} bytes): {download_url}")
            return ""

        if len(pdf_bytes) < 100:
            logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): {download_url}")
            return ""

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    parts.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(parts).strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {download_url}: {e}")
            return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "LC/NPC-Acts",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("download_url", ""),
            "act_number": raw.get("act_number", 0),
            "year": raw.get("year", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0

        for year in YEARS:
            if max_records and count >= max_records:
                return

            docs = self._parse_year_page(year)
            if not docs:
                logger.info(f"No Acts found for year {year}")
                continue

            logger.info(f"Year {year}: {len(docs)} Acts found")

            for doc in docs:
                if max_records and count >= max_records:
                    return

                text = self._extract_pdf_text(doc["download_url"])
                if not text or len(text) < 50:
                    logger.warning(
                        f"Insufficient text ({len(text)} chars): {doc['title'][:60]}"
                    )
                    continue

                doc_id = f"LC-ACT-{doc['year']}-{doc['act_number']:04d}"
                raw = {
                    "doc_id": doc_id,
                    "title": doc["title"],
                    "act_title": doc["act_title"],
                    "text": text,
                    "date": f"{doc['year']}-01-01",
                    "download_url": doc["download_url"],
                    "act_number": doc["act_number"],
                    "year": doc["year"],
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} Acts fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=30)

    def test(self) -> bool:
        docs = self._parse_year_page(2024)
        if not docs:
            logger.error("Cannot parse Acts page for 2024")
            return False

        logger.info(f"Year 2024 page OK: {len(docs)} Acts")

        doc = docs[0]
        logger.info(f"Testing PDF download: {doc['title'][:60]}")
        text = self._extract_pdf_text(doc["download_url"])
        logger.info(f"PDF text: {len(text)} chars")

        return len(text) > 50


def main():
    parser = argparse.ArgumentParser(description="LC/NPC-Acts data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LCNPCActsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            text_len = len(normalized.get("text", ""))
            logger.info(
                f"[{count + 1}] {normalized.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
