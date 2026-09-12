#!/usr/bin/env python3
"""
BZ/Gazette - Belize Statutory Instruments (Government Gazette)

Statutory Instruments (subsidiary legislation) are published in the Belize
Government Gazette and mirrored on the National Assembly website. The
www.gazette.gov.bz domain currently does not resolve; the National Assembly
SI archive is the authoritative public-facing source.

Complements BZ/NationalAssembly (Acts of Parliament).

Data source: https://www.nationalassembly.gov.bz/statutory-instruments/
License: Public domain (official government legislation)
"""

import argparse
import hashlib
import html
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.nationalassembly.gov.bz"
SOURCE_ID = "BZ/Gazette"
SAMPLE_DIR = Path(__file__).parent / "sample"
YEARS = list(range(2017, 2027))


class GazetteFetcher:
    """Fetcher for Belize Gazette Statutory Instruments via National Assembly."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html, */*',
        })

    def fetch_year_sis(self, year: int) -> List[Dict[str, Any]]:
        """Fetch the list of SIs for a given year from its listing page."""
        url = f"{BASE_URL}/statutory-instruments-{year}/"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch year page {year}: {e}")
            return []

        results = []
        seen = set()
        for match in re.finditer(r'<a[^>]*href="([^"]+\.pdf)"[^>]*>([^<]*)</a>', resp.text):
            pdf_path = match.group(1)
            raw_title = html.unescape(match.group(2)).strip()

            # Only include actual SIs: e.g. "SI No. 12 of 2025 ..."
            if not re.search(r'SI[\s\-]*No\.?\s*\d+', raw_title, re.IGNORECASE) and \
               not re.search(r'/SI[\s\-_]*No[\s\-_.]*\d+', pdf_path, re.IGNORECASE):
                continue

            pdf_url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            results.append({"url": pdf_url, "title": raw_title or pdf_path.rsplit("/", 1)[-1], "year": year})

        return results

    def extract_pdf_text(self, pdf_bytes: bytes) -> Optional[str]:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                full_text = "\n\n".join(pages_text)
                return full_text if len(full_text) > 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return None

    def download_and_extract(self, pdf_url: str) -> Optional[str]:
        try:
            resp = self.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {pdf_url}")
                return None
            return self.extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

    def parse_si_metadata(self, title: str, url: str, year: int) -> Dict[str, Any]:
        """Extract SI number and clean title."""
        m = re.search(r'SI[\s\-]*No\.?\s*(\d+)', title, re.IGNORECASE)
        if not m:
            m = re.search(r'SI[\s\-_.]*No[\s\-_.]*(\d+)', url, re.IGNORECASE)
        si_number = int(m.group(1)) if m else None

        clean_title = re.sub(
            r'^SI[\s\-]*No\.?\s*\d+\s*(?:of\s*\d{4})?\s*[-–—]?\s*',
            '',
            title,
            flags=re.IGNORECASE,
        ).strip()
        if not clean_title:
            clean_title = title

        return {
            "si_number": si_number,
            "clean_title": clean_title,
            "date": f"{year}-01-01",
        }

    def normalize(self, si: Dict[str, Any], text: str) -> Dict[str, Any]:
        meta = self.parse_si_metadata(si["title"], si["url"], si["year"])
        doc_id = hashlib.sha256(si["url"].encode()).hexdigest()[:16]

        return {
            "_id": f"BZ-SI-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": si["title"],
            "text": text,
            "date": meta["date"],
            "url": si["url"],
            "si_number": meta["si_number"],
            "year": si["year"],
            "country": "BZ",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        for year in YEARS:
            logger.info(f"Fetching SIs for {year}")
            sis = self.fetch_year_sis(year)
            logger.info(f"  Found {len(sis)} SIs for {year}")
            for si in sis:
                time.sleep(1.5)
                text = self.download_and_extract(si["url"])
                if text:
                    yield self.normalize(si, text)
                else:
                    logger.warning(f"  No text extracted for: {si['title']}")

    def fetch_updates(self, since: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """Fetch SIs from the current and prior year (cheap incremental update)."""
        current_year = datetime.now(timezone.utc).year
        for year in (current_year - 1, current_year):
            sis = self.fetch_year_sis(year)
            for si in sis:
                time.sleep(1.5)
                text = self.download_and_extract(si["url"])
                if text:
                    yield self.normalize(si, text)

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        records = []
        sample_years = [2023, 2024, 2025, 2026]
        for year in sample_years:
            if len(records) >= max_records:
                break
            sis = self.fetch_year_sis(year)
            # Take a few from start
            for si in sis[:4]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                text = self.download_and_extract(si["url"])
                if text:
                    record = self.normalize(si, text)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {si['title'][:80]} ({len(text)} chars)")
                else:
                    logger.warning(f"  Skipped (no text): {si['title']}")
        return records


def bootstrap_sample():
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = GazetteFetcher()
    records = fetcher.fetch_sample(max_records=15)

    for i, record in enumerate(records):
        outfile = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    texts = [r.get("text", "") for r in records]
    non_empty = sum(1 for t in texts if len(t) > 100)
    avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
    logger.info(f"Validation: {non_empty}/{len(records)} records have text (avg {avg_len:.0f} chars)")

    return records


def bootstrap_full():
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = GazetteFetcher()
    count = 0
    for record in fetcher.fetch_all():
        outfile = SAMPLE_DIR / f"record_{count+1:04d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        if count % 50 == 0:
            logger.info(f"Progress: {count} records saved")
    logger.info(f"Complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="BZ/Gazette - Belize Statutory Instruments Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
