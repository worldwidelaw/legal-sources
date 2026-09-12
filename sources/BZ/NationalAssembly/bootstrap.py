#!/usr/bin/env python3
"""
BZ/NationalAssembly - Belize National Assembly Acts of Parliament

Fetches sessional Acts from the National Assembly of Belize (2015-2026).
Each year has a listing page with PDF download links.
Complements BZ/AGMLaws which covers consolidated laws and post-2020 acts/SI.

Data source: https://www.nationalassembly.gov.bz/acts-of-parliament/
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
SOURCE_ID = "BZ/NationalAssembly"
SAMPLE_DIR = Path(__file__).parent / "sample"
YEARS = list(range(2015, 2027))


class NationalAssemblyFetcher:
    """Fetcher for Belize National Assembly Acts of Parliament."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html, */*',
        })

    def fetch_year_acts(self, year: int) -> List[Dict[str, str]]:
        """Fetch the list of acts for a given year from its listing page."""
        url = f"{BASE_URL}/acts-of-parliament/acts-parliament-{year}/"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch year page {year}: {e}")
            return []

        results = []
        # Find all PDF links with their titles
        for match in re.finditer(r'<a[^>]*href="([^"]+\.pdf)"[^>]*>([^<]*)</a>', resp.text):
            pdf_path = match.group(1)
            raw_title = html.unescape(match.group(2)).strip()

            # Only include actual Acts of Parliament (skip Standing Orders, etc.)
            if not re.search(r'Act\s+No\.?\s*\d+', raw_title, re.IGNORECASE):
                continue

            pdf_url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
            results.append({"url": pdf_url, "title": raw_title, "year": year})

        return results

    def extract_pdf_text(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF bytes using pdfplumber."""
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
        """Download a PDF and extract its text."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {pdf_url}")
                return None
            return self.extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

    def parse_act_metadata(self, title: str, year: int) -> Dict[str, Any]:
        """Extract act number and clean title from raw title string."""
        act_match = re.search(r'Act\s+No\.?\s*(\d+)', title, re.IGNORECASE)
        act_number = int(act_match.group(1)) if act_match else None

        # Clean title: remove "Act No. X of YYYY – " prefix if present, decode entities
        clean_title = re.sub(r'^Act\s+No\.?\s*\d+\s*(?:of\s*\d{4})?\s*[-–—]\s*', '', title).strip()
        if not clean_title:
            clean_title = title

        return {
            "act_number": act_number,
            "clean_title": clean_title,
            "date": f"{year}-01-01",
        }

    def normalize(self, act: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize an act record into the standard schema."""
        meta = self.parse_act_metadata(act["title"], act["year"])
        doc_id = hashlib.sha256(act["url"].encode()).hexdigest()[:16]

        return {
            "_id": f"BZ-NA-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": act["title"],
            "text": text,
            "date": meta["date"],
            "url": act["url"],
            "act_number": meta["act_number"],
            "year": act["year"],
            "country": "BZ",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all acts with full text."""
        for year in YEARS:
            logger.info(f"Fetching acts for {year}")
            acts = self.fetch_year_acts(year)
            logger.info(f"  Found {len(acts)} acts for {year}")
            for act in acts:
                time.sleep(1.5)
                text = self.download_and_extract(act["url"])
                if text:
                    yield self.normalize(act, text)
                else:
                    logger.warning(f"  No text extracted for: {act['title']}")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of records for testing."""
        records = []
        # Sample from different years for diversity
        sample_years = [2015, 2017, 2019, 2021, 2024]

        for year in sample_years:
            if len(records) >= max_records:
                break
            acts = self.fetch_year_acts(year)
            for act in acts[:3]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                text = self.download_and_extract(act["url"])
                if text:
                    record = self.normalize(act, text)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {act['title']} ({len(text)} chars)")
                else:
                    logger.warning(f"  Skipped (no text): {act['title']}")

        return records


def bootstrap_sample():
    """Run sample mode: fetch ~15 diverse records and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = NationalAssemblyFetcher()
    records = fetcher.fetch_sample(max_records=15)

    for i, record in enumerate(records):
        outfile = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    # Validate
    texts = [r.get("text", "") for r in records]
    non_empty = sum(1 for t in texts if len(t) > 100)
    avg_len = sum(len(t) for t in texts) / max(len(texts), 1)
    logger.info(f"Validation: {non_empty}/{len(records)} records have text (avg {avg_len:.0f} chars)")

    return records


def bootstrap_full():
    """Run full mode: fetch all acts."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = NationalAssemblyFetcher()
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
    parser = argparse.ArgumentParser(description="BZ/NationalAssembly - Belize National Assembly Acts Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
