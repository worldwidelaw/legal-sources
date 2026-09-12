#!/usr/bin/env python3
"""
BZ/AGMLaws - Belize Consolidated Laws (Attorney General's Ministry)

Fetches legislation from the Attorney General's Ministry of Belize:
  - Substantive Laws (Revised Edition 2020) across 18 volumes
  - Subsidiary Laws (R.E. 2020)
  - Annual Acts (2021-2026)
  - Statutory Instruments (2021-2026)

Data source: https://www.agm.gov.bz/laws/
API: POST /api-laws/ with action + volume parameters, returns JSON list of PDF links
License: Public domain (official government legislation)
"""

import argparse
import hashlib
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import unquote

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.agm.gov.bz"
SOURCE_ID = "BZ/AGMLaws"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Categories: (action, volume, category_label)
SUBSTANTIVE_VOLUMES = [(1000, str(v), f"Substantive Laws Vol. {v}") for v in range(1, 19)]
SUBSIDIARY = [(1001, "law_subsidiary", "Subsidiary Laws (R.E. 2020)")]
ACTS_YEARS = [(1001, f"{y}_act", f"Acts {y}") for y in range(2021, 2027)]
SI_YEARS = [(1001, f"{y}_SI", f"Statutory Instruments {y}") for y in range(2021, 2027)]

ALL_CATEGORIES = SUBSTANTIVE_VOLUMES + SUBSIDIARY + ACTS_YEARS + SI_YEARS


class AGMLawsFetcher:
    """Fetcher for Belize Attorney General's Ministry legislation."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json, text/html, */*',
            'Content-Type': 'application/x-www-form-urlencoded',
        })

    def fetch_category(self, action: int, volume: str) -> List[Dict[str, str]]:
        """Fetch the list of laws for a given category/volume."""
        url = f"{BASE_URL}/api-laws/"
        try:
            resp = self.session.post(url, data={"action": action, "volume": volume}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("rc")) != "1":
                return []
            results = []
            for item in data.get("data", []):
                html = item[0] if isinstance(item, list) else item
                match = re.search(r'href="([^"]+)"[^>]*>([^<]+)', html)
                if match:
                    pdf_path = match.group(1)
                    title = match.group(2).strip()
                    pdf_url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
                    results.append({"url": pdf_url, "title": title})
            return results
        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.warning(f"Failed to fetch category action={action} volume={volume}: {e}")
            return []

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
            content_type = resp.headers.get('Content-Type', '')
            if 'pdf' not in content_type and not pdf_url.endswith('.pdf'):
                logger.warning(f"Not a PDF: {pdf_url} (Content-Type: {content_type})")
                return None
            return self.extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

    def parse_metadata(self, title: str, pdf_url: str, category: str) -> Dict[str, Any]:
        """Extract structured metadata from title and URL."""
        # Extract chapter number from title (e.g., "Cap 01", "CAP 106.01")
        cap_match = re.search(r'(?:Cap|CAP)\s*(\d+(?:\.\d+)?)', title)
        chapter = cap_match.group(1) if cap_match else None

        # Extract Act number and year (e.g., "Act No. 24 of 2022")
        act_match = re.search(r'Act\s*No\.?\s*(\d+)\s*of\s*(\d{4})', title)
        act_number = act_match.group(1) if act_match else None
        act_year = act_match.group(2) if act_match else None

        # Extract SI number (e.g., "SI NO. 1 of 2026")
        si_match = re.search(r'SI\s*(?:NO\.?)?\s*(\d+)\s*of\s*(\d{4})', title)
        si_number = si_match.group(1) if si_match else None
        si_year = si_match.group(2) if si_match else None

        # Determine date
        date = None
        if act_year:
            date = f"{act_year}-01-01"
        elif si_year:
            date = f"{si_year}-01-01"
        elif "R.E. 2020" in category or "Substantive" in category:
            date = "2020-12-31"

        # Determine data subtype
        if "Substantive" in category or "Subsidiary" in category:
            doc_type = "consolidated_law"
        elif "Acts" in category:
            doc_type = "act"
        else:
            doc_type = "statutory_instrument"

        return {
            "chapter": chapter,
            "act_number": act_number,
            "si_number": si_number,
            "year": act_year or si_year,
            "date": date,
            "doc_type": doc_type,
            "category": category,
        }

    def normalize(self, title: str, pdf_url: str, text: str, category: str) -> Dict[str, Any]:
        """Normalize a law record into the standard schema."""
        meta = self.parse_metadata(title, pdf_url, category)
        doc_id = hashlib.sha256(pdf_url.encode()).hexdigest()[:16]

        return {
            "_id": f"BZ-AGM-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": meta["date"],
            "url": pdf_url,
            "chapter": meta["chapter"],
            "act_number": meta["act_number"],
            "si_number": meta["si_number"],
            "doc_type": meta["doc_type"],
            "category": meta["category"],
            "country": "BZ",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all documents with full text."""
        for action, volume, category in ALL_CATEGORIES:
            logger.info(f"Fetching category: {category}")
            laws = self.fetch_category(action, volume)
            logger.info(f"  Found {len(laws)} laws in {category}")
            for law in laws:
                time.sleep(1.5)  # Rate limit
                text = self.download_and_extract(law["url"])
                if text:
                    yield self.normalize(law["title"], law["url"], text, category)
                else:
                    logger.warning(f"  No text extracted for: {law['title']}")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of records for testing."""
        records = []
        # Sample from different categories for diversity
        sample_categories = [
            (1000, "1", "Substantive Laws Vol. 1"),       # 3 from vol 1
            (1000, "5", "Substantive Laws Vol. 5"),       # 3 from vol 5
            (1001, "law_subsidiary", "Subsidiary Laws (R.E. 2020)"),  # 3 subsidiary
            (1001, "2023_act", "Acts 2023"),              # 3 acts
            (1001, "2023_SI", "Statutory Instruments 2023"),  # 3 SI
        ]

        for action, volume, category in sample_categories:
            if len(records) >= max_records:
                break
            laws = self.fetch_category(action, volume)
            for law in laws[:3]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                text = self.download_and_extract(law["url"])
                if text:
                    record = self.normalize(law["title"], law["url"], text, category)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {law['title']} ({len(text)} chars)")
                else:
                    logger.warning(f"  Skipped (no text): {law['title']}")

        return records


def bootstrap_sample():
    """Run sample mode: fetch ~15 diverse records and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = AGMLawsFetcher()
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
    """Run full mode: fetch all laws."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = AGMLawsFetcher()
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
    parser = argparse.ArgumentParser(description="BZ/AGMLaws - Belize Consolidated Laws Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
