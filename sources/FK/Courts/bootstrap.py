#!/usr/bin/env python3
"""
FK/Courts - Falkland Islands Court Decisions

Fetches court judgments from the Falkland Islands Courts & Tribunals Service.
Covers civil, criminal, and coroner decisions. All judgments are PDFs.

Data source: https://www.gov.fk/courts/judgments/
License: Open government data (official court decisions)
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
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pypdf
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.gov.fk/courts/judgments/"
SOURCE_ID = "FK/Courts"
SAMPLE_DIR = Path(__file__).parent / "sample"

CATEGORY_PAGES = [
    ("https://www.gov.fk/courts/judgments/civil/", "civil"),
    ("https://www.gov.fk/courts/judgments/criminal/", "criminal"),
    ("https://www.gov.fk/courts/judgments/coroner/", "coroner"),
]


class FalklandIslandsCourtsFetcher:
    """Fetcher for Falkland Islands court judgments."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })
        self._seen_urls = set()

    def get_judgments_from_page(self, page_url: str, category: str) -> List[Dict[str, str]]:
        """Parse a category page and extract judgment titles + PDF URLs."""
        try:
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {page_url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        entries = []

        # Find h3 headings (judgment titles) followed by PDF download links
        headings = soup.find_all('h3')
        for h3 in headings:
            title = h3.get_text(strip=True)
            if not title:
                continue
            # Skip the publishing policy heading
            if 'publishing policy' in title.lower():
                continue

            # Find the next PDF link after this heading
            pdf_link = None
            for sibling in h3.find_next_siblings():
                a_tag = sibling.find('a', href=re.compile(r'\.pdf', re.I))
                if a_tag:
                    pdf_link = a_tag['href']
                    break
                # Stop if we hit another heading
                if sibling.name and sibling.name.startswith('h'):
                    break

            if not pdf_link:
                # Check within the h3 itself or its parent
                a_tag = h3.find('a', href=re.compile(r'\.pdf', re.I))
                if a_tag:
                    pdf_link = a_tag['href']

            if pdf_link and pdf_link not in self._seen_urls:
                self._seen_urls.add(pdf_link)
                entries.append({
                    "url": pdf_link,
                    "title": unescape(title),
                    "category": category,
                })

        logger.info(f"Found {len(entries)} judgments from {category} page")
        return entries

    def download_pdf_text(self, url: str) -> Optional[str]:
        """Download PDF and extract text using pypdf."""
        try:
            resp = self.session.get(url, timeout=120)
            if resp.status_code != 200:
                return None
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type and resp.content[:4] != b'%PDF':
                return None
            if len(resp.content) < 500:
                return None

            reader = pypdf.PdfReader(io.BytesIO(resp.content))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)

            full_text = "\n\n".join(pages_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            return full_text if len(full_text) > 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def extract_year(self, title: str, url: str) -> Optional[int]:
        """Extract year from title or URL."""
        # Look for [YYYY] pattern in title (common in case citations)
        match = re.search(r'\[(\d{4})\]', title)
        if match:
            y = int(match.group(1))
            if 1800 <= y <= 2030:
                return y
        # Look for (YYYY) pattern
        match = re.search(r'\((\d{4})\)', title)
        if match:
            y = int(match.group(1))
            if 1800 <= y <= 2030:
                return y
        # Look for standalone year in URL filename
        filename = url.split('/')[-1]
        match = re.search(r'(\d{4})', filename)
        if match:
            y = int(match.group(1))
            if 1800 <= y <= 2030:
                return y
        return None

    def normalize(self, entry: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize into standard schema."""
        doc_id = hashlib.sha256(entry["url"].encode()).hexdigest()[:16]
        year = self.extract_year(entry["title"], entry["url"])
        date = f"{year}-01-01" if year else None

        return {
            "_id": f"FK-CT-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": entry["title"],
            "text": text,
            "date": date,
            "url": entry["url"],
            "category": entry["category"],
            "year": year,
            "country": "FK",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all judgments with full text."""
        for page_url, category in CATEGORY_PAGES:
            entries = self.get_judgments_from_page(page_url, category)
            for i, entry in enumerate(entries):
                logger.info(f"  [{category} {i+1}/{len(entries)}] {entry['title'][:60]}")
                time.sleep(1.5)
                text = self.download_pdf_text(entry["url"])
                if text:
                    yield self.normalize(entry, text)
                else:
                    logger.warning(f"    No text extracted")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample across all categories."""
        records = []
        per_category = max(max_records // 3, 4)

        for page_url, category in CATEGORY_PAGES:
            if len(records) >= max_records:
                break
            entries = self.get_judgments_from_page(page_url, category)
            for entry in entries[:per_category]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                text = self.download_pdf_text(entry["url"])
                if text:
                    record = self.normalize(entry, text)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {entry['title'][:50]} ({len(text)} chars)")
                else:
                    logger.warning(f"  No text for: {entry['title'][:50]}")

        return records


def bootstrap_sample():
    """Run sample mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = FalklandIslandsCourtsFetcher()
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
    """Run full mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = FalklandIslandsCourtsFetcher()
    count = 0
    for record in fetcher.fetch_all():
        outfile = SAMPLE_DIR / f"record_{count+1:04d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
    logger.info(f"Complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="FK/Courts Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
