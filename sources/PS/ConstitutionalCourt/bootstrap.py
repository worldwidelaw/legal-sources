#!/usr/bin/env python3
"""
PS/ConstitutionalCourt - Palestinian Supreme Constitutional Court

Fetches decisions and provisions from the Palestinian Supreme Constitutional
Court (المحكمة الدستورية العليا). Three categories: interpretive decisions
(court_decisions, ~41), constitutional provisions (court_provisions, ~128),
and competence disputes (competence_disputes, ~8).
All documents are in Arabic as PDFs with extractable text.

Data source: https://www.tscc.pna.ps
License: Public domain (official government court decisions)
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

import pypdf
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.tscc.pna.ps"
SOURCE_ID = "PS/ConstitutionalCourt"
SAMPLE_DIR = Path(__file__).parent / "sample"

PAGES = [
    {"id": "court_decisions", "category": "interpretive_decision"},
    {"id": "court_provisions", "category": "constitutional_provision"},
    {"id": "competence_disputes", "category": "competence_dispute"},
]


class ConstitutionalCourtFetcher:
    """Fetcher for Palestinian Supreme Constitutional Court decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/pdf,*/*',
        })
        self.session.verify = False

    def fetch_entries(self, page_id: str, category: str) -> List[Dict[str, str]]:
        """Parse accordion entries from a page to get titles, subjects, and PDF links."""
        url = f"{BASE_URL}/pages?id={page_id}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        entries = []
        # Pattern: <button>TITLE</button> ... <p>SUBJECT</p> ... <a href="PDF_URL">
        matches = re.findall(
            r'<button[^>]*>(.*?)</button>.*?<p[^>]*>(.*?)</p>.*?href="(https://www\.tscc\.pna\.ps/_upload/_documents/court_[^"]+\.pdf)"',
            resp.text, re.DOTALL
        )

        for raw_title, raw_subject, pdf_url in matches:
            title = re.sub(r'<[^>]+>', '', raw_title).strip()
            title = html.unescape(title).strip()
            subject = re.sub(r'<[^>]+>', '', raw_subject).strip()
            subject = html.unescape(subject).strip()

            # Skip modal popup entries (× close buttons)
            if title in ('×', '&times;', '') or len(title) < 3:
                continue

            # Clean up quotes and whitespace
            title = re.sub(r'^["\s"]+|["\s"]+$', '', title)
            subject = re.sub(r'^الموضوع:\s*', '', subject)

            entries.append({
                "title": title,
                "subject": subject,
                "url": pdf_url,
                "category": category,
                "page_id": page_id,
            })

        logger.info(f"Found {len(entries)} entries on {page_id}")
        return entries

    def extract_year_number(self, title: str) -> Dict[str, Optional[str]]:
        """Extract year and case number from Arabic title."""
        # Patterns like: رقم (2024/1) or رقم (2024/6)
        year_match = re.search(r'(\d{4})/(\d+)', title)
        if year_match:
            return {"year": year_match.group(1), "number": year_match.group(2)}
        # Pattern: رقم (08) لسنة (7) قضائية
        num_match = re.search(r'رقم\s*\((\d+)\)', title)
        year_match2 = re.search(r'لسنة\s*\((\d+)\)', title)
        if num_match:
            return {
                "year": year_match2.group(1) if year_match2 else None,
                "number": num_match.group(1),
            }
        return {"year": None, "number": None}

    def extract_pdf_text(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF bytes using pypdf (correct Arabic RTL handling)."""
        try:
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            full_text = "\n\n".join(pages_text)
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
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
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes)")
                return None
            return self.extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"Download failed: {e}")
            return None

    def normalize(self, entry: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize a court entry into the standard schema."""
        doc_id = hashlib.sha256(entry["url"].encode()).hexdigest()[:16]
        meta = self.extract_year_number(entry["title"])
        year = meta.get("year")
        date = f"{year}-01-01" if year and len(str(year)) == 4 else None

        return {
            "_id": f"PS-TSCC-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": entry["title"],
            "subject": entry.get("subject", ""),
            "text": text,
            "date": date,
            "url": entry["url"],
            "year": int(year) if year and year.isdigit() and len(year) == 4 else None,
            "case_number": meta.get("number"),
            "category": entry["category"],
            "country": "PS",
            "language": "ar",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all decisions and provisions with full text."""
        for page in PAGES:
            entries = self.fetch_entries(page["id"], page["category"])
            for i, entry in enumerate(entries):
                logger.info(f"[{i+1}/{len(entries)}] {page['id']}: {entry['title'][:60]}")
                time.sleep(1.5)
                text = self.download_and_extract(entry["url"])
                if text:
                    yield self.normalize(entry, text)
                else:
                    logger.warning(f"  No text: {entry['title'][:60]}")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample from all decision categories."""
        records = []
        per_page = {
            "court_decisions": 5,
            "court_provisions": 7,
            "competence_disputes": 3,
        }
        for page in PAGES:
            entries = self.fetch_entries(page["id"], page["category"])
            n = min(per_page.get(page["id"], 5), len(entries))
            step = max(1, len(entries) // n)
            sample_indices = [i * step for i in range(n)]

            for idx in sample_indices:
                if len(records) >= max_records:
                    break
                if idx >= len(entries):
                    continue
                entry = entries[idx]
                time.sleep(1.5)
                text = self.download_and_extract(entry["url"])
                if text:
                    record = self.normalize(entry, text)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {entry['title'][:50]} ({len(text)} chars)")
                else:
                    logger.warning(f"  Skipped: {entry['title'][:50]}")

        return records


def bootstrap_sample():
    """Run sample mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = ConstitutionalCourtFetcher()
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
    fetcher = ConstitutionalCourtFetcher()
    count = 0
    for record in fetcher.fetch_all():
        outfile = SAMPLE_DIR / f"record_{count+1:04d}.json"
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        if count % 20 == 0:
            logger.info(f"Progress: {count} records saved")
    logger.info(f"Complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="PS/ConstitutionalCourt Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
