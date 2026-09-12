#!/usr/bin/env python3
"""
BT/OAG - Bhutan Office of Attorney General Acts

Fetches Acts of Bhutan from the OAG website. ~120 acts available as PDFs
spanning 1953-2024, including the Constitution, Penal Code, etc.
English versions are prioritized; Dzongkha-only acts are included if no
English version exists.

Data source: https://oag.gov.bt/language/en/resources/acts-2/
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

BASE_URL = "https://oag.gov.bt"
ACTS_PAGE = f"{BASE_URL}/language/en/resources/acts-2/"
SOURCE_ID = "BT/OAG"
SAMPLE_DIR = Path(__file__).parent / "sample"


class OAGFetcher:
    """Fetcher for Bhutan Office of Attorney General Acts."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/pdf,*/*',
        })

    def fetch_act_list(self) -> List[Dict[str, str]]:
        """Fetch and parse the acts listing page for PDF links.

        HTML structure: ActTitle (<a href="...pdf">English</a>/<a href="...pdf">Dzongkha</a>)<br/>
        We extract act titles from surrounding text and associate them with English PDF links.
        """
        resp = self.session.get(ACTS_PAGE, timeout=30)
        resp.raise_for_status()

        acts = []
        # Pattern: "Act Title (links)" separated by <br /> tags
        # Split content into segments between <br> tags
        # Each segment has format: Title (<a href="url">Lang</a>/...)
        segments = re.split(r'<br\s*/?>|\n', resp.text)

        for segment in segments:
            # Skip segments without PDF links
            if '.pdf' not in segment:
                continue

            # Extract the title: text before the first "("  that contains a link
            # Strip any leading HTML tags
            clean_seg = re.sub(r'^[^A-Z]*', '', segment.strip())
            # Get title part (everything before the parenthetical with links)
            title_match = re.match(r'(.+?)\s*\(', clean_seg)
            if not title_match:
                continue
            title = html.unescape(title_match.group(1)).strip()
            # Remove any stray HTML
            title = re.sub(r'<[^>]+>', '', title).strip()
            if not title or len(title) < 5:
                continue

            # Find all PDF links in this segment
            pdf_links = re.findall(
                r'<a[^>]*href="([^"]*\.pdf)"[^>]*>([^<]*)</a>',
                segment, re.IGNORECASE
            )
            if not pdf_links:
                continue

            # Prefer English link; fall back to "English & Dzongkha" or first link
            chosen_url = None
            for url, lang in pdf_links:
                lang_lower = lang.strip().lower()
                if 'english' in lang_lower or 'eng' in lang_lower:
                    chosen_url = url
                    break
            if not chosen_url:
                # Take "both" or first available
                chosen_url = pdf_links[0][0]

            # Normalize URL
            if not chosen_url.startswith("http"):
                chosen_url = BASE_URL + chosen_url
            # Fix double slashes in path
            chosen_url = re.sub(r'(?<!:)//', '/', chosen_url)

            acts.append({"url": chosen_url, "title": title})

        logger.info(f"Found {len(acts)} acts on page")
        return acts

    def extract_year(self, title: str) -> Optional[int]:
        """Try to extract a year from the act title."""
        # Look for 4-digit years in common positions
        years = re.findall(r'\b(19[5-9]\d|20[0-2]\d)\b', title)
        if years:
            return int(years[-1])  # Last year mentioned (usually the act year)
        return None

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
                # Clean up excessive whitespace
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
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None
            return self.extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

    def normalize(self, act: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize an act record into the standard schema."""
        doc_id = hashlib.sha256(act["url"].encode()).hexdigest()[:16]
        year = self.extract_year(act["title"])
        date = f"{year}-01-01" if year else None

        return {
            "_id": f"BT-OAG-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": act["title"],
            "text": text,
            "date": date,
            "url": act["url"],
            "year": year,
            "country": "BT",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all acts with full text."""
        acts = self.fetch_act_list()

        for i, act in enumerate(acts):
            logger.info(f"[{i+1}/{len(acts)}] Downloading: {act['title']}")
            time.sleep(1.5)
            text = self.download_and_extract(act["url"])
            if text:
                yield self.normalize(act, text)
            else:
                logger.warning(f"  No text extracted for: {act['title']}")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of records for testing."""
        acts = self.fetch_act_list()

        # Pick a diverse sample: beginning, middle, end of list
        indices = []
        step = max(1, len(acts) // max_records)
        for i in range(0, len(acts), step):
            indices.append(i)
            if len(indices) >= max_records + 5:  # Extra to handle failures
                break

        records = []
        for idx in indices:
            if len(records) >= max_records:
                break
            act = acts[idx]
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
    fetcher = OAGFetcher()
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
    fetcher = OAGFetcher()
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
    parser = argparse.ArgumentParser(description="BT/OAG - Bhutan OAG Acts Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
