#!/usr/bin/env python3
"""
BZ/CourtOfAppeal - Belize Court of Appeal Judgments

Fetches Court of Appeal judgments (Criminal and Civil Appeals) from the
Judiciary of Belize website. Judgments span 1977-2022 and are available
as PDFs on the official judiciary.bz WordPress site.

Data source: https://judiciary.bz/judgements3/
License: Public domain (official government court decisions)
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

import pdfplumber
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://judiciary.bz"
SOURCE_ID = "BZ/CourtOfAppeal"
SAMPLE_DIR = Path(__file__).parent / "sample"


class CourtOfAppealFetcher:
    """Fetcher for Belize Court of Appeal judgments."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,*/*',
        })

    def get_year_pages(self) -> List[Tuple[str, str, str]]:
        """Parse the main judgements3 page to get all year page URLs.

        Returns list of (url, year_label, appeal_type) tuples.
        The page has two sections: Criminal Appeals (first set) and
        Civil Appeals (second set). We identify them by duplicate years.
        """
        url = f"{BASE_URL}/judgements3/"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch main page: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Collect all year links with real URLs (not '#')
        all_links = soup.find_all('a', href=True)
        year_links = []
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            if not re.match(r'^\d{4}$', text):
                continue
            if href == '#' or href.endswith('#'):
                continue

            if href.startswith('/'):
                full_url = f"{BASE_URL}{href}"
            elif href.startswith('http'):
                full_url = href
            else:
                full_url = f"{BASE_URL}/{href}"

            year_links.append((full_url, text))

        # Split into criminal (first half) and civil (second half)
        # The page lists Criminal first, then Civil. We detect the split
        # by finding where a year repeats (e.g., 2022 appears twice).
        seen_years = set()
        split_idx = len(year_links)
        for i, (_, year) in enumerate(year_links):
            if year in seen_years:
                split_idx = i
                break
            seen_years.add(year)

        year_pages = []
        for url, year in year_links[:split_idx]:
            year_pages.append((url, year, "criminal"))
        for url, year in year_links[split_idx:]:
            year_pages.append((url, year, "civil"))

        logger.info(f"Found {len(year_pages)} year pages ({split_idx} criminal, {len(year_links) - split_idx} civil)")
        return year_pages

    def get_judgments_from_page(self, page_url: str) -> List[Dict[str, str]]:
        """Extract all judgment PDF links from a year page."""
        try:
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {page_url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        judgments = []
        seen_urls = set()

        # Find all PDF links (only wp-content links are still live)
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '.pdf' not in href.lower():
                continue
            # Skip old dead links (judiciary.bz/supreme_court/ etc.)
            if 'wp-content' not in href and 'belizelaw.org' not in href:
                continue

            # Normalize URL
            if href.startswith('/'):
                pdf_url = f"https://www.judiciary.bz{href}"
            elif href.startswith('http'):
                # Upgrade http to https
                pdf_url = href.replace('http://', 'https://')
            else:
                pdf_url = f"https://www.judiciary.bz/{href}"

            # Deduplicate
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            # Get title from link text or filename
            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                # Extract from filename
                filename = pdf_url.split('/')[-1]
                title = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
                title = re.sub(r'[-_]+', ' ', title)

            judgments.append({"url": pdf_url, "title": title})

        return judgments

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

    def parse_case_metadata(self, title: str, pdf_url: str, appeal_type: str, year_label: str) -> Dict[str, Any]:
        """Extract structured metadata from case title."""
        # Extract appeal number and year
        appeal_match = re.search(
            r'(?:Criminal|Civil)\s*Appeal\s*(?:No\.?)\s*(\d+)\s*(?:of\s*)?(\d{4})',
            title, re.IGNORECASE
        )
        appeal_number = appeal_match.group(1) if appeal_match else None
        case_year = appeal_match.group(2) if appeal_match else None

        # Also try: "Indictment No. C7 of 2016"
        if not appeal_match:
            indict_match = re.search(r'(?:Indictment|Application)\s*(?:No\.?)\s*\w*\s*(?:of\s*)?(\d{4})', title, re.IGNORECASE)
            if indict_match:
                case_year = indict_match.group(1)

        # Extract parties
        parties_match = re.search(r'[-–—]\s*(.+)', title)
        parties = parties_match.group(1).strip() if parties_match else None
        if not parties:
            # Try "v" pattern in title
            v_match = re.search(r'(.+?)\s+v\s+(.+)', title, re.IGNORECASE)
            if v_match:
                parties = f"{v_match.group(1).strip()} v {v_match.group(2).strip()}"

        # Determine date from year label or case year
        date = f"{year_label}-01-01" if year_label else None
        if case_year:
            date = f"{case_year}-01-01"

        return {
            "appeal_number": appeal_number,
            "case_year": case_year,
            "parties": parties,
            "date": date,
            "appeal_type": appeal_type,
        }

    def normalize(self, title: str, pdf_url: str, text: str, appeal_type: str, year_label: str) -> Dict[str, Any]:
        """Normalize a judgment record into the standard schema."""
        # Clean up filename-derived titles (replace dashes with spaces)
        if re.match(r'^[\w]+-[\w]+-', title) and ' ' not in title:
            title = re.sub(r'-+', ' ', title)

        meta = self.parse_case_metadata(title, pdf_url, appeal_type, year_label)
        doc_id = hashlib.sha256(pdf_url.encode()).hexdigest()[:16]

        return {
            "_id": f"BZ-COA-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": meta["date"],
            "url": pdf_url,
            "appeal_number": meta["appeal_number"],
            "appeal_type": meta["appeal_type"],
            "parties": meta["parties"],
            "court": "Court of Appeal",
            "country": "BZ",
            "language": "en",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all judgments with full text."""
        year_pages = self.get_year_pages()
        for page_url, year_label, appeal_type in year_pages:
            logger.info(f"Fetching {appeal_type} appeals {year_label}: {page_url}")
            judgments = self.get_judgments_from_page(page_url)
            logger.info(f"  Found {len(judgments)} judgments")
            for j in judgments:
                time.sleep(1.5)
                text = self.download_and_extract(j["url"])
                if text:
                    yield self.normalize(j["title"], j["url"], text, appeal_type, year_label)
                else:
                    logger.warning(f"  No text extracted: {j['title']}")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of records for testing."""
        records = []
        year_pages = self.get_year_pages()

        if not year_pages:
            logger.error("No year pages found")
            return []

        # Sample from different years and types for diversity
        # Skip very recent (often empty) and very old (often dead links)
        criminal_pages = [(u, y, t) for u, y, t in year_pages if t == "criminal"]
        civil_pages = [(u, y, t) for u, y, t in year_pages if t == "civil"]

        # Pick pages likely to have content (2015-2020 range is reliable)
        sample_pages = []
        for pages in [criminal_pages, civil_pages]:
            for p in pages:
                if p[1] in ("2017", "2016", "2015", "2020", "2014"):
                    sample_pages.append(p)
                    if len(sample_pages) >= 4:
                        break
            if len(sample_pages) >= 4:
                break

        for page_url, year_label, appeal_type in sample_pages:
            if len(records) >= max_records:
                break
            logger.info(f"Sampling from {appeal_type} {year_label}: {page_url}")
            judgments = self.get_judgments_from_page(page_url)
            logger.info(f"  Found {len(judgments)} judgments on page")

            for j in judgments[:5]:  # Up to 5 from each page
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                text = self.download_and_extract(j["url"])
                if text:
                    record = self.normalize(j["title"], j["url"], text, appeal_type, year_label)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {j['title'][:60]}... ({len(text)} chars)")
                else:
                    logger.warning(f"  Skipped (no text): {j['title']}")

        return records


def bootstrap_sample():
    """Run sample mode: fetch ~15 diverse records and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = CourtOfAppealFetcher()
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
    """Run full mode: fetch all judgments."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = CourtOfAppealFetcher()
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
    parser = argparse.ArgumentParser(description="BZ/CourtOfAppeal - Belize Court of Appeal Judgments")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Run full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
