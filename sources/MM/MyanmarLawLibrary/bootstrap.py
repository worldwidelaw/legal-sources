#!/usr/bin/env python3
"""
MM/MyanmarLawLibrary - Myanmar Law Library

Fetches Myanmar laws from myanmar-law-library.org. Laws are stored as PDFs
embedded in HTML pages. Text extracted via pypdf (Burmese Unicode).
Coverage: 1988-present (post-SLORC era).

Data source: http://www.myanmar-law-library.org/
License: Open government data (official legislation)
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
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import pypdf
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "http://www.myanmar-law-library.org/"
SOURCE_ID = "MM/MyanmarLawLibrary"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Year pages organized by government era
YEAR_PAGES = [
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/national-league-for-democracy-2016/myanmar-laws-2020/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/national-league-for-democracy-2016/myanmar-laws-2019/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/national-league-for-democracy-2016/myanmar-laws-2018/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/national-league-for-democracy-2016/myanmar-laws-2017/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/national-league-for-democracy-2016/myanmar-laws-2016/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/state-administration-council-2021/myanmar-laws-2021/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/state-administration-council-2021/myanmar-laws-2022/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/union-solidarity-and-development-party-laws-2012-2016/myanmar-laws-2015/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/union-solidarity-and-development-party-laws-2012-2016/myanmar-laws-2014/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/union-solidarity-and-development-party-laws-2012-2016/myanmar-laws-2013/",
    "law-library/laws-and-regulations/laws/myanmar-laws-1988-until-now/union-solidarity-and-development-party-laws-2012-2016/myanmar-laws-2012/",
]


class MyanmarLawFetcher:
    """Fetcher for Myanmar Law Library."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })

    def get_law_pages(self, year_page_url: str) -> List[Dict[str, str]]:
        """Get individual law page URLs from a year index page."""
        url = urljoin(BASE_URL, year_page_url)
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return []

        # Extract .html law page links
        pattern = re.compile(
            rf'href="({re.escape(year_page_url)}[^"]+\.html)"'
        )
        matches = pattern.findall(resp.text)

        # Also try relative links
        rel_pattern = re.compile(r'href="([^"]+\.html)"')
        rel_matches = rel_pattern.findall(resp.text)

        pages = []
        seen = set()
        for href in rel_matches:
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = urljoin(BASE_URL, href)
            else:
                # Root-relative paths (no leading /) must join with BASE_URL
                full_url = urljoin(BASE_URL, href)

            # Only include law pages from this year directory
            if year_page_url.rstrip('/').split('/')[-1] in full_url and full_url not in seen:
                seen.add(full_url)
                # Extract title from URL
                slug = full_url.rstrip('/').split('/')[-1].replace('.html', '')
                title = slug.replace('-', ' ').title()
                pages.append({"url": full_url, "title": title})

        logger.info(f"Found {len(pages)} law pages in {year_page_url.split('/')[-2]}")
        return pages

    def extract_pdf_url(self, page_url: str) -> Optional[str]:
        """Extract PDF URL from a law HTML page."""
        try:
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch page {page_url}: {e}")
            return None

        # Look for PDF in iframe or direct link
        # Pattern: src="IMG/pdf/filename.pdf" or href="...pdf"
        pdf_match = re.search(r'(?:src|href)="([^"]*\.pdf)"', resp.text)
        if pdf_match:
            pdf_path = pdf_match.group(1)
            if pdf_path.startswith('http'):
                return pdf_path
            # PDF paths like "IMG/pdf/file.pdf" are relative to site root
            return urljoin(BASE_URL, pdf_path)

        return None

    def extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
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
            return full_text if len(full_text) > 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def extract_year_from_url(self, url: str) -> Optional[str]:
        """Extract year from URL path (last myanmar-laws-YYYY match)."""
        matches = re.findall(r'myanmar-laws-(\d{4})', url)
        if matches:
            return matches[-1]
        match = re.search(r'(\d{4})', url.split('/')[-1])
        if match:
            return match.group(1)
        return None

    def extract_law_number(self, title: str) -> Optional[str]:
        """Extract law number from title."""
        match = re.search(r'no[.-]?\s*(\d+[-/]\d+)', title, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r'no[.-]?\s*(\d+)', title, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def normalize(self, entry: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize a law entry into standard schema."""
        doc_id = hashlib.sha256(entry["url"].encode()).hexdigest()[:16]
        year = self.extract_year_from_url(entry["url"])
        law_num = self.extract_law_number(entry["title"])
        date = f"{year}-01-01" if year else None

        return {
            "_id": f"MM-MLL-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": entry["title"],
            "text": text,
            "date": date,
            "url": entry["url"],
            "year": int(year) if year else None,
            "law_number": law_num,
            "country": "MM",
            "language": "my",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all laws with full text."""
        for year_page in YEAR_PAGES:
            pages = self.get_law_pages(year_page)
            for i, entry in enumerate(pages):
                logger.info(f"  [{i+1}/{len(pages)}] {entry['title'][:50]}")
                time.sleep(1.5)
                pdf_url = self.extract_pdf_url(entry["url"])
                if not pdf_url:
                    logger.warning(f"    No PDF found")
                    continue
                time.sleep(1.0)
                text = self.extract_pdf_text(pdf_url)
                if text:
                    yield self.normalize(entry, text)
                else:
                    logger.warning(f"    No text extracted")

    def fetch_sample(self, max_records: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample across years."""
        records = []
        # Sample from different year pages
        sample_pages = YEAR_PAGES[:5]  # 2020, 2019, 2018, 2017, 2016

        for year_page in sample_pages:
            if len(records) >= max_records:
                break
            pages = self.get_law_pages(year_page)
            # Take first 3 from each year
            for entry in pages[:3]:
                if len(records) >= max_records:
                    break
                time.sleep(1.5)
                pdf_url = self.extract_pdf_url(entry["url"])
                if not pdf_url:
                    logger.warning(f"  No PDF: {entry['title'][:50]}")
                    continue
                time.sleep(1.0)
                text = self.extract_pdf_text(pdf_url)
                if text:
                    record = self.normalize(entry, text)
                    records.append(record)
                    logger.info(f"  Sample {len(records)}/{max_records}: {entry['title'][:50]} ({len(text)} chars)")
                else:
                    logger.warning(f"  No text: {entry['title'][:50]}")

        return records


def bootstrap_sample():
    """Run sample mode."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = MyanmarLawFetcher()
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
    fetcher = MyanmarLawFetcher()
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
    parser = argparse.ArgumentParser(description="MM/MyanmarLawLibrary Fetcher")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample()
        else:
            bootstrap_full()
