#!/usr/bin/env python3
"""
UZ/ConstitutionalCourt — Uzbekistan Constitutional Court Decisions

Fetches decisions from https://www.konstsud.uz. Scrapes the decisions page
for PDF links, downloads, and extracts text via pypdf. Small corpus (~15-20
decisions since 2021). Uzbek language.
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
from typing import Dict, Iterator, List, Optional, Tuple
from html import unescape
from urllib.parse import urljoin

import pypdf
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.konstsud.uz"
SOURCE_ID = "UZ/ConstitutionalCourt"
SAMPLE_DIR = Path(__file__).parent / "sample"
MIN_TEXT_LENGTH = 200

DECISIONS_PAGES = [
    "/uz/docs/konstitutsiyaviy-sudi-qarorlari",
    "/ru/docs/konstitutsiyaviy-sudi-qarorlari",
]


class UzConstitutionalCourtFetcher:
    """Fetcher for Uzbekistan Constitutional Court decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _extract_pdf_links(self) -> List[Tuple[str, str]]:
        """Scrape decisions pages for PDF links. Returns [(url, context_text)]."""
        pdf_links = {}
        for page_path in DECISIONS_PAGES:
            url = BASE_URL + page_path
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                html = resp.text

                # Find PDF links with surrounding text for context
                for m in re.finditer(
                    r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
                    html, re.DOTALL | re.IGNORECASE
                ):
                    href = m.group(1)
                    context = re.sub(r'<[^>]+>', '', m.group(2)).strip()
                    # Skip local Windows paths
                    if href.startswith("C:") or href.startswith("file:"):
                        continue
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in pdf_links:
                        pdf_links[full_url] = context

                # Also find PDF links in list items with text before/after
                for m in re.finditer(
                    r'<li[^>]*>(.*?)</li>', html, re.DOTALL | re.IGNORECASE
                ):
                    li_html = m.group(1)
                    pdf_m = re.search(r'href="([^"]*\.pdf)"', li_html, re.I)
                    if pdf_m:
                        href = pdf_m.group(1)
                        if href.startswith("C:") or href.startswith("file:"):
                            continue
                        full_url = urljoin(BASE_URL, href)
                        context = re.sub(r'<[^>]+>', ' ', li_html).strip()
                        context = re.sub(r'\s+', ' ', context)
                        if full_url not in pdf_links or len(context) > len(pdf_links[full_url]):
                            pdf_links[full_url] = context

                time.sleep(1)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")

        return list(pdf_links.items())

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download and extract text from a PDF."""
        try:
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()
            reader = pypdf.PdfReader(io.BytesIO(resp.content))
            pages_text = []
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    pages_text.append(t.strip())
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"Failed to extract PDF {pdf_url}: {e}")
            return ""

    def _extract_date_from_url(self, url: str) -> Optional[str]:
        """Try to extract a date from the PDF URL path (e.g., /uploads/2024/07/...)."""
        m = re.search(r'/uploads/(\d{4})/(\d{2})/', url)
        if m:
            return f"{m.group(1)}-{m.group(2)}-01"
        return None

    def _make_id(self, url: str) -> str:
        """Generate a stable ID from the PDF URL."""
        # Use the filename without extension
        filename = url.rstrip('/').split('/')[-1]
        return re.sub(r'\.pdf$', '', filename, flags=re.I)

    def normalize(self, pdf_url: str, context: str, text: str) -> Optional[Dict]:
        """Normalize a downloaded decision into a standard record."""
        if len(text) < MIN_TEXT_LENGTH:
            return None

        doc_id = self._make_id(pdf_url)
        date = self._extract_date_from_url(pdf_url)

        # Try to extract title from first line of text or context
        title = context if context else ""
        if not title:
            first_lines = text[:500].split('\n')
            title = ' '.join(first_lines[:3]).strip()
        title = unescape(title)
        title = re.sub(r'\s+', ' ', title)[:300]

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "language": "uz",
        }

    def fetch_all(self) -> Iterator[Dict]:
        """Yield all normalized decisions."""
        pdf_links = self._extract_pdf_links()
        logger.info(f"Found {len(pdf_links)} PDF links")

        total = 0
        for pdf_url, context in pdf_links:
            logger.info(f"Downloading {pdf_url}...")
            text = self._extract_pdf_text(pdf_url)
            if not text or len(text) < MIN_TEXT_LENGTH:
                logger.warning(f"Skipping {pdf_url}: insufficient text ({len(text)} chars)")
                continue

            record = self.normalize(pdf_url, context, text)
            if record:
                total += 1
                yield record
            time.sleep(1)

        logger.info(f"Total records fetched: {total}")


def bootstrap_sample(max_records: int = 20):
    """Fetch sample records and save to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = UzConstitutionalCourtFetcher()
    count = 0
    for record in fetcher.fetch_all():
        if count >= max_records:
            break
        out_path = SAMPLE_DIR / f"{record['_id']}.json"
        out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Saved {out_path.name}: {record['title'][:80]}")
        count += 1
    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def bootstrap_full():
    """Fetch all records (same as sample for this small corpus)."""
    return bootstrap_sample(max_records=999)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="UZ/ConstitutionalCourt bootstrap")
    parser.add_argument("action", choices=["bootstrap", "bootstrap-full"],
                        help="bootstrap = sample, bootstrap-full = all")
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--max", type=int, default=20, help="Max sample records")
    args = parser.parse_args()

    if args.action == "bootstrap" or args.sample:
        bootstrap_sample(args.max)
    else:
        bootstrap_full()
