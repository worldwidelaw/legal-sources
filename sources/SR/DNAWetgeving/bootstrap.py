#!/usr/bin/env python3
"""
SR/DNAWetgeving -- Suriname Parliament Legislation (DNA)

Fetches ~320 Suriname laws from dna.sr (De Nationale Assemblee).
Pre-2005 consolidated texts (~186) have text-extractable PDFs.
Post-2005 laws (~137) are mostly scanned PDFs; text extraction attempted.

Strategy:
  - Scrape index pages for law links (geldende-teksten-t-m-2005, wetten-na-2005)
  - Fetch each law page for PDF URL
  - Download PDF and extract text via pypdf

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
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SR.DNAWetgeving")

BASE_URL = "https://www.dna.sr"

DUTCH_MONTHS = {
    "januari": 1, "februari": 2, "maart": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "augustus": 8,
    "september": 9, "oktober": 10, "november": 11, "december": 12,
}

INDEX_PAGES = [
    (
        f"{BASE_URL}/wetgeving/surinaamse-wetten/geldende-teksten-t-m-2005/",
        "geldende-teksten-t-m-2005",
    ),
    (
        f"{BASE_URL}/wetgeving/surinaamse-wetten/wetten-na-2005/",
        "wetten-na-2005",
    ),
]

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None
    logger.warning("pypdf not installed; PDF text extraction unavailable")


class DNAWetgevingScraper(BaseScraper):
    """Scraper for SR/DNAWetgeving -- Suriname Parliament Laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
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

    def _get_law_links(self, index_url: str, section: str) -> List[Tuple[str, str, str]]:
        """Scrape index page for (title, law_page_url, section) tuples."""
        from html.parser import HTMLParser

        resp = self._request(index_url)
        if resp is None:
            return []

        class LinkExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.links = []
                self.current_href = None
                self.current_text = []

            def handle_starttag(self, tag, attrs):
                if tag == "a":
                    attrs_d = dict(attrs)
                    href = attrs_d.get("href", "")
                    if f"/{section}/" in href and href != f"/wetgeving/surinaamse-wetten/{section}/" and href.count("/") > 4:
                        self.current_href = href
                        self.current_text = []

            def handle_data(self, data):
                if self.current_href is not None:
                    self.current_text.append(data)

            def handle_endtag(self, tag):
                if tag == "a" and self.current_href is not None:
                    title = "".join(self.current_text).strip()
                    if title:
                        full_url = BASE_URL + self.current_href if self.current_href.startswith("/") else self.current_href
                        self.links.append((title, full_url, section))
                    self.current_href = None

        parser = LinkExtractor()
        parser.feed(resp.text)
        return parser.links

    def _get_pdf_url(self, law_page_url: str) -> Optional[str]:
        """Fetch a law page and extract the PDF download URL."""
        resp = self._request(law_page_url)
        if resp is None:
            return None

        # Look for PDF link in HTML
        m = re.search(r'href="([^"]*\.pdf[^"]*)"', resp.text, re.IGNORECASE)
        if m:
            pdf_path = m.group(1)
            if pdf_path.startswith("/"):
                return BASE_URL + pdf_path
            return pdf_path
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pypdf."""
        if PdfReader is None:
            return ""
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages_text = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            full_text = "\n\n".join(pages_text).strip()
            # Clean up common artifacts
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)
            return full_text
        except Exception as e:
            logger.warning(f"PDF extraction error: {e}")
            return ""

    def _extract_year(self, title: str) -> Optional[str]:
        """Try to extract a year from the law title."""
        m = re.search(r"(\d{4})", title)
        if m:
            year = int(m.group(1))
            if 1800 <= year <= 2030:
                return m.group(1)
        return None

    def _extract_date(self, text: str, title: str) -> Optional[str]:
        """Best available enactment date, in ISO 8601.

        Surinamese instruments open by dating themselves -- "WET van 29 november
        1915", "DECREET van 8 december 1984", "STAATSBESLUIT van 15 september
        1981" -- so the document's own first line is both more precise and more
        widely present than the year embedded in its title (only 7 of 15 sample
        titles carry one). Prefer it, fall back to the title year, then null.
        """
        head = text[:1000].replace("\n", " ")
        m = re.search(
            r"\bvan\s+(\d{1,2})\s+(" + "|".join(DUTCH_MONTHS) + r")\s+(\d{4})\b",
            head,
            re.IGNORECASE,
        )
        if m:
            day, month, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            if 1800 <= year <= 2030 and 1 <= day <= 31:
                return f"{year:04d}-{DUTCH_MONTHS[month]:02d}-{day:02d}"

        year = self._extract_year(title)
        return f"{year}-01-01" if year else None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("law_id", ""),
            "_source": "SR/DNAWetgeving",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            # null, not "", when the instrument dates neither itself nor its title
            "date": raw.get("date") or None,
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "section": raw.get("section", ""),
        }

    def fetch_all(
        self, max_records: int = None, skip_stored: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        """Walk the index pages and yield each law with text from its PDF.

        skip_stored: skip laws already in the storage index. The law_id comes
        from the listing URL slug, so this is decided *before* the detail page
        and the PDF download -- see fetch_updates().
        """
        count = 0
        skipped_stored = 0
        seen_ids = set()

        for index_url, section in INDEX_PAGES:
            law_links = self._get_law_links(index_url, section)
            logger.info(f"Section '{section}': {len(law_links)} laws found")

            for title, law_url, sec in law_links:
                if max_records and count >= max_records:
                    return

                # Create a stable ID from the URL slug
                slug = law_url.rstrip("/").split("/")[-1]
                law_id = f"SR-DNA-{slug}"

                if law_id in seen_ids:
                    continue
                seen_ids.add(law_id)

                # The dedup key is _id, which is law_id, so the storage index
                # answers "already got this law?" before we pay for the detail
                # page and the PDF.
                if skip_stored and self.storage.exists(law_id):
                    skipped_stored += 1
                    continue

                pdf_url = self._get_pdf_url(law_url)
                if not pdf_url:
                    logger.warning(f"No PDF found for: {title}")
                    continue

                # Download PDF
                resp = self._request(pdf_url, timeout=120)
                if resp is None:
                    logger.warning(f"Failed to download PDF: {pdf_url}")
                    continue

                # Skip very large PDFs (>50MB)
                if len(resp.content) > 50 * 1024 * 1024:
                    logger.warning(f"PDF too large ({len(resp.content)} bytes): {title}")
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text) < 100:
                    logger.warning(
                        f"Insufficient text ({len(text)} chars) from PDF: {title}"
                    )
                    continue

                date = self._extract_date(text, title)

                raw = {
                    "law_id": law_id,
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": law_url,
                    "pdf_url": pdf_url,
                    "section": sec,
                }
                count += 1
                yield raw

        if skipped_stored:
            logger.info(f"Skipped {skipped_stored} laws already in the storage index")
        logger.info(f"Completed: {count} laws fetched with full text")

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Yield only laws we have not stored yet.

        `since` is deliberately unused: the DNA index pages carry no publication
        stamp, and the year parsed out of a law's title is its enactment year,
        not the date DNA posted the PDF. The seen-id checkpoint is the only
        availability-based comparator available here, and it is what append_only
        dedups on anyway.
        """
        yield from self.fetch_all(skip_stored=True)

    def test(self) -> bool:
        law_links = self._get_law_links(INDEX_PAGES[0][0], INDEX_PAGES[0][1])
        if not law_links:
            logger.error("Cannot fetch law index from dna.sr")
            return False

        logger.info(f"Index OK: {len(law_links)} laws on first page")

        if law_links:
            title, url, sec = law_links[0]
            pdf_url = self._get_pdf_url(url)
            if pdf_url:
                resp = self._request(pdf_url, timeout=120)
                if resp:
                    text = self._extract_pdf_text(resp.content)
                    logger.info(f"PDF OK: {title} ({len(text)} chars)")
                else:
                    logger.warning("Could not download sample PDF")
            else:
                logger.warning("No PDF URL found on sample page")

        return True


def main():
    parser = argparse.ArgumentParser(description="SR/DNAWetgeving data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DNAWetgevingScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        # Delegate to BaseScraper rather than writing records by hand: the
        # hand-rolled loop this replaces dumped *raw* fetch_all() output into
        # sample/, so normalize() never ran (records carried no _id/_source/
        # _type/_fetched_at) and a full crawl produced no data/records.jsonl for
        # the fleet to ingest. See issue #1596.
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        # BaseScraper.update() derives `since` from status.yaml:last_run and
        # routes through fetch_updates(), writing like bootstrap does.
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
