#!/usr/bin/env python3
"""
CR/MH-TaxGuidance -- Costa Rica Ministry of Finance Tax Guidance

Fetches regulatory documents (circulars, resolutions, directives, criteria)
from the Costa Rica Ministry of Finance (Ministerio de Hacienda).

Strategy:
  - Scrape the DocumentosInteres.html page for PDF links (~2800 documents)
  - Each PDF link has a title and relative URL
  - Download PDFs and extract text via pdfplumber
  - Content is in Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import json
import re
import sys
import time
import logging
import hashlib
import tempfile
from html.parser import HTMLParser
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CR.MH-TaxGuidance")

BASE_URL = "https://www.hacienda.go.cr"
INDEX_URL = f"{BASE_URL}/DocumentosInteres.html"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Patterns to extract dates from filenames
DATE_PATTERNS = [
    re.compile(r'(\d{4})[-_](\d{2})[-_](\d{2})'),  # 2024-01-15
    re.compile(r'(\d{4})_(\d{2})_(\d{2})'),          # 2024_01_15
    re.compile(r'-(\d{4})\.pdf', re.IGNORECASE),     # trailing -2024.pdf
]

YEAR_PATTERN = re.compile(r'(20[12]\d)')


class PDFLinkExtractor(HTMLParser):
    """Extract PDF links and their text labels from HTML."""

    def __init__(self):
        super().__init__()
        self.links = []
        self.in_link = False
        self.current_href = ""
        self.current_text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            href = attrs_dict.get("href", "")
            if href.lower().endswith(".pdf"):
                self.in_link = True
                self.current_href = href
                self.current_text = ""

    def handle_data(self, data):
        if self.in_link:
            self.current_text += data

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            title = " ".join(self.current_text.split()).strip()
            if title and self.current_href:
                self.links.append({
                    "href": self.current_href,
                    "title": title,
                })
            self.in_link = False


class MHTaxGuidanceScraper(BaseScraper):
    """
    Scraper for CR/MH-TaxGuidance -- Costa Rica Ministry of Finance.
    Country: CR
    URL: https://www.hacienda.go.cr/DocumentosInteres.html

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_index(self) -> list[dict]:
        """Fetch and parse the document index page."""
        r = self.session.get(INDEX_URL, timeout=60)
        r.raise_for_status()
        r.encoding = "utf-8"

        parser = PDFLinkExtractor()
        parser.feed(r.text)
        logger.info(f"Found {len(parser.links)} PDF links on index page")
        return parser.links

    def _extract_date(self, href: str, title: str) -> Optional[str]:
        """Try to extract a date from filename or title."""
        text = f"{href} {title}"
        for pat in DATE_PATTERNS:
            m = pat.search(text)
            if m:
                groups = m.groups()
                if len(groups) == 3:
                    try:
                        y, mo, d = int(groups[0]), int(groups[1]), int(groups[2])
                        if 2000 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
                            return f"{y:04d}-{mo:02d}-{d:02d}"
                    except (ValueError, IndexError):
                        pass
                elif len(groups) == 1:
                    return f"{groups[0]}-01-01"

        m = YEAR_PATTERN.search(text)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def _make_id(self, href: str) -> str:
        """Generate a stable ID from the PDF path."""
        clean = unquote(href).strip()
        return hashlib.sha256(clean.encode()).hexdigest()[:16]

    def _download_pdf_text(self, href: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        if href.startswith("http"):
            url = href
        else:
            url = f"{BASE_URL}/{href.lstrip('/')}"

        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()

            if not r.content[:5].startswith(b"%PDF"):
                logger.warning(f"Not a PDF: {href[:80]}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
                f.write(r.content)
                f.flush()
                pdf = pdfplumber.open(f.name)
                pages = []
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    if text.strip():
                        pages.append(text)
                pdf.close()
                return "\n\n".join(pages) if pages else None

        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF {href[:60]}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to extract PDF text {href[:60]}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        href = raw.get("href", "")
        title = raw.get("title", "").strip()
        if not title:
            title = unquote(Path(href).stem).replace("_", " ").replace("-", " ")

        date = self._extract_date(href, title)
        doc_id = self._make_id(href)

        if href.startswith("http"):
            full_url = href
        else:
            full_url = f"{BASE_URL}/{href.lstrip('/')}"

        return {
            "_id": doc_id,
            "_source": "CR/MH-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": full_url,
            "institution": "Ministerio de Hacienda de Costa Rica",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all documents from the index page."""
        links = self._fetch_index()
        yielded = 0
        skipped = 0

        for i, link in enumerate(links):
            href = link["href"]
            title = link["title"]

            logger.info(f"[{i+1}/{len(links)}] Downloading: {title[:60]}")
            text = self._download_pdf_text(href)

            if not text:
                skipped += 1
                logger.warning(f"Skipped (no text): {title[:60]}")
                continue

            link["text"] = text
            yield link
            yielded += 1
            time.sleep(1)

        logger.info(
            f"Finished: {yielded} documents with full text, "
            f"{skipped} skipped (no text extractable)"
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent documents (re-fetch all, filter by date)."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        links = self._fetch_index()

        for link in links:
            date = self._extract_date(link["href"], link["title"])
            if date and date >= since_str:
                text = self._download_pdf_text(link["href"])
                if text:
                    link["text"] = text
                    yield link
                    time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="CR/MH-TaxGuidance data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = MHTaxGuidanceScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            links = scraper._fetch_index()
            logger.info(f"OK: Found {len(links)} PDF links on index page")
            if links:
                logger.info(f"First: {links[0]['title'][:80]}")
                logger.info(f"  URL: {links[0]['href'][:80]}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=getattr(args, "sample_size", 15),
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
