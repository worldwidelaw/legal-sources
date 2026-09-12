#!/usr/bin/env python3
"""
MM/LincolnLegal -- Myanmar Laws in English (Lincoln Legal Services)

Scrapes the laws-in-english listing page for PDF links, downloads each
PDF, and extracts full text via pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import io
import re
import html as html_lib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MM.LincolnLegal")

BASE_URL = "https://www.lincolnmyanmar.com"
LISTING_URL = f"{BASE_URL}/laws-in-english/"


def _strip_tags(text: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    date_str = date_str.strip().rstrip(")")
    for fmt in [
        "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y",
        "%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y",
    ]:
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _slug_from_url(url: str) -> str:
    """Extract a slug from a PDF URL for use as ID."""
    fname = url.rstrip("/").rsplit("/", 1)[-1]
    fname = re.sub(r"\.pdf$", "", fname, flags=re.I)
    fname = re.sub(r"[^a-zA-Z0-9_-]", "_", fname)
    return fname[:80]


class LincolnLegalScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_law_links(self) -> list[dict]:
        """Scrape listing page for PDF links with titles."""
        try:
            resp = self.session.get(LISTING_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        html = resp.text

        # Find all links to PDFs with surrounding text for title/date
        # Pattern: text before <a href="...pdf">title</a> (date)
        entries = []
        seen_urls = set()

        # Find all <a> tags linking to PDFs
        for m in re.finditer(
            r'<a\s+[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
            html,
            re.DOTALL | re.I,
        ):
            url = m.group(1)
            link_text = _strip_tags(m.group(2))

            # Make URL absolute
            if not url.startswith("http"):
                url = urljoin(BASE_URL, url)

            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Look for date in nearby context (before the link)
            start = max(0, m.start() - 200)
            context = html[start:m.end() + 100]

            # Try to find a date pattern like "(18 May 2026)" or "18 May 2026"
            date_match = re.search(
                r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|'
                r'August|September|October|November|December)\s+\d{4})',
                context,
            )
            date_str = date_match.group(1) if date_match else None
            date = _parse_date(date_str) if date_str else None

            # Use link text as title, fall back to filename
            title = link_text if link_text and len(link_text) > 3 else _slug_from_url(url).replace("_", " ").replace("-", " ")

            entries.append({
                "url": url,
                "title": title.strip(),
                "date": date,
                "slug": _slug_from_url(url),
            })

        logger.info(f"Found {len(entries)} PDF links on listing page")
        return entries

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        try:
            time.sleep(2.0)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(pages) if pages else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_law_links()
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for entry in self.fetch_all():
            yield entry

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw.get("url", "")
        title = raw.get("title", "")
        slug = raw.get("slug", "")
        date = raw.get("date")

        if not url:
            return None

        # Download and extract PDF text
        text = self._extract_pdf_text(url)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {slug}: {len(text) if text else 0} chars")
            return None

        doc_id = f"MM-Lincoln-{slug}"

        return {
            "_id": doc_id,
            "_source": "MM/LincolnLegal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = LincolnLegalScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_law_links()
        if not entries:
            print("FAILED: no law PDFs found")
            sys.exit(1)
        print(f"OK: found {len(entries)} law PDFs")
        for e in entries[:5]:
            print(f"  {e['title'][:60]} ({e.get('date','no date')})")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
