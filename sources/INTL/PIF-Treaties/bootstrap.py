#!/usr/bin/env python3
"""
INTL/PIF-Treaties -- Pacific Islands Forum Secretariat - Treaty Collection

Scrapes the PIF treaty collection page for PDF treaty documents.
Downloads PDFs and extracts full text via pdfminer.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
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

from pdfminer.high_level import extract_text as pdfminer_extract
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.PIF-Treaties")

BASE_URL = "https://forumsec.org"
TREATY_PAGE = f"{BASE_URL}/publications/treaty-collection"
DELAY = 2.0


def _slug(url: str) -> str:
    """Derive a clean ID slug from a PDF URL."""
    fname = url.rstrip("/").rsplit("/", 1)[-1]
    fname = re.sub(r"\.pdf$", "", fname, flags=re.I)
    fname = re.sub(r"[^a-zA-Z0-9_.-]", "_", fname)
    fname = re.sub(r"_+", "_", fname).strip("_")
    return fname[:120]


def _extract_year(title: str, url: str) -> Optional[str]:
    """Try to extract a year from the title or PDF filename (not upload path)."""
    # Try title first
    m = re.search(r"\b(19[5-9]\d|20[0-2]\d)\b", title)
    if m:
        return f"{m.group(1)}-01-01"
    # Try only the filename part of URL (avoid upload date in path)
    fname = url.rsplit("/", 1)[-1] if "/" in url else url
    m = re.search(r"\b(19[5-9]\d|20[0-2]\d)\b", fname)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _clean_title(title: str) -> str:
    """Clean up title artifacts."""
    title = re.sub(r"\s*[-_]?\s*\d*\s*compressed\s*$", "", title, flags=re.I)
    title = re.sub(r"\s*[-_]?\s*\d*\s*min\s*$", "", title, flags=re.I)
    title = re.sub(r"\s*[-_]?\s*1\s*$", "", title)
    return title.strip()


class PIFTreatiesScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_treaty_entries(self) -> list[dict]:
        """Scrape the treaty collection page for PDF links and titles."""
        try:
            resp = self.session.get(TREATY_PAGE, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch treaty page: {e}")
            return []

        html = resp.text
        entries = []
        seen = set()

        # Find all PDF links with their associated text
        # Pattern: <a href="/sites/default/files/...pdf" ...>Title</a>
        for m in re.finditer(
            r'<a[^>]+href="(/sites/default/files/[^"]*\.pdf)"[^>]*>(.*?)</a>',
            html, re.DOTALL | re.I
        ):
            pdf_path = m.group(1)
            link_text = re.sub(r"<[^>]+>", "", m.group(2)).strip()
            link_text = html_lib.unescape(link_text)

            pdf_url = f"{BASE_URL}{pdf_path}"
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            # Try to find a better title from nearby heading or card context
            title = link_text if len(link_text) > 10 else ""
            if not title:
                # Derive from filename
                title = _slug(pdf_url).replace("-", " ").replace("_", " ")
                title = re.sub(r"\s*\d+\s*compressed\s*$", "", title, flags=re.I)
                title = re.sub(r"\s*min\s*$", "", title, flags=re.I)
                title = title.strip().title()

            entries.append({
                "pdf_url": pdf_url,
                "title": title,
            })

        # Also look for PDF links from nearby heading context
        # The page uses card structures with title + download link
        self._enrich_titles(html, entries)

        logger.info(f"Found {len(entries)} treaty PDFs")
        return entries

    def _enrich_titles(self, html: str, entries: list[dict]) -> None:
        """Try to enrich entries with titles from page context."""
        for entry in entries:
            pdf_path = entry["pdf_url"].replace(BASE_URL, "")
            pos = html.find(pdf_path)
            if pos == -1:
                continue

            # Look back ~1500 chars for a heading or title
            context = html[max(0, pos - 1500):pos]
            # Look for h3/h4/h5 headings
            headings = re.findall(
                r'<h[2-5][^>]*>(.*?)</h[2-5]>',
                context, re.DOTALL | re.I
            )
            if headings:
                candidate = re.sub(r"<[^>]+>", "", headings[-1]).strip()
                candidate = html_lib.unescape(candidate)
                if len(candidate) > 10 and len(candidate) < 200:
                    entry["title"] = candidate

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            time.sleep(DELAY)
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            if len(resp.content) < 200:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            text = pdfminer_extract(io.BytesIO(resp.content))
            return text if text and len(text.strip()) >= 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_treaty_entries()
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url", "")
        if not pdf_url:
            return None

        title = raw.get("title", "")
        slug = _slug(pdf_url)

        text = self._extract_pdf_text(pdf_url)
        if not text or len(text.strip()) < 50:
            logger.warning(f"Insufficient text for {slug}: {len(text) if text else 0} chars")
            return None

        date = _extract_year(title, pdf_url)
        doc_id = f"INTL-PIF-{slug}"

        return {
            "_id": doc_id,
            "_source": "INTL/PIF-Treaties",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": _clean_title(title) or slug.replace("_", " ").title(),
            "text": text.strip(),
            "date": date,
            "url": pdf_url,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = PIFTreatiesScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_treaty_entries()
        if not entries:
            print("FAILED: no treaties found")
            sys.exit(1)
        print(f"OK: found {len(entries)} treaty PDFs")
        for e in entries[:5]:
            print(f"  {e['title'][:70]}")
            print(f"    PDF: {e['pdf_url']}")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
