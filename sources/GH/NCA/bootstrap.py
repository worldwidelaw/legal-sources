#!/usr/bin/env python3
"""
GH/NCA -- Ghana National Communications Authority Regulations & Decisions

Scrapes the NCA regulatory framework page for PDF links to Ghana telecom
legislation, regulations, and guidelines. Downloads PDFs and extracts
full text via pdfplumber.

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
logger = logging.getLogger("legal-data-hunter.GH.NCA")

BASE_URL = "https://nca.org.gh"
LISTING_URL = f"{BASE_URL}/regulatory-framework/"
DELAY = 2.0

# Legislation PDFs use "Act" or "L.I." in title
LEGISLATION_PATTERNS = re.compile(r"\bAct\b|\bL\.I\.\s*\d+|\bRegulation", re.I)


def _slug_from_url(url: str) -> str:
    fname = url.rstrip("/").rsplit("/", 1)[-1]
    fname = re.sub(r"\.pdf$", "", fname, flags=re.I)
    fname = re.sub(r"%20", "_", fname)
    fname = re.sub(r"[^a-zA-Z0-9_.-]", "_", fname)
    return fname[:100]


def _extract_year(title: str) -> Optional[str]:
    m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _doc_type(title: str) -> str:
    if LEGISLATION_PATTERNS.search(title):
        return "legislation"
    return "doctrine"


class NCAScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_entries(self) -> list[dict]:
        try:
            resp = self.session.get(LISTING_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch regulatory framework page: {e}")
            return []

        html = resp.text
        entries = []
        seen_urls = set()

        # Page uses a card layout: <div class="item"> with
        # <h4 class="job-name">Title</h4> and <a href="...pdf">Download</a>
        for m in re.finditer(
            r'<div\s+class="item">(.*?)</div>\s*</div>\s*</div>\s*</div>',
            html,
            re.DOTALL,
        ):
            block = m.group(1)
            pdf_m = re.search(r'href="([^"]*\.pdf)"', block, re.I)
            if not pdf_m:
                continue

            raw_url = pdf_m.group(1)
            if not raw_url.startswith("http"):
                raw_url = urljoin(BASE_URL, raw_url)

            if raw_url in seen_urls:
                continue
            seen_urls.add(raw_url)

            # Title from <h4 class="job-name">
            title_m = re.search(r'<h4[^>]*class="job-name"[^>]*>(.*?)</h4>', block, re.DOTALL)
            title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""

            if not title:
                title = _slug_from_url(raw_url).replace("_", " ").replace("-", " ")

            title = html_lib.unescape(title).strip()
            date = _extract_year(title)

            entries.append({
                "title": title,
                "url": raw_url,
                "date": date,
                "slug": _slug_from_url(raw_url),
            })

        logger.info(f"Found {len(entries)} PDF links on regulatory framework page")
        return entries

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        try:
            time.sleep(DELAY)
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
        entries = self._get_entries()
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

        text = self._extract_pdf_text(url)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {slug}: {len(text) if text else 0} chars")
            return None

        doc_id = f"GH-NCA-{slug}"

        return {
            "_id": doc_id,
            "_source": "GH/NCA",
            "_type": _doc_type(title),
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
    scraper = NCAScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        if not entries:
            print("FAILED: no documents found")
            sys.exit(1)
        print(f"OK: found {len(entries)} regulatory documents")
        for e in entries[:5]:
            print(f"  {e['title'][:60]} ({e.get('date','no date')})")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
