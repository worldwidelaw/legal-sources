#!/usr/bin/env python3
"""
INTL/EACCA -- East African Community Competition Authority - Legal Framework

Scrapes the EACCA legal framework pages for PDF documents across categories
(competition-act, regulations-and-rules, gazettes, guidelines, mous).
Downloads PDFs and extracts full text via pdfplumber.

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
logger = logging.getLogger("legal-data-hunter.INTL.EACCA")

BASE_URL = "https://www.eacompetition.org"
DELAY = 2.0

# Category pages under /laws/
LAW_CATEGORIES = [
    f"{BASE_URL}/laws/category/competition-act",
    f"{BASE_URL}/laws/category/regulations-and-rules",
    f"{BASE_URL}/laws/category/gazettes",
    f"{BASE_URL}/laws/category/guidelines",
    f"{BASE_URL}/laws/category/mous",
]

# Patterns to classify as legislation vs doctrine
LEGISLATION_PATTERNS = re.compile(
    r"\bAct\b|\bRegulation|\bRules?\b|\bL\.I\.\s*\d+", re.I
)


def _slug_from_url(url: str) -> str:
    """Create a slug from a PDF URL for use as ID."""
    fname = url.rstrip("/").rsplit("/", 1)[-1]
    fname = re.sub(r"\.pdf$", "", fname, flags=re.I)
    fname = re.sub(r"[%()]", "_", fname)
    fname = re.sub(r"[^a-zA-Z0-9_.-]", "_", fname)
    fname = re.sub(r"_+", "_", fname).strip("_")
    return fname[:120]


def _extract_date(date_str: str) -> Optional[str]:
    """Parse date strings like 'Aug 07, 2025' or 'Oct 26, 2010'."""
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: extract year
    m = re.search(r"\b(19\d{2}|20\d{2})\b", date_str)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _doc_type(title: str) -> str:
    """Classify document as legislation or doctrine."""
    if LEGISLATION_PATTERNS.search(title):
        return "legislation"
    return "doctrine"


class EACCAScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _scrape_category(self, url: str) -> list[dict]:
        """Scrape a single category page for PDF entries."""
        try:
            time.sleep(DELAY)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return []

        html = resp.text
        entries = []
        seen = set()

        # Pattern 1: Look for PDF links in <a href="...pdf"> with nearby title text
        # The site uses /uploads/*.pdf for all documents
        for m in re.finditer(
            r'<a[^>]+href="([^"]*?/uploads/[^"]*?\.pdf)"[^>]*>(.*?)</a>',
            html,
            re.DOTALL | re.I,
        ):
            pdf_url = m.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL, pdf_url)

            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            link_text = re.sub(r"<[^>]+>", "", m.group(2)).strip()

            entries.append({
                "pdf_url": pdf_url,
                "link_text": link_text,
                "category_url": url,
            })

        # Pattern 2: Also look for standalone PDF links not inside an <a> with display text
        for m in re.finditer(
            r'href="([^"]*?/uploads/[^"]*?\.pdf)"',
            html,
            re.I,
        ):
            pdf_url = m.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL, pdf_url)
            if pdf_url not in seen:
                seen.add(pdf_url)
                entries.append({
                    "pdf_url": pdf_url,
                    "link_text": "",
                    "category_url": url,
                })

        # Try to find titles and dates from the page context
        # The site typically uses structures like:
        # <h4>Title</h4> ... <span class="date">Date</span> ... <a href="...pdf">
        self._enrich_entries(html, entries)

        return entries

    def _enrich_entries(self, html: str, entries: list[dict]) -> None:
        """Try to extract titles and dates from page context for each entry."""
        # Look for document blocks with title, date, and download link
        # Common pattern: title in h4/h3/strong, date in span/small, PDF in a href
        for entry in entries:
            pdf_url = entry["pdf_url"]
            # Find the PDF URL in the HTML and look backwards for title/date
            pos = html.find(pdf_url)
            if pos == -1:
                # Try without domain
                short = pdf_url.replace(BASE_URL, "")
                pos = html.find(short)
            if pos == -1:
                continue

            # Look back ~2000 chars for title and date context
            context = html[max(0, pos - 2000):pos + 200]

            # Extract title from heading tags
            title_matches = re.findall(
                r'<(?:h[2-5]|strong)[^>]*>(.*?)</(?:h[2-5]|strong)>',
                context, re.DOTALL | re.I
            )
            if title_matches:
                # Use the last (closest) heading
                raw_title = re.sub(r"<[^>]+>", "", title_matches[-1]).strip()
                raw_title = html_lib.unescape(raw_title)
                if len(raw_title) > 10 and not entry.get("title"):
                    entry["title"] = raw_title

            # Extract date
            date_matches = re.findall(
                r'(?:Date|Posted|Published)[:\s]*([A-Z][a-z]{2,8}\s+\d{1,2},?\s+\d{4})',
                context, re.I
            )
            if not date_matches:
                date_matches = re.findall(
                    r'\b([A-Z][a-z]{2,8}\s+\d{1,2},?\s+\d{4})\b',
                    context
                )
            if date_matches:
                entry["date_str"] = date_matches[-1]

    def _get_all_entries(self) -> list[dict]:
        """Collect PDF entries from all category pages."""
        all_entries = []
        seen_urls = set()

        for cat_url in LAW_CATEGORIES:
            logger.info(f"Scraping category: {cat_url}")
            entries = self._scrape_category(cat_url)
            for entry in entries:
                if entry["pdf_url"] not in seen_urls:
                    seen_urls.add(entry["pdf_url"])
                    all_entries.append(entry)

        logger.info(f"Found {len(all_entries)} unique PDF documents across all categories")
        return all_entries

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            time.sleep(DELAY)
            resp = self.session.get(url, timeout=90)
            resp.raise_for_status()

            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None

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

    def _build_title(self, entry: dict) -> str:
        """Determine the best title for an entry."""
        if entry.get("title"):
            return entry["title"]
        if entry.get("link_text") and len(entry["link_text"]) > 10:
            return entry["link_text"]
        # Fallback: derive from filename
        slug = _slug_from_url(entry["pdf_url"])
        return slug.replace("_", " ").replace("-", " ").title()

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_all_entries()
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for entry in self.fetch_all():
            yield entry

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw.get("pdf_url", "")
        if not pdf_url:
            return None

        title = self._build_title(raw)
        slug = _slug_from_url(pdf_url)
        date_str = raw.get("date_str", "")
        date = _extract_date(date_str) if date_str else None

        # If no date from context, try to extract year from title
        if not date:
            m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
            if m:
                date = f"{m.group(1)}-01-01"

        text = self._extract_pdf_text(pdf_url)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {slug}: {len(text) if text else 0} chars")
            return None

        doc_id = f"INTL-EACCA-{slug}"

        return {
            "_id": doc_id,
            "_source": "INTL/EACCA",
            "_type": _doc_type(title),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = EACCAScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_all_entries()
        if not entries:
            print("FAILED: no documents found")
            sys.exit(1)
        print(f"OK: found {len(entries)} documents")
        for e in entries[:10]:
            title = scraper._build_title(e)
            print(f"  {title[:70]}")
            print(f"    PDF: {e['pdf_url']}")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
