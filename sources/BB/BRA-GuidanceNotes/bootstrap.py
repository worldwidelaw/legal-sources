#!/usr/bin/env python3
"""
BB/BRA-GuidanceNotes -- Barbados Revenue Authority Guidance Notes

Fetches tax guidance notes from bra.gov.bb. Listing page scraped for
individual note URLs, then each page is fetched for HTML intro text
plus attached PDF full text extracted via pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
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

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BB.BRA-GuidanceNotes")

BASE_URL = "https://bra.gov.bb"
LIST_URL = f"{BASE_URL}/News/Guidance-Notes/"


def _strip_tags(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse date string like 'Monday, March 16, 2026' to ISO format."""
    date_str = date_str.strip()
    for fmt in [
        "%A, %B %d, %Y",
        "%B %d, %Y",
        "%A, %b %d, %Y",
        "%b %d, %Y",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    return None


class BRAGuidanceNotesScraper(BaseScraper):
    """Scraper for BB/BRA-GuidanceNotes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_note_links(self) -> list[dict]:
        """Scrape listing page for individual note URLs and titles."""
        try:
            resp = self.session.get(LIST_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        html = resp.text
        # Extract entries with dates using pattern: <a href="...">Title</a> ... date
        entries_with_dates = re.findall(
            r'<a\s+href="(/News/Guidance-Notes/[^"]+)"[^>]*>\s*([^<]+?)\s*</a>'
            r'.*?(\w+\s+\d{1,2},\s+\d{4})',
            html,
            re.DOTALL,
        )
        date_map = {}
        for path, _title, date_str in entries_with_dates:
            slug = path.rstrip("/").rsplit("/", 1)[-1]
            date_map[slug] = _parse_date(date_str)

        links = re.findall(
            r'href="(/News/Guidance-Notes/[^"]+)"[^>]*>\s*([^<]+?)\s*</a>',
            html,
        )
        seen = set()
        results = []
        for path, title in links:
            path = path.rstrip("/")
            if path in seen or path == "/News/Guidance-Notes":
                continue
            title = html_lib.unescape(title).strip()
            if not title or title == "All Guidance Notes":
                continue
            seen.add(path)
            slug = path.rsplit("/", 1)[-1]
            results.append({
                "url": f"{BASE_URL}{path}",
                "slug": slug,
                "title_from_list": title,
                "date_from_list": date_map.get(slug),
            })

        logger.info(f"Found {len(results)} guidance notes on listing page")
        return results

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if "pdf" not in resp.headers.get("Content-Type", "").lower():
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

    def _fetch_note_page(self, url: str) -> Optional[dict]:
        """Fetch individual note page and extract content."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        html = resp.text

        # Extract title from h2
        h2_match = re.search(r"<h2[^>]*>(.*?)</h2>", html, re.DOTALL)
        title = _strip_tags(h2_match.group(1)) if h2_match else None

        # Extract date from icon-time element
        date_match = re.search(
            r'icon-time[^>]*>.*?<a[^>]*>([^<]+)</a>',
            html,
            re.DOTALL,
        )
        date_str = None
        if date_match:
            date_str = _parse_date(date_match.group(1))

        # Extract entry content - find the div with entry- class containing paragraphs
        content_match = re.search(
            r'<div\s+class="entry[^"]*"[^>]*>(.*?)</div>\s*(?:</div>|\s*<div\s+class=")',
            html,
            re.DOTALL,
        )
        if content_match:
            raw_content = content_match.group(1)
        else:
            # Fallback: collect all paragraphs in the main content area
            paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, re.DOTALL)
            raw_content = "\n\n".join(paragraphs)

        text = _strip_tags(raw_content)

        # Extract PDF attachment links
        pdf_links = re.findall(
            r'href="([^"]*(?:attachment\?file=[^"]*\.pdf[^"]*|[^"]*\.pdf))"',
            html,
            re.I,
        )

        # Download and extract PDF text if available
        pdf_text = ""
        for pdf_path in pdf_links:
            pdf_url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
            pdf_url = html_lib.unescape(pdf_url)
            extracted = self._extract_pdf_text(pdf_url)
            if extracted:
                pdf_text += extracted + "\n\n"

        # Prefer PDF text (full document) over HTML intro (summary)
        if pdf_text.strip() and len(pdf_text.strip()) > len(text):
            text = pdf_text.strip()

        return {
            "title": title,
            "date": date_str,
            "text": text,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        notes = self._get_note_links()
        for note in notes:
            yield note

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for note in self.fetch_all():
            yield note

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw.get("url", "")
        slug = raw.get("slug", "")
        if not url or not slug:
            return None

        page = self._fetch_note_page(url)
        if not page:
            return None

        title = page.get("title") or raw.get("title_from_list", "")
        text = page.get("text", "")

        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {slug}: {len(text)} chars")
            return None

        date = page.get("date") or raw.get("date_from_list")

        return {
            "_id": f"BB-BRA-GN-{slug}",
            "_source": "BB/BRA-GuidanceNotes",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "slug": slug,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = BRAGuidanceNotesScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        notes = scraper._get_note_links()
        if not notes:
            print("FAILED: no guidance notes found")
            sys.exit(1)
        print(f"OK: found {len(notes)} guidance notes")
        for n in notes[:3]:
            print(f"  {n['slug']}: {n['title_from_list'][:60]}")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
