#!/usr/bin/env python3
"""
MS/FSC -- Montserrat Financial Services Commission Legislation

Downloads legislation from the Montserrat FSC website. Full text extracted
from PDFs via pdfplumber.

Strategy:
  1. Scrape https://www.fscmontserrat.org/legislations/ for PDF links
  2. Download each PDF
  3. Extract full text via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
from html import unescape
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
import pdfplumber
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MS.FSC")

SOURCE_ID = "MS/FSC"
BASE_URL = "https://www.fscmontserrat.org"
LISTING_URL = f"{BASE_URL}/legislations/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _make_id(url: str) -> str:
    """Generate a stable ID from the PDF URL."""
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _clean_title(raw: str) -> str:
    """Clean HTML entities and tags from title text."""
    raw = re.sub(r'<[^>]+>', '', raw).strip()
    raw = unescape(raw)
    raw = raw.replace('\xa0', ' ')  # &nbsp;
    raw = re.sub(r'\s+', ' ', raw).strip()
    return raw


def _title_from_url(url: str) -> str:
    """Derive a readable title from the PDF filename."""
    fname = unquote(url.rsplit("/", 1)[-1])
    fname = re.sub(r'\.pdf$', '', fname, flags=re.IGNORECASE)
    fname = fname.replace("-", " ").replace("_", " ")
    fname = re.sub(r'\s+', ' ', fname).strip()
    return fname


def _extract_year(title: str) -> Optional[str]:
    """Extract a year from the title for date estimation."""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    return f"{m.group(1)}-01-01" if m else None


def _normalize_url(url: str) -> str:
    """Ensure URL uses HTTPS."""
    if url.startswith("http://"):
        url = "https://" + url[7:]
    return url


class MSFSCScraper(BaseScraper):
    """Scraper for MS/FSC -- Montserrat Financial Services Commission."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self) -> requests.Session:
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf,*/*;q=0.8",
            })
        return self.session

    def _fetch_pdf_links(self) -> List[Tuple[str, str]]:
        """Fetch all PDF links from the legislation page.
        Returns list of (pdf_url, title) tuples.
        """
        sess = self._get_session()
        self.rate_limiter.wait()
        resp = sess.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text

        links = re.findall(
            r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        )

        seen = set()
        result = []
        for url, raw_title in links:
            url = _normalize_url(url.strip())
            if url in seen:
                continue
            seen.add(url)

            title = _clean_title(raw_title)
            if not title or len(title) < 3:
                title = _title_from_url(url)

            result.append((url, title))

        logger.info("Found %d unique PDF links on legislation page", len(result))
        return result

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        sess = self._get_session()
        try:
            self.rate_limiter.wait()
            resp = sess.get(pdf_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", pdf_url, e)
            return None

        if resp.content[:4] != b"%PDF":
            logger.warning("Not a PDF: %s", pdf_url)
            return None

        try:
            pages_text = []
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            full_text = "\n\n".join(pages_text)
            if len(full_text.strip()) < 50:
                logger.warning("Insufficient text from %s: %d chars",
                               pdf_url, len(full_text))
                return None
            return full_text
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", pdf_url, e)
            return None

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            links = self._fetch_pdf_links()
            has_links = len(links) > 0
            logger.info("Page test: %d PDF links — %s",
                        len(links), "OK" if has_links else "FAIL")

            if links:
                pdf_url, title = links[0]
                sess = self._get_session()
                self.rate_limiter.wait()
                resp = sess.get(pdf_url, timeout=30)
                pdf_ok = resp.content[:4] == b"%PDF"
                logger.info("PDF test: %s (%d bytes) — %s",
                            title[:60], len(resp.content),
                            "OK" if pdf_ok else "FAIL")
                return has_links and pdf_ok

            return has_links
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all legislation records with full text."""
        links = self._fetch_pdf_links()
        if not links:
            logger.error("No PDF links found")
            return

        count = 0
        for i, (pdf_url, title) in enumerate(links):
            logger.info("Downloading [%d/%d]: %s", i + 1, len(links), title[:80])
            text = self._download_pdf_text(pdf_url)
            if text is None:
                logger.warning("Skipping (no text): %s", title[:80])
                continue

            date = _extract_year(title)

            yield {
                "id": _make_id(pdf_url),
                "title": title,
                "text": text,
                "date": date,
                "url": pdf_url,
            }
            count += 1

        logger.info("Completed: %d records with full text", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since a given date."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        return {
            "_id": raw["id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = MSFSCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
