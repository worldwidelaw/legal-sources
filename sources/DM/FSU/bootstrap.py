#!/usr/bin/env python3
"""
DM/FSU -- Dominica Financial Services Unit Legislation

Fetches the curated list of ~27 financial services regulatory PDFs from
https://fsu.gov.dm/legislation. Downloads each PDF and extracts full text.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DM.FSU")

FSU_URL = "https://fsu.gov.dm/legislation"
FSU_BASE = "https://fsu.gov.dm"
GOV_BASE = "https://dominica.gov.dm"


def _extract_year(title: str, url: str) -> str:
    """Extract year from URL path or title."""
    m = re.search(r'/laws/(\d{4})/', url)
    if m:
        return m.group(1)
    m = re.search(r'\b(18\d{2}|19\d{2}|20\d{2})\b', title)
    if m:
        return m.group(1)
    if 'chap' in url.lower():
        return "1990"
    return ""


def _resolve_url(href: str) -> str:
    """Resolve a link from the FSU page to an absolute URL."""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        # Relative links on fsu.gov.dm
        return FSU_BASE + href
    return urljoin(FSU_URL, href)


class DominicaFSUScraper(BaseScraper):
    """Scraper for DM/FSU."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/pdf,*/*",
            })
        return self.session

    def _fetch_legislation_page(self) -> str:
        """Fetch the FSU legislation page."""
        self.rate_limiter.wait()
        sess = self._get_session()
        resp = sess.get(FSU_URL, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _parse_links(self, html: str) -> List[Dict[str, str]]:
        """Extract PDF/document links from the FSU legislation page."""
        results = []
        seen = set()
        # Match <a href="...">Title</a> patterns pointing to PDFs
        pattern = r'<a\s+[^>]*href=["\']([^"\']+\.pdf)["\'][^>]*>(.*?)</a>'
        for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
            href = match.group(1).strip()
            title = re.sub(r'<[^>]+>', '', match.group(2)).strip()
            title = re.sub(r'\s+', ' ', title)
            if not title or not href:
                continue
            url = _resolve_url(href)
            if url in seen:
                continue
            seen.add(url)
            results.append({"url": url, "title": title})
        return results

    def _download_pdf_text(self, url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {doc_id}: {e}")
            return ""

        if len(pdf_bytes) < 100:
            return ""

        text = extract_pdf_markdown(
            source="DM/FSU",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""
        return text

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title", "Unknown")
        url = raw.get("url", "")
        year = _extract_year(title, url)

        pdf_filename = url.split("/")[-1] if url else ""
        doc_id = hashlib.md5(url.encode()).hexdigest()[:12]

        return {
            "_id": f"DM/FSU/{doc_id}",
            "_source": "DM/FSU",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_prefetched_text", ""),
            "date": f"{year}-01-01" if year else "",
            "url": url,
            "year": year,
            "pdf_filename": pdf_filename,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        limit = 15 if sample else None
        count = 0

        logger.info("Fetching FSU legislation page...")
        html = self._fetch_legislation_page()
        links = self._parse_links(html)
        logger.info(f"Found {len(links)} PDF links on FSU legislation page")

        for entry in links:
            if limit and count >= limit:
                break

            url = entry["url"]
            title = entry["title"]
            pdf_filename = url.split("/")[-1]
            doc_id = hashlib.md5(url.encode()).hexdigest()[:12]

            logger.info(f"  Downloading: {title[:60]}...")
            text = self._download_pdf_text(url, doc_id)
            if not text or len(text) < 50:
                logger.warning(f"  Skipping {title[:50]} - insufficient text ({len(text)} chars)")
                continue

            entry["_prefetched_text"] = text
            yield entry
            count += 1
            logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}/{len(links)} PDFs")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = DominicaFSUScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing FSU legislation page access...")
        html = scraper._fetch_legislation_page()
        links = scraper._parse_links(html)
        print(f"Found {len(links)} PDF links")
        if links:
            entry = links[0]
            print(f"  First: {entry['title'][:60]}")
            print(f"  URL: {entry['url']}")
            text = scraper._download_pdf_text(entry['url'], "test")
            print(f"  PDF text: {len(text)} chars")
            if text:
                print(f"  Preview: {text[:200]}")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
