#!/usr/bin/env python3
"""
DM/Legislation -- Laws of Dominica

Fetches legislation from the official Dominica government portal.
The site uses a Joomla-based search with paginated results, each linking
to a PDF file. Covers Revised Laws 1990 (chapters) and Acts/SROs 1991-2025.

Method: POST search for broad term "a" to get all results (~2400 PDFs),
paginate through 30-per-page results, download and extract PDF text.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DM.Legislation")

BASE_URL = "https://dominica.gov.dm"
SEARCH_URL = f"{BASE_URL}/laws-of-dominica"


def _extract_year(title: str, pdf_path: str) -> str:
    """Try to extract year from title or PDF path."""
    # From path like /laws/2007/act12-2007.pdf
    m = re.search(r'/laws/(\d{4})/', pdf_path)
    if m:
        return m.group(1)
    # From title like "Act No. 12 of 2007"
    m = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    if m:
        return m.group(1)
    # Revised Laws are from 1990
    if 'chap' in pdf_path.lower() or 'chapter' in title.lower():
        return "1990"
    return ""


def _classify_type(title: str, pdf_path: str) -> str:
    """Classify as act, chapter (revised law), or SRO."""
    lower = title.lower()
    path_lower = pdf_path.lower()
    if 'sro' in lower or 'sro' in path_lower or 'statutory' in lower:
        return "SRO"
    if 'chap' in path_lower or 'chapter' in lower:
        return "Chapter"
    return "Act"


class DominicaLegislationScraper(BaseScraper):
    """Scraper for DM/Legislation."""

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

    def _search_page(self, page: int = 1) -> str:
        """Fetch a search results page."""
        self.rate_limiter.wait()
        sess = self._get_session()

        if page == 1:
            # Initial POST search
            resp = sess.post(
                f"{SEARCH_URL}?page=1",
                data={"searchname": "a", "s": "title", "searchbtn": "Search"},
                timeout=30,
            )
        else:
            # Subsequent pages via GET
            resp = sess.get(
                f"{SEARCH_URL}?page={page}&term=a&sort=title",
                timeout=30,
            )

        resp.raise_for_status()
        return resp.text

    def _parse_results(self, html: str) -> List[Dict[str, str]]:
        """Parse search results to extract PDF links and titles."""
        results = []
        # Pattern: <li><a href='...pdf'>Title</a></li>
        # Also handle double-quote variants
        pattern = r"<li>\s*<a\s+href=['\"]([^'\"]+\.pdf)['\"][^>]*>(.*?)</a>"
        for match in re.finditer(pattern, html, re.DOTALL | re.IGNORECASE):
            pdf_path = match.group(1).strip()
            title = re.sub(r'<[^>]+>', '', match.group(2)).strip()
            if pdf_path and title:
                results.append({"pdf_path": pdf_path, "title": title})
        return results

    def _get_total_pages(self, html: str) -> int:
        """Extract total page count from the pagination widget.

        Looks specifically inside the <ul class="pagination ..."> element
        to avoid picking up unrelated page= references elsewhere in the
        HTML (JSON-LD, canonical URLs, etc.).
        """
        # Extract the pagination widget HTML first
        pag_match = re.search(
            r'<ul[^>]*class="[^"]*pagination[^"]*"[^>]*>(.*?)</ul>',
            html, re.DOTALL | re.IGNORECASE,
        )
        if pag_match:
            pag_html = pag_match.group(1)
            pages = re.findall(r'page=(\d+)', pag_html)
            if pages:
                return max(int(p) for p in pages)

        # Broader fallback: any page= in the full HTML
        pages = re.findall(r'page=(\d+)', html)
        if pages:
            return max(int(p) for p in pages)
        return 1

    def _download_pdf_text(self, pdf_path: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        url = pdf_path if pdf_path.startswith("http") else urljoin(BASE_URL, pdf_path)
        self.rate_limiter.wait()
        sess = self._get_session()

        try:
            resp = sess.get(url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF for {doc_id}: {e}")
            return ""

        if len(pdf_bytes) < 100:
            return ""

        text = extract_pdf_markdown(
            source="DM/Legislation",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

        return text

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title", "Unknown")
        pdf_path = raw.get("pdf_path", "")
        year = _extract_year(title, pdf_path)
        doc_type = _classify_type(title, pdf_path)

        # Create stable ID from PDF filename
        pdf_filename = pdf_path.split("/")[-1] if pdf_path else ""
        doc_id = hashlib.md5(pdf_filename.encode()).hexdigest()[:12]

        url = pdf_path if pdf_path.startswith("http") else urljoin(BASE_URL, pdf_path)

        return {
            "_id": f"DM/Legislation/{doc_id}",
            "_source": "DM/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_prefetched_text", ""),
            "date": f"{year}-01-01" if year else "",
            "url": url,
            "year": year,
            "doc_type": doc_type,
            "pdf_filename": pdf_filename,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        limit = 15 if sample else None
        count = 0
        seen_pdfs = set()
        consecutive_empty = 0  # safety: stop after 2 consecutive empty pages

        logger.info("Fetching first page to determine total pages...")
        html = self._search_page(1)
        total_pages = self._get_total_pages(html)
        max_pages = min(3, total_pages) if sample else None  # None = no cap
        logger.info(
            f"Total pages detected: {total_pages}, "
            f"processing: {max_pages if max_pages else 'all (until empty)'}"
        )

        page = 0
        while True:
            page += 1

            # Respect sample-mode page cap
            if max_pages and page > max_pages:
                break
            if limit and count >= limit:
                break

            if page > 1:
                logger.info(f"Fetching page {page}/{total_pages}...")
                try:
                    html = self._search_page(page)
                except Exception as e:
                    logger.error(f"Failed to fetch page {page}: {e}")
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        logger.warning("Two consecutive failed/empty pages, stopping.")
                        break
                    continue

            results = self._parse_results(html)
            if not results:
                consecutive_empty += 1
                logger.info(f"No results on page {page} ({consecutive_empty} consecutive empty).")
                if consecutive_empty >= 2:
                    break
                continue
            consecutive_empty = 0

            for entry in results:
                if limit and count >= limit:
                    break

                pdf_path = entry["pdf_path"]
                if pdf_path in seen_pdfs:
                    continue
                seen_pdfs.add(pdf_path)

                title = entry["title"]
                pdf_filename = pdf_path.split("/")[-1]
                doc_id = hashlib.md5(pdf_filename.encode()).hexdigest()[:12]

                text = self._download_pdf_text(pdf_path, doc_id)
                if not text or len(text) < 50:
                    logger.warning(f"  Skipping {title[:50]} - no/short text")
                    continue

                entry["_prefetched_text"] = text
                yield entry
                count += 1
                logger.info(f"  [{count}] {title[:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count} (pages scanned: {page}, unique PDFs seen: {len(seen_pdfs)})")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = DominicaLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing Dominica legislation portal access...")
        html = scraper._search_page(1)
        results = scraper._parse_results(html)
        total_pages = scraper._get_total_pages(html)
        print(f"Page 1: {len(results)} results, {total_pages} total pages")
        if results:
            r = results[0]
            print(f"  First: {r['title'][:60]}")
            print(f"  PDF: {r['pdf_path']}")
            text = scraper._download_pdf_text(r['pdf_path'], "test")
            print(f"  PDF text: {len(text)} chars")
        print("Test PASSED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
