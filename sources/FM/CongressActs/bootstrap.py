#!/usr/bin/env python3
"""
FM/CongressActs -- FSM Congress Public Laws & Resolutions

Fetches legislation from the official FSM Congress website (cfsm.gov.fm).
Covers:
  - Public Laws (10th-24th Congress)
  - Congressional Resolutions (14th-24th Congress)

Documents are PDFs hosted on WordPress. We crawl index pages for each congress
session, extract PDF links, download the PDFs, and extract full text via pdfplumber.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import io
import logging
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FM.CongressActs")

BASE_URL = "https://www.cfsm.gov.fm"

# URL patterns for congress pages
PUBLIC_LAW_PAGES = [
    "/24th-public-laws/",
    "/23rd-cfsm-public-laws/",
    "/22nd-public-laws/",
    "/21st-cfsm-public-laws/",
    "/20th-cfsm-public-laws/",
    "/19th-cfsm-public-laws/",
    "/18th-cfsm-public-laws/",
    "/17th-public-laws/",
    "/16th-cfsm-public-laws/",
    "/15th-cfsm-public-laws/",
    "/14th-cfsm-public-laws/",
    "/13th-public-laws/",
    "/12th-cfsm-public-laws/",
    "/11th-public-laws/",
    "/10th-public-laws/",
]

RESOLUTION_PAGES = [
    "/24th-resolutions/",
    "/23rd-cfsm-resolutions/",
    "/22nd-cfsm-resolutions/",
    "/21st-cfsm-resolutions/",
    "/20th-cfsm-resolutions/",
    "/19th-cfsm-resolutions/",
    "/18th-cfsm-resolutions/",
    "/17th-cfsm-resolutions/",
    "/16th-cfsm-resolutions/",
    "/15th-cfsm-resolutions/",
    "/14th-resolutions/",
]

_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Map ordinal prefixes to congress numbers
ORDINAL_MAP = {
    "10th": 10, "11th": 11, "12th": 12, "13th": 13, "14th": 14,
    "15th": 15, "16th": 16, "17th": 17, "18th": 18, "19th": 19,
    "20th": 20, "21st": 21, "22nd": 22, "23rd": 23, "24th": 24,
}


def _congress_number_from_path(path: str) -> int:
    """Extract congress number from a URL path like /24th-public-laws/."""
    for prefix, num in ORDINAL_MAP.items():
        if prefix in path:
            return num
    return 0


def _extract_pdf_links(html: str, base_url: str) -> List[Dict[str, str]]:
    """Extract PDF links and their anchor text from HTML."""
    results = []
    # Match <a> tags containing .pdf links
    pattern = r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>'
    for m in re.finditer(pattern, html, re.IGNORECASE | re.DOTALL):
        href = m.group(1)
        text = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        full_url = urljoin(base_url, href)
        results.append({"url": full_url, "link_text": text})
    return results


class FSMCongressScraper(BaseScraper):
    """Scraper for FM/CongressActs - FSM Congress Public Laws & Resolutions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_BROWSER_HEADERS)

            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page. Returns content or None on error."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_pdf_bytes(self, url: str) -> Optional[bytes]:
        """Download a PDF. Returns bytes or None on error."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        import pdfplumber

        text_parts = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"PDF extraction error: {e}")
            return ""
        return "\n\n".join(text_parts)

    def _parse_public_law_title(self, text: str, link_text: str) -> str:
        """Extract a meaningful title from PDF text or link text."""
        # Try to find "AN ACT" description in the text
        m = re.search(r'AN ACT\s*\n(.+?)(?:\n\s*BE IT ENACTED|\n\s*Section)', text, re.DOTALL)
        if m:
            title = re.sub(r'\s+', ' ', m.group(1)).strip()
            if len(title) > 20:
                return title[:300]
        # Try "A RESOLUTION" for resolutions
        m = re.search(r'A RESOLUTION\s*\n(.+?)(?:\n\s*WHEREAS|\n\s*NOW,? THEREFORE)', text, re.DOTALL)
        if m:
            title = re.sub(r'\s+', ' ', m.group(1)).strip()
            if len(title) > 20:
                return title[:300]
        # Fall back to link text
        if link_text and len(link_text) > 5:
            return link_text
        # Extract from first line pattern
        m = re.search(r'PUBLIC LAW NO\.\s*([\d-]+)', text)
        if m:
            return f"Public Law No. {m.group(1)}"
        m = re.search(r'CR\s*([\d-]+)', text)
        if m:
            return f"Congressional Resolution {m.group(1)}"
        return link_text or "Untitled"

    def _parse_law_number(self, url: str, text: str) -> str:
        """Extract law/resolution number from URL or text."""
        # From URL: PUBLIC-LAW-NO.-24-64.pdf or CR-24-113.pdf
        m = re.search(r'PUBLIC-LAW-NO\.-?([\d]+-[\d]+)', url, re.IGNORECASE)
        if m:
            return f"PL {m.group(1)}"
        m = re.search(r'CR-([\d]+-[\d]+)', url, re.IGNORECASE)
        if m:
            return f"CR {m.group(1)}"
        # From text
        m = re.search(r'PUBLIC LAW NO\.\s*([\d]+-[\d]+)', text)
        if m:
            return f"PL {m.group(1)}"
        m = re.search(r'C\.?R\.?\s*(?:NO\.?\s*)?([\d]+-[\d]+)', text)
        if m:
            return f"CR {m.group(1)}"
        # From filename
        basename = url.split('/')[-1].replace('.pdf', '')
        return basename

    def _discover_pdfs(self, pages: List[str], doc_type: str) -> Generator[Dict, None, None]:
        """Discover all PDF links from a list of index pages."""
        for page_path in pages:
            congress_num = _congress_number_from_path(page_path)
            page_url = BASE_URL + page_path
            logger.info(f"Crawling {doc_type} page: {page_url}")

            html = self._fetch_page(page_url)
            if not html:
                continue

            pdf_links = _extract_pdf_links(html, page_url)
            # Filter to only relevant PDFs (skip presidential comms, etc.)
            for link_info in pdf_links:
                url = link_info["url"]
                url_lower = url.lower()
                # Skip presidential communications and non-law documents
                if "presidential-comm" in url_lower:
                    continue
                if "committee" in url_lower and doc_type == "public_law":
                    continue
                # Must be a public law or resolution PDF
                if doc_type == "public_law" and "public-law" not in url_lower and "pl-" not in url_lower:
                    # Some older congresses may use different naming
                    if "law" not in url_lower and "act" not in url_lower:
                        continue
                yield {
                    "pdf_url": url,
                    "link_text": link_info["link_text"],
                    "congress_number": congress_num,
                    "doc_type": doc_type,
                    "index_page": page_url,
                }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all public laws and resolutions."""
        # Fetch public laws first
        for item in self._discover_pdfs(PUBLIC_LAW_PAGES, "public_law"):
            pdf_bytes = self._fetch_pdf_bytes(item["pdf_url"])
            if pdf_bytes:
                item["pdf_bytes"] = pdf_bytes
                yield item

        # Then resolutions
        for item in self._discover_pdfs(RESOLUTION_PAGES, "resolution"):
            pdf_bytes = self._fetch_pdf_bytes(item["pdf_url"])
            if pdf_bytes:
                item["pdf_bytes"] = pdf_bytes
                yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental updates - re-fetch the most recent congress only."""
        # Only check the latest congress for new laws
        latest_laws = [PUBLIC_LAW_PAGES[0]]
        latest_resolutions = [RESOLUTION_PAGES[0]]

        for item in self._discover_pdfs(latest_laws, "public_law"):
            pdf_bytes = self._fetch_pdf_bytes(item["pdf_url"])
            if pdf_bytes:
                item["pdf_bytes"] = pdf_bytes
                yield item

        for item in self._discover_pdfs(latest_resolutions, "resolution"):
            pdf_bytes = self._fetch_pdf_bytes(item["pdf_url"])
            if pdf_bytes:
                item["pdf_bytes"] = pdf_bytes
                yield item

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw PDF data into standardized record."""
        pdf_bytes = raw.get("pdf_bytes")
        if not pdf_bytes:
            return None

        text = self._extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text from {raw['pdf_url']}")
            return None

        law_number = self._parse_law_number(raw["pdf_url"], text)
        title = self._parse_public_law_title(text, raw.get("link_text", ""))
        congress_num = raw.get("congress_number", 0)
        doc_type = raw.get("doc_type", "public_law")

        # Generate stable ID from URL
        url_hash = hashlib.md5(raw["pdf_url"].encode()).hexdigest()[:12]
        doc_id = f"FM-congress-{law_number.replace(' ', '-')}-{url_hash}"

        return {
            "_id": doc_id,
            "_source": "FM/CongressActs",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,  # PDFs don't consistently include dates
            "url": raw["pdf_url"],
            "law_number": law_number,
            "congress_number": congress_num,
            "doc_type": doc_type,
            "link_text": raw.get("link_text", ""),
        }


# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = FSMCongressScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        # Quick connectivity test
        import requests
        try:
            resp = requests.get(BASE_URL, headers=_BROWSER_HEADERS, timeout=15)
            print(f"Connection OK: HTTP {resp.status_code}")
            # Try fetching a known law page
            resp2 = requests.get(
                BASE_URL + "/24th-public-laws/",
                headers=_BROWSER_HEADERS, timeout=15
            )
            print(f"24th Public Laws page: HTTP {resp2.status_code}")
            # Check for PDF links
            pdf_count = len(re.findall(r'\.pdf"', resp2.text))
            print(f"PDF links found: {pdf_count}")
        except Exception as e:
            print(f"Connection FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(f"\nBootstrap complete:")
        print(f"  Records fetched: {stats['records_fetched']}")
        if sample_mode:
            print(f"  Sample records saved: {stats.get('sample_records_saved', 0)}")
        else:
            print(f"  New: {stats['records_new']}")
            print(f"  Updated: {stats['records_updated']}")
            print(f"  Skipped: {stats['records_skipped']}")
        print(f"  Errors: {stats['errors']}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
