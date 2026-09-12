#!/usr/bin/env python3
"""
LC/FSRA -- Saint Lucia Financial Services Regulatory Authority

Downloads legislation, regulations, guidelines, and directives from
fsrastlucia.org. Full text extracted from PDFs via pdfplumber.

Strategy:
  - The FSRA site is Joomla-based with legislation/guidelines across
    multiple sector-specific pages.
  - We fetch each page's HTML, extract PDF links, dedup, download, and
    extract text.
  - Documents are classified as "legislation" (acts, regulations, SROs)
    or "doctrine" (guidelines, circulars, advisories, frameworks).

Coverage:
  - FSRA Acts (main + amendments)
  - Insurance Act + amendments, guidelines
  - Co-operative Societies Act + regulations
  - International Banks Act + amendments
  - International Insurance Act
  - International Mutual Funds Act
  - Registered Agent and Trustee Licensing Act
  - Money Services Business Act + amendments, guidelines
  - Virtual Assets Business Act + regulations
  - Pension Fund Plans legislation
  - Various circulars, advisories, and supervisory frameworks

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import logging
import hashlib
import re
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LC.FSRA")

BASE_URL = "https://fsrastlucia.org"

# Pages to scrape, with (path, doc_type, label)
PAGES = [
    # Main FSRA legislation
    ("/index.php/legislation", "legislation", "FSRA Acts"),
    # Insurance
    ("/index.php/insurance-pensions/insurance-companies/legislation", "legislation", "Insurance Legislation"),
    ("/index.php/insurance-pensions/insurance-companies/guidelines", "doctrine", "Insurance Guidelines"),
    ("/index.php/insurance-pensions/insurance-intermediaries/guidelines", "doctrine", "Intermediaries Guidelines"),
    ("/index.php/insurance-pensions/pension-fund-plans/legislation", "legislation", "Pension Legislation"),
    ("/index.php/insurance-pensions/pension-fund-plans/guidelines", "doctrine", "Pension Guidelines"),
    # Credit unions
    ("/index.php/credit-unions/legislation", "legislation", "Credit Unions Legislation"),
    ("/index.php/credit-unions/guidelines", "doctrine", "Credit Unions Guidelines"),
    # International sector
    ("/index.php/international-sector/international-banks/legislation", "legislation", "Intl Banks Legislation"),
    ("/index.php/international-sector/international-banks/guidelines", "doctrine", "Intl Banks Guidelines"),
    ("/index.php/international-sector/international-insurance/legislation", "legislation", "Intl Insurance Legislation"),
    ("/index.php/international-sector/international-insurance/guidelines", "doctrine", "Intl Insurance Guidelines"),
    ("/index.php/international-sector/international-mutual-funds/legislation", "legislation", "Intl Mutual Funds Legislation"),
    ("/index.php/international-sector/international-mutual-funds/guidelines", "doctrine", "Intl Mutual Funds Guidelines"),
    # Other
    ("/index.php/registered-agents-trustees/legislation", "legislation", "Registered Agents Legislation"),
    ("/index.php/registered-agents-trustees/guidelines", "doctrine", "Registered Agents Guidelines"),
    ("/index.php/money-services-business/legislation", "legislation", "MSB Legislation"),
    ("/index.php/money-services-business/guidelines", "doctrine", "MSB Guidelines"),
    ("/index.php/other-regulated-entities/virtual-asset/legislation", "legislation", "Virtual Assets Legislation"),
    ("/index.php/other-regulated-entities/virtual-asset/guidelines", "doctrine", "Virtual Assets Guidelines"),
    ("/index.php/other-regulated-entities/office-of-the-supervisor-of-insolvency/legislation", "legislation", "Insolvency Legislation"),
]

# Skip non-document files
SKIP_PATTERNS = [
    r"\.jpg$", r"\.jpeg$", r"\.png$", r"\.gif$",
    r"banking.*details.*payments",
]


def _make_doc_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _normalize_url(href: str) -> str:
    href = href.strip()
    if href.startswith("/"):
        href = BASE_URL + href
    if href.startswith("http://"):
        href = "https://" + href[7:]
    return href


def _extract_year_from_title(title: str) -> str:
    m = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    return f"{m.group(1)}-01-01" if m else ""


def _should_skip(url: str) -> bool:
    url_lower = url.lower()
    return any(re.search(pat, url_lower) for pat in SKIP_PATTERNS)


def _classify_type(title: str, page_type: str) -> str:
    title_lower = title.lower()
    if any(kw in title_lower for kw in ["act", "regulation", "order", "sro", "bill", "s.i."]):
        return "legislation"
    if any(kw in title_lower for kw in ["guideline", "circular", "advisory", "framework",
                                         "complaint", "notice", "public advisory"]):
        return "doctrine"
    return page_type


def _clean_title(raw: str) -> str:
    raw = re.sub(r'<[^>]+>', '', raw).strip()
    raw = unescape(raw)
    raw = re.sub(r'\s+', ' ', raw).strip()
    if not raw:
        return raw
    # Fall back to filename if title is empty/generic
    return raw


def _title_from_url(url: str) -> str:
    """Extract a title from the PDF filename."""
    fname = unquote(url.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").replace("_", " ").replace("-", " ")
    fname = re.sub(r'\s+', ' ', fname).strip()
    return fname


class LCFSRAScraper(BaseScraper):
    """Scraper for LC/FSRA — Saint Lucia Financial Services Regulatory Authority."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/pdf,*/*",
            })
        return self.session

    def _fetch_page_pdfs(self, path: str, page_type: str,
                         label: str) -> List[Tuple[str, str, str]]:
        """Fetch PDF links from a page. Returns list of (url, title, doc_type)."""
        sess = self._get_session()
        docs = []
        url = BASE_URL + path

        try:
            self.rate_limiter.wait()
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            logger.warning("Failed to fetch '%s': %s", label, e)
            return docs

        # Extract all links to PDFs
        links = re.findall(
            r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        )

        for href, raw_title in links:
            pdf_url = _normalize_url(href)
            if _should_skip(pdf_url):
                continue

            title = _clean_title(raw_title)
            if not title or len(title) < 3:
                title = _title_from_url(pdf_url)
            if not title:
                continue

            doc_type = _classify_type(title, page_type)
            docs.append((pdf_url, title, doc_type))

        logger.info("Found %d PDFs on '%s'", len(docs), label)
        return docs

    def _build_full_catalog(self) -> List[Tuple[str, str, str]]:
        catalog = []
        seen = set()

        for path, page_type, label in PAGES:
            entries = self._fetch_page_pdfs(path, page_type, label)
            for url, title, doc_type in entries:
                if url not in seen:
                    seen.add(url)
                    catalog.append((url, title, doc_type))

        logger.info("Total catalog: %d unique documents", len(catalog))
        return catalog

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"LC/FSRA/{raw['doc_id']}",
            "_source": "LC/FSRA",
            "_type": raw.get("doc_type", "doctrine"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
        }

    def _download_and_extract(self, url: str, doc_id: str,
                              doc_type: str) -> Optional[str]:
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", url, e)
            return None

        if len(resp.content) < 200:
            logger.warning("Skipping %s — too small (%d bytes)", url, len(resp.content))
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
            logger.warning("Skipping %s — not a PDF (Content-Type: %s)", url, content_type)
            return None

        table = doc_type if doc_type in ("legislation", "doctrine") else "legislation"
        text = extract_pdf_markdown(
            source="LC/FSRA",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table=table,
        ) or ""

        return text if len(text) >= 50 else None

    def fetch_all(self, sample=False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        catalog = self._build_full_catalog()

        for url, title, doc_type in catalog:
            if limit and count >= limit:
                break

            doc_id = _make_doc_id(url)
            logger.info("[%d/%d] %s", count + 1, len(catalog), title[:70])

            text = self._download_and_extract(url, doc_id, doc_type)
            if not text:
                continue

            date = _extract_year_from_title(title)

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "doc_type": doc_type,
            }
            count += 1
            logger.info("  OK: %s (%d chars)", title[:50], len(text))

        logger.info("Total records yielded: %d / %d catalog entries", count, len(catalog))

    def fetch_updates(self, since=None):
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        try:
            sess = self._get_session()
            resp = sess.get(f"{BASE_URL}/index.php/legislation", timeout=30)
            resp.raise_for_status()
            has_pdf = ".pdf" in resp.text
            logger.info("Page test: legislation — %s (has PDF links: %s)",
                        "OK" if resp.ok else "FAIL", has_pdf)

            # Test PDF download
            pdf_url = f"{BASE_URL}/images/Legislation/fsra/Financial_Services_Regulatory_Authority_Act_2011.pdf"
            self.rate_limiter.wait()
            resp = sess.get(pdf_url, timeout=30)
            resp.raise_for_status()
            pdf_ok = len(resp.content) > 1000
            logger.info("PDF test: FSRA Act 2011 (%d bytes) — %s",
                        len(resp.content), "OK" if pdf_ok else "FAIL")
            return has_pdf and pdf_ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = LCFSRAScraper()

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
