#!/usr/bin/env python3
"""
BN/AGC-Legislation -- Brunei Attorney General's Chambers Legislation

Fetches consolidated Laws of Brunei from the AGC website. Laws are listed
on a single page organized by chapter number, linking to PDFs.
Full text is extracted from PDFs via common.pdf_extract.

Endpoint:
  - Listing: https://www.agc.gov.bn/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx
  - PDFs: https://www.agc.gov.bn/AGC%20Images/LAWS/ACT_PDF/...
          https://www.agc.gov.bn/AGC%20Images/LOB/PDF/...

Data:
  - ~220 chapters, ~310+ PDF documents (acts + subsidiary legislation)
  - Full text extracted from PDFs
  - Language: English (some Malay versions available)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BN.AGC-Legislation")

BASE_URL = "https://www.agc.gov.bn"
LISTING_URL = f"{BASE_URL}/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx"

# Match PDF links in the HTML
PDF_RE = re.compile(
    r'href=["\'](/AGC%20Images/[^"\']+\.pdf)["\']',
    re.IGNORECASE,
)

# Extract chapter number from URL or filename
CHAPTER_RE = re.compile(r'[Cc](?:ap|hp|hapter)\.?\s*(\d+)', re.IGNORECASE)

# Extract title from table cells - look for text near PDF links
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

# Skip Malay (BM) versions
BM_RE = re.compile(r'/\(BM\)/', re.IGNORECASE)


def strip_html(s: str) -> str:
    text = TAG_RE.sub(" ", s)
    text = html_mod.unescape(text)
    return WS_RE.sub(" ", text).strip()


def doc_id_from_url(pdf_path: str) -> str:
    """Derive a stable doc ID from the PDF URL path."""
    fname = unquote(pdf_path.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").replace(".PDF", "")
    # Clean up common patterns
    fname = fname.replace(" ", "_").replace("(", "").replace(")", "")
    return fname


def extract_chapter(pdf_path: str) -> str:
    """Extract chapter number from URL."""
    decoded = unquote(pdf_path)
    m = CHAPTER_RE.search(decoded)
    return m.group(1) if m else ""


def title_from_url(pdf_path: str) -> str:
    """Generate a readable title from the PDF URL."""
    decoded = unquote(pdf_path)
    fname = decoded.rsplit("/", 1)[-1].replace(".pdf", "").replace(".PDF", "")
    # Clean up
    title = fname.replace("_", " ").replace("  ", " ")
    # Remove common prefixes
    title = re.sub(r'^(cap|Cap|CAP|Chp|Chapter)\s*\.?\s*', 'Chapter ', title)
    return title.strip()


class BNLegislationScraper(BaseScraper):
    """Scraper for BN/AGC-Legislation -- Laws of Brunei."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _extract_titles_from_html(self, html: str) -> Dict[str, str]:
        """Extract chapter number -> title mapping from the HTML table."""
        titles = {}
        # Pattern: look for table rows with chapter numbers and titles
        # The HTML has a table structure with chapter numbers and act names
        row_re = re.compile(
            r'<td[^>]*>\s*(\d+)\s*</td>\s*<td[^>]*>(.*?)</td>',
            re.DOTALL | re.IGNORECASE,
        )
        for match in row_re.finditer(html):
            chap_num = match.group(1).strip()
            title_html = match.group(2)
            # Extract just the act name, stripping links and tags
            title = strip_html(title_html)
            # Take first meaningful line (before subsidiary legislation)
            title = title.split("  ")[0].strip()
            if title and len(title) > 3:
                titles[chap_num] = title
        return titles

    def _list_laws(self) -> List[Tuple[str, str, str]]:
        """Scrape all (title, pdf_url, chapter) from the listing page."""
        logger.info(f"Fetching listing from {LISTING_URL}")
        resp = self._get(LISTING_URL)
        html = resp.text

        # Extract title mapping from table
        title_map = self._extract_titles_from_html(html)
        logger.info(f"Extracted {len(title_map)} chapter titles from table")

        results = []
        seen = set()

        for match in PDF_RE.finditer(html):
            pdf_path = html_mod.unescape(match.group(1))

            # Skip Malay versions
            if BM_RE.search(pdf_path):
                continue

            if pdf_path in seen:
                continue
            seen.add(pdf_path)

            chapter = extract_chapter(pdf_path)
            # Try to get title from table, fall back to URL-derived title
            title = title_map.get(chapter, title_from_url(pdf_path))

            results.append((title, pdf_path, chapter))

        logger.info(f"Found {len(results)} unique PDF documents (English)")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        return {
            "_id": f"BN/AGC-Legislation/{doc_id}",
            "_source": "BN/AGC-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("pdf_url", ""),
            "doc_id": doc_id,
            "chapter": raw.get("chapter", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        laws = self._list_laws()
        if sample:
            laws = laws[:25]  # extra in case some fail

        for title, pdf_path, chapter in laws:
            if limit and count >= limit:
                break

            doc_id = doc_id_from_url(pdf_path)
            pdf_url = BASE_URL + pdf_path
            logger.info(f"  [{count+1}] Ch.{chapter} {title[:50]}...")

            try:
                text = extract_pdf_markdown(
                    source="BN/AGC-Legislation",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"    PDF extraction failed for {doc_id}: {e}")
                text = None

            if not text or len(text.strip()) < 50:
                logger.warning(f"    Skipping {doc_id} - no/short text")
                continue

            record = self.normalize({
                "title": title,
                "text": text,
                "pdf_url": pdf_url,
                "doc_id": doc_id,
                "chapter": chapter,
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No update mechanism — static PDF collection."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    scraper = BNLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        import requests
        try:
            resp = requests.get(
                LISTING_URL,
                headers={"User-Agent": "LegalDataHunter/1.0"},
                timeout=30,
            )
            print(f"Connection OK: {resp.status_code}")
        except Exception as e:
            print(f"Connection failed: {e}")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
