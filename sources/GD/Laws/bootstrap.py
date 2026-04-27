#!/usr/bin/env python3
"""
GD/Laws -- Laws of Grenada (Attorney General)

Fetches consolidated legislation from the official Laws of Grenada portal
at laws.gov.gd. Chapters are organized alphabetically (A-Y, ~345 chapters).
Each chapter page has a PDF download link. Full text extracted via
common.pdf_extract.

Endpoint:
  - Chapter listings: https://laws.gov.gd/index.php/chapters/{letter-range}
  - PDF downloads: https://laws.gov.gd/index.php/chapters/{range}/{id}-{slug}/download

Data:
  - ~345 consolidated chapters (statutes)
  - Full text extracted from PDFs
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_mod
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GD.Laws")

BASE_URL = "https://laws.gov.gd"

# Alphabetical chapter listing pages
CHAPTER_PAGES = [
    "/index.php/chapters/a-1-22a",
    "/index.php/chapters/b-23-39",
    "/index.php/chapters/c-39a-75d",
    "/index.php/chapters/d-76-84a",
    "/index.php/chapters/e-85-98a",
    "/index.php/chapters/f-99-119",
    "/index.php/chapters/g-120-132",
    "/index.php/chapters/h-132a-143",
    "/index.php/chapters/i-144-155",
    "/index.php/chapters/j-156",
    "/index.php/chapters/l-157-176",
    "/index.php/chapters/m-177-202d",
    "/index.php/chapters/n-203-216",
    "/index.php/chapters/o-217-223",
    "/index.php/chapters/p-224-269a",
    "/index.php/chapters/q-270-272",
    "/index.php/chapters/r-273-292",
    "/index.php/chapters/s-292a-315a",
    "/index.php/chapters/t-315b-329",
    "/index.php/chapters/u-330-332",
    "/index.php/chapters/v-333-333b",
    "/index.php/chapters/w-334-344",
    "/index.php/chapters/y-345",
]

# Regex to find chapter links on listing pages
CHAPTER_LINK_RE = re.compile(
    r'<a[^>]+href=["\'](/index\.php/chapters/[^"\']+/(\d+)-[^"\']+)["\'][^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Regex to find download link on chapter detail page
DOWNLOAD_LINK_RE = re.compile(
    r'<a[^>]+href=["\'](/index\.php/chapters/[^"\']+/download)["\']',
    re.DOTALL | re.IGNORECASE,
)

# Alternative: find viewdocument link and derive download
VIEWDOC_LINK_RE = re.compile(
    r'<a[^>]+href=["\'](/index\.php/chapters/[^"\']+/viewdocument/\d+)["\']',
    re.DOTALL | re.IGNORECASE,
)

# Regex to find any download link (more permissive)
ANY_DOWNLOAD_RE = re.compile(
    r'<a[^>]+href=["\'](/index\.php/[^"\']+/download)["\']',
    re.DOTALL | re.IGNORECASE,
)

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
CHAPTER_NUM_RE = re.compile(r"chapter\s+(\d+[a-zA-Z]?)", re.IGNORECASE)


def strip_html(s: str) -> str:
    text = TAG_RE.sub(" ", s)
    text = html_mod.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def extract_chapter_number(title: str) -> str:
    m = CHAPTER_NUM_RE.search(title)
    return m.group(1) if m else ""


class GDLawsScraper(BaseScraper):
    """Scraper for GD/Laws -- Laws of Grenada."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36 "
                          "LegalDataHunter/1.0",
        })

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _list_chapters(self) -> List[Dict[str, str]]:
        """Crawl all chapter listing pages and collect chapter detail URLs."""
        chapters = []
        seen_ids = set()

        for page_path in CHAPTER_PAGES:
            url = BASE_URL + page_path
            logger.info(f"Fetching chapter listing: {page_path}")
            try:
                resp = self._get(url)
            except Exception as e:
                logger.warning(f"  Failed to fetch {page_path}: {e}")
                continue

            for match in CHAPTER_LINK_RE.finditer(resp.text):
                detail_path = match.group(1)
                item_id = match.group(2)
                raw_title = strip_html(match.group(3))

                if item_id in seen_ids:
                    continue
                seen_ids.add(item_id)

                chapters.append({
                    "detail_path": detail_path,
                    "item_id": item_id,
                    "title": raw_title,
                    "category": page_path.split("/")[-1],
                })

        logger.info(f"Found {len(chapters)} chapters total")
        return chapters

    def _find_download_url(self, detail_path: str) -> str:
        """Visit a chapter detail page and find the PDF download URL."""
        url = BASE_URL + detail_path
        try:
            resp = self._get(url)
        except Exception as e:
            logger.warning(f"  Failed to fetch detail page {detail_path}: {e}")
            return ""

        # Look for download link
        m = ANY_DOWNLOAD_RE.search(resp.text)
        if m:
            return BASE_URL + m.group(1)

        # Try viewdocument link and convert to download
        m = VIEWDOC_LINK_RE.search(resp.text)
        if m:
            view_path = m.group(1)
            # /index.php/chapters/.../viewdocument/994 -> /index.php/chapters/.../download
            # Extract the parent path: everything before /viewdocument/
            parent = view_path.rsplit("/viewdocument/", 1)[0]
            return BASE_URL + parent + "/download"

        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        chapter_num = extract_chapter_number(raw.get("title", ""))
        return {
            "_id": f"GD/Laws/{doc_id}",
            "_source": "GD/Laws",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("page_url", ""),
            "doc_id": doc_id,
            "chapter_number": chapter_num,
            "pdf_url": raw.get("pdf_url", ""),
            "category": raw.get("category", "chapters"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        chapters = self._list_chapters()
        if sample:
            chapters = chapters[:25]  # extra buffer in case some fail

        for ch in chapters:
            if limit and count >= limit:
                break

            title = ch["title"]
            detail_path = ch["detail_path"]
            item_id = ch["item_id"]

            logger.info(f"  [{count+1}] {title}")

            pdf_url = self._find_download_url(detail_path)
            if not pdf_url:
                logger.warning(f"    No download link found for {title}")
                continue

            doc_id = f"cap-{item_id}"

            # Download PDF ourselves (server requires browser-like UA)
            try:
                pdf_resp = self._get(pdf_url)
                pdf_bytes = pdf_resp.content
                if len(pdf_bytes) < 100:
                    logger.warning(f"    PDF too small ({len(pdf_bytes)} bytes) for {title}")
                    continue
            except Exception as e:
                logger.warning(f"    PDF download failed for {doc_id}: {e}")
                continue

            try:
                text = extract_pdf_markdown(
                    source="GD/Laws",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"    PDF extraction failed for {doc_id}: {e}")
                text = None

            if not text or len(text.strip()) < 50:
                logger.warning(f"    Skipping {doc_id} - no/short text ({len(text.strip()) if text else 0} chars)")
                continue

            record = self.normalize({
                "title": title,
                "text": text,
                "pdf_url": pdf_url,
                "page_url": BASE_URL + detail_path,
                "doc_id": doc_id,
                "category": ch.get("category", ""),
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No incremental update mechanism — static PDF collection."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    scraper = GDLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing connectivity to laws.gov.gd...")
        resp = scraper._get(BASE_URL)
        print(f"Status: {resp.status_code}, Length: {len(resp.text)}")
        print("OK" if resp.status_code == 200 else "FAILED")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
