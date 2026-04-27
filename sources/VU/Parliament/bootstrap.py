#!/usr/bin/env python3
"""
VU/Parliament -- Vanuatu Parliament Bills

Fetches parliamentary bill PDFs from parliament.gov.vu and extracts full text.

Strategy:
  1. Parse the bills listing page for PDF links (2025 sessions)
  2. Crawl /images/Bills/ and /images/Bills for 2024 Second ordinary/
     directory listings for additional PDFs
  3. Download each PDF and extract text via pdfplumber
  4. Normalize into standard schema

Data:
  - ~80-100 bill PDFs (EN + FR versions) from 2024-2025 sessions
  - Topics: finance, taxation, data protection, cybercrime, land reform,
    electoral law, child protection, intellectual property, etc.
  - Languages: English and French (bilingual parliament)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import hashlib
import logging
import time
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, unquote

import requests
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VU.Parliament")

BASE_URL = "https://parliament.gov.vu"
BILLS_PAGE = f"{BASE_URL}/index.php/parliamentary-business/bills"
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB

# Directory listings with self-hosted bill PDFs
BILL_DIRS = [
    "/images/Bills/",
    "/images/Bills%20for%202024%20Second%20ordinary/",
    "/images/Bills/Second%20Ordinary%20session%202025/",
]


class VUParliamentScraper(BaseScraper):
    """Scraper for VU/Parliament -- Vanuatu Parliament Bills."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _wait(self):
        """Rate limiter."""
        time.sleep(self.config.get("fetch", {}).get("rate_limit", 2.0))

    def _get_pdfs_from_listing_page(self) -> List[Dict[str, str]]:
        """Get PDF links from the main bills listing page."""
        self._wait()
        resp = self.session.get(BILLS_PAGE, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        pdfs = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower():
                continue
            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            # Try to get title from nearby text
            title = a.get_text(strip=True)
            if not title:
                # Extract from URL filename
                title = self._title_from_url(full_url)

            pdfs.append({"url": full_url, "title": title})

        logger.info(f"Found {len(pdfs)} PDFs from bills listing page")
        return pdfs

    def _crawl_directory(self, dir_url: str, depth: int = 0) -> List[Dict[str, str]]:
        """Crawl an Apache directory listing for PDF files."""
        if depth > 2:
            return []

        self._wait()
        pdfs = []
        try:
            resp = self.session.get(dir_url, timeout=30)
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("?") or href == "/images/":
                    continue
                full = urljoin(dir_url if dir_url.endswith("/") else dir_url + "/", href)
                if href.lower().endswith(".pdf"):
                    title = self._title_from_url(full)
                    pdfs.append({"url": full, "title": title})
                elif href.endswith("/") and depth < 2:
                    pdfs.extend(self._crawl_directory(full, depth + 1))
        except Exception as e:
            logger.warning(f"Error crawling {dir_url}: {e}")
        return pdfs

    def _title_from_url(self, url: str) -> str:
        """Extract a human-readable title from a PDF URL."""
        filename = unquote(url.split("/")[-1])
        if filename.lower().endswith(".pdf"):
            filename = filename[:-4]
        # Clean up underscores and extra spaces
        title = filename.replace("_", " ").strip()
        # Remove repeated spaces
        title = re.sub(r"\s+", " ", title)
        return title

    def _make_doc_id(self, url: str) -> str:
        """Generate a stable document ID from URL."""
        path = unquote(url.replace(BASE_URL, ""))
        # Clean up path for ID
        path = path.replace("/images/", "").replace("/", "__")
        path = re.sub(r"[^\w\-.]", "_", path)
        if path.lower().endswith(".pdf"):
            path = path[:-4]
        # Truncate if too long
        if len(path) > 120:
            path = path[:100] + "_" + hashlib.md5(path.encode()).hexdigest()[:8]
        return path

    def _extract_year(self, title: str, url: str) -> Optional[str]:
        """Extract year from title or URL."""
        for text in [title, url]:
            years = re.findall(r"\b(20[12]\d)\b", text)
            if years:
                return years[-1]
        return None

    def _detect_language(self, title: str, text: str) -> str:
        """Detect if document is French or English."""
        fr_indicators = ["projet de loi", "relatif", "modification", "abrogation",
                         "dispositions", "loi no"]
        combined = (title + " " + text[:500]).lower()
        fr_count = sum(1 for ind in fr_indicators if ind in combined)
        return "fr" if fr_count >= 2 else "en"

    def _download_and_extract(self, url: str) -> Optional[str]:
        """Download a PDF and extract text with pdfplumber."""
        self._wait()
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"PDF download returned {resp.status_code}: {url}")
                return None
            if len(resp.content) > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                return None
            if len(resp.content) < 1000:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None

            pdf = pdfplumber.open(io.BytesIO(resp.content))
            text_parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
            pdf.close()

            text = "\n".join(text_parts)
            return text if len(text) >= 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def _collect_all_pdfs(self) -> List[Dict[str, str]]:
        """Collect PDFs from all sources, deduplicated by URL."""
        seen = set()
        all_pdfs = []

        # 1. From the main bills listing page
        for pdf in self._get_pdfs_from_listing_page():
            if pdf["url"] not in seen:
                seen.add(pdf["url"])
                all_pdfs.append(pdf)

        # 2. From directory listings
        for dir_path in BILL_DIRS:
            dir_url = BASE_URL + dir_path
            logger.info(f"Crawling directory: {unquote(dir_path)}")
            for pdf in self._crawl_directory(dir_url):
                if pdf["url"] not in seen:
                    seen.add(pdf["url"])
                    all_pdfs.append(pdf)

        logger.info(f"Total unique PDFs collected: {len(all_pdfs)}")
        return all_pdfs

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all bill documents with full text."""
        pdfs = self._collect_all_pdfs()
        count = 0
        skipped = 0

        for info in pdfs:
            text = self._download_and_extract(info["url"])
            if not text:
                skipped += 1
                continue

            doc_id = self._make_doc_id(info["url"])
            year = self._extract_year(info["title"], info["url"])
            lang = self._detect_language(info["title"], text)

            count += 1
            yield {
                "_id": doc_id,
                "_url": info["url"],
                "_title": info["title"],
                "_text": text,
                "_year": year,
                "_lang": lang,
            }

            if count % 10 == 0:
                logger.info(f"  {count} bills fetched ({skipped} skipped)")

        logger.info(f"Total: {count} bills with text ({skipped} skipped)")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all (no incremental updates available)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw bill data into standard schema."""
        text = raw.get("_text", "").strip()
        if not text:
            return None

        year = raw.get("_year")
        date_iso = f"{year}-01-01" if year else None

        return {
            "_id": f"VU/Parliament/{raw['_id']}",
            "_source": "VU/Parliament",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["_title"],
            "text": text,
            "date": date_iso,
            "url": raw["_url"],
            "language": raw.get("_lang", "en"),
            "authority": "Parliament of the Republic of Vanuatu",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="VU/Parliament Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VUParliamentScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records -- {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
