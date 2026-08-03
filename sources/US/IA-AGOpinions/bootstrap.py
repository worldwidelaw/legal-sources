#!/usr/bin/env python3
"""
US/IA-AGOpinions -- Iowa Attorney General Opinions

Fetches the full text of the Iowa Attorney General's opinions, which the
Iowa Legislature republishes openly as born-digital annual compilation
volumes (the AG's own site routes full text through Westlaw). Each volume
gathers every published (or "unpublished") opinion the AG issued that year —
the AG's authoritative interpretations of Iowa law issued at the request of
state officers and county attorneys (doctrine).

Strategy (official site www.legis.iowa.gov):
  1. The single page /publications/attorneyGeneralOpinions is a server-
     rendered HTML table listing every volume (1896-present): one row per
     annual "Iowa Attorney General Opinions {YEAR}" / "Report {YEARS}" /
     "Unpublished Opinions {YEAR}" PDF at /docs/publications/AGO/{id}.pdf.
  2. For each volume, download the PDF and extract the full text (fitz/
     PyMuPDF; almost every volume carries a text layer, born-digital or
     pre-OCR'd — even the 800+ page mid-century volumes). One record = one
     annual volume (the volumes are the granular unit the state publishes;
     individual opinions are delimited only by inline "#YY-N-N" syllabus
     markers, not by document boundaries).

Usage:
  python bootstrap.py bootstrap            # Full pull (all volumes)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample volumes
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IA-AGOpinions")

BASE_URL = "https://www.legis.iowa.gov"
LISTING_URL = f"{BASE_URL}/publications/attorneyGeneralOpinions"

# One table row: <a href="/docs/publications/AGO/{id}.pdf">{name}</a> ... <td>{year}</td>
ROW_RE = re.compile(
    r'<a\s+href="(/docs/publications/AGO/(\d+)\.pdf)"[^>]*>([^<]+)</a>\s*'
    r'</td>\s*<td>\s*(\d{4})\s*</td>',
    re.I | re.S,
)


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class IAAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=120,
        )
        self.delay = 1.0

    def _get(self, url: str, retries: int = 4) -> bytes | None:
        """Fetch a URL (bytes) with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_text(self, url: str, retries: int = 4) -> str:
        data = self._get(url, retries=retries)
        return data.decode("utf-8", "replace") if data else ""

    def discover_volumes(self) -> list:
        """Return raw volume metadata dicts (newest first)."""
        html = self._get_text(LISTING_URL)
        items = []
        seen = set()
        for href, doc_id, name, year in ROW_RE.findall(html or ""):
            if doc_id in seen:
                continue
            seen.add(doc_id)
            items.append(
                {
                    "doc_id": doc_id,
                    "title": unescape(re.sub(r"\s+", " ", name)).strip(),
                    "year": int(year),
                    "pdf_url": BASE_URL + href,
                    "url": LISTING_URL,
                }
            )
        items.sort(key=lambda it: it["year"], reverse=True)
        return items

    def _ocr_pdf(self, doc) -> str:
        """OCR fallback for image-only scans (rare)."""
        try:
            import pytesseract
            from PIL import Image
            import io
        except Exception:
            return ""
        parts = []
        for page in doc:
            try:
                pix = page.get_pixmap(dpi=200)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                parts.append(pytesseract.image_to_string(img))
            except Exception as e:
                logger.debug(f"OCR page failed: {e}")
        return "\n".join(parts)

    def extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract full text from a volume PDF, OCR'ing image-only scans."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open failed: {e}")
            return ""
        try:
            parts = []
            for page in doc:
                parts.append(page.get_text())
                # Flush per-page layout caches so the 800+ page mid-century
                # volumes don't accumulate multiple GB of parsed layout.
                try:
                    page.clean_contents()
                except Exception:
                    pass
            text = clean_text("\n".join(parts))
            if len(text) < 200:
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def normalize(self, raw: dict) -> dict | None:
        """Download the volume PDF, extract full text, build the record.

        Runs in worker threads under bootstrap_fast, so the PDF download and
        text extraction (the expensive part) overlap across volumes.
        """
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for volume {raw['doc_id']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        year = raw["year"]
        return {
            "_id": f"US/IA-AGOpinions/{raw['doc_id']}",
            "_source": "US/IA-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "volume_year": year,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": f"{year:04d}-01-01",
        }

    def test_api(self) -> bool:
        """Test connectivity: discover volumes and pull one volume's text."""
        logger.info("Testing Iowa AG opinions listing...")
        try:
            items = self.discover_volumes()
            if not items:
                logger.error("  Discovery returned no volumes")
                return False
            logger.info(f"  Discovery OK ({len(items)} volumes)")
            rec = None
            for it in items:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"'{rec['title']}', date={rec.get('date')})"
                )
            else:
                logger.error("  Full-text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw volume metadata dicts (newest first).

        The framework (bootstrap / bootstrap_fast) calls normalize() on each
        raw dict, which downloads the PDF and extracts the full text.
        """
        items = self.discover_volumes()
        logger.info(f"Discovered {len(items)} volumes to fetch")
        yield from items

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw volume metadata for volumes on/after `since` (by year)."""
        for raw in self.fetch_all():
            if not since or f"{raw['year']:04d}-12-31" >= since:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IA-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IAAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
