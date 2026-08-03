#!/usr/bin/env python3
"""
US/NE-AGOpinions -- Nebraska Attorney General Opinions

Fetches the full text of official written opinions issued by the Nebraska
Attorney General under Neb. Rev. Stat. sec. 84-205. Each opinion is the
AG's authoritative interpretation of Nebraska law issued at the request
of the Legislature, a state officer or a county attorney (doctrine).

Strategy (official site ago.nebraska.gov, Drupal):
  1. The single archive page /opinions/archive is a server-rendered HTML
     table listing EVERY opinion (1990-present, ~1,190 rows). Each row
     carries the opinion number, the issue date (MM/DD/YY), the subject
     title, a detail-page (node) link and a born-digital PDF link at
     /sites/default/files/docs/opinions/{file}.pdf.
  2. For each row, download the PDF and extract the full opinion text.
     Almost every PDF carries a text layer (born-digital or pre-OCR'd by
     the AG office); the rare image-only scan is OCR'd with Tesseract.

The archive is newest-first, so the framework's sample pull draws from the
clean modern opinions. Numbers are canonicalised to "YY-NNN" (the older
five-digit "90003" display form and the modern "26-003" form both map to
the same canonical number).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NE-AGOpinions")

BASE_URL = "https://ago.nebraska.gov"
ARCHIVE_URL = f"{BASE_URL}/opinions/archive"

ROW_RE = re.compile(r"<tr[^>]*>.*?</tr>", re.S | re.I)
PDF_HREF_RE = re.compile(
    r'href="(https://ago\.nebraska\.gov/sites/default/files/docs/opinions/[^"]+\.pdf)"',
    re.I,
)
NODE_HREF_RE = re.compile(r'href="(/opinions/[a-z][^"]*)"', re.I)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")


def _cell_text(html_fragment: str) -> str:
    txt = TAG_RE.sub("", html_fragment)
    return re.sub(r"\s+", " ", BeautifulSoup(txt, "html.parser").text).strip()


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def canonical_number(raw_num: str) -> str | None:
    """Canonicalise the opinion number cell to 'YY-NNN'."""
    n = re.sub(r"\s+", "", raw_num or "")
    m = re.fullmatch(r"(\d{2})-?(\d{3})([A-Za-z]?)", n)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}{m.group(3).upper()}"


def parse_date(cell: str) -> str | None:
    """Convert a 'MM/DD/YY' cell to an ISO date, pivoting the 2-digit year."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2})", cell or "")
    if not m:
        return None
    mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    year = 2000 + yy if yy < 50 else 1900 + yy
    try:
        return f"{year:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return None


class NEAGOpinionsScraper(BaseScraper):

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
            timeout=90,
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

    def parse_archive(self, html: str) -> list:
        """Parse the archive table into a list of raw opinion metadata dicts."""
        items = []
        seen = set()
        for row in ROW_RE.findall(html or ""):
            pdf = PDF_HREF_RE.search(row)
            if not pdf:
                continue
            cells = [_cell_text(c) for c in TD_RE.findall(row)]
            cells = [c for c in cells if c]
            if not cells:
                continue
            number = canonical_number(cells[0])
            if not number or number in seen:
                continue
            seen.add(number)
            date = parse_date(cells[1]) if len(cells) > 1 else None
            title = cells[2] if len(cells) > 2 else ""
            node = NODE_HREF_RE.search(row)
            url = BASE_URL + node.group(1) if node else pdf.group(1)
            items.append(
                {
                    "opinion_number": number,
                    "date": date,
                    "title_raw": title,
                    "pdf_url": pdf.group(1),
                    "url": url,
                }
            )
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
        """Extract full text from an opinion PDF, OCR'ing image-only scans."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open failed: {e}")
            return ""
        try:
            parts = [page.get_text() for page in doc]
            text = clean_text("\n".join(parts))
            if len(text) < 200:
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def normalize(self, raw: dict) -> dict | None:
        """Download the opinion PDF, extract full text, build the record.

        Runs in worker threads under bootstrap_fast, so the PDF download and
        text extraction (the expensive part) overlap across opinions.
        """
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['opinion_number']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        number = raw["opinion_number"]
        subject = re.sub(r"\s+", " ", raw.get("title_raw") or "").strip()
        title = f"Nebraska AG Opinion {number}"
        if subject:
            short = subject if len(subject) <= 200 else subject[:197] + "..."
            title = f"{title} — {short}"
        date = raw.get("date")
        if not date:
            m = re.match(r"(\d{2})-", number)
            if m:
                yy = int(m.group(1))
                year = 2000 + yy if yy < 50 else 1900 + yy
                date = f"{year:04d}-01-01"

        return {
            "_id": f"US/NE-AGOpinions/{number}",
            "_source": "US/NE-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "subject": subject or None,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": date,
        }

    def test_api(self) -> bool:
        """Test connectivity: parse the archive and pull one opinion's text."""
        logger.info("Testing Nebraska AG opinions archive...")
        try:
            html = self._get_text(ARCHIVE_URL)
            items = self.parse_archive(html)
            if not items:
                logger.error("  Archive parse returned no opinions")
                return False
            logger.info(f"  Archive parse OK ({len(items)} opinions)")
            rec = None
            for it in items:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"{rec['opinion_number']}, date={rec.get('date')})"
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
        """Yield raw opinion metadata dicts (newest first).

        The framework (bootstrap / bootstrap_fast) calls normalize() on each
        raw dict, which downloads the PDF and extracts the full text.
        """
        html = self._get_text(ARCHIVE_URL)
        items = self.parse_archive(html)
        logger.info(f"Archive: {len(items)} opinions to fetch")
        yield from items

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw opinion metadata issued on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NE-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NEAGOpinionsScraper()

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
