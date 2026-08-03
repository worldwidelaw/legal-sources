#!/usr/bin/env python3
"""
US/VA-AGOpinions -- Virginia Attorney General Official Opinions

Fetches the full text of official advisory opinions issued by the Attorney
General of Virginia under Va. Code sec. 2.2-505. Each opinion is the AG's
authoritative interpretation of Virginia law issued at the request of the
Governor, a member of the General Assembly, a constitutional officer or
other officials named in the statute (doctrine).

Strategy (official site www.oag.state.va.us, Joomla):
  1. The listing page /annual-reports-opinions/official-opinions links a
     per-year "article" page for every year (2008-present). A few years
     carry more than one article page (e.g. "2023-official-opinions",
     "...-official-opinions-2").
  2. Each year article page is server-rendered HTML that links every
     opinion's born-digital PDF at /files/Opinions/{YEAR}/{file}.pdf. The
     opinion number (e.g. "22-058") and the requester surname are encoded
     in the filename.
  3. For each PDF, download and extract the full opinion text (fitz/PyMuPDF;
     Tesseract OCR fallback for the rare image-only scan). The issue date is
     parsed from the "Month DD, YYYY" line in the PDF body, falling back to
     the /Opinions/{YEAR}/ folder year.

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
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VA-AGOpinions")

BASE_URL = "https://www.oag.state.va.us"
LISTING_URL = f"{BASE_URL}/annual-reports-opinions/official-opinions"

# Per-year article pages linked from the listing (Joomla view=article).
YEAR_PAGE_RE = re.compile(
    r'href="([^"]*official-opinions\?view=article[^"]*)"', re.I
)
# Opinion PDF links inside a year page: /files/Opinions/{YEAR}/{file}.pdf
PDF_HREF_RE = re.compile(
    r'href="([^"]*/files/Opinions/\d{4}/[^"]+\.pdf)"', re.I
)
# Opinion number embedded in a filename, e.g. "22-058-Youngkin-issued.pdf".
NUMBER_RE = re.compile(r"(\d{2})-(\d{3})([A-Za-z]?)")
# Issue date in the PDF body, e.g. "October 21, 2022".
BODY_DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def canonical_number(filename: str) -> str | None:
    """Canonicalise the opinion number from a PDF filename to 'YY-NNN'."""
    m = NUMBER_RE.search(filename or "")
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}{m.group(3).upper()}"


def requester_from_filename(filename: str) -> str | None:
    """Best-effort requester surname(s) from the filename.

    e.g. '22-058-Youngkin-issued.pdf' -> 'Youngkin';
         '21-102-Herring-Sullivan-Issued.pdf' -> 'Herring-Sullivan'.
    """
    stem = re.sub(r"\.pdf$", "", filename or "", flags=re.I)
    m = NUMBER_RE.search(stem)
    if not m:
        return None
    tail = stem[m.end():].strip(" -_")
    # Drop trailing "issued"/"opinion" noise tokens.
    tokens = [t for t in re.split(r"[-_ ]+", tail) if t]
    drop = {"issued", "opinion", "opinions", "final", "revised", "corrected"}
    tokens = [t for t in tokens if t.lower() not in drop]
    name = "-".join(tokens).strip("-")
    return name or None


def year_from_url(pdf_url: str) -> int | None:
    m = re.search(r"/Opinions/(\d{4})/", pdf_url, re.I)
    return int(m.group(1)) if m else None


def parse_body_date(text: str) -> str | None:
    """Return the first 'Month DD, YYYY' in the opinion body as an ISO date."""
    m = BODY_DATE_RE.search(text or "")
    if not m:
        return None
    mm = MONTHS.get(m.group(1).lower())
    dd = int(m.group(2))
    yyyy = int(m.group(3))
    if not mm or not (1 <= dd <= 31) or not (1990 <= yyyy <= 2100):
        return None
    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"


class VAAGOpinionsScraper(BaseScraper):

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

    def discover_year_pages(self) -> list:
        """Return the absolute URLs of every per-year opinion article page."""
        html = self._get_text(LISTING_URL)
        pages = set()
        for href in YEAR_PAGE_RE.findall(html or ""):
            href = unescape(href)
            pages.add(urljoin(BASE_URL, href))
        # The listing page itself sometimes links current-year PDFs directly.
        pages.add(LISTING_URL)
        return sorted(pages)

    def discover_opinions(self) -> list:
        """Walk year pages and return raw opinion metadata dicts (deduped)."""
        items = []
        seen = set()
        for page in self.discover_year_pages():
            html = self._get_text(page)
            for href in PDF_HREF_RE.findall(html or ""):
                href = unescape(href)
                pdf_url = urljoin(BASE_URL, href)
                filename = pdf_url.rsplit("/", 1)[-1]
                number = canonical_number(filename)
                if not number or number in seen:
                    continue
                seen.add(number)
                items.append(
                    {
                        "opinion_number": number,
                        "requester": requester_from_filename(filename),
                        "folder_year": year_from_url(pdf_url),
                        "pdf_url": pdf_url,
                        "url": page,
                    }
                )
        # Newest first so the framework's sample pull draws from clean modern
        # PDFs. Sort chronologically by real 4-digit year then sequence, since
        # the raw 'YY-NNN' string would rank 1999 ('99-') above 2026 ('26-').
        def _order_key(it):
            m = NUMBER_RE.search(it["opinion_number"])
            yy, seq = int(m.group(1)), int(m.group(2))
            year = 2000 + yy if yy < 50 else 1900 + yy
            return (year, seq)

        items.sort(key=_order_key, reverse=True)
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
        requester = raw.get("requester")
        title = f"Virginia AG Opinion {number}"
        if requester:
            title = f"{title} — Request of {requester.replace('-', ' & ')}"

        date = parse_body_date(text)
        if not date:
            year = raw.get("folder_year")
            if not year:
                m = re.match(r"(\d{2})-", number)
                if m:
                    yy = int(m.group(1))
                    year = 2000 + yy if yy < 50 else 1900 + yy
            if year:
                date = f"{year:04d}-01-01"

        return {
            "_id": f"US/VA-AGOpinions/{number}",
            "_source": "US/VA-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "requester": requester,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": date,
        }

    def test_api(self) -> bool:
        """Test connectivity: discover opinions and pull one opinion's text."""
        logger.info("Testing Virginia AG opinions site...")
        try:
            items = self.discover_opinions()
            if not items:
                logger.error("  Discovery returned no opinions")
                return False
            logger.info(f"  Discovery OK ({len(items)} opinions)")
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
        items = self.discover_opinions()
        logger.info(f"Discovered {len(items)} opinions to fetch")
        yield from items

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw opinion metadata for opinions on/after `since` (by year)."""
        for raw in self.fetch_all():
            year = raw.get("folder_year")
            if not since or (year and f"{year:04d}-12-31" >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/VA-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VAAGOpinionsScraper()

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
