#!/usr/bin/env python3
"""
US/KY-PSC -- Kentucky Public Service Commission Orders

Fetches the full text of Orders issued by the Kentucky Public Service
Commission (PSC) adjudicating utility rate cases, certificate applications,
complaints and other dockets across the electric, natural-gas, water/sewer and
telecommunications industries. Each Order is an administrative adjudication of
a specific case by the Commission = case_law. Public domain (US state
government edict).

Strategy (official "Order Vault", psc.ky.gov):
  1. The PSC publishes every Order as a born-digital PDF under
     https://psc.ky.gov/order_vault/Orders_{YYYY}/. Each year folder is an
     IIS directory listing (server-rendered <pre> of <a href> links), one
     link per Order PDF.
  2. The PDF filename encodes the case number and the order date:
     "{CCCCNNNNN}_{MMDDYYYY}[_NN].pdf" — the first 9 digits are the case
     number (4-digit case-year + 5-digit sequence, e.g. 202600097 ->
     2026-00097), the next 8 digits are the order date, and an optional _NN
     suffix distinguishes multiple orders issued in the same case on the
     same day.
  3. fetch_all() walks the year folders newest-first (back to the vault
     floor, ~1989), yields one raw dict per Order PDF. normalize() downloads
     the PDF and extracts the full text (fitz/PyMuPDF; Tesseract OCR fallback
     for the rare image-only scan). The authoritative case number is parsed
     from the Order body ("CASE NO. ...") with the filename code as fallback.

Newest-first iteration means the framework's sample pull draws from clean
modern born-digital Orders.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.KY-PSC")

BASE_URL = "https://psc.ky.gov"
VAULT_URL = f"{BASE_URL}/order_vault/Orders_"

# Corpus floor: the Order Vault has year folders from 1980, but real content
# starts in the late 1980s.
FIRST_YEAR = 1989

# Directory-listing anchor: /order_vault/Orders_YYYY/{9digits}_{8digits}[_NN].pdf
HREF_RE = re.compile(
    r'href="(/order_vault/Orders_\d{4}/(\d{9})_(\d{8})(?:_(\d+))?\.pdf)"',
    re.I,
)
CASENO_RE = re.compile(r"CASE\s+NO\.?\s*[:\s]*([0-9]{4}-[0-9]{5})", re.I)


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_filename_date(mmddyyyy: str) -> str | None:
    """Convert an 'MMDDYYYY' filename token to an ISO date."""
    if not mmddyyyy or len(mmddyyyy) != 8:
        return None
    mm, dd, yyyy = mmddyyyy[0:2], mmddyyyy[2:4], mmddyyyy[4:8]
    try:
        m, d, y = int(mm), int(dd), int(yyyy)
        if not (1 <= m <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100):
            return None
        return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        return None


class KYPSCScraper(BaseScraper):

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
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
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

    def _list_year(self, year: int) -> list:
        """Parse a year's Order Vault directory listing into raw dicts."""
        html = self._get_text(f"{VAULT_URL}{year}/")
        items = []
        seen = set()
        for m in HREF_RE.finditer(html):
            path, casecode, mmddyyyy, suffix = m.groups()
            if path in seen:
                continue
            seen.add(path)
            stem = path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            case_number = f"{casecode[:4]}-{casecode[4:]}"
            items.append(
                {
                    "stem": stem,
                    "case_number": case_number,
                    "date": parse_filename_date(mmddyyyy),
                    "suffix": suffix,
                    "pdf_url": BASE_URL + path,
                    "url": BASE_URL + path,
                }
            )
        return items

    def _ocr_pdf(self, doc) -> str:
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
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        if pdf_bytes[:5] != b"%PDF-":
            logger.debug(f"{raw['stem']}: not a PDF payload")
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['stem']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        # Authoritative case number from the Order body, fallback to filename code.
        case_number = raw.get("case_number")
        cm = CASENO_RE.search(text[:1500])
        if cm:
            case_number = cm.group(1)

        # Title from the "In the Matter of:" caption when available.
        title = f"KY PSC Order — Case No. {case_number}"
        mm = re.search(r"In the Matter of:?\s*(.+?)\)", text[:1200], re.S | re.I)
        if mm:
            caption = re.sub(r"\s+", " ", mm.group(1)).strip(" .)-")
            caption = re.sub(r"\s*\)\s*$", "", caption)
            if 5 <= len(caption) <= 300:
                title = f"KY PSC Order — Case No. {case_number}: {caption}"

        return {
            "_id": f"US/KY-PSC/{raw['stem']}",
            "_source": "US/KY-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "title": title,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    def test_api(self) -> bool:
        logger.info("Testing KY PSC Order Vault...")
        try:
            rows = []
            year = datetime.now(timezone.utc).year
            while year >= FIRST_YEAR and not rows:
                rows = self._list_year(year)
                if rows:
                    logger.info(f"  Orders_{year}: {len(rows)} PDFs")
                    break
                year -= 1
            if not rows:
                logger.error("  No Orders found in recent years")
                return False
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"case {rec['case_number']}, date={rec.get('date')})"
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
        """Yield raw Order metadata dicts, newest year first.

        The framework calls normalize() on each raw dict, which downloads the
        PDF and extracts the full text.
        """
        seen = set()
        empty_streak = 0
        year = datetime.now(timezone.utc).year
        while year >= FIRST_YEAR:
            rows = self._list_year(year)
            if not rows:
                empty_streak += 1
                if empty_streak >= 5:
                    logger.info("Reached 5 consecutive empty years; stopping.")
                    break
                year -= 1
                continue
            empty_streak = 0
            for r in rows:
                if r["stem"] in seen:
                    continue
                seen.add(r["stem"])
                yield r
            year -= 1

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KY-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KYPSCScraper()

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
