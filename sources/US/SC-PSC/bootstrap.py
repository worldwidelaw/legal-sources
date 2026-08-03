#!/usr/bin/env python3
"""
US/SC-PSC -- South Carolina Public Service Commission Orders

Fetches the full text of Orders issued by the South Carolina Public Service
Commission (PSC) adjudicating utility rate, complaint and certificate dockets
across the electric, gas, water/sewer, telecommunications and transportation
industries. Each Order is an administrative adjudication of a specific docket
by the Commission (or a Hearing Officer Directive on a docket) = case_law.

Strategy (official DMS: dms.psc.sc.gov, ASP.NET MVC):
  1. The Orders search endpoint /Web/Orders/Search is a GET form that returns
     a server-rendered HTML datatable. Filtering by an issue-date window
     (StartDate/EndDate, M/D/YYYY) returns every Order in that window: the
     Order number (e.g. "2024-1", "2024-1H"), industry, a summary (order
     title + docket description), the issue date, and a direct attachment
     link /Attachments/Order/{guid} to the born-digital Order PDF.
  2. The result set is capped at 1000 rows, so fetch_all() walks the corpus
     one month at a time (newest first) to stay under the cap; the modern
     PSC issues ~100-150 Orders/month at peak, well below the limit.
  3. For each row, download the Order PDF and extract the full text
     (fitz/PyMuPDF; born-digital text layer, Tesseract OCR fallback for the
     rare image-only scan). The docket number is parsed from the PDF body.

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
import calendar
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
logger = logging.getLogger("legal-data-hunter.US.SC-PSC")

BASE_URL = "https://dms.psc.sc.gov"
SEARCH_URL = f"{BASE_URL}/Web/Orders/Search"

# Corpus floor: the SC PSC DMS Orders series begins in the late 1980s.
FIRST_YEAR = 1985

ORDERNUM_RE = re.compile(r"showOrderDetail\('Order ([^']+)',\s*'(\d+)'\)")
ATTLINK_RE = re.compile(r'href="(/Attachments/Order/[0-9a-fA-F-]+)"')
DOCKET_RE = re.compile(r"DOCKET\s+NO\.?\s*([0-9]{4}-[0-9]+-[A-Z]+)", re.I)


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(cell: str) -> str | None:
    """Convert an 'M/D/YYYY' cell to an ISO date."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", cell or "")
    if not m:
        return None
    mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return None


def _month_windows():
    """Yield (start, end) 'M/D/YYYY' strings, newest month first, back to FIRST_YEAR."""
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    while year >= FIRST_YEAR:
        last = calendar.monthrange(year, month)[1]
        yield (f"{month}/1/{year}", f"{month}/{last}/{year}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1


class SCPSCScraper(BaseScraper):

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

    def parse_results(self, html: str) -> list:
        """Parse an Orders search results page into raw metadata dicts."""
        items = []
        soup = BeautifulSoup(html or "", "html.parser")
        table = soup.find("table", class_=re.compile("datatable-standard"))
        if not table:
            return items
        body = table.find("tbody")
        if not body:
            return items
        for tr in body.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            # Order number from the showOrderDetail() link
            a = tds[0].find("a")
            m = ORDERNUM_RE.search(a.get("href", "")) if a else None
            if not m:
                continue
            order_number = m.group(1).strip()
            internal_id = m.group(2)

            industry = tds[1].get_text(" ", strip=True)
            summary = tds[2].get_text(" ", strip=True)
            date = parse_date(tds[3].get_text(" ", strip=True))

            # Attachment PDF link (take the first)
            att = tds[4].find("a", class_="attlink")
            att_href = att.get("href") if att else None
            if not att_href:
                am = ATTLINK_RE.search(str(tds[4]))
                att_href = am.group(1) if am else None
            if not att_href:
                continue

            items.append(
                {
                    "order_number": order_number,
                    "internal_id": internal_id,
                    "industry": industry or None,
                    "summary": summary or None,
                    "date": date,
                    "pdf_url": BASE_URL + att_href,
                    "url": f"{BASE_URL}/Web/Orders/Detail?id={internal_id}",
                }
            )
        return items

    def _fetch_window(self, start: str, end: str) -> list:
        url = (
            f"{SEARCH_URL}?StartDate={start}&EndDate={end}"
            f"&Number=&SearchKeyword="
        )
        html = self._get_text(url)
        rows = self.parse_results(html)
        if len(rows) >= 1000:
            logger.warning(
                f"Window {start}..{end} hit the 1000-row cap; may be truncated"
            )
        return rows

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
        """Extract full text from an Order PDF, OCR'ing image-only scans."""
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
        """Download the Order PDF, extract full text, build the record."""
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for Order {raw['order_number']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        order_number = raw["order_number"]
        summary = re.sub(r"\s+", " ", raw.get("summary") or "").strip()
        title = f"SC PSC Order No. {order_number}"
        if summary:
            short = summary if len(summary) <= 250 else summary[:247] + "..."
            title = f"{title} — {short}"

        docket = None
        dm = DOCKET_RE.search(text[:2000])
        if dm:
            docket = dm.group(1).upper()

        return {
            "_id": f"US/SC-PSC/{order_number}",
            "_source": "US/SC-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "order_number": order_number,
            "docket_number": docket,
            "industry": raw.get("industry"),
            "title": title,
            "summary": summary or None,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    def test_api(self) -> bool:
        """Test connectivity: search a recent window and pull one Order's text."""
        logger.info("Testing SC PSC Orders search...")
        try:
            rows = []
            for start, end in _month_windows():
                rows = self._fetch_window(start, end)
                if rows:
                    logger.info(f"  Window {start}..{end}: {len(rows)} Orders")
                    break
            if not rows:
                logger.error("  No Orders found in recent windows")
                return False
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"Order {rec['order_number']}, docket={rec.get('docket_number')}, "
                    f"date={rec.get('date')})"
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
        """Yield raw Order metadata dicts, newest month first.

        The framework calls normalize() on each raw dict, which downloads the
        PDF and extracts the full text.
        """
        seen = set()
        empty_streak = 0
        for start, end in _month_windows():
            rows = self._fetch_window(start, end)
            if not rows:
                empty_streak += 1
                # Stop once we hit a long run of empty months near the floor.
                if empty_streak >= 24:
                    logger.info("Reached 24 consecutive empty months; stopping.")
                    break
                continue
            empty_streak = 0
            for r in rows:
                key = r["order_number"]
                if key in seen:
                    continue
                seen.add(key)
                yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw Order metadata issued on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/SC-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCPSCScraper()

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
