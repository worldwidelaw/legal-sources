#!/usr/bin/env python3
"""
US/WV-TaxAppeals -- West Virginia Office of Tax Appeals
Redacted Final Decisions (case_law)

Fetches the full text of the West Virginia Office of Tax Appeals' published
redacted final decisions. The Office of Tax Appeals is the independent state
tribunal that hears and decides all West Virginia state tax disputes (consumer
sales & use, personal income, corporate net income, business franchise,
severance, and ad valorem / property tax), so every published document is an
adjudication of a specific case -> case_law.

After a decision becomes final it is redacted and filed in the State Register
maintained by the West Virginia Secretary of State, who publishes the whole
searchable corpus online.

Access (no JavaScript, no CAPTCHA, no auth):
  The corpus is indexed on one server-rendered results page:
    https://apps.sos.wv.gov/adlaw/taxappealdecisions/
  The static HTML ships one <tr> per decision with the docket number
  (in <strong>), the "Date Issued" (M/D/YYYY), and a
  <a href="readpdf.aspx?did=N"> link to the redacted decision PDF, followed by
  a description row (<em>...</em>) summarising the appeal. The PDF itself is
  served at:
    https://apps.sos.wv.gov/adlaw/taxappealdecisions/readpdf.aspx?did=N

  Text layer: the newest decisions are born-digital (pdfplumber extracts the
  full text directly); most older decisions are SCANNED IMAGES with no text
  layer. common.pdf_extract extracts born-digital text directly and falls back
  to OCR (PyMuPDF/pdf2image -> pytesseract) for the scanned ones, which is
  gated on the `tesseract` binary being installed. Run on a vantage that has
  tesseract to obtain full text for the scanned majority; budget for OCR
  slowness (~591 decisions).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WV-TaxAppeals")

BASE_URL = "https://apps.sos.wv.gov/adlaw/taxappealdecisions/"
INDEX_URL = "https://apps.sos.wv.gov/adlaw/taxappealdecisions/"
PDF_URL = "https://apps.sos.wv.gov/adlaw/taxappealdecisions/readpdf.aspx?did={did}"

MIN_TEXT_CHARS = 200

# One decision = a <tr> with docket/date/did, optionally followed by a
# description row (<tr class="underlined"><td colspan="3"><em>...</em>).
ROW_RE = re.compile(
    r'<tr><td><strong>(?P<docket>[^<]+)</strong></td>'
    r'<td[^>]*>(?P<date>[^<]*)</td>'
    r'<td[^>]*><a href="readpdf\.aspx\?did=(?P<did>\d+)"[^>]*>View</a>.*?</td></tr>'
    r'(?:\s*<tr class="underlined"><td colspan="3"><em>(?P<desc>.*?)</em>)?',
    re.I | re.S,
)
TAG_RE = re.compile(r"<[^>]+>")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_html(s: str) -> str:
    s = TAG_RE.sub(" ", s or "")
    s = html_module.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def _parse_date(raw_date: str) -> str | None:
    """Parse the 'Date Issued' column (M/D/YYYY) into an ISO date."""
    raw_date = (raw_date or "").strip()
    if not raw_date:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw_date, fmt).date().isoformat()
        except ValueError:
            continue
    return None


class WVTaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self, sample: bool = False) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        page = self._get_text(INDEX_URL)
        if not page:
            logger.error("Failed to fetch the WV tax appeal decisions index")
            return docs
        for m in ROW_RE.finditer(page):
            did = m.group("did")
            if did in seen:
                continue
            seen.add(did)
            docket = _strip_html(m.group("docket"))
            desc = _strip_html(m.group("desc") or "")
            title = f"WV Office of Tax Appeals Decision {docket}" if docket else \
                f"WV Office of Tax Appeals Decision (did {did})"
            docs.append({
                "did": did,
                "slug": f"did-{did}",
                "docket_number": docket or None,
                "title": title,
                "description": desc or None,
                "date": _parse_date(m.group("date")),
                "pdf_url": PDF_URL.format(did=did),
            })
        logger.info(f"Discovered {len(docs)} WV Office of Tax Appeals decisions")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/WV-TaxAppeals",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        # Some readpdf.aspx?did=N ids return a placeholder image-shell PDF whose
        # only OCR'd text is the SoS "Image not found. Please try to view again"
        # boilerplate (~300 chars) — that clears MIN_TEXT_CHARS but carries no
        # decision, so drop it explicitly.
        if "image not found" in text.lower():
            logger.warning(f"Missing document (SoS 'Image not found' placeholder): {doc['slug']}")
            return None
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars) — scanned PDF, "
                           f"OCR (tesseract) required: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing West Virginia Office of Tax Appeals decisions...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            # The newest decisions are born-digital; probe from the top.
            for doc in docs[:5]:
                raw = self._build_raw(doc)
                if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                    logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                                f"{raw.get('docket_number')}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  Text extraction failed on the first 5 — the older PDFs "
                         "are scanned; OCR (tesseract) must be available on this host")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        docket = raw.get("docket_number")
        title = raw.get("title") or "West Virginia Office of Tax Appeals decision"
        return {
            "_id": f"US/WV-TaxAppeals/{raw['slug']}",
            "_source": "US/WV-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "court": "West Virginia Office of Tax Appeals",
            "title": title[:300],
            "summary": raw.get("description"),
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WV",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 60:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WV-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WVTaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
