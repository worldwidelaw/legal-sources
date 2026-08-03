#!/usr/bin/env python3
"""
US/WI-TaxAppeals -- Wisconsin Tax Appeals Commission
Rulings & Orders (case_law)

Fetches the full text of the Wisconsin Tax Appeals Commission's published
adjudicative decisions. The Commission is the independent state agency that
hears and decides all Wisconsin state tax disputes (income/franchise,
sales & use, and property/manufacturing), so every published document is an
adjudication of a specific case -> case_law.

Both the Commission's own Rulings & Orders and the affirming Court of
Appeals / Circuit Court / Supreme Court opinions (published alongside them)
are official public records of the State of Wisconsin.

Access (no JavaScript, no CAPTCHA, no auth):
  The corpus is indexed on 26 server-rendered alphabetical pages:
    https://taxappeals.wi.gov/Pages/Decision%20Pages/All/{A..Z}.aspx
  Each page ships direct <a href="/Documents/....pdf"> links in the static
  HTML. The PDFs live under https://taxappeals.wi.gov/Documents/... in
  era-partitioned folders (Decisions/1987-1997/, 1998-2002/, 2003-2021/) plus
  recent loose /Documents/*.pdf files.

  The filename encodes the metadata, e.g.
    "Abukhamireh, Munem 20.I.188.pdf"                -> docket 20.I.188
    "Adumat, Gerald 91.I.247 092691 TAC.pdf"         -> date 1991-09-26, TAC
    "A GAGLIANO CO INC ... 012203 TAC.pdf"           -> date 2003-01-22, TAC
  Suffixes: TAC = Commission ruling; CT APP / CIRCUIT / SUPREME = the
  reviewing court's opinion on the same case.

  CAVEAT -- OCR REQUIRED: the decision PDFs are SCANNED IMAGES with no text
  layer (pdfplumber/pypdf return 0 chars). Full text is obtained via the OCR
  fallback in common.pdf_extract (PyMuPDF/pdf2image -> pytesseract), which is
  gated on the `tesseract` binary being installed. Run on a vantage that has
  tesseract; budget for OCR slowness across the multi-thousand-decision
  corpus (consider per-letter/era partitioning to fit a fleet slot).

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
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WI-TaxAppeals")

BASE_URL = "https://taxappeals.wi.gov"
INDEX_URL = "https://taxappeals.wi.gov/Pages/Decision%20Pages/All/{letter}.aspx"
LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

MIN_TEXT_CHARS = 200
HREF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
# A standalone 6-digit MMDDYY token in the filename (the decision date).
DATE_TOK_RE = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")
# Known trailing suffixes marking who authored the document.
SUFFIX_RE = re.compile(
    r"\b(TAC|CT\s*APP|COURT\s*OF\s*APPEALS|CIRCUIT|CIRCUIT\s*COURT|SUPREME|"
    r"SUPREME\s*COURT|REMAND|ORDER|DECISION)\b",
    re.I,
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _slug(pdf_url: str) -> str:
    base = unquote(pdf_url).rsplit("/", 1)[-1].rsplit(".", 1)[0]
    base = re.sub(r"[^A-Za-z0-9._,-]+", "-", base).strip("-")[:100]
    return base


def _parse_date(filename: str) -> str | None:
    """Extract an ISO date from a MMDDYY token in the filename, if present."""
    for m in DATE_TOK_RE.finditer(filename):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            continue
        # Corpus spans 1984-present: pivot YY<40 -> 20YY else 19YY.
        year = 2000 + yy if yy < 40 else 1900 + yy
        try:
            return datetime(year, mm, dd).date().isoformat()
        except ValueError:
            continue
    return None


def _authoring_body(filename: str) -> str:
    m = SUFFIX_RE.search(filename)
    if not m:
        # No explicit suffix: default to the Commission itself.
        return "Wisconsin Tax Appeals Commission"
    tag = re.sub(r"\s+", " ", m.group(1).upper()).strip()
    if tag == "TAC" or tag in ("ORDER", "DECISION", "REMAND"):
        return "Wisconsin Tax Appeals Commission"
    if tag in ("CT APP", "COURT OF APPEALS"):
        return "Wisconsin Court of Appeals"
    if tag in ("CIRCUIT", "CIRCUIT COURT"):
        return "Wisconsin Circuit Court"
    if tag in ("SUPREME", "SUPREME COURT"):
        return "Wisconsin Supreme Court"
    return "Wisconsin Tax Appeals Commission"


def _title_and_docket(filename: str) -> tuple[str, str | None]:
    """Build a human title and best-effort docket from the filename stem."""
    stem = unquote(filename).rsplit("/", 1)[-1].rsplit(".", 1)[0]
    stem = re.sub(r"\s+", " ", stem).strip()
    # Docket patterns seen: 20.I.188, 07.T.141, 91.I.247, 25I086, 00M04, 20.S.44
    dm = re.search(r"\b\d{2}\.?[A-Z]{1,2}\.?\d{2,4}\b", stem)
    docket = dm.group(0) if dm else None
    return stem[:300], docket


class WITaxAppealsScraper(BaseScraper):

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
        letters = LETTERS[:3] if sample else LETTERS
        for letter in letters:
            page = self._get_text(INDEX_URL.format(letter=letter))
            if not page:
                logger.warning(f"Failed to fetch index page for '{letter}'")
                continue
            hrefs = HREF_RE.findall(page)
            new = 0
            for href in hrefs:
                pdf_url = urljoin(BASE_URL, html_module.unescape(href.strip()))
                # Only Commission document PDFs live under /Documents/.
                if "/Documents/" not in pdf_url and "/documents/" not in pdf_url:
                    continue
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                filename = pdf_url.rsplit("/", 1)[-1]
                title, docket = _title_and_docket(filename)
                docs.append({
                    "pdf_url": pdf_url,
                    "slug": _slug(pdf_url),
                    "title": title,
                    "docket_number": docket,
                    "date": _parse_date(unquote(filename)),
                    "authoring_body": _authoring_body(unquote(filename)),
                })
                new += 1
            logger.info(f"Discovered {new} PDFs on index page '{letter}'")
            if sample and len(docs) >= 40:
                break
        logger.info(f"Discovered {len(docs)} total WI Tax Appeals documents")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/WI-TaxAppeals",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars) — scanned PDF, "
                           f"OCR (tesseract) required: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Wisconsin Tax Appeals Commission decisions...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('docket_number')}")
            else:
                logger.error("  Text extraction failed or too short — the PDFs are "
                             "scanned; OCR (tesseract) must be available on this host")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        docket = raw.get("docket_number")
        body = raw.get("authoring_body") or "Wisconsin Tax Appeals Commission"
        title = raw.get("title") or "Wisconsin Tax Appeals Commission decision"
        if docket and docket not in title:
            full_title = f"{title} ({docket})"
        else:
            full_title = title
        return {
            "_id": f"US/WI-TaxAppeals/{raw['slug']}",
            "_source": "US/WI-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "court": body,
            "title": full_title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WI",
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
            if sample and examined >= 40:
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

    parser = argparse.ArgumentParser(description="US/WI-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WITaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
