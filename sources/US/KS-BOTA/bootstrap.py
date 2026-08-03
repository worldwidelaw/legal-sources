#!/usr/bin/env python3
"""
US/KS-BOTA -- Kansas Board of Tax Appeals (BOTA) -- Published Decisions & Orders (case_law)

Fetches the full text of the Kansas Board of Tax Appeals' curated
"Selected / Published Decisions". BOTA (formerly the Court of Tax Appeals /
COTA) is the independent quasi-judicial agency that adjudicates ALL Kansas tax
appeals -- property valuation & equalization (docket suffix EQ / PV / PVX),
divisions-of-taxation income/sales/use determinations (DT), and tax-exemption
applications (TX / PR) -- so every published document is an adjudication of a
specific case -> case_law, jurisdiction US-KS.

Distinct from US/KS-TaxRulings (Kansas Dept of Revenue interpretive guidance =
doctrine).

Access (no JavaScript, no CAPTCHA, no auth):
  The curated "Published / Selected Decisions" page
    https://bota.kansas.gov/decisions-orders/published-decisions/
  is a WordPress page that server-renders ~25 direct <a href> links to the
  decision PDFs under
    https://bota.kansas.gov/wp-content/uploads/Selected_Decisions/
  Filenames encode the year, docket, and tax-type suffix, e.g.
    2004_3806_EQ.pdf, 2005_786_DT.pdf, 2008-7226-EQ-Final-Order.pdf,
    2010-8538-PVX-Summary-Judgment.pdf, 2009_553_TX.pdf.

  Text layer: a few of the decisions are born-digital (pdfplumber extracts the
  text directly); most are SCANNED IMAGES with no text layer. common.pdf_extract
  extracts born-digital text directly and falls back to OCR (PyMuPDF ->
  pytesseract) for the scanned ones, which requires the `tesseract` binary on
  the host (export PATH to include it if installed off-PATH).

  The FULL docket corpus (beyond the ~25 curated decisions) lives behind the
  Grails/JSP search app at https://www.kansas.gov/bota-search/ ("COTA-Search"),
  a form-POST search whose result rows link the order PDFs; enumerating it is a
  follow-up beyond the curated set handled here.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py bootstrap --full     # Fetch all curated decisions
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
logger = logging.getLogger("legal-data-hunter.US.KS-BOTA")

BASE_URL = "https://bota.kansas.gov/"
INDEX_URL = "https://bota.kansas.gov/decisions-orders/published-decisions/"

MIN_TEXT_CHARS = 200

# Direct links to the decision PDFs under the Selected_Decisions upload folder.
PDF_HREF_RE = re.compile(
    r'href="(?P<url>[^"]*?/wp-content/uploads/Selected_Decisions/[^"]+?\.pdf)"',
    re.I,
)
TAG_RE = re.compile(r"<[^>]+>")

# Tax-type suffix -> human label (for context only; docket is the filename stem).
TYPE_LABELS = {
    "EQ": "Equalization Appeal",
    "PV": "Property Valuation Appeal",
    "PVX": "Property Valuation (Complaint) Appeal",
    "DT": "Division of Taxation Appeal",
    "TX": "Tax Exemption Application",
    "PR": "Payment Under Protest",
    "RE": "Real Estate",
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_from_filename(filename: str) -> dict:
    """Derive docket, tax type, and year from a Selected_Decisions filename.

    Examples:
      2004_3806_EQ.pdf                 -> year 2004, docket 2004-3806, type EQ
      2005_786_DT.pdf                  -> year 2005, docket 2005-786,  type DT
      2008-7226-EQ-Final-Order.pdf     -> year 2008, docket 2008-7226, type EQ
      2010-8538-PVX-Summary-Judgment   -> year 2010, docket 2010-8538, type PVX
    """
    stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
    tokens = re.split(r"[_-]", stem)
    year = tokens[0] if tokens and re.fullmatch(r"(19|20)\d{2}", tokens[0]) else None
    # docket = year + case number (second token if it is numeric)
    docket = None
    if year and len(tokens) >= 2 and re.fullmatch(r"\d+", tokens[1]):
        docket = f"{year}-{tokens[1]}"
    # tax type = the first token matching a known suffix
    tax_type = None
    for t in tokens[1:]:
        up = t.upper()
        if up in TYPE_LABELS:
            tax_type = up
            break
    # a readable "stage" from the remaining descriptive tokens
    desc_tokens = [t for t in tokens if t and not re.fullmatch(r"(19|20)\d{2}|\d+", t)
                   and t.upper() not in TYPE_LABELS]
    stage = " ".join(desc_tokens).replace("etal", "et al.").strip() or None
    return {"year": year, "docket": docket, "tax_type": tax_type, "stage": stage,
            "stem": stem}


class KSBotaScraper(BaseScraper):

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
            logger.error("Failed to fetch the KS BOTA published-decisions index")
            return docs
        for m in PDF_HREF_RE.finditer(page):
            url = urljoin(BASE_URL, html_module.unescape(m.group("url")))
            if url in seen:
                continue
            seen.add(url)
            filename = url.rsplit("/", 1)[-1]
            info = _parse_from_filename(filename)
            docket = info["docket"]
            type_label = TYPE_LABELS.get(info["tax_type"] or "", "Decision")
            title = f"Kansas Board of Tax Appeals — {type_label}"
            if docket:
                title += f" (Docket {docket})"
            if info["stage"]:
                title += f" — {info['stage']}"
            docs.append({
                "slug": info["stem"],
                "docket_number": docket,
                "tax_type": info["tax_type"],
                "title": title,
                "date": (f"{info['year']}-01-01" if info["year"] else None),
                "pdf_url": url,
            })
        logger.info(f"Discovered {len(docs)} KS BOTA published decisions")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/KS-BOTA",
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
        logger.info("Testing Kansas Board of Tax Appeals published decisions...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            for doc in docs[:5]:
                raw = self._build_raw(doc)
                if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                    logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                                f"{raw.get('docket_number')}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  Text extraction failed on the first 5 — the PDFs are "
                         "scanned; OCR (tesseract) must be available on this host")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        docket = raw.get("docket_number")
        title = raw.get("title") or "Kansas Board of Tax Appeals decision"
        return {
            "_id": f"US/KS-BOTA/{raw['slug']}",
            "_source": "US/KS-BOTA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "tax_type": raw.get("tax_type"),
            "court": "Kansas Board of Tax Appeals",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-KS",
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
            if sample and examined >= 25:
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

    parser = argparse.ArgumentParser(description="US/KS-BOTA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KSBotaScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
