#!/usr/bin/env python3
"""
IE/RTB -- Residential Tenancies Board of Ireland (Determination Orders)

The Residential Tenancies Board (RTB) is Ireland's statutory dispute-resolution
body for the private/AHB/cost/SSA residential rental sector (Residential
Tenancies Act 2004 as amended). It resolves landlord/tenant disputes through
adjudication and Tenancy Tribunal hearings; the binding outcome of each dispute
is issued as a **Determination Order** under s.121 of the Act. Determination
Orders are legally binding on the parties and enforceable in the Circuit Court —
i.e. adjudicative case law.

The RTB publishes the full Determination Orders on its website as one WordPress
post per order, with the order document attached as a PDF. The corpus is large:

    Adjudication Orders                     ~16,400
    Tribunal Orders                         ~5,100
    Court Decisions (Enforcement of Orders) ~33
    -----------------------------------------------
    Total                                   ~21,500 orders (2015-present)

Strategy:
  - Enumerate posts via the public WordPress REST API, one custom post type per
    order category: /wp-json/wp/v2/{adjudication-order,tribunal-order,
    court-decision-order} (100/page, deep pagination supported).
  - Each post carries the parties (title), the dispute-type taxonomy term and
    links its Determination Order PDF via /wp-json/wp/v2/media?parent={id}.
  - The PDFs are SCANNED IMAGES (no text layer) -> extract full text via
    tesseract OCR (page rasterised with PyMuPDF, piped to tesseract on stdin).
    A born-digital text layer, if present, is used in preference to OCR.
  - The Case Reference (DR/TR number) and the "made on" date are parsed from the
    extracted text.

Data:
  - ~21,500 Determination Orders, 2015-present
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent orders)
  python bootstrap.py test               # Quick connectivity test
"""

import os
import re
import sys
import html
import shutil
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.RTB")

BASE_URL = "https://rtb.ie"
API = "/wp-json/wp/v2"
# Post types that hold a Determination Order PDF. "court-order" (4 posts) carries
# no PDF and is intentionally omitted.
POST_TYPES = ["adjudication-order", "tribunal-order", "court-decision-order"]
COURT_NAMES = {
    "adjudication-order": "Residential Tenancies Board (Adjudication)",
    "tribunal-order": "Residential Tenancies Board (Tenancy Tribunal)",
    "court-decision-order": "Residential Tenancies Board (Court Decision — Enforcement of Order)",
}
PER_PAGE = 100
OCR_DPI = int(os.environ.get("PDF_OCR_DPI", "300"))
OCR_MAX_PAGES = int(os.environ.get("PDF_OCR_MAX_PAGES", "20"))

CASE_REF_RE = re.compile(r"\b((?:DR|TR)\d{3,}(?:-\d{3,})*)\b", re.IGNORECASE)
MONTHS = ("January|February|March|April|May|June|July|August|September|"
         "October|November|December")
MADE_ON_RE = re.compile(
    r"made by the Residential Tenancies Board on\s+"
    r"(\d{1,2})(?:st|nd|rd|th)?\s+(" + MONTHS + r")\s+(\d{4})",
    re.IGNORECASE,
)
ANY_DATE_RE = re.compile(
    r"(\d{1,2})(?:st|nd|rd|th)?\s+(" + MONTHS + r")\s+(\d{4})", re.IGNORECASE
)
MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}


def _resolve_tesseract() -> Optional[str]:
    for cand in (os.environ.get("TESSERACT_CMD"), "tesseract",
                 "/opt/homebrew/bin/tesseract", "/usr/bin/tesseract",
                 "/usr/local/bin/tesseract"):
        if cand and shutil.which(cand):
            return shutil.which(cand)
        if cand and os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return None


TESSERACT_BIN = _resolve_tesseract()


def _ocr_page(png_bytes: bytes, lang: str = "eng") -> str:
    """OCR a single rasterised page. Feeds tesseract on stdin (works even where
    the local leptonica build cannot open temp files)."""
    if not TESSERACT_BIN:
        return ""
    try:
        out = subprocess.run(
            [TESSERACT_BIN, "stdin", "stdout", "-l", lang],
            input=png_bytes, capture_output=True, timeout=120,
        )
        return out.stdout.decode("utf-8", "replace")
    except Exception as e:
        logger.warning(f"OCR failed: {e}")
        return ""


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a Determination Order PDF: prefer an embedded text layer,
    otherwise OCR each page image."""
    if not fitz:
        raise RuntimeError("PyMuPDF (fitz) is required to extract RTB PDFs")
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        layer = "\n".join(page.get_text() for page in doc).strip()
        if len(layer) >= 200:
            return layer
        # Scanned image -> OCR.
        chunks: List[str] = []
        for i, page in enumerate(doc):
            if i >= OCR_MAX_PAGES:
                break
            png = page.get_pixmap(dpi=OCR_DPI).tobytes("png")
            chunks.append(_ocr_page(png))
        return "\n".join(c for c in chunks if c).strip()
    finally:
        doc.close()


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in text.replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _parse_date(text: str) -> Optional[str]:
    m = MADE_ON_RE.search(text) or ANY_DATE_RE.search(text)
    if not m:
        return None
    day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTH_NUM.get(mon)
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


class RTBScraper(BaseScraper):
    """Scraper for the RTB Determination Orders (WordPress REST API + OCR)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )
        self._dispute_types: Optional[Dict[int, str]] = None

    # -- API helpers -----------------------------------------------------
    def _get_json(self, path: str):
        self.rate_limiter.wait()
        resp = self.client.get(path)
        if resp.status_code != 200:
            return None, resp.status_code
        try:
            import json
            return json.loads(resp.content.decode("utf-8", "replace")), 200
        except Exception:
            return None, resp.status_code

    def _dispute_type_map(self) -> Dict[int, str]:
        if self._dispute_types is None:
            self._dispute_types = {}
            data, _ = self._get_json(f"{API}/dispute-type?per_page=100")
            if isinstance(data, list):
                for t in data:
                    self._dispute_types[t.get("id")] = t.get("name", "")
        return self._dispute_types

    def _pdf_url_for_post(self, post_id: int) -> Optional[str]:
        data, _ = self._get_json(f"{API}/media?parent={post_id}&per_page=20")
        if not isinstance(data, list):
            return None
        for m in data:
            if m.get("mime_type") == "application/pdf":
                return m.get("source_url")
        return None

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"pdf {url}: HTTP {resp.status_code}")
                return None
            data = resp.content
            if not data[:5].startswith(b"%PDF"):
                logger.warning(f"pdf {url}: not a PDF")
                return None
            return data
        except Exception as e:
            logger.warning(f"Error fetching pdf {url}: {e}")
            return None

    def _iter_posts(self, post_type: str) -> Generator[Dict[str, Any], None, None]:
        page = 1
        while True:
            data, status = self._get_json(
                f"{API}/{post_type}?per_page={PER_PAGE}&page={page}"
                f"&orderby=id&order=asc"
                f"&_fields=id,link,slug,date,title,dispute-type"
            )
            if status == 400 or not data:
                break  # past the last page
            if not isinstance(data, list) or not data:
                break
            for post in data:
                yield post
            if len(data) < PER_PAGE:
                break
            page += 1

    # -- core ------------------------------------------------------------
    def _build_raw(self, post: Dict[str, Any], post_type: str) -> Optional[Dict[str, Any]]:
        pid = post.get("id")
        pdf_url = self._pdf_url_for_post(pid)
        if not pdf_url:
            return None
        pdf = self._fetch_pdf(pdf_url)
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.warning(f"extract post {pid} failed: {e}")
            return None
        if not text:
            return None
        title = (post.get("title") or {}).get("rendered", "") or post.get("slug", "")
        title = html.unescape(title)
        dtypes = [self._dispute_type_map().get(t, "") for t in (post.get("dispute-type") or [])]
        return {
            "id": pid,
            "post_type": post_type,
            "title": re.sub(r"\s+", " ", title).strip(),
            "link": post.get("link"),
            "dispute_types": [d for d in dtypes if d],
            "text": text,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for post_type in POST_TYPES:
            for post in self._iter_posts(post_type):
                raw = self._build_raw(post, post_type)
                if raw:
                    produced += 1
                    yield raw
        if produced == 0:
            raise RuntimeError(
                "RTB REST API returned 0 Determination Orders — site blocked or API changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        for post_type in POST_TYPES:
            page = 1
            while True:
                data, status = self._get_json(
                    f"{API}/{post_type}?per_page={PER_PAGE}&page={page}"
                    f"&orderby=modified&order=desc&modified_after={since_iso}"
                    f"&_fields=id,link,slug,date,title,dispute-type"
                )
                if status == 400 or not isinstance(data, list) or not data:
                    break
                for post in data:
                    raw = self._build_raw(post, post_type)
                    if raw:
                        yield raw
                if len(data) < PER_PAGE:
                    break
                page += 1

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 120:
            return None
        pid = raw.get("id")
        post_type = raw.get("post_type", "")
        title = raw.get("title") or f"RTB Determination Order {pid}"

        ref_m = CASE_REF_RE.search(text)
        case_ref = ref_m.group(1).upper() if ref_m else None
        iso_date = _parse_date(text)

        return {
            "_id": f"IE-RTB-{post_type}-{pid}",
            "_source": "IE/RTB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": raw.get("link") or BASE_URL,
            "case_ref": case_ref,
            "order_type": post_type,
            "dispute_types": raw.get("dispute_types") or [],
            "court": COURT_NAMES.get(post_type, "Residential Tenancies Board"),
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        print("Testing RTB Determination Orders API...")
        data, status = self._get_json(
            f"{API}/adjudication-order?per_page=1&_fields=id,title")
        print(f"  adjudication-order list: HTTP {status}; got {len(data or [])}")
        if data:
            pid = data[0]["id"]
            url = self._pdf_url_for_post(pid)
            print(f"  post {pid} pdf: {url}")
            if url:
                pdf = self._fetch_pdf(url)
                if pdf:
                    t = _pdf_text(pdf)
                    print(f"  extracted {len(t)} chars (OCR={not bool(TESSERACT_BIN) is False}) - OK")
        print(f"  tesseract: {TESSERACT_BIN}")


def main():
    scraper = RTBScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
