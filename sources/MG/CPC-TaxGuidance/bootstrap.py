#!/usr/bin/env python3
"""
MG/CPC-TaxGuidance -- Madagascar DGI tax administrative doctrine

Fetches administrative tax doctrine of Madagascar's Direction Générale des
Impôts (DGI) from the official "Textes Réglementaires" portal:
    https://portal.impots.mg/textes/

The portal renders a single HTML table (~328 rows) where each row carries:
  - data-status        -> document type (Note, Circulaire, Décision, ...)
  - a[href$=.pdf]      -> direct PDF link (relative to /textes/)
  - span.media-meta    -> publication date (dd/mm/yyyy)
  - h4.title a         -> title (incl. reference number)
  - p.summary          -> short object / summary
  - view.php?ref=CODE  -> stable per-document reference code

This source keeps only *doctrine* document types (administrative guidance)
and excludes pure legislation (codes, lois, décrets, arrêtés). Full text is
extracted from born-digital PDFs with PyMuPDF (fallback: pdfminer); scanned
image-only documents with no text layer are skipped.

Usage:
  python bootstrap.py bootstrap --sample          # Sample records
  python bootstrap.py bootstrap --sample --count 12
  python bootstrap.py bootstrap                    # Full bootstrap
  python bootstrap.py update                       # Re-scan (upsert)
  python bootstrap.py test-api                     # Connectivity check
"""

import sys
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import urllib3
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MG.CPC-TaxGuidance")

BASE = "https://portal.impots.mg/textes"
LISTING_URL = f"{BASE}/"
AJAX_URL = f"{BASE}/modele/req_filter.php"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# Document types that count as administrative doctrine (guidance).
# Everything else on the portal (Loi, Arrêté, CGI, CDI, CPF, Décret,
# Ordonnance) is legislation and is excluded from this source.
DOCTRINE_TYPES = {
    "Note", "Avis", "Décision", "Communiqué",
    "Instruction", "Circulaire", "Filazana",
}

# Minimum characters of extracted text to count as full text. Communiqués
# can be short (a single notice), so the floor is modest; scanned/image-only
# PDFs yield ~0 characters and are dropped.
MIN_TEXT_CHARS = 250

_REF_RE = re.compile(r"ref=([A-Za-z0-9\-]+)")
_PDF_REF_RE = re.compile(r"Ref-([A-Za-z0-9]+-[A-Za-z0-9]+)\.pdf", re.I)


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF using pdfplumber, PyMuPDF, or pdfminer."""
    if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
        return ""
    text = ""
    # Try pdfplumber first (most reliable, widely available)
    try:
        import io
        import pdfplumber

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            text = "\n".join(parts)
    except Exception as e:
        logger.debug(f"pdfplumber failed ({e}); trying PyMuPDF")
    # Fallback: PyMuPDF
    if len(text.strip()) < MIN_TEXT_CHARS:
        try:
            import fitz  # PyMuPDF

            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                alt = "\n".join(page.get_text() for page in doc)
            finally:
                doc.close()
            if len(alt.strip()) > len(text.strip()):
                text = alt
        except Exception as e:
            logger.debug(f"PyMuPDF failed ({e}); trying pdfminer")
    # Fallback: pdfminer
    if len(text.strip()) < MIN_TEXT_CHARS:
        try:
            import io
            from pdfminer.high_level import extract_text as _pm_extract

            alt = _pm_extract(io.BytesIO(pdf_bytes)) or ""
            if len(alt.strip()) > len(text.strip()):
                text = alt
        except Exception as e:
            logger.debug(f"pdfminer failed ({e})")
    return _clean_text(text)


def _clean_text(text: str) -> str:
    """Collapse excessive whitespace while preserving paragraph breaks."""
    if not text:
        return ""
    text = text.replace("\x0c", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(s: str) -> Optional[str]:
    """dd/mm/yyyy -> ISO yyyy-mm-dd."""
    s = (s or "").strip()
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        d, mo, y = m.groups()
        try:
            return datetime(int(y), int(mo), int(d)).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


class MadagascarDGIScraper(BaseScraper):
    """Scraper for MG/CPC-TaxGuidance — Madagascar DGI tax doctrine."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.session.verify = False  # host serves an incomplete TLS chain

    # -- Parsing ------------------------------------------------------------

    def _list_rows(self) -> list:
        """Return parsed metadata dicts for all doctrine documents.

        The portal loads rows via AJAX POST to modele/req_filter.php.
        A direct GET to the listing page returns only the empty table shell.
        """
        resp = self.session.post(
            AJAX_URL,
            data={"action": "search", "data": ""},
            timeout=90,
            headers={"Referer": LISTING_URL},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # AJAX returns bare <tr> elements (not wrapped in <table>)
        rows = soup.select("tr[data-status]")
        out = []
        for tr in rows:
            doc_type = (tr.get("data-status") or "").strip()
            if doc_type not in DOCTRINE_TYPES:
                continue
            pdf_a = tr.find("a", href=re.compile(r"\.pdf$", re.I))
            if not pdf_a:
                continue
            pdf_href = pdf_a["href"]
            pdf_url = pdf_href if pdf_href.startswith("http") else f"{BASE}/{pdf_href.lstrip('/')}"

            # Reference code (stable per document)
            ref = None
            view_a = tr.find("a", href=re.compile(r"view\.php\?ref="))
            if view_a:
                m = _REF_RE.search(view_a["href"])
                if m:
                    ref = m.group(1)
            if not ref:
                m = _PDF_REF_RE.search(pdf_href)
                ref = m.group(1) if m else pdf_href.rsplit("/", 1)[-1].replace(".pdf", "")

            date_el = tr.select_one("span.media-meta")
            date_str = _parse_date(date_el.get_text(strip=True)) if date_el else None

            title_el = tr.select_one("h4.title a")
            title = title_el.get_text(" ", strip=True) if title_el else doc_type

            summary_el = tr.select_one("p.summary")
            summary = summary_el.get_text(" ", strip=True) if summary_el else ""

            detail_url = None
            if view_a:
                href = view_a["href"]
                detail_url = href if href.startswith("http") else f"{BASE}/{href.lstrip('/')}"

            out.append({
                "ref": ref,
                "doc_type": doc_type,
                "title": title,
                "summary": summary,
                "date": date_str,
                "pdf_url": pdf_url,
                "detail_url": detail_url or pdf_url,
            })
        logger.info(f"Found {len(out)} doctrine documents in portal listing")
        return out

    def _fetch_doc(self, meta: dict) -> dict:
        """Download a document's PDF and attach extracted text."""
        text = ""
        try:
            pr = self.session.get(meta["pdf_url"], timeout=120,
                                  headers={"Referer": LISTING_URL})
            pr.raise_for_status()
            text = _extract_pdf_text(pr.content)
        except Exception as e:
            logger.warning(f"  {meta['ref']}: pdf fetch failed ({e})")
        meta = dict(meta)
        meta["_text"] = text
        return meta

    # -- Core methods -------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw doctrine records (with extracted text)."""
        for meta in self._list_rows():
            self.rate_limiter.wait()
            yield self._fetch_doc(meta)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No native since-filter; re-scan (upsert dedup handles existing)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw record into the standard schema, or None to skip."""
        text = (raw.get("_text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            return None  # scanned / image-only doc with no usable text layer
        ref = raw["ref"]
        return {
            "_id": f"MG-DGI-{ref}",
            "_source": "MG/CPC-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or ref,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("detail_url"),
            "pdf_url": raw.get("pdf_url"),
            "ref": ref,
            "doc_type": raw.get("doc_type"),
            "summary": raw.get("summary"),
            "issuer": "Direction Générale des Impôts (Madagascar)",
            "jurisdiction": "MG",
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="MG/CPC-TaxGuidance DGI doctrine fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=12, help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MadagascarDGIScraper()

    if args.command == "test-api":
        print("Testing Madagascar DGI portal...")
        rows = scraper._list_rows()
        print(f"  Doctrine documents listed: {len(rows)}")
        if rows:
            rec = scraper._fetch_doc(rows[len(rows)//2])
            print(f"  Sample: {rec['title']} [{rec['doc_type']}] chars={len(rec['_text'])}")
        print("API test PASSED")

    elif args.command == "bootstrap":
        sample_mode = args.sample
        print(f"Starting bootstrap (sample={sample_mode})...")
        stats = scraper.bootstrap(sample_mode=sample_mode,
                                  sample_size=args.count if sample_mode else 10)
        print("\nBootstrap complete:")
        print(f"  Records fetched: {stats.get('records_fetched', 0)}")
        print(f"  Records new: {stats.get('records_new', 0)}")
        print(f"  Errors/skips: {stats.get('errors', 0)}")
        if sample_mode:
            print(f"  Sample records saved to: {scraper.source_dir / 'sample'}")

    elif args.command == "update":
        print("Starting incremental update...")
        stats = scraper.bootstrap(sample_mode=False)
        print("\nUpdate complete:")
        print(f"  Records new: {stats.get('records_new', 0)}")
