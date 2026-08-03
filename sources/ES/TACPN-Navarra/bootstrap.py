#!/usr/bin/env python3
"""
ES/TACPN-Navarra -- Tribunal Administrativo de Contratos Públicos de Navarra
(TACPN)

Navarra regional public-procurement appeals tribunal. It resolves the
"reclamación especial en materia de contratación pública" and issues binding
acuerdos. Sibling of the central ES/TACRC (captcha-blocked) and the built
ES/TARCJA-Andalucia / ES/OARC-Euskadi regional tribunals.

Strategy:
  - The public "Todos los acuerdos" list (Liferay Asset Publisher, 20/page) at
        https://portalcontratacion.navarra.es/es/todos-acuerdos
    links directly to born-digital acuerdo PDFs under /documents/... . Paginate
    via the Asset Publisher `_cur` param until a page yields no PDF links.
  - Full text is extracted from each born-digital PDF with PyMuPDF (fitz).
    No OCR needed. The acuerdo number + date + parties are parsed from the
    PDF body ("ACUERDO N/YYYY, de <day> de <month> ...").

Data:
  - ~1,095 acuerdos, ~2010-present
  - Type: case_law
  - License: Gobierno de Navarra open data (PSI reuse)
  - Language: Spanish (es)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (VPS entrypoint)
  python bootstrap.py update             # Incremental (recent pages)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.tacpn")

HOST = "https://portalcontratacion.navarra.es"
INSTANCE = "com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_bJBLb1tjplPX"
LIST_URL = (
    f"{HOST}/es/todos-acuerdos"
    f"?p_p_id={INSTANCE}&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view"
    f"&_{INSTANCE}_delta=20&p_r_p_resetCur=false&_{INSTANCE}_cur="
)
# Full hrefs to born-digital acuerdo PDFs, e.g.
#   /documents/880958/0/Acuerdo+52-2026.pdf/<uuid>?t=1780675449237
PDF_HREF_RE = re.compile(r'href="(/documents/\d+/[^"]+?\.pdf/[0-9a-f\-]+(?:\?t=\d+)?)"',
                         re.IGNORECASE)
UUID_RE = re.compile(r"\.pdf/([0-9a-f\-]+)", re.IGNORECASE)
ACUERDO_RE = re.compile(r"ACUERDO\s+(\d+)\s*/\s*(\d{4})", re.IGNORECASE)
# The acuerdo header dates the decision, e.g. "ACUERDO 74/2026, 10 de julio, del
# Tribunal ..." or the older "ACUERDO 5/2013, de 16 de mayo de 2013, ...". The
# "de" before the day is optional; anchoring to the FIRST ACUERDO occurrence (the
# header) avoids matching a *cited* acuerdo further down the body.
DATE_RE = re.compile(
    r"ACUERDO\s+\d+\s*/\s*(\d{4})\s*,?\s*(?:de\s+)?(\d{1,2})\s+de\s+([a-záéíóúñ]+)",
    re.IGNORECASE,
)
MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
MAX_PAGES = 120  # safety ceiling (~55 real pages)
MIN_TEXT_CHARS = 200


class TACPNScraper(BaseScraper):
    """
    Scraper for ES/TACPN-Navarra -- Navarra procurement appeals tribunal.
    Country: ES  (jurisdiction ES-NC)
    Data types: case_law
    Auth: none (Gobierno de Navarra open data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
        })

    # ── helpers ────────────────────────────────────────────────────────

    def _list_page(self, page: int) -> list:
        """Return the list of full acuerdo-PDF hrefs on a given list page."""
        self.rate_limiter.wait()
        resp = self.session.get(f"{LIST_URL}{page}", timeout=90, allow_redirects=True)
        resp.raise_for_status()
        hrefs = [html.unescape(h) for h in PDF_HREF_RE.findall(resp.text)]
        # Preserve order, drop dups within the page
        seen, out = set(), []
        for h in hrefs:
            if h not in seen:
                seen.add(h)
                out.append(h)
        return out

    @staticmethod
    def _uuid(href: str) -> str:
        m = UUID_RE.search(href)
        return m.group(1) if m else href

    def _extract_pdf_text(self, url: str) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        resp = self.session.get(url, timeout=120, allow_redirects=True)
        if resp.status_code == 404:
            return ""
        resp.raise_for_status()
        if not resp.content or b"%PDF" not in resp.content[:1024]:
            return ""
        doc = fitz.open(stream=resp.content, filetype="pdf")
        try:
            parts = [page.get_text() for page in doc]
        finally:
            doc.close()
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── data acquisition ───────────────────────────────────────────────

    def _iter_hrefs(self, max_pages: int = MAX_PAGES) -> Generator[str, None, None]:
        """Yield every acuerdo-PDF href across all list pages, deduped by uuid."""
        seen = set()
        for page in range(1, max_pages + 1):
            try:
                hrefs = self._list_page(page)
            except Exception as e:
                logger.warning(f"List page {page} failed: {e}")
                break
            if not hrefs:
                logger.info(f"Empty list page {page} — end of corpus")
                break
            new = 0
            for h in hrefs:
                u = self._uuid(h)
                if u in seen:
                    continue
                seen.add(u)
                new += 1
                yield h
            logger.info(f"Page {page}: {len(hrefs)} links, {new} new (total {len(seen)})")

    def fetch_all(self) -> Generator[dict, None, None]:
        for href in self._iter_hrefs():
            yield {"href": href}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental: scan the first few (newest-first) pages only."""
        seen = set()
        for page in range(1, 8):
            try:
                hrefs = self._list_page(page)
            except Exception as e:
                logger.warning(f"List page {page} failed: {e}")
                break
            if not hrefs:
                break
            for h in hrefs:
                u = self._uuid(h)
                if u not in seen:
                    seen.add(u)
                    yield {"href": h}

    # ── normalization ──────────────────────────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        href = raw.get("href", "")
        if not href:
            return None
        url = href if href.startswith("http") else f"{HOST}{href}"
        try:
            text = self._extract_pdf_text(url)
        except Exception as e:
            logger.debug(f"PDF extract failed for {href}: {e}")
            return None
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text for {href}: {len(text)} chars")
            return None

        uuid = self._uuid(href)
        num = year = ""
        mnum = ACUERDO_RE.search(text)
        if mnum:
            num, year = mnum.group(1), mnum.group(2)

        date = ""
        mdate = DATE_RE.search(text)
        if mdate:
            yr, day, month = mdate.group(1), int(mdate.group(2)), mdate.group(3).lower()
            mo = MONTHS.get(month)
            if mo:
                date = f"{yr}-{mo:02d}-{day:02d}"
        if not date and year:
            date = f"{year}-01-01"

        acuerdo_id = f"{num}-{year}" if num and year else uuid
        title = f"Acuerdo {num}/{year} del Tribunal Administrativo de Contratos Públicos de Navarra" \
            if num and year else "Acuerdo del Tribunal Administrativo de Contratos Públicos de Navarra"
        # A richer title from the acuerdo header sentence, if present
        hdr = re.search(r"(ACUERDO\s+\d+\s*/\s*\d{4}[^\n]{0,240})", text, re.IGNORECASE)
        if hdr:
            title = re.sub(r"\s+", " ", hdr.group(1)).strip()

        return {
            # Required base fields
            "_id": f"ES-NC-TACPN-{acuerdo_id}",
            "_source": "ES/TACPN-Navarra",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "acuerdo_number": f"{num}/{year}" if num and year else "",
            "document_uuid": uuid,
            "tribunal": "Tribunal Administrativo de Contratos Públicos de Navarra (TACPN)",
            "jurisdiction": "ES-NC",
            "language": "es",
            "country": "ES",
        }

    # ── connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print("Testing TACPN (Navarra procurement tribunal) list + PDF...")
        try:
            hrefs = self._list_page(1)
            print(f"\n1. Page 1 acuerdo links: {len(hrefs)}")
            if hrefs:
                url = f"{HOST}{hrefs[0]}"
                print(f"   First: {url}")
                text = self._extract_pdf_text(url)
                print(f"   Extracted text: {len(text)} chars")
                rec = self.normalize({"href": hrefs[0]})
                if rec:
                    print(f"   _id: {rec['_id']}  date: {rec['date']}")
                    print(f"   title: {rec['title'][:120]}")
                    print(f"   sample: {text[:180]!r}")
        except Exception as e:
            print(f"   error: {e}")
        print("\nTest complete!")


def main():
    scraper = TACPNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} "
                  f"records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        print(f"\nbootstrap-fast complete: {stats['records_fetched']} fetched, "
              f"{stats['records_new']} new, {stats['errors']} errors")
        print(json.dumps(stats, indent=2, default=str))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
