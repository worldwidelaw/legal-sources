#!/usr/bin/env python3
"""
ES/TCCSP-Catalunya -- Tribunal Català de Contractes del Sector Públic (TCCSP)

Catalonia's regional public-procurement appeals tribunal. It resolves the
"recurs especial en matèria de contractació" and issues binding resolucions
(full text, in Catalan). Sibling of the central ES/TACRC (captcha-blocked) and
the built regional tribunals ES/TARCJA-Andalucia, ES/OARC-Euskadi,
ES/TACP-Madrid, ES/TACPA-Aragon, ES/TACPN-Navarra, ES/TACPC-Canarias and
ES/TARCCyL.

Strategy:
  - The resolucions are published on the Generalitat de Catalunya contracting
    portal, organised by year:
        https://contractacio.gencat.cat/ca/contacte/tccsp/resolucions-tccsp/{YEAR}/
    Each year page lists 50 resolucions and paginates via a
    "?page{TOKEN}={N}&googleoff=1" query (the TOKEN is the portlet instance id,
    discovered from the page's own pagination links). Every listed resolució
    links DIRECTLY to a born-digital PDF under
        /web/.content/contacte/tccsp/resolucions/{YEAR}/resolucio_num._{N}_{YEAR}.pdf
  - Full text is extracted from each born-digital PDF with PyMuPDF (fitz).
    No OCR needed. The número/year come from the PDF filename; the decision date
    is parsed from the PDF header ("Barcelona, <day> de <month> de <year>").

Data:
  - ~800-1,000+ resolucions, 2012-present
  - Type: case_law
  - License: Generalitat de Catalunya open data (PSI reuse)
  - Language: Catalan (ca)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (VPS entrypoint)
  python bootstrap.py update             # Incremental (recent years)
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
logger = logging.getLogger("legal-data-hunter.ES.tccsp")

HOST = "https://contractacio.gencat.cat"
INDEX_URL = f"{HOST}/ca/contacte/tccsp/resolucions-tccsp/"

# Per-year listing pages, e.g. /ca/contacte/tccsp/resolucions-tccsp/2025/
YEAR_HREF_RE = re.compile(
    r'href="(/ca/contacte/tccsp/resolucions-tccsp/(\d{4})/)"', re.IGNORECASE)
# Direct born-digital resolució PDFs
PDF_HREF_RE = re.compile(
    r'href="(/web/\.content/contacte/tccsp/resolucions/\d{4}/[^"]+?\.pdf)"',
    re.IGNORECASE)
# Pagination token: ?page{TOKEN}={N}&...
PAGE_TOKEN_RE = re.compile(r'\?(page[0-9a-f\-]+)=\d+', re.IGNORECASE)
# número/year from the PDF filename, e.g. resolucio_num._465_2025.pdf
FNUM_RE = re.compile(r"_(\d+)_(\d{4})\.pdf$", re.IGNORECASE)
# Header date: "Barcelona, 28 de novembre de 2025"
DATE_RE = re.compile(
    r"Barcelona\s*,\s*(\d{1,2})\s+d[e']?\s*([a-zàèéíòóúïüç]+)\s+de\s+(\d{4})",
    re.IGNORECASE)
MONTHS_CA = {
    "gener": 1, "febrer": 2, "març": 3, "marc": 3, "abril": 4, "maig": 5,
    "juny": 6, "juliol": 7, "agost": 8, "setembre": 9, "octubre": 10,
    "novembre": 11, "desembre": 12,
}
MAX_PAGES = 60  # per-year safety ceiling (50/page → 3000)
MIN_TEXT_CHARS = 200


class TCCSPScraper(BaseScraper):
    """
    Scraper for ES/TCCSP-Catalunya -- Catalan procurement appeals tribunal.
    Country: ES  (jurisdiction ES-CT)
    Data types: case_law
    Auth: none (Generalitat de Catalunya open data)
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

    def _get(self, url: str, timeout: int = 90) -> requests.Response:
        self.rate_limiter.wait()
        return self.session.get(url, timeout=timeout, allow_redirects=True)

    def _year_urls(self) -> list:
        """Discover per-year listing URLs from the resolucions index."""
        resp = self._get(INDEX_URL)
        resp.raise_for_status()
        pairs = YEAR_HREF_RE.findall(resp.text)
        seen, out = set(), []
        for href, year in pairs:
            href = html.unescape(href)
            if href not in seen:
                seen.add(href)
                out.append((int(year), f"{HOST}{href}"))
        out.sort(key=lambda x: -x[0])  # newest year first
        return out

    @staticmethod
    def _pdf_hrefs(page_html: str) -> list:
        hrefs = [html.unescape(h) for h in PDF_HREF_RE.findall(page_html)]
        seen, out = set(), []
        for h in hrefs:
            if h not in seen:
                seen.add(h)
                out.append(h)
        return out

    def _extract_pdf_text(self, url: str) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        resp = self._get(url, timeout=120)
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

    def _iter_year_hrefs(self, year: int, year_url: str) -> Generator[str, None, None]:
        """Yield every resolució-PDF href for one year, paging as needed."""
        # Page 1 (no page param)
        resp = self._get(year_url)
        resp.raise_for_status()
        token_m = PAGE_TOKEN_RE.search(resp.text)
        token = token_m.group(1) if token_m else None

        seen = set()
        page = 1
        while page <= MAX_PAGES:
            if page == 1:
                page_html = resp.text
            else:
                if not token:
                    break
                url = f"{year_url}index.html?{token}={page}&googleoff=1"
                try:
                    r = self._get(url)
                    if r.status_code == 404:
                        break
                    r.raise_for_status()
                    page_html = r.text
                except Exception as e:
                    logger.warning(f"Year {year} page {page} failed: {e}")
                    break
            hrefs = self._pdf_hrefs(page_html)
            new = 0
            for h in hrefs:
                if h in seen:
                    continue
                seen.add(h)
                new += 1
                yield h
            logger.info(f"Year {year} page {page}: {len(hrefs)} links, {new} new")
            if new == 0:
                break
            page += 1
        logger.info(f"Year {year}: {len(seen)} resolucions")

    def _iter_hrefs(self, years: Optional[list] = None) -> Generator[str, None, None]:
        if years is None:
            years = self._year_urls()
        for year, year_url in years:
            yield from self._iter_year_hrefs(year, year_url)

    def fetch_all(self) -> Generator[dict, None, None]:
        for href in self._iter_hrefs():
            yield {"href": href}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental: current + previous year only."""
        all_years = self._year_urls()
        recent = [y for y in all_years if y[0] >= since.year - 1][:2] or all_years[:2]
        for href in self._iter_hrefs(years=recent):
            yield {"href": href}

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

        # número/year from the PDF filename (unique per file)
        num = year = ""
        fname = href.rsplit("/", 1)[-1]
        mnum = FNUM_RE.search(fname)
        if mnum:
            num, year = mnum.group(1), mnum.group(2)

        # decision date from the PDF header ("Barcelona, 28 de novembre de 2025")
        date = ""
        mdate = DATE_RE.search(text)
        if mdate:
            day, month, yr = int(mdate.group(1)), mdate.group(2).lower(), mdate.group(3)
            mo = MONTHS_CA.get(month)
            if mo:
                date = f"{yr}-{mo:02d}-{day:02d}"
        if not date and year:
            date = f"{year}-01-01"

        res_id = f"{num}-{year}" if num and year else fname.rsplit(".", 1)[0]
        title = f"Resolució {num}/{year} del Tribunal Català de Contractes del Sector Públic" \
            if num and year else "Resolució del Tribunal Català de Contractes del Sector Públic"
        # A richer title from the header line, if present
        hdr = re.search(r"(Resoluci[oó]\s+n[uú]m\.?\s*:?\s*\d+\s*/\s*\d{4}[^\n]{0,180})",
                        text, re.IGNORECASE)
        if hdr:
            title = re.sub(r"\s+", " ", hdr.group(1)).strip()

        return {
            # Required base fields
            "_id": f"ES-CT-TCCSP-{res_id}",
            "_source": "ES/TCCSP-Catalunya",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "resolucio_number": f"{num}/{year}" if num and year else "",
            "tribunal": "Tribunal Català de Contractes del Sector Públic (TCCSP)",
            "jurisdiction": "ES-CT",
            "language": "ca",
            "country": "ES",
        }

    # ── connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print("Testing TCCSP (Catalan procurement tribunal)...")
        try:
            years = self._year_urls()
            print(f"\n1. Year pages found: {[y for y, _ in years]}")
            if years:
                y, yurl = years[0]
                hrefs = list(self._iter_year_hrefs(y, yurl))
                print(f"2. Year {y} total PDF links: {len(hrefs)}")
                if hrefs:
                    url = f"{HOST}{hrefs[0]}"
                    print(f"   First: {url}")
                    rec = self.normalize({"href": hrefs[0]})
                    if rec:
                        print(f"   _id: {rec['_id']}  date: {rec['date']}")
                        print(f"   title: {rec['title'][:120]}")
                        print(f"   text: {len(rec['text'])} chars")
                        print(f"   sample: {rec['text'][:160]!r}")
        except Exception as e:
            print(f"   error: {e}")
        print("\nTest complete!")


def main():
    scraper = TCCSPScraper()

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
