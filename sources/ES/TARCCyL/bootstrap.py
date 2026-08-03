#!/usr/bin/env python3
"""
ES/TARCCyL -- Tribunal Administrativo de Recursos Contractuales de Castilla y
León (TARCCYL)

Castilla y León regional public-procurement appeals tribunal, hosted by the
Consejo Consultivo de Castilla y León (cccyl.es). It resolves the "recurso
especial en materia de contratación pública" and issues binding resoluciones.
Sibling of the central ES/TACRC (captcha-blocked) and the built regional
tribunals ES/TARCJA-Andalucia / ES/OARC-Euskadi / ES/TACP-Madrid /
ES/TACPA-Aragon / ES/TACPN-Navarra / ES/TACPC-Canarias.

Strategy:
  - The public resoluciones section is organised by year:
        https://www.cccyl.es/es/tribunal-administrativo-recursos-contractuales-castilla-leo/resoluciones/resoluciones-ano-{YEAR}
    Each year page lists ~10 resoluciones and paginates via a
    ".nodos,{offset},10" suffix (offset 0, 10, 20, ...). Every listed
    resolución links DIRECTLY to a born-digital PDF under its
    ".ficheros/{id}-Resolucion...pdf" path.
  - Full text is extracted from each born-digital PDF with PyMuPDF (fitz).
    No OCR needed. The resolución number/year come from the URL path; the
    decision date is parsed from the PDF header ("Resolución N/YYYY, de <day>
    de <month>...").

Data:
  - ~600-1,000 resoluciones, ~2017-present
  - Type: case_law
  - License: Junta de Castilla y León open data (PSI reuse)
  - Language: Spanish (es)

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
logger = logging.getLogger("legal-data-hunter.ES.tarccyl")

HOST = "https://www.cccyl.es"
BASE_PATH = "/es/tribunal-administrativo-recursos-contractuales-castilla-leo/resoluciones"
INDEX_URL = f"{HOST}{BASE_PATH}"

# The resoluciones section is split into per-year pages, e.g.
#   /.../resoluciones/resoluciones-ano-2025
YEAR_HREF_RE = re.compile(
    r'href="(' + re.escape(BASE_PATH) + r'/resoluciones-ano-(\d{4}))"',
    re.IGNORECASE,
)
# Direct born-digital PDF hrefs, e.g.
#   /.../resoluciones-ano-2025/resolucion-258-2025.ficheros/98728-Resoluci%C3%B3n%20258-2025.pdf
PDF_HREF_RE = re.compile(
    r'href="(' + re.escape(BASE_PATH) + r'/resoluciones-ano-\d{4}/'
    r'resolucion-\d+-\d{4}\.ficheros/[^"]+?\.pdf)"',
    re.IGNORECASE,
)
# resolucion-{num}-{year} in the PDF path (fallback number source)
RESNUM_RE = re.compile(r"/resolucion-(\d+)-(\d{4})\.ficheros/", re.IGNORECASE)
# Authoritative header sentence, capturing number + year + date, e.g.
#   "Resolución 258/2025, de 29 de diciembre, del Tribunal ..."
# The number here can differ from the URL path number (the site occasionally
# links a file under a neighbouring number), so the PDF body wins.
DATE_RE = re.compile(
    r"Resoluci[oó]n\s+(\d+)\s*/\s*(\d{4})\s*,?\s*(?:de\s+)?(\d{1,2})\s+de\s+([a-záéíóúñ]+)",
    re.IGNORECASE,
)
MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
PAGE_SIZE = 10
MAX_OFFSET = 2000  # safety ceiling per year (~200 pages)
MIN_TEXT_CHARS = 200


class TARCCyLScraper(BaseScraper):
    """
    Scraper for ES/TARCCyL -- Castilla y León procurement appeals tribunal.
    Country: ES  (jurisdiction ES-CL)
    Data types: case_law
    Auth: none (Junta de Castilla y León open data)
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
        """Discover the per-year listing URLs from the resoluciones index."""
        resp = self._get(INDEX_URL)
        resp.raise_for_status()
        pairs = YEAR_HREF_RE.findall(resp.text)
        seen, out = set(), []
        for href, year in pairs:
            href = html.unescape(href)
            if href not in seen:
                seen.add(href)
                out.append((int(year), f"{HOST}{href}"))
        # Newest year first
        out.sort(key=lambda x: -x[0])
        return out

    def _pdf_hrefs_on(self, year_url: str, offset: int) -> list:
        """Return PDF hrefs on one paginated slice of a year page."""
        url = year_url if offset == 0 else f"{year_url}.nodos,{offset},{PAGE_SIZE}"
        resp = self._get(url)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        hrefs = [html.unescape(h) for h in PDF_HREF_RE.findall(resp.text)]
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

    def _iter_hrefs(self, years: Optional[list] = None) -> Generator[str, None, None]:
        """Yield every resolución-PDF href across all year pages, deduped."""
        if years is None:
            years = self._year_urls()
        seen = set()
        for year, year_url in years:
            got_year = 0
            for offset in range(0, MAX_OFFSET + 1, PAGE_SIZE):
                try:
                    hrefs = self._pdf_hrefs_on(year_url, offset)
                except Exception as e:
                    logger.warning(f"Year {year} offset {offset} failed: {e}")
                    break
                if not hrefs:
                    break
                new = 0
                for h in hrefs:
                    if h in seen:
                        continue
                    seen.add(h)
                    new += 1
                    got_year += 1
                    yield h
                logger.info(f"Year {year} offset {offset}: {len(hrefs)} links, {new} new")
                if new == 0:
                    # Same page repeated (past the end) — stop this year.
                    break
            logger.info(f"Year {year}: {got_year} resoluciones (running total {len(seen)})")

    def fetch_all(self) -> Generator[dict, None, None]:
        for href in self._iter_hrefs():
            yield {"href": href}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental: scan the current + previous year only."""
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

        # The URL path number/year is the site's per-file identifier and is the
        # only value guaranteed unique per document. The PDF body header number
        # is occasionally off-by-one (the site mislabels a file), so trusting it
        # would collapse two distinct resoluciones onto one _id and lose one.
        # Hence: _id/number come from the URL; the decision DATE comes from the
        # (authoritative) PDF header.
        num = year = ""
        mnum = RESNUM_RE.search(href)
        if mnum:
            num, year = mnum.group(1), mnum.group(2)

        date = ""
        mdate = DATE_RE.search(text)
        if mdate:
            hdr_year, day, month = mdate.group(2), int(mdate.group(3)), mdate.group(4).lower()
            mo = MONTHS.get(month)
            if mo:
                date = f"{hdr_year}-{mo:02d}-{day:02d}"
        if not date and year:
            date = f"{year}-01-01"

        res_id = f"{num}-{year}" if num and year else href.rsplit("/", 1)[-1]
        title = f"Resolución {num}/{year} del Tribunal Administrativo de Recursos Contractuales de Castilla y León" \
            if num and year else "Resolución del Tribunal Administrativo de Recursos Contractuales de Castilla y León"
        # A richer title from the header sentence, if present
        hdr = re.search(r"(Resoluci[oó]n\s+\d+\s*/\s*\d{4}[^\n]{0,240})", text, re.IGNORECASE)
        if hdr:
            title = re.sub(r"\s+", " ", hdr.group(1)).strip()

        return {
            # Required base fields
            "_id": f"ES-CL-TARCCYL-{res_id}",
            "_source": "ES/TARCCyL",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "resolucion_number": f"{num}/{year}" if num and year else "",
            "tribunal": "Tribunal Administrativo de Recursos Contractuales de Castilla y León (TARCCYL)",
            "jurisdiction": "ES-CL",
            "language": "es",
            "country": "ES",
        }

    # ── connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print("Testing TARCCyL (Castilla y León procurement tribunal)...")
        try:
            years = self._year_urls()
            print(f"\n1. Year pages found: {[y for y, _ in years]}")
            if years:
                y, yurl = years[0]
                hrefs = self._pdf_hrefs_on(yurl, 0)
                print(f"2. Year {y} page 1 PDF links: {len(hrefs)}")
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
    scraper = TARCCyLScraper()

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
