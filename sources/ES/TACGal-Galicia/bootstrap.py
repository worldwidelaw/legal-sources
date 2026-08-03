#!/usr/bin/env python3
"""
ES/TACGal-Galicia -- Tribunal Administrativo de Contratación Pública de Galicia (TACGal)

Galicia regional public-procurement appeals tribunal. It resolves the
"recurso especial en materia de contratación pública" (Law 9/2017) and issues
binding resolucións. Sibling of the central ES/TACRC (captcha-blocked) and the
built regional tribunals ES/TARCJA-Andalucia / ES/OARC-Euskadi /
ES/TACPN-Navarra / ES/TACPA-Aragon / ES/TCCSP-Catalunya / ES/TARCCyL /
ES/TACPC-Canarias.

Strategy:
  - The public "Resolucións" search (sixtacweb app on tacgal.xunta.gal) renders
    a paginated HTML table (100/page) at
        https://tacgal.xunta.gal/sixtacweb/resolucions/?locale=es&page=N&size=100
    Each row carries the resolution number, recurso number, decision date
    (DD-MM-YYYY), decision (Estima/Desestima/...), contract type, appealed act,
    a short description, and a download anchor whose id is the document id, e.g.
        <a id="9488" class="descargaDocumento" href="#">.
  - Each document is fetched from
        /sixtacweb/resolucions/download/{docId}
    which returns HTTP 200 application/pdf. The stored file is a born-digital
    PDF wrapped in a multipart/form-data envelope; the inner PDF (from the first
    "%PDF" marker to the last "%%EOF") is extracted and read with PyMuPDF (fitz).
    No OCR needed. The download endpoint does NOT enforce the site's reCAPTCHA
    (the JS attaches a token only for logging; the servlet serves the PDF on a
    plain GET of the download URL).

Data:
  - ~1,992 resolucións, ~2018-present
  - Type: case_law
  - License: Xunta de Galicia open data (PSI reuse, Ley 37/2007)
  - Language: Galician / Spanish (gl/es)

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
logger = logging.getLogger("legal-data-hunter.ES.tacgal-galicia")

HOST = "https://tacgal.xunta.gal"
LIST_URL = f"{HOST}/sixtacweb/resolucions/?locale=es&page={{page}}&size=100"
DOWNLOAD_URL = f"{HOST}/sixtacweb/resolucions/download/{{doc_id}}"

TBODY_RE = re.compile(r"<tbody[^>]*>(.*?)</tbody>", re.S | re.I)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
DOCID_RE = re.compile(r'id="(\d+)"\s+class="descargaDocumento"', re.I)
TOTAL_RE = re.compile(r"([\d\.]+)\s*resultado", re.I)

MAX_PAGES = 60  # safety ceiling (~20 real pages at size=100)
MIN_TEXT_CHARS = 200


def _clean(cell_html: str) -> str:
    txt = re.sub(r"<[^>]+>", " ", cell_html)
    return html.unescape(re.sub(r"\s+", " ", txt)).strip()


def _extract_inner_pdf(blob: bytes) -> bytes:
    """The download servlet returns a born-digital PDF wrapped in a
    multipart/form-data envelope. Return the inner PDF bytes."""
    if not blob:
        return b""
    start = blob.find(b"%PDF")
    if start < 0:
        return b""
    end = blob.rfind(b"%%EOF")
    if end < 0:
        return blob[start:]
    return blob[start:end + len(b"%%EOF")]


class TACGalScraper(BaseScraper):
    """
    Scraper for ES/TACGal -- Galicia procurement appeals tribunal.
    Country: ES  (jurisdiction ES-GA)
    Data types: case_law
    Auth: none (Xunta de Galicia open data)
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
        """Return a list of row dicts (metadata + doc_id) for a list page."""
        self.rate_limiter.wait()
        resp = self.session.get(LIST_URL.format(page=page), timeout=90,
                                allow_redirects=True)
        resp.raise_for_status()
        m = TBODY_RE.search(resp.text)
        if not m:
            return []
        rows = []
        for row_html in ROW_RE.findall(m.group(1)):
            cells = CELL_RE.findall(row_html)
            if len(cells) < 8:
                continue
            mid = DOCID_RE.search(row_html)
            if not mid:
                continue
            rows.append({
                "doc_id": mid.group(1),
                "resolucion": _clean(cells[0]),
                "recurso": _clean(cells[1]),
                "fecha": _clean(cells[2]),
                "decision": _clean(cells[3]),
                "tipo_contrato": _clean(cells[4]),
                "acto_recorrido": _clean(cells[5]),
                "descripcion": _clean(cells[6]),
            })
        return rows

    def _download_text(self, doc_id: str) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        resp = self.session.get(DOWNLOAD_URL.format(doc_id=doc_id), timeout=120,
                                allow_redirects=True)
        if resp.status_code == 404:
            return ""
        resp.raise_for_status()
        pdf = _extract_inner_pdf(resp.content)
        if not pdf or b"%PDF" not in pdf[:1024]:
            return ""
        doc = fitz.open(stream=pdf, filetype="pdf")
        try:
            parts = [page.get_text() for page in doc]
        finally:
            doc.close()
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── data acquisition ───────────────────────────────────────────────

    def _iter_rows(self, max_pages: int = MAX_PAGES) -> Generator[dict, None, None]:
        seen = set()
        for page in range(1, max_pages + 1):
            try:
                rows = self._list_page(page)
            except Exception as e:
                logger.warning(f"List page {page} failed: {e}")
                break
            if not rows:
                logger.info(f"Empty list page {page} — end of corpus")
                break
            new = 0
            for r in rows:
                if r["doc_id"] in seen:
                    continue
                seen.add(r["doc_id"])
                new += 1
                yield r
            logger.info(f"Page {page}: {len(rows)} rows, {new} new (total {len(seen)})")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_rows()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental: scan the first few (newest-first) pages only."""
        seen = set()
        for page in range(1, 4):
            try:
                rows = self._list_page(page)
            except Exception as e:
                logger.warning(f"List page {page} failed: {e}")
                break
            if not rows:
                break
            for r in rows:
                if r["doc_id"] not in seen:
                    seen.add(r["doc_id"])
                    yield r

    # ── normalization ──────────────────────────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        doc_id = raw.get("doc_id", "")
        if not doc_id:
            return None
        try:
            text = self._download_text(doc_id)
        except Exception as e:
            logger.debug(f"PDF extract failed for doc {doc_id}: {e}")
            return None
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text for doc {doc_id}: {len(text)} chars")
            return None

        resolucion = raw.get("resolucion", "").strip()  # e.g. "0090/2026"
        num_year = ""
        mnum = re.match(r"0*(\d+)\s*/\s*(\d{4})", resolucion)
        if mnum:
            num_year = f"{int(mnum.group(1))}/{mnum.group(2)}"

        # Decision date from the row (DD-MM-YYYY → ISO)
        date = ""
        fecha = raw.get("fecha", "").strip()
        mdate = re.match(r"(\d{2})-(\d{2})-(\d{4})", fecha)
        if mdate:
            date = f"{mdate.group(3)}-{mdate.group(2)}-{mdate.group(1)}"
        elif mnum:
            date = f"{mnum.group(2)}-01-01"

        rid = resolucion.replace("/", "-") if resolucion else doc_id
        desc = raw.get("descripcion", "").strip()
        title = f"Resolución {resolucion} do Tribunal Administrativo de Contratación Pública de Galicia"
        if desc:
            title += f" — {desc}"

        return {
            # Required base fields
            "_id": f"ES-GA-TACGal-{rid}",
            "_source": "ES/TACGal-Galicia",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": DOWNLOAD_URL.format(doc_id=doc_id),
            # Additional metadata
            "resolucion_number": num_year,
            "recurso_number": raw.get("recurso", "").strip(),
            "decision": raw.get("decision", "").strip(),
            "contract_type": raw.get("tipo_contrato", "").strip(),
            "appealed_act": raw.get("acto_recorrido", "").strip(),
            "document_id": doc_id,
            "tribunal": "Tribunal Administrativo de Contratación Pública de Galicia (TACGal)",
            "jurisdiction": "ES-GA",
            "language": "gl",
            "country": "ES",
        }

    # ── connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print("Testing TACGal (Galicia procurement tribunal) list + PDF...")
        try:
            rows = self._list_page(1)
            print(f"\n1. Page 1 rows: {len(rows)}")
            if rows:
                r = rows[0]
                print(f"   First: res {r['resolucion']} doc {r['doc_id']} ({r['fecha']})")
                text = self._download_text(r["doc_id"])
                print(f"   Extracted text: {len(text)} chars")
                rec = self.normalize(r)
                if rec:
                    print(f"   _id: {rec['_id']}  date: {rec['date']}")
                    print(f"   title: {rec['title'][:120]}")
                    print(f"   sample: {text[:180]!r}")
        except Exception as e:
            print(f"   error: {e}")
        print("\nTest complete!")


def main():
    scraper = TACGalScraper()

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
