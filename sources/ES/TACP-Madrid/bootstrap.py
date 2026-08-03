#!/usr/bin/env python3
"""
ES/TACP-Madrid -- Tribunal Administrativo de Contratacion Publica de la
Comunidad de Madrid (TACP Madrid)

Regional public-procurement appeals tribunal for the Community of Madrid. It
resolves the "recurso especial en materia de contratacion" (special appeals in
public procurement) and issues binding resoluciones. This is the largest of the
Spanish regional TARC set and a sibling of the built ES/TARCJA-Andalucia and the
captcha-blocked central tribunal (ES/TACRC).

Strategy:
  - The public search UI at
        https://www.comunidad.madrid/tacp/busquedaresoluciones
    is a Drupal "Views" listing. Its rows carry ALL the metadata we need
    (resolution number, date, thematic summary, and a link to the born-digital
    PDF) so no per-resolution detail page fetch is required.
  - Pagination is driven by the Drupal Views AJAX endpoint:
        POST https://www.comunidad.madrid/tacp/views/ajax
        view_name=busquedaresoluciones&view_display_id=page&page=N&_drupal_ajax=1
    which returns a JSON array of AJAX commands; the "insert" command holds the
    rendered results HTML (5 resoluciones per page, ~5,791 total => ~1,159 pages).
  - Full text lives in each resolution's born-digital PDF, hosted under
        https://www.comunidad.madrid/tacp/sites/default/files/{filename}
    (also reachable via the tokenised /tacp/file/{fid}/download href in the row),
    extracted with PyMuPDF (fitz). No OCR needed (born-digital text layer).

Data:
  - ~5,791 resoluciones, roughly 2011-present
  - Type: case_law  (jurisdiction ES-MD)
  - License: Comunidad de Madrid open data (PSI reuse, Ley 37/2007)
  - Language: Spanish (es)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records for validation
  python bootstrap.py bootstrap-fast     # Concurrent full pull (VPS entrypoint)
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmllib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.ES.tacp-madrid")

BASE = "https://www.comunidad.madrid"
SEARCH_URL = "https://www.comunidad.madrid/tacp/busquedaresoluciones"
AJAX_URL = "https://www.comunidad.madrid/tacp/views/ajax"
FILES_BASE = "https://www.comunidad.madrid/tacp/sites/default/files/"
VIEW_DOM_ID = "0dd7d05489b9016d47493f3810733642"
MIN_TEXT_CHARS = 200
MAX_EMPTY_PAGES = 3  # stop after this many consecutive empty pages

# One row = number link + fecha + body summary + documento (pdf) link.
RE_RESO = re.compile(r'/tacp/resolucion/([0-9A-Za-z_-]+)"\s*>\s*([0-9]+/[0-9]+)')
RE_FECHA = re.compile(r'(\d{2}/\d{2}/\d{4})')
RE_PDF = re.compile(r'href="([^"]*/tacp/[^"]*?)"[^>]*>\s*([^<]*?\.pdf)\s*<', re.I)


def _clean(fragment: str) -> str:
    """Strip HTML tags + decode entities + collapse whitespace."""
    txt = re.sub(r"<[^>]+>", " ", fragment)
    txt = htmllib.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


class TACPMadridScraper(BaseScraper):
    """
    Scraper for ES/TACP-Madrid -- Madrid procurement appeals tribunal.
    Country: ES  (jurisdiction ES-MD)
    Data types: case_law
    Auth: none (Comunidad de Madrid open data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "*/*",
            "Referer": SEARCH_URL,
        })
        self._primed = False

    # -- helpers --------------------------------------------------------

    def _prime(self):
        """Fetch the search page once so the session carries any cookies."""
        if self._primed:
            return
        try:
            self.rate_limiter.wait()
            self.session.get(SEARCH_URL, timeout=60)
        except Exception as e:
            logger.debug(f"Prime request failed (continuing): {e}")
        self._primed = True

    def _fetch_page(self, page: int) -> str:
        """Return the rendered results HTML for a listing page (may be empty)."""
        self._prime()
        self.rate_limiter.wait()
        resp = self.session.post(
            AJAX_URL,
            data={
                "view_name": "busquedaresoluciones",
                "view_display_id": "page",
                "view_dom_id": VIEW_DOM_ID,
                "page": str(page),
                "_drupal_ajax": "1",
            },
            headers={"X-Requested-With": "XMLHttpRequest"},
            timeout=90,
        )
        resp.raise_for_status()
        try:
            commands = resp.json()
        except ValueError:
            raise RuntimeError(
                f"TACP-Madrid AJAX page {page} returned non-JSON "
                f"(len={len(resp.content)}); host may be blocking this vantage"
            )
        for c in commands:
            if c.get("command") == "insert" and isinstance(c.get("data"), str):
                return c["data"]
        return ""

    @staticmethod
    def _parse_rows(html: str) -> list:
        """Parse a results-page HTML fragment into per-row raw dicts."""
        rows = []
        matches = list(RE_RESO.finditer(html))
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(html)
            block = html[start:end]

            reso_id = m.group(1)
            number = m.group(2)

            fecha = ""
            mf = re.search(r"views-field-field-fecha(.*?)</div>", block, re.S)
            if mf:
                mfd = RE_FECHA.search(mf.group(1))
                if mfd:
                    fecha = mfd.group(1)

            summary = ""
            mb = re.search(
                r'views-field-body.*?<span class="field-content">(.*?)</span>',
                block, re.S,
            )
            if mb:
                summary = _clean(mb.group(1))

            pdf_href = ""
            pdf_name = ""
            mp = RE_PDF.search(block)
            if mp:
                pdf_href = mp.group(1)
                pdf_name = mp.group(2).strip()

            rows.append({
                "reso_id": reso_id,
                "number": number,
                "fecha": fecha,
                "summary": summary,
                "pdf_href": pdf_href,
                "pdf_name": pdf_name,
            })
        return rows

    def _extract_pdf_text(self, url: str) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        resp = self.session.get(url, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        if not resp.content:
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

    @staticmethod
    def _iso_date(fecha: str) -> str:
        if not fecha:
            return ""
        try:
            return datetime.strptime(fecha, "%d/%m/%Y").date().isoformat()
        except ValueError:
            return ""

    def _pdf_urls(self, raw: dict):
        """Yield candidate PDF URLs (tokenised href first, static path fallback)."""
        href = raw.get("pdf_href", "")
        if href:
            yield urljoin(BASE, href)
        name = raw.get("pdf_name", "")
        if name:
            candidate = FILES_BASE + name
            if candidate not in (urljoin(BASE, href) if href else ""):
                yield candidate

    # -- data acquisition ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Paginate the Views AJAX endpoint and yield every raw resolution row."""
        page = 0
        empties = 0
        while True:
            html = self._fetch_page(page)
            rows = self._parse_rows(html)
            if not rows:
                empties += 1
                logger.info(f"Page {page}: no rows (empty {empties}/{MAX_EMPTY_PAGES})")
                if empties >= MAX_EMPTY_PAGES:
                    break
                page += 1
                continue
            empties = 0
            logger.info(f"Page {page}: {len(rows)} resoluciones")
            for row in rows:
                yield row
            page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield resolutions dated on/after `since` (newest-first listing)."""
        since_date = since.date() if isinstance(since, datetime) else since
        for row in self.fetch_all():
            iso = self._iso_date(row.get("fecha", ""))
            if iso:
                try:
                    if datetime.strptime(iso, "%Y-%m-%d").date() < since_date:
                        # Listing is strictly newest-first; once we pass the
                        # cutoff every later row is older too.
                        break
                except ValueError:
                    pass
            yield row

    # -- normalization --------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        number = raw.get("number", "")
        reso_id = raw.get("reso_id", "")

        text = ""
        pdf_url = ""
        last_err = None
        for url in self._pdf_urls(raw):
            try:
                text = self._extract_pdf_text(url)
                pdf_url = url
                if text and len(text) >= MIN_TEXT_CHARS:
                    break
            except Exception as e:
                last_err = e
                continue

        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(
                f"Insufficient text for {number or reso_id}: "
                f"{len(text)} chars (last_err={last_err})"
            )
            return None

        # The listing anchor text is unreliable (some rows carry a data-entry
        # typo, e.g. "251/22026"); the PDF filename is authoritative, e.g.
        # resolucion_356-2026_expediente_314-2026.pdf
        pdf_name = raw.get("pdf_name", "")
        mn = re.search(r"resoluci[oó]n_([0-9]+)-([0-9]{4})", pdf_name, re.I)
        if mn:
            number = f"{mn.group(1)}/{mn.group(2)}"
        expediente = ""
        me = re.search(r"expediente_([0-9]+)-([0-9]{4})", pdf_name, re.I)
        if me:
            expediente = f"{me.group(1)}/{me.group(2)}"

        date = self._iso_date(raw.get("fecha", ""))
        title = f"Resolución {number}" if number else f"Resolución TACP {reso_id}"

        return {
            # Required base fields
            "_id": f"ES-MD-TACP-{reso_id}",
            "_source": "ES/TACP-Madrid",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": urljoin(BASE, f"/tacp/resolucion/{reso_id}"),
            # Additional metadata
            "resolution_number": number,
            "expediente_number": expediente,
            "summary": raw.get("summary", ""),
            "pdf_url": pdf_url,
            "tribunal": "Tribunal Administrativo de Contratación Pública de la Comunidad de Madrid",
            "jurisdiction": "ES-MD",
            "language": "es",
            "country": "ES",
        }

    # -- connectivity test ---------------------------------------------

    def test_connection(self):
        print("Testing TACP-Madrid (Madrid procurement tribunal) Views AJAX...")
        try:
            html = self._fetch_page(0)
            rows = self._parse_rows(html)
            print(f"\n1. Page 0 rows: {len(rows)}")
            if rows:
                r = rows[0]
                print(f"   First: Resolución {r['number']} ({r['fecha']}) "
                      f"id={r['reso_id']}")
                print(f"   Summary: {r['summary'][:120]!r}")
                print(f"   PDF href: {r['pdf_href']}  name={r['pdf_name']}")
                rec = self.normalize(r)
                if rec:
                    print(f"   Extracted text: {len(rec['text'])} chars")
                    print(f"   Sample: {rec['text'][:200]!r}")
                    print(f"   date={rec['date']} expediente={rec['expediente_number']}")
                else:
                    print("   normalize returned None (no full text)")
        except Exception as e:
            print(f"   error: {e}")
        print("\nTest complete!")


def main():
    scraper = TACPMadridScraper()

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
