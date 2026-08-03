#!/usr/bin/env python3
"""
ES/OARC-Euskadi -- Órgano Administrativo de Recursos Contractuales de la
Comunidad Autónoma de Euskadi (OARC / KEAO)

Regional public-procurement appeals body for the Basque Country. It resolves
the "recurso especial en materia de contratación" (special appeals in public
procurement) for Basque contracting authorities and issues binding resoluciones.
Open-data-accessible sibling of ES/TARCJA-Andalucia; both are alternatives to
the captcha-blocked central tribunal (ES/TACRC).

Strategy:
  - Official Gobierno Vasco open-data catalogue returns the full dataset as a
    single JSON dump (metadata + per-record dataXML/physicalUrl links):
        GET https://opendata.euskadi.eus/contenidos/ds_juridicos/resoluciones_oarc/opendata/documentacion.json
  - Each record's dataXML (a small ISO-8859-1 XML) carries the exact born-digital
    PDF <url>; full text is extracted from that PDF with PyMuPDF (no OCR).

Data:
  - ~2,595 resoluciones, 2011-present
  - Type: case_law
  - License: Gobierno Vasco open data (CC BY 4.0 / PSI reuse)
  - Language: Spanish (es) — some bilingual es/eu

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

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
logger = logging.getLogger("legal-data-hunter.ES.oarc")

CATALOG = ("https://opendata.euskadi.eus/contenidos/ds_juridicos/"
           "resoluciones_oarc/opendata/documentacion.json")
PDF_HOST = "https://www.contratacion.euskadi.eus"
MIN_TEXT_CHARS = 200


class OARCScraper(BaseScraper):
    """
    Scraper for ES/OARC-Euskadi -- Basque procurement appeals body.
    Country: ES  (jurisdiction ES-PV)
    Data types: case_law
    Auth: none (Gobierno Vasco open data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    # ── helpers ────────────────────────────────────────────────────────

    def _pdf_url_from_dataxml(self, data_xml_url: str) -> Optional[str]:
        """Fetch the record's dataXML and pull the exact born-digital PDF url."""
        if not data_xml_url:
            return None
        self.rate_limiter.wait()
        resp = self.session.get(data_xml_url, timeout=60, allow_redirects=True)
        resp.raise_for_status()
        xml = resp.content.decode("iso-8859-1", "replace")
        # The canonical relative path is in the trailing <url><![CDATA[ ... ]]></url>
        m = re.search(r"<url><!\[CDATA\[(.*?)\]\]></url>", xml, re.S)
        if not m:
            return None
        rel = m.group(1).strip()
        if not rel:
            return None
        if rel.startswith("http"):
            return quote(rel, safe=":/?&=%")
        return PDF_HOST + quote(rel, safe="/")

    def _extract_pdf_text(self, url: str) -> str:
        """Download a PDF and extract its text with PyMuPDF."""
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        self.rate_limiter.wait()
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
    def _iso_date(spanish_date: str) -> str:
        """Convert 'DD/MM/YYYY' to 'YYYY-MM-DD'; return '' if unparseable."""
        if not spanish_date:
            return ""
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", spanish_date.strip())
        if not m:
            return ""
        dd, mm, yyyy = m.groups()
        return f"{yyyy}-{mm}-{dd}"

    # ── data acquisition ───────────────────────────────────────────────

    def _fetch_catalog(self) -> list:
        """Fetch the whole OARC dataset (single JSON dump)."""
        self.rate_limiter.wait()
        logger.info("Fetching OARC catalogue...")
        resp = self.session.get(CATALOG, timeout=180, allow_redirects=True)
        resp.raise_for_status()
        records = resp.json()
        logger.info(f"Catalogue has {len(records)} resoluciones")
        return records

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every resolution record that exposes a dataXML link."""
        for record in self._fetch_catalog():
            if record.get("dataXML"):
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield resolutions dated on/after `since` (catalogue has no delta)."""
        since_date = since.date() if isinstance(since, datetime) else since
        for record in self._fetch_catalog():
            if not record.get("dataXML"):
                continue
            iso = self._iso_date(record.get("legalDocResolutionDate", "")
                                 or record.get("legalDocPublishDate", ""))
            if iso:
                try:
                    if datetime.strptime(iso, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield record

    # ── normalization ──────────────────────────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw record into the standard schema with FULL TEXT."""
        try:
            pdf_url = self._pdf_url_from_dataxml(raw.get("dataXML", ""))
        except Exception as e:
            logger.debug(f"dataXML fetch failed: {e}")
            return None
        if not pdf_url:
            return None

        try:
            text = self._extract_pdf_text(pdf_url)
        except Exception as e:
            logger.debug(f"PDF extract failed ({pdf_url}): {e}")
            return None

        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text: {len(text)} chars ({pdf_url})")
            return None

        # Resolution number: prefer folder id in physicalUrl (e.g. .../100_2012/...)
        res_number = ""
        phys = raw.get("physicalUrl", "") or ""
        mnum = re.search(r"/resolucion_oarc/([^/]+)/", phys)
        if mnum:
            res_number = mnum.group(1).replace("_", "/")

        title = raw.get("legalDocTitle", "") or raw.get("documentName", "")
        res_date = self._iso_date(raw.get("legalDocResolutionDate", ""))
        pub_date = self._iso_date(raw.get("legalDocPublishDate", ""))

        # Stable id from PDF path
        oid = re.sub(r"[^A-Za-z0-9_]+", "-", res_number) or str(abs(hash(pdf_url)))

        return {
            # Required base fields
            "_id": f"ES-PV-OARC-{oid}",
            "_source": "ES/OARC-Euskadi",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": res_date or pub_date,
            "url": raw.get("physicalUrl", "") or pdf_url,
            # Additional metadata
            "resolution_number": res_number,
            "summary": raw.get("documentDescription", "") or "",
            "contracting_authority": raw.get("contractingAuthority", "") or "",
            "publication_date": pub_date,
            "pdf_url": pdf_url,
            "tribunal": "Órgano Administrativo de Recursos Contractuales de la Comunidad Autónoma de Euskadi",
            "jurisdiction": "ES-PV",
            "language": "es",
            "country": "ES",
        }

    # ── connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print("Testing OARC (Basque procurement body) open-data catalogue...")
        try:
            records = self._fetch_catalog()
            print(f"\n1. Catalogue records: {len(records)}")
            sample = next((r for r in records if r.get("dataXML")), None)
            if sample:
                print(f"   Sample title: {sample.get('legalDocTitle','')[:70]}")
                pdf = self._pdf_url_from_dataxml(sample["dataXML"])
                print(f"   PDF: {pdf}")
                text = self._extract_pdf_text(pdf)
                print(f"   Extracted text: {len(text)} chars")
                print(f"   Sample: {text[:200]!r}")
        except Exception as e:
            print(f"   error: {e}")
        print("\nTest complete!")


def main():
    scraper = OARCScraper()

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
