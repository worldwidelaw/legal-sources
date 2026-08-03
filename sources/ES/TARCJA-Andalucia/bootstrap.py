#!/usr/bin/env python3
"""
ES/TARCJA-Andalucia -- Tribunal Administrativo de Recursos Contractuales
de la Junta de Andalucia (TARCJA)

Regional public-procurement appeals tribunal for Andalusia. It resolves the
"recurso especial en materia de contratacion" (special appeals in public
procurement) and issues binding resoluciones. This is the open-data-accessible
counterpart to the captcha-blocked central tribunal (ES/TACRC).

Strategy:
  - Official Junta de Andalucia open-data REST API returns the full dataset
    as JSON in a single call (metadata + summary + attached-file PDF uri):
        GET https://datos.juntadeandalucia.es/api/v0/tender-court-decisions/all?format=json
  - Full text lives in the born-digital resolution PDF, hosted at
        https://www.juntadeandalucia.es{uri}
    extracted with PyMuPDF (fitz). No OCR needed (born-digital).

Endpoints:
  - Dataset dump:   /api/v0/tender-court-decisions/all?format=json
  - Detail (unused): /api/v0/tender-court-decisions/{bid}
  - Count:          /api/v0/tender-court-decisions/count

Data:
  - ~6,600 resoluciones, 2011-present
  - Type: case_law
  - License: Junta de Andalucia open data (PSI reuse)
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
logger = logging.getLogger("legal-data-hunter.ES.tarcja")

API_ALL = "https://datos.juntadeandalucia.es/api/v0/tender-court-decisions/all"
API_COUNT = "https://datos.juntadeandalucia.es/api/v0/tender-court-decisions/count"
PDF_HOST = "https://www.juntadeandalucia.es"
MIN_TEXT_CHARS = 200


class TARCJAScraper(BaseScraper):
    """
    Scraper for ES/TARCJA-Andalucia -- Andalusian procurement appeals tribunal.
    Country: ES  (jurisdiction ES-AN)
    Data types: case_law
    Auth: none (Junta de Andalucia open data)
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

    @staticmethod
    def _first(value):
        """Return the first element of a list, else the value itself, else None."""
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def _pdf_uri(self, raw: dict) -> Optional[str]:
        """Dig the born-digital PDF uri out of the nested attached_file structure."""
        att = self._first(raw.get("attached_file"))
        if not isinstance(att, dict):
            return None
        fichero = self._first(att.get("field_adjunto_fichero"))
        if not isinstance(fichero, dict):
            return None
        media = self._first(fichero.get("field_media_file"))
        if not isinstance(media, dict):
            return None
        uri = media.get("uri")
        return uri or None

    def _extract_pdf_text(self, url: str) -> str:
        """Download a PDF and extract its text with PyMuPDF."""
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
        # Normalize whitespace: collapse runs of blank lines, strip trailing spaces
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _res_meta(raw: dict):
        """Return (resolution_number, resolution_date) from the nested resolution list."""
        res = TARCJAScraper._first(raw.get("resolution"))
        if isinstance(res, dict):
            return res.get("resolution_number", ""), res.get("resolution_date", "")
        return "", ""

    @staticmethod
    def _flatten_field(raw: dict, key: str, subkey: str) -> str:
        """attached list -> [{subkey: 'val'}] -> 'val'."""
        item = TARCJAScraper._first(raw.get(key))
        if isinstance(item, dict):
            return item.get(subkey, "") or ""
        return ""

    # ── data acquisition ───────────────────────────────────────────────

    def _fetch_dataset(self) -> list:
        """Fetch the entire dataset (single JSON dump)."""
        self.rate_limiter.wait()
        logger.info("Fetching TARCJA full dataset...")
        resp = self.session.get(f"{API_ALL}?format=json", timeout=180, allow_redirects=True)
        resp.raise_for_status()
        records = resp.json()
        logger.info(f"Dataset has {len(records)} resoluciones")
        return records

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every resolution record that has an attached PDF."""
        for record in self._fetch_dataset():
            if not record.get("id"):
                continue
            if not self._pdf_uri(record):
                logger.debug(f"No PDF for {record.get('id')}")
                continue
            yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield resolutions dated on/after `since` (API has no delta endpoint)."""
        since_date = since.date() if isinstance(since, datetime) else since
        for record in self._fetch_dataset():
            if not record.get("id"):
                continue
            _, res_date = self._res_meta(record)
            if res_date:
                try:
                    d = datetime.strptime(res_date[:10], "%Y-%m-%d").date()
                    if d < since_date:
                        continue
                except ValueError:
                    pass
            if not self._pdf_uri(record):
                continue
            yield record

    # ── normalization ──────────────────────────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw record into the standard schema with FULL TEXT."""
        doc_id = str(raw.get("id", ""))
        uri = self._pdf_uri(raw)
        if not uri:
            return None

        pdf_url = uri if uri.startswith("http") else f"{PDF_HOST}{uri}"
        try:
            text = self._extract_pdf_text(pdf_url)
        except Exception as e:
            logger.debug(f"PDF extract failed for {doc_id} ({pdf_url}): {e}")
            return None

        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text for {doc_id}: {len(text)} chars")
            return None

        res_number, res_date = self._res_meta(raw)
        title = raw.get("title", "") or (f"Resolución {res_number}" if res_number else doc_id)
        description = raw.get("description", "") or ""
        resource_number = raw.get("resource_number", "") or ""
        type_contract = self._flatten_field(raw, "type_contract", "type_contract")
        contested_act = self._flatten_field(raw, "contested_act", "contested_act")
        resolution_type = self._flatten_field(raw, "resolution_type", "resolution_type")

        date = ""
        if res_date:
            date = res_date[:10]

        return {
            # Required base fields
            "_id": f"ES-AN-TARCJA-{doc_id}",
            "_source": "ES/TARCJA-Andalucia",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": pdf_url,
            # Additional metadata
            "resolution_number": res_number,
            "resource_number": resource_number,
            "summary": description,
            "type_contract": type_contract,
            "contested_act": contested_act,
            "resolution_type": resolution_type,
            "tribunal": "Tribunal Administrativo de Recursos Contractuales de la Junta de Andalucía",
            "jurisdiction": "ES-AN",
            "language": "es",
            "country": "ES",
        }

    # ── connectivity test ──────────────────────────────────────────────

    def test_connection(self):
        print("Testing TARCJA (Andalusia procurement tribunal) open-data API...")
        try:
            self.rate_limiter.wait()
            c = self.session.get(f"{API_COUNT}", timeout=60)
            print(f"\n1. Count endpoint HTTP {c.status_code}: {c.text[:120]}")
        except Exception as e:
            print(f"   count error: {e}")

        try:
            records = self._fetch_dataset()
            print(f"\n2. Dataset records: {len(records)}")
            sample = next((r for r in records if self._pdf_uri(r)), None)
            if sample:
                uri = self._pdf_uri(sample)
                pdf_url = uri if uri.startswith("http") else f"{PDF_HOST}{uri}"
                print(f"   Sample id: {sample.get('id')}")
                print(f"   Title: {sample.get('title')}")
                print(f"   PDF: {pdf_url}")
                text = self._extract_pdf_text(pdf_url)
                print(f"   Extracted text: {len(text)} chars")
                print(f"   Sample: {text[:200]!r}")
        except Exception as e:
            print(f"   dataset error: {e}")
        print("\nTest complete!")


def main():
    scraper = TARCJAScraper()

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
