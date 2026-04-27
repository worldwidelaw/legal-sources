#!/usr/bin/env python3
"""
SV/CorteSuprema -- El Salvador Supreme Court Jurisprudence Fetcher

Fetches Salvadoran court decisions from jurisprudencia.gob.sv.

Strategy:
  - Use advanced search POST endpoint to list decisions by court level
  - Parse HTML result tables to extract case metadata and PDF paths
  - Extract rich metadata (court, tipo_proceso, materia, fallo) inline from
    the search results HTML, avoiding extra HTTP requests per record
  - Download PDFs and extract full text via pdfplumber
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import io
import html as htmlmod
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urlencode, unquote, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SV.CorteSuprema")

BASE_URL = "https://www.jurisprudencia.gob.sv"
SEARCH_URL = f"{BASE_URL}/busqueda/result.php"

# Court level 1 IDs for iterating
NIVEL1_COURTS = {
    86: "Corte Suprema de Justicia en Corte Plena",
    1: "Salas",
    101: "Cámaras",
    17: "Tribunales de Sentencia",
    1189: "Juzgados",
}

# Reasonable page size to avoid timeouts on VPS
SEARCH_PAGE_SIZE = 200


class CorteSupremaScraper(BaseScraper):
    """Scraper for SV/CorteSuprema -- Salvadoran court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session = None
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=120)
        except ImportError:
            self.client = None

    def _http_get(self, url: str, timeout: int = 120) -> Optional[bytes]:
        """HTTP GET returning raw bytes."""
        for attempt in range(4):
            try:
                if self.client:
                    resp = self.client.get(url)
                    if resp.status_code == 200:
                        return resp.content
                    if resp.status_code in (400, 404, 500):
                        return None
                    logger.warning(f"HTTP {resp.status_code} for {url[:100]}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "application/pdf,*/*",
                    })
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        return resp.read()
            except Exception as e:
                if "404" in str(e) or "400" in str(e):
                    return None
                logger.warning(f"Attempt {attempt+1}/4 GET failed for {url[:80]}: {e}")
                time.sleep(3 * (attempt + 1))
        return None

    def _http_post(self, url: str, data: Dict[str, Any],
                   timeout: int = 120) -> Optional[str]:
        """HTTP POST returning response text."""
        for attempt in range(4):
            try:
                if self.client:
                    resp = self.client.post(url, data=data)
                    if resp.status_code == 200:
                        return resp.text
                    logger.warning(f"HTTP {resp.status_code} for POST {url[:80]}")
                else:
                    import urllib.request
                    encoded = urlencode(data).encode("utf-8")
                    req = urllib.request.Request(url, data=encoded, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/120.0.0.0 Safari/537.36",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "text/html,*/*",
                    })
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1}/4 POST failed for {url[:80]}: {e}")
                time.sleep(3 * (attempt + 1))
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes, db_id: str = "") -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="SV/CorteSuprema",
            source_id=f"SV-CorteSuprema-{db_id}" if db_id else "",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _parse_search_results(self, html_text: str) -> List[Dict[str, str]]:
        """Parse HTML search results into a list of decision metadata.

        Extracts both URL params (db_id, pdf_path, date, reference) and
        inline metadata (court, tipo_proceso, materia, fallo, tipo_resolucion)
        from the search results HTML in a single pass. This avoids needing
        separate HTTP requests to showFile.php for metadata.
        """
        results = []

        # Split into table rows — each <tr> contains one decision
        row_blocks = re.split(r'<tr[^>]*>', html_text)

        seen_ids = set()
        for block in row_blocks:
            # Look for showFile link in this row
            link_match = re.search(
                r"showFile\.php\?([^'\"<>]+)", block
            )
            if not link_match:
                continue

            # Parse URL params
            params_str = link_match.group(1)
            params = {}
            for part in params_str.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k] = unquote(v)

            number = params.get("number", "")
            if not number or number in seen_ids:
                continue
            seen_ids.add(number)

            # Parse date from DD/MM/YYYY to ISO
            fecha_raw = params.get("fecha", "")
            date_iso = ""
            if fecha_raw and "/" in fecha_raw:
                parts = fecha_raw.split("/")
                if len(parts) == 3:
                    date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"

            record = {
                "db_id": number,
                "pdf_path": params.get("data", ""),
                "date": date_iso,
                "reference": params.get("numero", ""),
            }

            # Extract inline metadata from the same row block
            # Format: <b><i>FieldName</b></i>: value<br
            meta_matches = re.findall(
                r'<b><i>(.*?)</b></i>:\s*(.*?)(?:<br|</a>|$)',
                block, re.DOTALL
            )
            for field_name, field_val in meta_matches:
                clean_val = re.sub(r'<[^>]+>', '', field_val).strip()
                clean_val = htmlmod.unescape(clean_val).strip()
                if not clean_val or clean_val == "-":
                    continue
                fn = field_name.strip().lower()
                if "origen" in fn:
                    record["origen"] = clean_val
                elif "tribunal" in fn:
                    record["court"] = clean_val
                elif "tipo de proceso" in fn:
                    record["tipo_proceso"] = clean_val
                elif "resoluci" in fn:
                    record["tipo_resolucion"] = clean_val
                elif "materia" in fn:
                    record["materia"] = clean_val
                elif "fallo" in fn:
                    record["fallo"] = clean_val

            # Fallback: use origen as court if tribunal not found
            if "court" not in record and "origen" in record:
                record["court"] = record["origen"]

            results.append(record)

        return results

    def _search_decisions(self, nivel1: int = -1, inicio: str = "-1",
                          fin: str = "-1", max_results: int = SEARCH_PAGE_SIZE,
                          ) -> List[Dict[str, str]]:
        """Search for decisions using advanced search."""
        data = {
            "avanzada": "true",
            "baseDatos": "1",  # Jurisprudencia
            "nivel1": str(nivel1),
            "nivel2": "-1",
            "nivel3": "-1",
            "nivel4": "-1",
            "maximo": str(max_results),
            "inicio": inicio,
            "fin": fin,
            "numeroReferencia": "-1",
            "nombreDocumento": "-1",
            "propiedades": "",
        }
        time.sleep(2)
        html = self._http_post(SEARCH_URL, data, timeout=180)
        if not html:
            return []
        return self._parse_search_results(html)

    def _fetch_decision_text(self, pdf_path: str, db_id: str = "") -> str:
        """Download PDF and extract text."""
        if not pdf_path:
            return ""
        # Build full URL — pdf_path already starts with "DocumentosBoveda/"
        url = f"{BASE_URL}/{pdf_path}"
        time.sleep(1.5)
        pdf_bytes = self._http_get(url, timeout=120)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {url[:100]}")
            return ""
        if len(pdf_bytes) < 100:
            return ""
        return self._extract_pdf_text(pdf_bytes, db_id=db_id)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        ref = raw.get("reference", "")
        court = raw.get("court", raw.get("tribunal", ""))
        title = f"{ref} - {court}" if ref and court else ref or court or f"SV-{raw.get('db_id', '')}"

        return {
            "_id": f"SV-CorteSuprema-{raw.get('db_id', '')}",
            "_source": "SV/CorteSuprema",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": f"{BASE_URL}/{raw.get('pdf_path', '')}",
            "reference_number": ref,
            "court": court,
            "tipo_proceso": raw.get("tipo_proceso", ""),
            "tipo_resolucion": raw.get("tipo_resolucion", ""),
            "materia": raw.get("materia", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions by iterating court levels.

        Uses paginated searches (SEARCH_PAGE_SIZE per request) to avoid
        timeouts on VPS. Extracts metadata inline from search results.
        Yields records even if PDF text extraction fails on first attempt,
        retrying once before giving up.
        """
        count = 0
        skipped = 0
        pdf_failures = 0

        for nivel1, court_name in NIVEL1_COURTS.items():
            logger.info(f"Searching court level: {court_name} (nivel1={nivel1})")
            # Use a moderate page size to avoid huge responses that time out
            results = self._search_decisions(
                nivel1=nivel1, max_results=SEARCH_PAGE_SIZE
            )
            logger.info(f"Found {len(results)} results for {court_name}")

            for result in results:
                # Fetch full text from PDF
                text = ""
                try:
                    text = self._fetch_decision_text(
                        result.get("pdf_path", ""),
                        db_id=result.get("db_id", ""),
                    )
                except Exception as e:
                    logger.warning(
                        f"PDF extraction error for "
                        f"{result.get('db_id', '?')}: {e}"
                    )

                if not text:
                    # Retry once after a longer pause (VPS rate limiting)
                    time.sleep(3)
                    try:
                        text = self._fetch_decision_text(
                            result.get("pdf_path", ""),
                            db_id=result.get("db_id", ""),
                        )
                    except Exception:
                        pass

                if not text:
                    pdf_failures += 1
                    if pdf_failures <= 5:
                        logger.warning(
                            f"No text after retry for "
                            f"{result.get('reference', result.get('db_id', '?'))}"
                        )
                    # After 10 consecutive PDF failures, likely IP-blocked —
                    # stop wasting time and skip remaining
                    if pdf_failures >= 10 and count == 0:
                        logger.error(
                            "10 consecutive PDF failures with 0 successes — "
                            "likely IP-blocked. Stopping."
                        )
                        return
                    continue

                # Reset consecutive failure counter on success
                pdf_failures = 0

                result["text"] = text
                result["court"] = result.get("court", court_name)

                count += 1
                skipped_so_far = skipped
                yield result

        logger.info(
            f"Completed: {count} decisions fetched, {skipped} skipped (no text)"
        )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions from a given date."""
        if not since:
            yield from self.fetch_all()
            return

        count = 0
        results = self._search_decisions(nivel1=-1, inicio=since, max_results=450)
        for result in results:
            text = self._fetch_decision_text(
                result.get("pdf_path", ""),
                db_id=result.get("db_id", ""),
            )
            if not text:
                continue
            result["text"] = text
            count += 1
            yield result

        logger.info(f"Updates: {count} decisions since {since}")

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test search endpoint
        results = self._search_decisions(nivel1=86, max_results=5)
        if not results:
            logger.error("Search returned no results")
            return False
        logger.info(f"Search OK: {len(results)} results from Corte Plena")

        # Verify inline metadata was extracted
        first = results[0]
        meta_keys = [k for k in ("court", "tipo_proceso", "materia") if first.get(k)]
        logger.info(f"Inline metadata fields found: {meta_keys}")

        # Test PDF download and text extraction
        text = self._fetch_decision_text(first.get("pdf_path", ""))
        if not text:
            logger.error("Could not extract text from PDF")
            return False
        logger.info(f"PDF text extraction OK: {len(text)} chars")
        logger.info(f"Text preview: {text[:200]}")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SV/CorteSuprema data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CorteSupremaScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample)

    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
