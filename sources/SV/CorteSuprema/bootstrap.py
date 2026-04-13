#!/usr/bin/env python3
"""
SV/CorteSuprema -- El Salvador Supreme Court Jurisprudence Fetcher

Fetches Salvadoran court decisions from jurisprudencia.gob.sv.

Strategy:
  - Use advanced search POST endpoint to list decisions by court level
  - Parse HTML result tables to extract case metadata and PDF paths
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
from urllib.parse import urlencode, unquote

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


class CorteSupremaScraper(BaseScraper):
    """Scraper for SV/CorteSuprema -- Salvadoran court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session = None
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=90)
        except ImportError:
            self.client = None

    def _http_get(self, url: str) -> Optional[bytes]:
        """HTTP GET returning raw bytes."""
        for attempt in range(3):
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
                        "User-Agent": "Mozilla/5.0 (LegalDataHunter)",
                    })
                    with urllib.request.urlopen(req, timeout=90) as resp:
                        return resp.read()
            except Exception as e:
                if "404" in str(e) or "400" in str(e):
                    return None
                logger.warning(f"Attempt {attempt+1} GET failed for {url[:80]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _http_post(self, url: str, data: Dict[str, Any]) -> Optional[str]:
        """HTTP POST returning response text."""
        for attempt in range(3):
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
                        "User-Agent": "Mozilla/5.0 (LegalDataHunter)",
                        "Content-Type": "application/x-www-form-urlencoded",
                    })
                    with urllib.request.urlopen(req, timeout=90) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} POST failed for {url[:80]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="SV/CorteSuprema",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _parse_search_results(self, html_text: str) -> List[Dict[str, str]]:
        """Parse HTML search results into a list of decision metadata."""
        results = []
        # Extract rows from showFile links
        # Pattern: showFile.php?bd=1&data=<path>&number=<id>&fecha=<date>&numero=<ref>&...
        pattern = r'showFile\.php\?([^"\']+)'
        matches = re.findall(pattern, html_text)

        seen_ids = set()
        for match in matches:
            params = {}
            for part in match.split("&"):
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

            results.append({
                "db_id": number,
                "pdf_path": params.get("data", ""),
                "date": date_iso,
                "reference": params.get("numero", ""),
            })

        # Also try to extract court/tribunal info from table cells
        # The HTML has rows with metadata after each showFile link
        row_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.findall(row_pattern, html_text, re.DOTALL)

        # Try to extract tribunal names from td cells
        for i, row in enumerate(rows):
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            # Look for tribunal name in cells
            if len(cells) >= 4:
                tribunal = re.sub(r'<[^>]+>', '', cells[3] if len(cells) > 3 else "").strip()
                # Match back to results by index if possible
                if i < len(results) and tribunal:
                    results[i]["court"] = tribunal

        return results

    def _search_decisions(self, nivel1: int = -1, inicio: str = "-1",
                          fin: str = "-1", max_results: int = 450) -> List[Dict[str, str]]:
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
        time.sleep(1.5)
        html = self._http_post(SEARCH_URL, data)
        if not html:
            return []
        return self._parse_search_results(html)

    def _fetch_decision_text(self, pdf_path: str) -> str:
        """Download PDF and extract text."""
        if not pdf_path:
            return ""
        # Build full URL — pdf_path already starts with "DocumentosBoveda/"
        url = f"{BASE_URL}/{pdf_path}"
        time.sleep(1)
        pdf_bytes = self._http_get(url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {url[:100]}")
            return ""
        if len(pdf_bytes) < 100:
            return ""
        return self._extract_pdf_text(pdf_bytes)

    def _fetch_metadata(self, db_id: str, pdf_path: str, fecha: str,
                        numero: str) -> Dict[str, str]:
        """Fetch metadata page for a decision."""
        url = (f"{BASE_URL}/busqueda/showFile.php?"
               f"bd=1&data={pdf_path}&number={db_id}"
               f"&fecha={fecha}&numero={numero}&cesta=0&singlePage=false")
        time.sleep(1)
        if self.client:
            try:
                resp = self.client.get(url)
                html = resp.text if resp.status_code == 200 else ""
            except Exception:
                html = ""
        else:
            import urllib.request
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (LegalDataHunter)",
                })
                with urllib.request.urlopen(req, timeout=60) as resp:
                    html = resp.read().decode("utf-8", errors="replace")
            except Exception:
                html = ""

        meta = {}
        if html:
            # Extract metadata fields
            field_patterns = {
                "tribunal": r'(?:Nombre\s+de\s+tribunal|Tribunal)[:\s]*</[^>]+>\s*<[^>]+>([^<]+)',
                "tipo_proceso": r'Tipo\s+de\s+proceso[:\s]*</[^>]+>\s*<[^>]+>([^<]+)',
                "tipo_resolucion": r'Tipo\s+de\s+Resoluci[oó]n[:\s]*</[^>]+>\s*<[^>]+>([^<]+)',
                "materia": r'Materia[:\s]*</[^>]+>\s*<[^>]+>([^<]+)',
                "fallo": r'Fallo[:\s]*</[^>]+>\s*<[^>]+>([^<]+)',
                "origen": r'Origen[:\s]*</[^>]+>\s*<[^>]+>([^<]+)',
            }
            for key, pat in field_patterns.items():
                m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
                if m:
                    val = htmlmod.unescape(m.group(1)).strip()
                    if val and val != "-":
                        meta[key] = val
        return meta

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
        """Fetch all decisions by iterating court levels."""
        count = 0
        for nivel1, court_name in NIVEL1_COURTS.items():
            logger.info(f"Searching court level: {court_name} (nivel1={nivel1})")
            results = self._search_decisions(nivel1=nivel1, max_results=450)
            logger.info(f"Found {len(results)} results for {court_name}")

            for result in results:
                # Fetch full text from PDF
                text = self._fetch_decision_text(result.get("pdf_path", ""))
                if not text:
                    logger.warning(f"No text for {result.get('reference', result.get('db_id', '?'))}")
                    continue

                result["text"] = text
                result["court"] = result.get("court", court_name)

                record = self.normalize(result)
                count += 1
                yield record

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions from a given date."""
        if not since:
            yield from self.fetch_all()
            return

        count = 0
        results = self._search_decisions(nivel1=-1, inicio=since, max_results=450)
        for result in results:
            text = self._fetch_decision_text(result.get("pdf_path", ""))
            if not text:
                continue
            result["text"] = text
            record = self.normalize(result)
            count += 1
            yield record

        logger.info(f"Updates: {count} decisions since {since}")

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test search endpoint
        results = self._search_decisions(nivel1=86, max_results=5)
        if not results:
            logger.error("Search returned no results")
            return False
        logger.info(f"Search OK: {len(results)} results from Corte Plena")

        # Test PDF download and text extraction
        first = results[0]
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
    args = parser.parse_args()

    scraper = CorteSupremaScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
