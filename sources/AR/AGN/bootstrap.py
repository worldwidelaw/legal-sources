#!/usr/bin/env python3
"""
AR/AGN -- Auditoría General de la Nación (Argentina) audit reports

Fetches audit report data from AGN's Drupal JSON:API backend.
Full text comes from the HTML 'cuerpo' field; for records where cuerpo is
empty, text is extracted from the FichaDePrensa PDF (fallback: Informe PDF).

~4,700+ reports (resoluciones) from 1993 to present.

API: https://webagnapi.agn.gob.ar/api/views/busqueda_avanzada/informes
No authentication required.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap --full     # Full fetch
    python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "AR/AGN"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.AGN")

API_BASE = "https://webagnapi.agn.gob.ar"
SEARCH_ENDPOINT = f"{API_BASE}/api/views/busqueda_avanzada/informes"
INCLUDES = "informe,resolucion_archivo,ficha"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/vnd.api+json, application/json",
}
REQUEST_DELAY = 1.0
MAX_PDF_SIZE = 30 * 1024 * 1024  # 30 MB limit per PDF


def strip_html(html: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = unescape(text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, fallback to pypdf."""
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text += t + "\n"
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        if text.strip():
            return text.strip()
    except Exception as e:
        logger.debug("pdfplumber failed: %s", e)

    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            t = page.extract_text()
            if t:
                text += t + "\n"
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        if text.strip():
            return text.strip()
    except Exception as e:
        logger.debug("PyPDF2 failed: %s", e)

    return ""


class AGNClient:
    """Client for the AGN Drupal JSON:API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.verify = False  # AGN has cert issues

    def get_page(self, page: int = 0, retries: int = 3) -> Optional[dict]:
        url = f"{SEARCH_ENDPOINT}?sort=-created&page={page}&include={INCLUDES}"
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                logger.warning("Page %d attempt %d failed: %s", page, attempt + 1, exc)
                if attempt < retries - 1:
                    time.sleep(2 ** (attempt + 1))
        return None

    def download_pdf(self, pdf_path: str) -> Optional[bytes]:
        """Download a PDF from AGN file server."""
        url = f"{API_BASE}{pdf_path}"
        try:
            resp = self.session.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            content_length = int(resp.headers.get("content-length", 0))
            if content_length > MAX_PDF_SIZE:
                logger.warning("PDF too large (%d bytes): %s", content_length, pdf_path)
                return None
            data = resp.content
            if len(data) > MAX_PDF_SIZE:
                return None
            return data
        except requests.RequestException as exc:
            logger.warning("PDF download failed for %s: %s", pdf_path, exc)
            return None

    def get_total_count(self) -> int:
        url = f"{SEARCH_ENDPOINT}?sort=-created&page=0"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return int(data.get("meta", {}).get("count", 0))
        except Exception:
            return 0


def build_file_index(included: list) -> Dict[str, dict]:
    """Build a lookup from file UUID to file attributes."""
    index = {}
    for item in (included or []):
        if item.get("type") == "file--file":
            index[item["id"]] = item.get("attributes", {})
    return index


def get_file_url(rel_data, file_index: dict, preferred_suffix: str = "") -> Optional[str]:
    """Get file URL from relationship data using file index."""
    if not rel_data:
        return None
    items = rel_data if isinstance(rel_data, list) else [rel_data]
    for item in items:
        fid = item.get("id")
        if fid and fid in file_index:
            uri = file_index[fid].get("uri", {})
            url = uri.get("url", "") if isinstance(uri, dict) else ""
            if url and (not preferred_suffix or url.endswith(preferred_suffix)):
                return url
    # If no match with preferred suffix, return first available
    for item in items:
        fid = item.get("id")
        if fid and fid in file_index:
            uri = file_index[fid].get("uri", {})
            url = uri.get("url", "") if isinstance(uri, dict) else ""
            if url:
                return url
    return None


def normalize(raw: dict, file_index: dict, client: AGNClient,
              download_pdfs: bool = True) -> Optional[dict]:
    """Normalize a raw JSON:API record into standard schema."""
    attrs = raw.get("attributes", {})
    rels = raw.get("relationships", {})

    nid = attrs.get("drupal_internal__nid")
    titulo = attrs.get("titulo", "")
    ano = attrs.get("ano")
    resolucion = attrs.get("resolucion")
    alias = (attrs.get("path") or {}).get("alias", "")

    # Text from cuerpo (HTML body)
    cuerpo_raw = (attrs.get("cuerpo") or {}).get("value", "") or ""
    text = strip_html(cuerpo_raw) if cuerpo_raw else ""

    # Get PDF URLs
    informe_data = (rels.get("informe") or {}).get("data")
    ficha_data = (rels.get("ficha") or {}).get("data")
    reso_data = (rels.get("resolucion_archivo") or {}).get("data")

    informe_url = get_file_url(informe_data, file_index, ".pdf")
    ficha_url = get_file_url(ficha_data, file_index, ".pdf")

    # If cuerpo is empty/short, try to extract text from PDFs
    if download_pdfs and len(text) < 100:
        pdf_text = ""
        # Try ficha first (smaller file, press factsheet)
        if ficha_url and not pdf_text:
            logger.info("Downloading ficha PDF for %s-%s...", ano, resolucion)
            pdf_bytes = client.download_pdf(ficha_url)
            if pdf_bytes:
                pdf_text = extract_pdf_text(pdf_bytes)

        # Fall back to informe (larger, full report)
        if informe_url and not pdf_text:
            logger.info("Downloading informe PDF for %s-%s...", ano, resolucion)
            pdf_bytes = client.download_pdf(informe_url)
            if pdf_bytes:
                pdf_text = extract_pdf_text(pdf_bytes)

        if pdf_text:
            text = pdf_text

    if not text:
        logger.warning("No text for nid=%s (%s-%s)", nid, ano, resolucion)
        return None

    # Date
    fecha_acta = attrs.get("fecha_acta")
    date = fecha_acta or (f"{ano}-01-01" if ano else None)

    # Periodo auditado
    periodo = attrs.get("periodo_auditado")
    periodo_str = None
    if periodo:
        start = periodo.get("value")
        end = periodo.get("end_value")
        if start and end:
            periodo_str = f"{start} / {end}"
        elif start:
            periodo_str = start

    # Actuacion
    actuacion_raw = (attrs.get("actuacion") or {}).get("value", "") or ""
    actuacion = strip_html(actuacion_raw) if actuacion_raw else ""

    # Organismo auditado (from taxonomy relationship)
    org_data = (rels.get("organismo_auditado") or {}).get("data", [])
    org_ids = []
    if isinstance(org_data, list):
        for od in org_data:
            nombre = (od.get("meta", {}) or {}).get("drupal_internal__target_id")
            if nombre:
                org_ids.append(str(nombre))

    # Tipo de auditoria
    tipo_data = (rels.get("tipo_de_auditoria") or {}).get("data", [])
    tipo_ids = []
    if isinstance(tipo_data, list):
        for td in tipo_data:
            tid = (td.get("meta", {}) or {}).get("drupal_internal__target_id")
            if tid:
                tipo_ids.append(str(tid))

    url = f"https://www.agn.gob.ar{alias}" if alias else f"https://www.agn.gob.ar/auditorias/buscador"
    pdf_full_url = f"{API_BASE}{informe_url}" if informe_url else None

    record_id = f"AGN-{ano}-{resolucion}" if ano and resolucion else f"AGN-nid-{nid}"

    return {
        "_id": record_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": titulo,
        "text": text,
        "date": date,
        "url": url,
        "pdf_url": pdf_full_url,
        "ano": ano,
        "resolucion": resolucion,
        "actuacion": actuacion,
        "fecha_acta": fecha_acta,
        "periodo_auditado": periodo_str,
        "organismo_auditado_ids": org_ids if org_ids else None,
        "tipo_de_auditoria_ids": tipo_ids if tipo_ids else None,
    }


def fetch_all(client: Optional[AGNClient] = None, sample: bool = False,
              download_pdfs: bool = True) -> Generator[dict, None, None]:
    """Fetch all AGN audit reports via paginated JSON:API.

    ``client`` is optional so the generic VPS runner can call ``fetch_all()``
    with no arguments; a client is constructed on demand when none is given.
    """
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass

    if client is None:
        client = AGNClient()

    page = 0
    total = client.get_total_count()
    logger.info("Total informes: %d", total)

    max_pages = 2 if sample else 9999
    count = 0

    while page < max_pages:
        logger.info("Fetching page %d...", page)
        data = client.get_page(page)
        if not data or not data.get("data"):
            break

        file_index = build_file_index(data.get("included", []))

        for raw in data["data"]:
            record = normalize(raw, file_index, client, download_pdfs=download_pdfs)
            if record:
                yield record
                count += 1
                if sample and count >= 15:
                    return

        if "next" not in data.get("links", {}):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    logger.info("Fetched %d records total", count)


def test_api():
    """Quick connectivity test."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    client = AGNClient()
    total = client.get_total_count()
    print(f"API reachable. Total informes: {total}")

    data = client.get_page(0)
    if data and data.get("data"):
        rec = data["data"][0]
        titulo = rec["attributes"].get("titulo", "?")
        print(f"First record: {titulo[:80]}")
    else:
        print("ERROR: Could not fetch page 0")
        sys.exit(1)


def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(description="AR/AGN bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch records")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
        return

    if args.command == "bootstrap":
        sample = args.sample
        client = AGNClient()

        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        count = 0
        for record in fetch_all(client, sample=sample, download_pdfs=True):
            outpath = SAMPLE_DIR / f"{record['_id']}.json"
            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("[%d] %s — %d chars", count, record["_id"], text_len)

        logger.info("Done. %d records saved to %s", count, SAMPLE_DIR)
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
