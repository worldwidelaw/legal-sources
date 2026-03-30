#!/usr/bin/env python3
"""
PY/CSJJurisprudencia -- Paraguay Supreme Court Jurisprudence

Fetches case law from the Corte Suprema de Justicia del Paraguay.

Data source: https://www.csj.gov.py/jurisprudencia/
License: Open Government Data (Paraguay)

Strategy:
  - Establish a session with the ASP.NET jurisprudencia portal
  - Submit a search via POST /Home/Busqueda with year criteria
  - Paginate results via DataTables server-side API (/Jurisprudencias/GetData)
  - Download PDF for each decision, extract text with PyPDF2
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
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
from typing import Generator, Optional
from urllib.parse import urlencode

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import PyPDF2
except ImportError:
    PyPDF2 = None

# Setup
SOURCE_ID = "PY/CSJJurisprudencia"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PY.CSJJurisprudencia")

# Configuration
BASE_URL = "https://www.csj.gov.py"
CRITERIOS_URL = f"{BASE_URL}/jurisprudencia/Home/Criterios"
BUSQUEDA_URL = f"{BASE_URL}/jurisprudencia/Home/Busqueda"
GETDATA_URL = f"{BASE_URL}/jurisprudencia/Jurisprudencias/GetData"
DOCUMENT_URL = f"{BASE_URL}/jurisprudencia/home/DocumentoJurisprudencia"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

RATE_LIMIT = 1.5  # seconds between requests
PAGE_SIZE = 25
# Available years in the system
YEARS = list(range(2026, 1992, -1))


def extract_csrf_token(html: str) -> Optional[str]:
    """Extract __RequestVerificationToken from the HTML form."""
    m = re.search(r'name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"', html)
    if not m:
        m = re.search(r'value="([^"]+)"\s*/?\s*>', html)
    return m.group(1) if m else None


def parse_dotnet_date(date_str: str) -> Optional[str]:
    """Convert .NET /Date(timestamp)/ to ISO 8601."""
    if not date_str:
        return None
    m = re.search(r'/Date\((-?\d+)\)/', date_str)
    if m:
        ts = int(m.group(1)) / 1000
        try:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except (OSError, ValueError):
            return None
    return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyPDF2."""
    if PyPDF2 is None:
        logger.warning("PyPDF2 not available, cannot extract PDF text")
        return ""
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
        return "\n\n".join(text_parts).strip()
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def clean_text(text: str) -> str:
    """Clean extracted text: normalize whitespace, remove artifacts."""
    if not text:
        return ""
    # Normalize line breaks
    text = re.sub(r'\r\n', '\n', text)
    # Collapse excessive whitespace within lines
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        line = re.sub(r'[ \t]+', ' ', line).strip()
        if line:
            cleaned.append(line)
    return '\n'.join(cleaned)


# DataTables column spec (required for server-side processing to return data)
DATATABLES_COLUMNS = (
    "columns%5B0%5D%5Bdata%5D=CodigoJurisprudencia"
    "&columns%5B0%5D%5Bsearchable%5D=true"
    "&columns%5B0%5D%5Borderable%5D=true"
    "&columns%5B1%5D%5Bdata%5D=TipoResolucionJudicial.DescripcionTipoResolucionJudicial"
    "&columns%5B2%5D%5Bdata%5D=NoResolucionJudicial"
    "&columns%5B3%5D%5Bdata%5D=FechaResolucionJudicial"
    "&columns%5B4%5D%5Bdata%5D=CaratulaPublicacion"
    "&columns%5B5%5D%5Bdata%5D=Sala.DescripcionSala"
    "&order%5B0%5D%5Bcolumn%5D=0"
    "&order%5B0%5D%5Bdir%5D=asc"
    "&search%5Bvalue%5D="
    "&search%5Bregex%5D=false"
)


class CSJSession:
    """Manages a session with the CSJ jurisprudencia portal."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.verify = False
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def init_search(self, year: int) -> int:
        """Initialize a search for a given year. Returns total record count."""
        # Step 1: Get the form page and CSRF token
        resp = self.session.get(CRITERIOS_URL)
        resp.raise_for_status()
        token = extract_csrf_token(resp.text)
        if not token:
            raise RuntimeError("Could not extract CSRF token from Criterios page")

        # Step 2: Submit search form
        form_data = {
            "__RequestVerificationToken": token,
            "PalabrasTexto": "",
            "TipoResolucion": "",
            "Numero": "",
            "Anno": str(year),
            "RangoFecha": "",
        }
        resp = self.session.post(BUSQUEDA_URL, data=form_data)
        resp.raise_for_status()

        # Step 3: Quick probe to get total count
        probe_data = f"draw=1&{DATATABLES_COLUMNS}&start=0&length=1"
        resp = self.session.post(GETDATA_URL, data=probe_data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        data = json.loads(resp.text, strict=False)
        return data.get("recordsFiltered", 0)

    def fetch_page(self, start: int, length: int, draw: int = 1) -> dict:
        """Fetch a page of results from the current search."""
        page_data = f"draw={draw}&{DATATABLES_COLUMNS}&start={start}&length={length}"
        resp = self.session.post(GETDATA_URL, data=page_data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
        resp.raise_for_status()
        return json.loads(resp.text, strict=False)

    def fetch_document_pdf(self, codigo: int) -> bytes:
        """Download the PDF document for a decision."""
        resp = self.session.get(f"{DOCUMENT_URL}?codigo={codigo}")
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" in content_type or "octet-stream" in content_type:
            return resp.content
        return b""


def normalize(raw: dict, pdf_text: str) -> dict:
    """Transform raw CSJ record + PDF text into standard schema."""
    codigo = raw.get("CodigoJurisprudencia", 0)
    date_str = parse_dotnet_date(raw.get("FechaResolucionJudicial", ""))
    year = None
    if date_str:
        try:
            year = int(date_str[:4])
        except (ValueError, TypeError):
            pass

    resolution_type = ""
    tipo = raw.get("TipoResolucionJudicial")
    if isinstance(tipo, dict):
        resolution_type = tipo.get("DescripcionTipoResolucionJudicial", "") or ""

    chamber = ""
    sala = raw.get("Sala")
    if isinstance(sala, dict):
        chamber = sala.get("DescripcionSala", "") or ""

    subject = ""
    materia = raw.get("Materia")
    if isinstance(materia, dict):
        subject = materia.get("DescripMateria", "") or ""

    title = raw.get("CaratulaPublicacion", "") or ""
    resolution_num = raw.get("NoResolucionJudicial", "")

    return {
        "_id": f"PY-CSJ-{codigo}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.strip(),
        "text": clean_text(pdf_text),
        "date": date_str,
        "url": f"{DOCUMENT_URL}?codigo={codigo}",
        "resolution_type": resolution_type,
        "resolution_number": str(resolution_num),
        "year": year,
        "chamber": chamber,
        "subject_matter": subject,
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all decisions, optionally limited to sample size."""
    if PyPDF2 is None:
        logger.error("PyPDF2 is required for PDF text extraction. Install: pip3 install PyPDF2")
        return

    csj = CSJSession()
    total_yielded = 0
    sample_limit = 15 if sample else None

    years_to_process = YEARS if not sample else [2024]

    for year in years_to_process:
        if sample_limit and total_yielded >= sample_limit:
            break

        logger.info(f"Initializing search for year {year}...")
        try:
            total = csj.init_search(year)
        except Exception as e:
            logger.warning(f"Failed to init search for year {year}: {e}")
            time.sleep(2)
            continue

        if total == 0:
            logger.info(f"Year {year}: 0 records, skipping")
            continue

        logger.info(f"Year {year}: {total} records found")

        start = 0
        draw = 1
        while start < total:
            if sample_limit and total_yielded >= sample_limit:
                break

            try:
                page = csj.fetch_page(start, PAGE_SIZE, draw)
            except Exception as e:
                logger.warning(f"Failed to fetch page at offset {start}: {e}")
                time.sleep(3)
                start += PAGE_SIZE
                draw += 1
                continue

            records = page.get("data", [])
            if not records:
                break

            for rec in records:
                if sample_limit and total_yielded >= sample_limit:
                    break

                codigo = rec.get("CodigoJurisprudencia")
                has_doc = rec.get("TieneDocumento", False)

                pdf_text = ""
                if has_doc and codigo:
                    try:
                        time.sleep(RATE_LIMIT)
                        pdf_bytes = csj.fetch_document_pdf(codigo)
                        if pdf_bytes:
                            pdf_text = extract_pdf_text(pdf_bytes)
                    except Exception as e:
                        logger.warning(f"PDF download failed for {codigo}: {e}")

                if not pdf_text:
                    logger.debug(f"No text for {codigo}, skipping")
                    continue

                record = normalize(rec, pdf_text)
                total_yielded += 1
                yield record

            start += PAGE_SIZE
            draw += 1
            time.sleep(RATE_LIMIT)

    logger.info(f"Total records yielded: {total_yielded}")


def test_api():
    """Quick connectivity and API test."""
    logger.info("Testing CSJ Jurisprudencia API...")

    csj = CSJSession()

    # Test search
    total = csj.init_search(2024)
    logger.info(f"Year 2024: {total} records")

    if total == 0:
        logger.error("No records found for 2024 — API may have changed")
        return False

    # Test pagination
    page = csj.fetch_page(0, 2)
    records = page.get("data", [])
    if not records:
        logger.error("No data in page response")
        return False

    logger.info(f"First record: {records[0].get('CaratulaPublicacion', '')[:80]}")

    # Test PDF download
    codigo = records[0].get("CodigoJurisprudencia")
    if codigo and records[0].get("TieneDocumento"):
        pdf_bytes = csj.fetch_document_pdf(codigo)
        logger.info(f"PDF downloaded: {len(pdf_bytes)} bytes")

        if PyPDF2:
            text = extract_pdf_text(pdf_bytes)
            logger.info(f"Extracted text: {len(text)} chars")
            logger.info(f"Preview: {text[:200]}...")
        else:
            logger.warning("PyPDF2 not installed, cannot test text extraction")

    logger.info("API test passed!")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = SAMPLE_DIR if sample else (SOURCE_DIR / "data")
    output_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        fname = f"{record['_id'].replace('/', '_')}.json"
        out_path = output_dir / fname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"[{count}] Saved: {record['_id']} — {record['title'][:60]}")

    logger.info(f"Bootstrap complete: {count} records saved to {output_dir}")
    return count


def main():
    parser = argparse.ArgumentParser(description="PY/CSJJurisprudencia data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample for validation")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
