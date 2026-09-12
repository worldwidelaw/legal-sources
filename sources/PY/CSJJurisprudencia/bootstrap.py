#!/usr/bin/env python3
"""
PY/CSJJurisprudencia -- Paraguay Supreme Court Jurisprudence

Fetches case law from the Corte Suprema de Justicia del Paraguay.

Data source: https://www.csj.gov.py/jurisprudencia/
License: Open Government Data (Paraguay)

Strategy:
  - Establish a session with the ASP.NET jurisprudencia portal
  - Submit a search via POST /Home/Busqueda with a year criterion
  - Page the whole year's metadata through the DataTables server-side API
    (/Jurisprudencias/GetData) BEFORE touching any PDF, so the search
    session is never held open across slow downloads
  - Download each decision PDF concurrently and extract its text
  - Append normalized records to data/records.jsonl as they complete

Issue #1404: the previous version crawled ~6,900 records in three days and
then read as stalled. Three separate causes, all fixed here:

  1. No request ever passed a ``timeout``. A host that accepts a connection
     and then trickles (or stops sending) bytes holds a single ``session.get``
     open indefinitely — the process stays alive, the log stays silent, and the
     worker looks hung. Every call now goes through ``request_with_deadline``.
  2. The full path wrote one JSON file per record into ``data/`` and kept no
     cursor, so a restart re-walked from 2026 and rewrote the same records
     forever. Output is now a single appended ``data/records.jsonl`` and the
     crawl resumes by skipping the codigos already in it.
  3. Serial download+extract runs ~5.5 s/record. The corpus is ~71,800
     decisions (measured across 1993-2026), i.e. ~110 h — past the fleet's
     100 h cap, so a full refresh could never finish. PDFs are fetched by a
     small worker pool, which brings a full pass under ~12 h.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Same, fleet entry point
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown
from common.http_client import request_with_deadline


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "PY/CSJJurisprudencia"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"

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

# Socket-level timeout, then a hard wall-clock ceiling for the whole call.
# The wall clock is what actually saves the run: `timeout` is per socket
# operation, so a trickling response resets it forever.
TIMEOUT = (15, 120)
WALL_TIMEOUT = 180
PDF_WALL_TIMEOUT = 300

RATE_LIMIT = 0.5   # seconds each PDF worker waits between downloads
PAGE_SIZE = 500    # metadata rows per DataTables call (server accepts 500)
PDF_WORKERS = int(os.environ.get("PY_CSJ_WORKERS", "5"))

# Stop cleanly and keep what we have rather than being killed at the fleet's
# 100 h ceiling with no exit code and no summary.
MAX_HOURS = float(os.environ.get("PY_CSJ_MAX_HOURS", "90"))

# Available years in the system (1994 has 0 records, 1993 has 1).
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
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="PY/CSJJurisprudencia",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


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


def new_session() -> requests.Session:
    """A bare session with SSL verification off (the host's chain is broken)."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    return session


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
    """Manages a search session with the CSJ jurisprudencia portal.

    Only the metadata search is stateful — /Home/Busqueda stores the year on
    the server side and GetData reads it back. Document downloads address a
    decision by its own codigo and carry no search state, which is what makes
    the PDF worker pool safe.
    """

    def __init__(self):
        self.session = new_session()

    def init_search(self, year: int) -> int:
        """Initialize a search for a given year. Returns total record count."""
        # Step 1: Get the form page and CSRF token
        resp = request_with_deadline(
            self.session, "GET", CRITERIOS_URL,
            WALL_TIMEOUT, timeout=TIMEOUT,
        )
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
        resp = request_with_deadline(
            self.session, "POST", BUSQUEDA_URL,
            WALL_TIMEOUT, data=form_data, timeout=TIMEOUT,
        )
        resp.raise_for_status()

        # Step 3: Quick probe to get total count
        probe_data = f"draw=1&{DATATABLES_COLUMNS}&start=0&length=1"
        resp = request_with_deadline(
            self.session, "POST", GETDATA_URL,
            WALL_TIMEOUT, data=probe_data, timeout=TIMEOUT,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = json.loads(resp.text, strict=False)
        return data.get("recordsFiltered", 0)

    def fetch_page(self, start: int, length: int, draw: int = 1) -> dict:
        """Fetch a page of results from the current search."""
        page_data = f"draw={draw}&{DATATABLES_COLUMNS}&start={start}&length={length}"
        resp = request_with_deadline(
            self.session, "POST", GETDATA_URL,
            WALL_TIMEOUT, data=page_data, timeout=TIMEOUT,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        return json.loads(resp.text, strict=False)

    def list_year(self, year: int) -> list:
        """Page a whole year's metadata into memory.

        Deliberately drained before any PDF is touched: the year lives in the
        server-side session, so interleaving slow downloads with paging risks
        the search state expiring mid-year.
        """
        total = self.init_search(year)
        if total == 0:
            return []

        rows = []
        start = 0
        draw = 1
        while start < total:
            try:
                page = self.fetch_page(start, PAGE_SIZE, draw)
            except Exception as e:
                logger.warning(f"Year {year}: page at offset {start} failed: {e}")
                start += PAGE_SIZE
                draw += 1
                continue

            records = page.get("data", [])
            if not records:
                break
            rows.extend(records)
            start += PAGE_SIZE
            draw += 1

        return rows


def fetch_document_pdf(session: requests.Session, codigo: int) -> bytes:
    """Download the PDF document for a decision (stateless, by codigo)."""
    resp = request_with_deadline(
        session, "GET", f"{DOCUMENT_URL}?codigo={codigo}",
        PDF_WALL_TIMEOUT, timeout=TIMEOUT,
    )
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


_local = threading.local()


def _worker_session() -> requests.Session:
    """One session per pool thread — requests.Session is not thread-safe."""
    session = getattr(_local, "session", None)
    if session is None:
        session = new_session()
        _local.session = session
    return session


def _fetch_one(rec: dict) -> Optional[dict]:
    """Download and extract one decision. Returns None if it has no text."""
    codigo = rec.get("CodigoJurisprudencia")
    if not codigo or not rec.get("TieneDocumento", False):
        return None

    try:
        time.sleep(RATE_LIMIT)
        pdf_bytes = fetch_document_pdf(_worker_session(), codigo)
    except Exception as e:
        logger.warning(f"PDF download failed for {codigo}: {e}")
        return None

    if not pdf_bytes:
        return None

    try:
        pdf_text = extract_pdf_text(pdf_bytes)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {codigo}: {e}")
        return None

    if not pdf_text:
        return None

    return normalize(rec, pdf_text)


def load_seen_ids() -> set:
    """Read the codigos already written, so a restart resumes instead of re-crawling.

    The old version kept no cursor at all: every relaunch walked from 2026 and
    rewrote the same leading records, which is how three days of fleet time
    produced ~6,900 rows (issue #1404).
    """
    seen = set()
    if not RECORDS_PATH.exists():
        return seen
    with open(RECORDS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                seen.add(json.loads(line)["_id"])
            except (ValueError, KeyError):
                continue
    return seen


def fetch_all(sample: bool = False,
              seen: Optional[set] = None,
              deadline: Optional[float] = None) -> Generator[dict, None, None]:
    """Fetch all decisions, optionally limited to sample size."""
    csj = CSJSession()
    seen = seen if seen is not None else set()
    total_yielded = 0
    sample_limit = 15 if sample else None

    years_to_process = YEARS if not sample else [2024]

    for year in years_to_process:
        if sample_limit and total_yielded >= sample_limit:
            break
        if deadline and time.time() > deadline:
            logger.warning(f"Wall-clock budget ({MAX_HOURS} h) reached — stopping before {year}")
            break

        logger.info(f"Listing year {year}...")
        try:
            rows = csj.list_year(year)
        except Exception as e:
            logger.warning(f"Failed to list year {year}: {e}")
            continue

        if not rows:
            logger.info(f"Year {year}: 0 records, skipping")
            continue

        pending = [r for r in rows
                   if f"PY-CSJ-{r.get('CodigoJurisprudencia')}" not in seen]
        logger.info(
            f"Year {year}: {len(rows)} records, {len(pending)} to fetch "
            f"({len(rows) - len(pending)} already have text)"
        )
        if sample_limit:
            pending = pending[:sample_limit * 3]

        year_count = 0
        with ThreadPoolExecutor(max_workers=PDF_WORKERS) as pool:
            for record in pool.map(_fetch_one, pending):
                if record is None:
                    continue
                seen.add(record["_id"])
                total_yielded += 1
                year_count += 1
                yield record

                if sample_limit and total_yielded >= sample_limit:
                    break
                if deadline and time.time() > deadline:
                    logger.warning(
                        f"Wall-clock budget ({MAX_HOURS} h) reached mid-year {year} — stopping"
                    )
                    break
                if year_count % 250 == 0:
                    logger.info(f"  year {year}: {year_count:,} fetched "
                                f"({total_yielded:,} this run)")

        logger.info(f"Year {year} done: {year_count:,} records ({total_yielded:,} this run)")

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
        pdf_bytes = fetch_document_pdf(new_session(), codigo)
        logger.info(f"PDF downloaded: {len(pdf_bytes)} bytes")

        text = extract_pdf_text(pdf_bytes)
        logger.info(f"Extracted text: {len(text)} chars")
        logger.info(f"Preview: {text[:200]}...")

    logger.info("API test passed!")
    return True


def bootstrap_sample(limit: int = 15) -> int:
    """Write a handful of validation records to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=True):
        fname = f"{record['_id'].replace('/', '_')}.json"
        with open(SAMPLE_DIR / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"[{count}] Saved: {record['_id']} — {record['title'][:60]}")
        if count >= limit:
            break

    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def run_full() -> int:
    """Stream the whole corpus to data/records.jsonl (what the pipeline ingests).

    Appends, and skips codigos already present, so a killed run resumes where
    it stopped instead of restarting at 2026.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    seen = load_seen_ids()
    if seen:
        logger.info(f"Resuming: {len(seen):,} records already in {RECORDS_PATH.name}")

    deadline = time.time() + MAX_HOURS * 3600
    count = 0

    with open(RECORDS_PATH, "a", encoding="utf-8") as out:
        for record in fetch_all(seen=seen, deadline=deadline):
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 250 == 0:
                out.flush()

    total = len(seen)
    logger.info(
        f"Bootstrap complete: {count:,} new records this run, "
        f"{total:,} total in {RECORDS_PATH}"
    )
    return total


def main():
    parser = argparse.ArgumentParser(description="PY/CSJJurisprudencia data fetcher")
    # bootstrap-fast is the fleet entry point; without it argparse exits 2 and
    # the wrapper falls back to re-ingesting sample/.
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a small sample for validation")
    parser.add_argument("--limit", type=int, default=15,
                        help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--workers", type=int, default=None,
                        help="PDF download workers (default 5)")
    parser.add_argument("--batch", type=int, default=None,
                        help="Unused, kept for fleet-wrapper compatibility")
    args = parser.parse_args()

    global PDF_WORKERS
    if args.workers:
        PDF_WORKERS = args.workers

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap" and args.sample:
        count = bootstrap_sample(args.limit)
    else:
        count = run_full()

    if count == 0:
        logger.error("No records fetched!")
        sys.exit(1)


if __name__ == "__main__":
    main()
