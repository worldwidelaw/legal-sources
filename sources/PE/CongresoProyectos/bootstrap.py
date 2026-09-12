#!/usr/bin/env python3
"""
PE/CongresoProyectos -- Peru Congress Bills and Legislative Proposals (SPLEY)

Fetches legislative proposals (proyectos de ley) from the Peru Congress
SPLEY portal REST API. Full text extracted from PDF documents.

Strategy:
  - List all bills via POST /proyecto-ley/lista-con-filtro
  - Fetch detail for each bill via GET /expediente/{enc_period}/{enc_pley}
  - Download PDF from /archivo/{b64_id}/pdf and extract text with pdfplumber
  - AES-ECB encryption for expediente path parameters (key embedded in frontend)

Source: https://wb2server.congreso.gob.pe/spley-portal/
Rate limit: 1 req/sec
No authentication required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all bills)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Alias for bootstrap
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import io
import time
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.CongresoProyectos")

API_BASE = "https://api.congreso.gob.pe/spley-portal-service"
PORTAL_BASE = "https://wb2server.congreso.gob.pe/spley-portal"

# AES-ECB key for encrypting expediente path params (from frontend JS bundle)
AES_KEY = b'ProdALg5ZrAsxBMD'

# Parliamentary period ID (2021-2026 is current)
PERIOD_ID = 2021


def _aes_encrypt(value: str) -> str:
    """Encrypt a value with AES-128-ECB for the expediente endpoint."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding as sym_padding

    padder = sym_padding.PKCS7(128).padder()
    padded = padder.update(str(value).encode('utf-8')) + padder.finalize()
    cipher = Cipher(algorithms.AES(AES_KEY), modes.ECB())
    enc = cipher.encryptor()
    encrypted = enc.update(padded) + enc.finalize()
    b64 = base64.b64encode(encrypted).decode('utf-8')
    return b64.replace('+', '-').replace('/', '_').replace('=', '')


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    import pdfplumber

    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f"PDF extraction error: {e}")
        return ""
    return "\n\n".join(text_parts).strip()


class CongresoProyectosScraper(BaseScraper):
    """
    Scraper for PE/CongresoProyectos -- Peru Congress Bills (SPLEY).
    Country: PE
    URL: https://wb2server.congreso.gob.pe/spley-portal/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _list_bills(self) -> list:
        """Fetch the full list of bills for the parliamentary period."""
        resp = self.client.post(
            f"{API_BASE}/proyecto-ley/lista-con-filtro",
            json_data={"perParId": PERIOD_ID},
            timeout=120,
        )
        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"Failed to list bills: {resp.status_code if resp else 'no response'}")

        data = resp.json().get("data", {})
        bills = data.get("proyectos", [])
        logger.info(f"Listed {len(bills)} bills for period {PERIOD_ID}")
        return bills

    def _fetch_detail(self, pley_num: int) -> Optional[dict]:
        """Fetch the expediente detail for a bill."""
        enc_period = _aes_encrypt(PERIOD_ID)
        enc_pley = _aes_encrypt(pley_num)

        self.rate_limiter.wait()
        try:
            resp = self.client.get(
                f"{API_BASE}/expediente/{enc_period}/{enc_pley}",
                timeout=30,
            )
            if resp is None or resp.status_code != 200:
                return None
            return resp.json().get("data")
        except Exception as e:
            logger.debug(f"Error fetching detail for {pley_num}: {e}")
            return None

    def _get_first_archivo_id(self, detail: dict) -> Optional[int]:
        """Get the primary PDF archivo ID from the expediente detail."""
        # Check general.proyectoArchivoId first
        gen = detail.get("general", {})
        main_id = gen.get("proyectoArchivoId")
        if main_id:
            return main_id

        # Fall back to first archivo in seguimientos
        for seg in detail.get("seguimientos", []):
            for archivo in seg.get("archivos", []):
                aid = archivo.get("proyectoArchivoId")
                if aid:
                    return aid

        # Fall back to top-level archivos
        for archivo in detail.get("archivos", []):
            aid = archivo.get("proyectoArchivoId")
            if aid:
                return aid

        return None

    def _fetch_pdf_text(self, archivo_id: int) -> str:
        """Download a PDF and extract its text.

        Uses requests directly (not HttpClient) to avoid Accept:application/json
        header conflict and to support streaming for large PDFs.
        Max PDF size: 15 MB.
        """
        import requests as _req

        MAX_PDF_BYTES = 15 * 1024 * 1024  # 15 MB

        b64_id = base64.b64encode(str(archivo_id).encode()).decode()
        url = f"{API_BASE}/archivo/{b64_id}/pdf"

        self.rate_limiter.wait()
        try:
            resp = _req.get(
                url,
                headers={
                    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                    "Accept": "application/pdf",
                },
                timeout=120,
                stream=True,
                verify=True,
            )
            if resp.status_code != 200:
                resp.close()
                return ""

            content_type = resp.headers.get("content-type", "")
            content_length = int(resp.headers.get("content-length", 0))

            if content_length > MAX_PDF_BYTES:
                logger.debug(f"PDF too large ({content_length / 1024 / 1024:.1f}MB), skipping {archivo_id}")
                resp.close()
                return ""

            # Stream-read with size cap
            chunks = []
            total = 0
            for chunk in resp.iter_content(chunk_size=65536):
                total += len(chunk)
                if total > MAX_PDF_BYTES:
                    logger.debug(f"PDF exceeded {MAX_PDF_BYTES} during download, skipping")
                    resp.close()
                    return ""
                chunks.append(chunk)
            resp.close()

            pdf_bytes = b"".join(chunks)
            if len(pdf_bytes) < 100:
                return ""

            return _extract_pdf_text(pdf_bytes)
        except Exception as e:
            logger.debug(f"Error fetching PDF {archivo_id}: {e}")
            return ""

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """Normalize date string to ISO 8601 date."""
        if not date_str:
            return None
        try:
            # Format: "2026-06-05T00:00:00.000-05:00"
            dt = datetime.fromisoformat(date_str)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return date_str[:10] if date_str and len(date_str) >= 10 else None

    def _process_bill(self, bill_summary: dict) -> Optional[dict]:
        """Process a single bill: fetch detail, download PDF, extract text."""
        pley_num = bill_summary.get("pleyNum")
        if not pley_num:
            return None

        # Fetch expediente detail
        detail = self._fetch_detail(pley_num)
        if not detail:
            logger.debug(f"No detail for bill {pley_num}")
            return None

        gen = detail.get("general", {})

        # Get the PDF text
        archivo_id = self._get_first_archivo_id(detail)
        text = ""
        if archivo_id:
            text = self._fetch_pdf_text(archivo_id)

        if not text or len(text) < 50:
            logger.debug(f"Insufficient text for bill {pley_num}: {len(text)} chars")
            return None

        proyecto_ley = gen.get("proyectoLey", bill_summary.get("proyectoLey", ""))
        title = gen.get("titulo", bill_summary.get("titulo", ""))
        date = self._normalize_date(
            gen.get("fecPresentacion", bill_summary.get("fecPresentacion"))
        )

        return {
            "_id": f"PE-congreso-{proyecto_ley}",
            "_source": "PE/CongresoProyectos",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{PORTAL_BASE}/#/expediente/{PERIOD_ID}/{pley_num}",
            "bill_number": proyecto_ley,
            "status": gen.get("desEstado", bill_summary.get("desEstado", "")),
            "proponent": gen.get("desProponente", bill_summary.get("desProponente", "")),
            "authors": bill_summary.get("autores", ""),
            "parliamentary_group": gen.get("desGpar", ""),
            "sumilla": gen.get("sumilla", ""),
        }

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self, oldest_first: bool = False) -> Generator[dict, None, None]:
        """Yield all bills with full text.

        Args:
            oldest_first: If True, process oldest bills first (useful for sample
                          mode since older bills are more likely to have PDFs).
        """
        bills = self._list_bills()
        if oldest_first:
            bills = list(reversed(bills))

        total_yielded = 0
        total_skipped = 0

        for i, bill in enumerate(bills):
            record = self._process_bill(bill)
            if record:
                total_yielded += 1
                if total_yielded % 50 == 0:
                    logger.info(
                        f"Progress: {total_yielded} fetched, {total_skipped} skipped, "
                        f"{i+1}/{len(bills)} processed"
                    )
                yield record
            else:
                total_skipped += 1
                if total_skipped % 100 == 0:
                    logger.info(f"Skipped {total_skipped} bills so far ({i+1}/{len(bills)})")

        logger.info(f"Done: {total_yielded} bills fetched, {total_skipped} skipped")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield bills presented since the given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        bills = self._list_bills()
        total_yielded = 0

        for bill in bills:
            date_str = self._normalize_date(bill.get("fecPresentacion"))
            if date_str and date_str >= since:
                record = self._process_bill(bill)
                if record:
                    total_yielded += 1
                    yield record
            elif date_str and date_str < since:
                # Bills are returned newest first, so stop when we pass the date
                break

        logger.info(f"Updates since {since}: {total_yielded} bills")

    def normalize(self, raw: dict) -> dict:
        """Normalize is handled inline in _process_bill."""
        return raw

    def test_api(self):
        """Test API connectivity."""
        logger.info("Testing SPLEY API connectivity...")

        # Test list endpoint
        resp = self.client.post(
            f"{API_BASE}/proyecto-ley/lista-con-filtro",
            json_data={"perParId": PERIOD_ID},
            timeout=30,
        )
        if resp and resp.status_code == 200:
            data = resp.json().get("data", {})
            count = len(data.get("proyectos", []))
            logger.info(f"List endpoint OK: {count} bills")
        else:
            logger.error(f"List endpoint failed: {resp.status_code if resp else 'no response'}")
            return

        # Test detail endpoint
        enc_period = _aes_encrypt(PERIOD_ID)
        enc_pley = _aes_encrypt(100)
        resp = self.client.get(
            f"{API_BASE}/expediente/{enc_period}/{enc_pley}",
            timeout=30,
        )
        if resp and resp.status_code == 200:
            logger.info("Detail endpoint OK")
        else:
            logger.error(f"Detail endpoint failed: {resp.status_code if resp else 'no response'}")

        logger.info("API test complete")


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = CongresoProyectosScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test-api] [--sample] [--full]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "bootstrap-fast"):
        sample_mode = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample_mode:
            max_records = 15
        elif "--full" in sys.argv:
            max_records = None
        else:
            max_records = None

        count = 0
        for record in scraper.fetch_all(oldest_first=sample_mode):
            if sample_mode:
                out_file = sample_dir / f"{count:04d}.json"
                out_file.write_text(
                    json.dumps(record, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                logger.info(f"Saved sample {count}: {record['bill_number']} ({len(record['text'])} chars)")

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Total records: {count}")
        if sample_mode:
            logger.info(f"Samples saved to {sample_dir}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
