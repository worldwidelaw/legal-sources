#!/usr/bin/env python3
"""
ID/BI -- Bank Indonesia Regulations (Peraturan Bank Indonesia)

Fetches regulations from JDIH Bank Indonesia (jdih.bi.go.id).

Strategy:
  - Listing page at /Web/DaftarPeraturan contains all regulation IDs
  - REST API at /api/WebJDIH/GetDataWebPeraturan?PeraturanID={id} for metadata
  - PDF download at /api/WebJDIH/DownloadFilePeraturan/{id}
  - Full text extracted from PDFs via pdfminer

Document types:
  - PBI: Peraturan Bank Indonesia (Bank Indonesia Regulations)
  - PADG: Peraturan Anggota Dewan Gubernur (Board of Governors Regulations)
  - SE: Surat Edaran (Circular Letters)

Data:
  - ~1000+ regulations from 1999 to present
  - Language: Indonesian (Bahasa Indonesia)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ID.BI")

JDIH_BASE = "https://jdih.bi.go.id"
LISTING_URL = f"{JDIH_BASE}/Web/DaftarPeraturan"
DETAIL_API = f"{JDIH_BASE}/api/WebJDIH/GetDataWebPeraturan"
PDF_API = f"{JDIH_BASE}/api/WebJDIH/DownloadFilePeraturan"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_DELAY = 2  # seconds between requests


def curl_get(url: str, timeout: int = 60) -> str:
    """GET via curl subprocess (bypasses Python SSL/TLS limitations)."""
    result = subprocess.run(
        [
            "curl", "-sL",
            "--connect-timeout", "15",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {UA}",
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            url,
        ],
        capture_output=True,
        text=True,
        timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"curl GET failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout


def curl_get_json(url: str, timeout: int = 60) -> dict:
    """GET JSON via curl subprocess."""
    result = subprocess.run(
        [
            "curl", "-sL",
            "--connect-timeout", "15",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {UA}",
            "-H", "Accept: application/json",
            url,
        ],
        capture_output=True,
        text=True,
        timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"curl GET JSON failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout)


def curl_get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET binary content via curl subprocess."""
    result = subprocess.run(
        [
            "curl", "-sL",
            "--connect-timeout", "15",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {UA}",
            url,
        ],
        capture_output=True,
        timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"curl GET bytes failed (exit {result.returncode}): {result.stderr.decode(errors='replace').strip()}"
        )
    return result.stdout


def get_all_regulation_ids() -> List[int]:
    """Extract all regulation IDs from the JDIH listing page."""
    logger.info("Fetching regulation listing page...")
    html = curl_get(LISTING_URL)
    if not html or len(html) < 1000:
        raise RuntimeError("Failed to fetch listing page")

    ids = set()
    for m in re.finditer(r'Detail/(\d+)', html):
        ids.add(int(m.group(1)))

    result = sorted(ids, reverse=True)  # newest first (higher IDs = newer)
    logger.info(f"Found {len(result)} regulation IDs")
    return result


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse .NET date string to ISO 8601 date."""
    if not date_str:
        return None
    try:
        # Format: "2026-03-31T00:00:00"
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return None


class BIScraper(BaseScraper):
    """
    Scraper for ID/BI -- Bank Indonesia Regulations.
    Country: ID
    URL: https://jdih.bi.go.id/Web/DaftarPeraturan

    Data types: doctrine
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(str(source_dir))
        self.source_id = "ID/BI"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all regulations from JDIH BI API."""
        reg_ids = get_all_regulation_ids()

        for i, reg_id in enumerate(reg_ids):
            logger.info(f"Processing regulation {i+1}/{len(reg_ids)}: ID={reg_id}")
            time.sleep(REQUEST_DELAY)

            record = self._fetch_regulation(reg_id)
            if record:
                yield record

    def _fetch_regulation(self, reg_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single regulation: metadata from API + full text from PDF."""
        # Step 1: Get metadata from API
        try:
            resp = curl_get_json(f"{DETAIL_API}?PeraturanID={reg_id}")
        except Exception as e:
            logger.warning(f"Failed to fetch metadata for ID {reg_id}: {e}")
            return None

        if resp.get("StatusCode") != 200 or not resp.get("Data"):
            logger.warning(f"API returned no data for ID {reg_id}")
            return None

        data = resp["Data"]
        title = data.get("Judul", "")
        nomor = data.get("NomorPeraturan", "")
        if nomor and title:
            title = f"{nomor} - {title}"
        elif nomor:
            title = nomor

        date = parse_date(data.get("TanggalPengundangan")) or parse_date(data.get("TanggalPenetapan"))

        # Step 2: Download and extract PDF
        time.sleep(REQUEST_DELAY)
        text = ""
        try:
            logger.info(f"Downloading PDF for ID {reg_id}...")
            pdf_bytes = curl_get_bytes(f"{PDF_API}/{reg_id}", timeout=120)
            if pdf_bytes and len(pdf_bytes) > 100 and pdf_bytes[:5] == b'%PDF-':
                text = extract_pdf_markdown(
                    self.source_id, str(reg_id), pdf_bytes=pdf_bytes
                )
                logger.info(f"Extracted {len(text)} chars from PDF for ID {reg_id}")
            else:
                logger.warning(f"PDF response is not valid for ID {reg_id} (size={len(pdf_bytes)})")
        except Exception as e:
            logger.warning(f"PDF extraction failed for ID {reg_id}: {e}")

        if not text:
            logger.warning(f"No full text for ID {reg_id}, skipping")
            return None

        reg_type = data.get("SingkatanJenisPeraturan", "")
        status = data.get("Status", "")

        record = {
            '_id': f"ID-BI-{reg_id}",
            '_source': self.source_id,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': text,
            'date': date,
            'url': f"{JDIH_BASE}/Web/DaftarPeraturan/Detail/{reg_id}",
            'regulation_number': data.get("NomorPeraturan"),
            'regulation_type': reg_type,
            'regulation_type_desc': data.get("JenisPeraturanDesc"),
            'status': status,
            'subject': data.get("Subjek"),
            'taxonomy': data.get("TaksonomiDesc"),
            'issuer': data.get("Teu"),
            'year': data.get("TahunTerbit"),
            'date_enacted': parse_date(data.get("TanggalPenetapan")),
            'date_published': parse_date(data.get("TanggalPengundangan")),
            'date_effective': parse_date(data.get("TanggalBerlaku")),
            'amends': data.get("Mengubah", "").strip() or None,
            'revokes': data.get("Mencabut", "").strip() or None,
            'related': data.get("PeraturanTerkait", "").strip() or None,
            'language': 'id',
        }

        return record

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent regulations (first 30 IDs = newest)."""
        reg_ids = get_all_regulation_ids()[:30]
        for i, reg_id in enumerate(reg_ids):
            logger.info(f"Checking update {i+1}/{len(reg_ids)}: ID={reg_id}")
            time.sleep(REQUEST_DELAY)
            record = self._fetch_regulation(reg_id)
            if record:
                yield record

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Records are already normalized in _fetch_regulation."""
        return raw


# ── CLI entrypoint ───────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = BIScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
