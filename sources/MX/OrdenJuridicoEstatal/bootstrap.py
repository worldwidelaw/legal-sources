#!/usr/bin/env python3
"""
MX/OrdenJuridicoEstatal -- Mexico Orden Jurídico Nacional (Federal + State Legislation)

Fetches consolidated texts of Mexican federal and state legislation.

Strategy:
  Federal laws (437):
    - Index page: ordenjuridico.gob.mx/leyes → parse wo{ID} file IDs
    - Full text HTML: Documentos/Federal/html/wo{ID}.html → strip to plain text
  State laws (~35K across 31 states):
    - Listing per state: compilacion.ordenjuridico.gob.mx/listPoder3.php?edo={N}
    - Metadata card: fichaOrdenamiento2.php?idArchivo={ID}&ambito=estatal → download URL
    - Documents in DOC or PDF format → extract text

No auth required. All open data.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import subprocess
import tempfile
import os
import platform
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.OrdenJuridicoEstatal")

FEDERAL_BASE = "https://www.ordenjuridico.gob.mx"
STATE_BASE = "http://compilacion.ordenjuridico.gob.mx"

# State name mapping for directory paths in download URLs
STATES = {
    1: ("AGUASCALIENTES", "MX-AGU"),
    2: ("BAJA CALIFORNIA", "MX-BCN"),
    3: ("BAJA CALIFORNIA SUR", "MX-BCS"),
    4: ("CAMPECHE", "MX-CAM"),
    5: ("COAHUILA", "MX-COA"),
    6: ("COLIMA", "MX-COL"),
    7: ("CHIAPAS", "MX-CHP"),
    8: ("CHIHUAHUA", "MX-CHH"),
    9: ("CIUDAD DE MEXICO", "MX-CMX"),
    10: ("DURANGO", "MX-DUR"),
    11: ("GUANAJUATO", "MX-GUA"),
    12: ("GUERRERO", "MX-GRO"),
    13: ("HIDALGO", "MX-HID"),
    14: ("JALISCO", "MX-JAL"),
    15: ("MEXICO", "MX-MEX"),
    16: ("MICHOACAN", "MX-MIC"),
    17: ("MORELOS", "MX-MOR"),
    18: ("NAYARIT", "MX-NAY"),
    19: ("NUEVO LEON", "MX-NLE"),
    20: ("OAXACA", "MX-OAX"),
    21: ("PUEBLA", "MX-PUE"),
    22: ("QUERETARO", "MX-QUE"),
    23: ("QUINTANA ROO", "MX-ROO"),
    24: ("SAN LUIS POTOSI", "MX-SLP"),
    25: ("SINALOA", "MX-SIN"),
    26: ("SONORA", "MX-SON"),
    27: ("TABASCO", "MX-TAB"),
    28: ("TAMAULIPAS", "MX-TAM"),
    29: ("TLAXCALA", "MX-TLA"),
    30: ("VERACRUZ", "MX-VER"),
    31: ("YUCATAN", "MX-YUC"),
    32: ("ZACATECAS", "MX-ZAC"),
}

SPANISH_MONTHS = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


def parse_spanish_date(s: str) -> Optional[str]:
    """Parse '14 de Agosto de 2017' → '2017-08-14'."""
    if not s:
        return None
    s = s.strip().lower()
    m = re.match(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", s)
    if m:
        day, month_name, year = m.groups()
        month = SPANISH_MONTHS.get(month_name)
        if month:
            return f"{year}-{month}-{int(day):02d}"
    return None


def parse_dmy_date(s: str) -> Optional[str]:
    """Parse 'DD-MM-YYYY' → 'YYYY-MM-DD'."""
    if not s:
        return None
    s = s.strip()
    m = re.match(r"(\d{2})-(\d{2})-(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def strip_html(html_content: str) -> str:
    """Strip HTML tags and clean text content."""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|table)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines).strip()


def extract_text_from_doc(doc_bytes: bytes) -> Optional[str]:
    """Extract text from old-format .doc file."""
    with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as f:
        f.write(doc_bytes)
        tmp_path = f.name
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(
                ['textutil', '-convert', 'txt', '-stdout', tmp_path],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        # Try antiword (Linux)
        result = subprocess.run(
            ['antiword', tmp_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    finally:
        os.unlink(tmp_path)
    return None


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="MX/OrdenJuridicoEstatal",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

class OrdenJuridicoScraper(BaseScraper):
    """Scraper for MX/OrdenJuridicoEstatal - consolidated federal + state legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.fed_client = HttpClient(
            base_url=FEDERAL_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "*/*",
            },
            timeout=120,
        )
        self.state_client = HttpClient(
            base_url=STATE_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "*/*",
            },
            timeout=120,
        )

    # ── Federal laws ──────────────────────────────────────────────

    def _fetch_federal_index(self):
        """Parse the /leyes page to get all federal law entries."""
        self.rate_limiter.wait()
        resp = self.fed_client.get("/leyes")
        if not resp or resp.status_code != 200:
            logger.error("Failed to fetch federal index")
            return []

        html = resp.text
        entries = []

        # Two-pass: first get all IDs + titles (works for both page sections)
        title_pattern = re.compile(
            r"class='basic'\s+id='[^']*/(wo\d+)\.doc'>\s*(.*?)\s*</a>",
            re.DOTALL,
        )
        # Then try to get dates from context around each match
        for m in title_pattern.finditer(html):
            wo_id = m.group(1)
            title = m.group(2).strip()
            # Look for dates in the next ~200 chars after the </a>
            after = html[m.end():m.end() + 300]
            dates = re.findall(r'(\d{2}-\d{2}-\d{4})', after)
            pub_date = parse_dmy_date(dates[0]) if len(dates) > 0 else None
            reform_date = parse_dmy_date(dates[1]) if len(dates) > 1 else None
            entries.append({
                "wo_id": wo_id,
                "title": title,
                "pub_date": pub_date,
                "reform_date": reform_date,
            })
        logger.info("Federal index: %d laws found", len(entries))
        return entries

    def _fetch_federal_text(self, wo_id: str) -> Optional[str]:
        """Download HTML version of a federal law and extract text."""
        self.rate_limiter.wait()
        url = f"/Documentos/Federal/html/{wo_id}.html"
        try:
            resp = self.fed_client.get(url)
            if not resp or resp.status_code != 200:
                logger.warning("No HTML for %s (status %s)", wo_id,
                               getattr(resp, 'status_code', 'N/A'))
                return None
            text = strip_html(resp.text)
            if len(text) < 100:
                return None
            return text
        except Exception as e:
            logger.warning("Error fetching federal text %s: %s", wo_id, e)
            return None

    # ── State laws ────────────────────────────────────────────────

    def _fetch_state_listing(self, edo: int):
        """Fetch all law entries for a state."""
        self.rate_limiter.wait()
        resp = self.state_client.get(
            f"/listPoder3.php?edo={edo}&catTipo=0&ordenar="
        )
        if not resp or resp.status_code != 200:
            logger.warning("Failed to fetch listing for state %d", edo)
            return []

        pattern = re.compile(
            r'idArchivo=(\d+)&ambito=estatal[^>]*>\s*(.*?)\s*</a>\s*</td>\s*'
            r'<td[^>]*>\s*(.*?)\s*</td>\s*'
            r'<td[^>]*>\s*(.*?)\s*</td>\s*'
            r'<td[^>]*>\s*(.*?)\s*</td>',
            re.DOTALL,
        )
        entries = []
        for m in pattern.finditer(resp.text):
            entries.append({
                "id_archivo": m.group(1),
                "title": m.group(2).strip(),
                "doc_type": m.group(3).strip(),
                "pub_date": parse_dmy_date(m.group(4).strip()),
                "status": m.group(5).strip(),
                "edo": edo,
            })
        return entries

    def _fetch_state_download_url(self, id_archivo: str):
        """Fetch ficha page to get the actual download URL and format."""
        self.rate_limiter.wait()
        resp = self.state_client.get(
            f"/fichaOrdenamiento2.php?idArchivo={id_archivo}&ambito=estatal"
        )
        if not resp or resp.status_code != 200:
            return None, None

        html = resp.text

        # Check for DOC download
        m = re.search(r'obtenerdoc\.php\?path=([^"&]+)&nombreclave=([^"&]+)', html)
        if m:
            return f"/obtenerdoc.php?path={m.group(1)}&nombreclave={m.group(2)}", "doc"

        # Check for PDF download
        m = re.search(r'obtenerpdf\.php\?path=([^"&]+)&nombreclave=([^"&]+)', html)
        if m:
            return f"/obtenerpdf.php?path={m.group(1)}&nombreclave={m.group(2)}", "pdf"

        # Check for HTML link
        m = re.search(r'href="([^"]*\.html?)"', html)
        if m:
            return m.group(1), "html"

        return None, None

    def _fetch_state_text(self, id_archivo: str) -> Optional[str]:
        """Download and extract text from a state law document."""
        url, fmt = self._fetch_state_download_url(id_archivo)
        if not url:
            logger.warning("No download URL for idArchivo=%s", id_archivo)
            return None

        self.rate_limiter.wait()
        try:
            resp = self.state_client.get(url)
            if not resp or resp.status_code != 200:
                logger.warning("Download failed for %s (status %s)", id_archivo,
                               getattr(resp, 'status_code', 'N/A'))
                return None

            content = resp.content
            if len(content) < 100:
                return None

            if fmt == "doc":
                return extract_text_from_doc(content)
            elif fmt == "pdf":
                return extract_text_from_pdf(content)
            elif fmt == "html":
                return strip_html(resp.text)
            return None
        except Exception as e:
            logger.warning("Error extracting text for %s: %s", id_archivo, e)
            return None

    # ── Generator methods ─────────────────────────────────────────

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all federal and state law entries."""
        # Federal laws first
        logger.info("Fetching federal consolidated laws...")
        fed_entries = self._fetch_federal_index()
        for entry in fed_entries:
            yield {"scope": "federal", **entry}

        # State laws
        for edo in sorted(STATES.keys()):
            state_name, iso_code = STATES[edo]
            logger.info("Fetching state %d (%s)...", edo, state_name)
            entries = self._fetch_state_listing(edo)
            logger.info("  Found %d entries for %s", len(entries), state_name)
            for entry in entries:
                entry["scope"] = "state"
                entry["state_name"] = state_name
                entry["iso_code"] = iso_code
                yield entry

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all (no incremental update available for this source)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw entry, fetching full text."""
        scope = raw.get("scope")

        if scope == "federal":
            wo_id = raw.get("wo_id")
            title = raw.get("title", "")
            if not wo_id or not title:
                return None

            text = self._fetch_federal_text(wo_id)
            if not text:
                logger.warning("No text for federal law %s: %s", wo_id, title[:60])
                return None

            return {
                "_id": f"MX_FED_{wo_id}",
                "_source": "MX/OrdenJuridicoEstatal",
                "_type": "legislation",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": text,
                "date": raw.get("reform_date") or raw.get("pub_date"),
                "date_published": raw.get("pub_date"),
                "date_reformed": raw.get("reform_date"),
                "url": f"{FEDERAL_BASE}/Documentos/Federal/html/{wo_id}.html",
                "jurisdiction": "MX",
                "scope": "federal",
                "language": "es",
            }

        elif scope == "state":
            id_archivo = raw.get("id_archivo")
            title = raw.get("title", "")
            if not id_archivo or not title:
                return None

            text = self._fetch_state_text(id_archivo)
            if not text:
                logger.warning("No text for state law %s: %s", id_archivo, title[:60])
                return None

            edo = raw.get("edo", 0)
            state_name = raw.get("state_name", "")
            iso_code = raw.get("iso_code", "")

            return {
                "_id": f"MX_EST_{id_archivo}",
                "_source": "MX/OrdenJuridicoEstatal",
                "_type": "legislation",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": text,
                "date": raw.get("pub_date"),
                "url": f"{STATE_BASE}/fichaOrdenamiento2.php?idArchivo={id_archivo}&ambito=estatal",
                "jurisdiction": iso_code or "MX",
                "scope": "state",
                "state": state_name,
                "doc_type": raw.get("doc_type", ""),
                "status": raw.get("status", ""),
                "language": "es",
            }

        return None


def main():
    scraper = OrdenJuridicoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing Orden Jurídico connectivity...")
        # Test federal
        entries = scraper._fetch_federal_index()
        if entries:
            logger.info("Federal: %d laws found", len(entries))
            text = scraper._fetch_federal_text(entries[0]["wo_id"])
            if text:
                logger.info("Federal text OK: %d chars for %s",
                            len(text), entries[0]["title"][:60])
            else:
                logger.error("Federal text FAILED for %s", entries[0]["wo_id"])
        else:
            logger.error("Federal index FAILED")

        # Test state
        state_entries = scraper._fetch_state_listing(1)
        if state_entries:
            logger.info("State (Aguascalientes): %d laws found", len(state_entries))
            text = scraper._fetch_state_text(state_entries[0]["id_archivo"])
            if text:
                logger.info("State text OK: %d chars for %s",
                            len(text), state_entries[0]["title"][:60])
            else:
                logger.error("State text FAILED for %s", state_entries[0]["id_archivo"])
        else:
            logger.error("State listing FAILED")

    elif command == "bootstrap":
        if sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
            logger.info("Sample bootstrap complete: %s", json.dumps(stats, indent=2))
        else:
            stats = scraper.bootstrap()
            logger.info("Full bootstrap complete: %s", json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
