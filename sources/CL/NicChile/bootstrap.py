#!/usr/bin/env python3
"""
CL/NicChile -- NIC Chile Domain Dispute Arbitration Rulings

Fetches arbitration rulings from NIC Chile's online dispute resolution
system for .cl domain names.

- ~15,000+ rulings available
- JSON API at sentenciasArbitrales.do (paginated, 30 per page)
- Full text in PDFs downloaded via downloadResolucion.do?uuid=<uuid>
- Spanish language

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.NicChile")

BASE_URL = "https://www.nic.cl/rcal"
API_URL = f"{BASE_URL}/sentenciasArbitrales.do"
PDF_URL = f"{BASE_URL}/downloadResolucion.do"
SOURCE_ID = "CL/NicChile"
PAGE_SIZE = 30


def _epoch_to_iso(epoch_ms: int) -> str:
    """Convert Java-style epoch milliseconds to ISO date string."""
    if not epoch_ms:
        return None
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError):
        return None


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        pdf = pdfplumber.open(io.BytesIO(content))
        pages_text = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        pdf.close()
        return "\n\n".join(pages_text)
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


class NicChileScraper(BaseScraper):
    def __init__(self):
        super().__init__(str(Path(__file__).resolve().parent))
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research)",
            "Accept": "application/json",
        })

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        page = 0
        total_fallos = 0

        # First request to get total count
        logger.info("Fetching first page to get total count...")
        params = {
            "totalFallos": 0,
            "page": 0,
            "filtroLimit": PAGE_SIZE,
            "dominio": "",
            "arbitro": "",
            "consignado": "1",
        }
        resp = self.session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        total_fallos = data.get("totalFallos", 0)
        pages_count = data.get("pagesCount", 0)
        logger.info("Total rulings: %d, pages: %d", total_fallos, pages_count)

        # Process first page
        yield from self._process_page(data)

        # Paginate through remaining pages
        for page in range(1, pages_count):
            time.sleep(1.5)
            params["totalFallos"] = total_fallos
            params["page"] = page

            try:
                resp = self.session.get(API_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning("Failed to fetch page %d: %s", page, e)
                continue

            yield from self._process_page(data)

            if page % 10 == 0:
                logger.info("Progress: page %d/%d", page, pages_count)

    def _process_page(self, data: dict) -> Generator[Dict[str, Any], None, None]:
        """Process a page of API results, downloading PDFs for each."""
        fallos = data.get("fallos", [])
        for fallo in fallos:
            uuid = fallo.get("archivoSentencia")
            domain = fallo.get("nombreDominio", "")

            if not uuid:
                logger.debug("No PDF UUID for %s, skipping", domain)
                continue

            time.sleep(1.5)

            try:
                pdf_resp = self.session.get(
                    PDF_URL, params={"uuid": uuid}, timeout=60
                )
                pdf_resp.raise_for_status()
            except Exception as e:
                logger.warning("Failed to download PDF for %s: %s", domain, e)
                continue

            text = _extract_pdf_text(pdf_resp.content)
            if not text or len(text) < 50:
                logger.warning("Insufficient text for %s (%d chars)", domain, len(text))
                continue

            parties = fallo.get("lstPartes", [])
            party_names = [p.get("nombre", "") for p in parties if p.get("nombre")]

            yield {
                "uuid": uuid,
                "domain_name": domain,
                "date_epoch": fallo.get("fechaSentencia"),
                "assignee": fallo.get("nombreParteAsignatario", ""),
                "arbitrator": fallo.get("arbitroSentencia"),
                "outcome": "revocante" if fallo.get("ganaRevocante") == 2 else "titular",
                "parties": party_names,
                "text": text,
                "pdf_size": len(pdf_resp.content),
            }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        date = _epoch_to_iso(raw.get("date_epoch"))
        domain = raw.get("domain_name", "unknown")
        parties = raw.get("parties", [])
        parties_str = " vs ".join(parties) if parties else ""

        title = f"Arbitraje dominio {domain}"
        if parties_str:
            title += f" — {parties_str}"

        return {
            "_id": raw["uuid"],
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"{PDF_URL}?uuid={raw['uuid']}",
            "domain_name": domain,
            "assignee": raw.get("assignee", ""),
            "arbitrator": raw.get("arbitrator"),
            "outcome": raw.get("outcome"),
            "parties": parties,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = NicChileScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
