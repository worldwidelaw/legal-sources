#!/usr/bin/env python3
"""
DO/CorteSuprema -- Dominican Republic Supreme Court Judicial Bulletins

Fetches monthly Boletines Judiciales from the Suprema Corte de Justicia
via the official consultation portal API. Each boletin is a PDF compilation
of all Supreme Court decisions for a given month.

API endpoint:
  - Boletines: POST /Home/GetBoletines (params: Ano, Mes)
  - PDFs hosted on Azure Blob Storage (consultaglobal.blob.core.windows.net)

Coverage: 1994-present (~380 monthly boletines)

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.CorteSuprema")

API_BASE = "https://consultasentenciascj.poderjudicial.gob.do"

SPANISH_MONTHS = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


class CorteSupremaScraper(BaseScraper):
    """
    Scraper for DO/CorteSuprema -- Dominican Republic Supreme Court.
    Fetches monthly Boletines Judiciales (judicial bulletins) as PDFs
    and extracts full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )

    def _get_boletines(self, year: int) -> list[dict]:
        """Fetch boletin metadata for a given year."""
        self.rate_limiter.wait()
        try:
            resp = self.client.post(
                "/Home/GetBoletines",
                data={"Ano": year, "Mes": ""},
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch boletines for {year}: {e}")
            return []

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download a boletin PDF from Azure Blob Storage."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(pdf_url)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {pdf_url}")
                return None
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"PDF download error: {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all boletines from 1994 to present."""
        current_year = datetime.now().year
        for year in range(1994, current_year + 1):
            boletines = self._get_boletines(year)
            if not boletines:
                continue
            logger.info(f"Year {year}: {len(boletines)} boletines")
            for b in boletines:
                b["_year"] = year
                yield b

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield boletines from the year of `since` to present."""
        current_year = datetime.now().year
        start_year = since.year
        for year in range(start_year, current_year + 1):
            boletines = self._get_boletines(year)
            for b in boletines:
                b["_year"] = year
                yield b

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download PDF and extract text, then normalize into standard schema."""
        pdf_url = raw.get("urlCuerpo", "")
        if not pdf_url:
            logger.debug(f"No PDF URL for boletin {raw.get('idCuerpo')}")
            return None

        year = raw.get("agnoCabecera", raw.get("_year", 0))
        month = raw.get("mesCabecera", 0)
        boletin_id = raw.get("idCuerpo", 0)
        month_name = SPANISH_MONTHS.get(month, str(month))

        logger.info(f"Downloading boletin {year}/{month_name}...")
        pdf_bytes = self._download_pdf(pdf_url)
        if not pdf_bytes:
            return None

        # Skip very large PDFs (>100 MB) in sample mode to avoid timeouts
        size_mb = len(pdf_bytes) / (1024 * 1024)
        logger.info(f"Boletin {year}/{month_name}: {size_mb:.1f} MB, extracting text...")

        text = extract_pdf_markdown(
            source="DO/CorteSuprema",
            source_id=f"{year}-{month:02d}",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {year}/{month_name}: {len(text)} chars")
            return None

        title = f"Boletín Judicial - {month_name} {year}"
        date = f"{year}-{month:02d}-01"

        return {
            "_id": f"DO-SCJ-BOLETIN-{year}-{month:02d}",
            "_source": "DO/CorteSuprema",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "boletin_id": boletin_id,
            "year": year,
            "month": month,
            "month_name": month_name,
            "cover_url": raw.get("urlCabecera", ""),
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="DO/CorteSuprema scraper")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample", action="store_true", help="Sample mode (10 records)"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CorteSupremaScraper()

    if args.command == "test-api":
        logger.info("Testing GetBoletines API...")
        boletines = scraper._get_boletines(2024)
        logger.info(f"Found {len(boletines)} boletines for 2024")
        if boletines:
            b = boletines[0]
            logger.info(f"First: id={b['idCuerpo']}, month={b['mesCabecera']}, url={b['urlCuerpo']}")
            # Try downloading a small one
            logger.info("Downloading first boletin...")
            pdf_bytes = scraper._download_pdf(b["urlCuerpo"])
            if pdf_bytes:
                logger.info(f"PDF size: {len(pdf_bytes)} bytes")
                text = extract_pdf_markdown(
                    source="DO/CorteSuprema",
                    source_id="test",
                    pdf_bytes=pdf_bytes,
                    table="case_law",
                )
                logger.info(f"Text length: {len(text or '')} chars")
                if text:
                    logger.info(f"Preview: {text[:200]}")
                    logger.info("API test PASSED")
                else:
                    logger.warning("No text extracted from PDF")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        last_run = scraper.status.get("last_run")
        if last_run:
            since = datetime.fromisoformat(last_run)
        else:
            since = datetime(2020, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
