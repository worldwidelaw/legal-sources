#!/usr/bin/env python3
"""
CL/SII-Oficios -- Chilean SII Tax Oficios (Administrative Rulings)

Fetches tax oficios (official interpretations) from Chile's SII via JSON API.

Strategy:
  - Use www3.sii.cl/getPublicacionesCTByMateria API to list oficios by category/year
  - Download PDF for each oficio from www4.sii.cl/gabineteAdmInternet
  - Extract full text from PDF using pdfplumber
  - Categories: RENTA (income tax), IVA (VAT), OTRAS (other taxes)

Data:
  - Oficios Ordinarios: official SII responses to tax queries
  - ~2,000+ documents from 2019-2026
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch current year only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.SII-Oficios")

LIST_API = "https://www3.sii.cl/getPublicacionesCTByMateria"
DOWNLOAD_URL = "https://www4.sii.cl/gabineteAdmInternet/descargaArchivo"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

CATEGORIES = ["RENTA", "IVA", "OTRAS"]
START_YEAR = 2019
CURRENT_YEAR = datetime.now().year


def _fetch_json(url: str, body: dict, timeout: int = 30) -> Any:
    """POST JSON and return parsed response."""
    data = json.dumps(body).encode("utf-8")
    req = Request(url, data=data, headers={
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    })
    resp = urlopen(req, timeout=timeout)
    return json.loads(resp.read().decode("utf-8"))


def _download_pdf(blob_id: str, filename: str, timeout: int = 60) -> Optional[bytes]:
    """Download a PDF from the SII archive endpoint."""
    params = (
        f"nombreDocumento={filename}"
        f"&extension=pdf"
        f"&acc=download"
        f"&id={blob_id}"
        f"&mediaType=application/pdf"
    )
    url = f"{DOWNLOAD_URL}?{params}"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError) as e:
        logger.warning("Failed to download %s: %s", filename, e)
        return None


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="CL/SII-Oficios",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def _parse_date(date_str: str) -> Optional[str]:
    """Parse date string like '23/12/2025' to ISO 8601."""
    if not date_str:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class SIIOficios(BaseScraper):
    SOURCE_ID = "CL/SII-Oficios"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all oficios from all categories and years."""
        for category in CATEGORIES:
            for year in range(START_YEAR, CURRENT_YEAR + 1):
                logger.info("Fetching %s/%d...", category, year)
                try:
                    items = _fetch_json(LIST_API, {"key": category, "year": str(year)})
                except Exception as e:
                    logger.warning("Failed to fetch %s/%d: %s", category, year, e)
                    continue

                if not isinstance(items, list):
                    continue

                logger.info("  %d oficios found for %s/%d", len(items), category, year)
                for item in items:
                    yield item
                    self.rate_limiter.wait()

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch oficios from current year only."""
        for category in CATEGORIES:
            try:
                items = _fetch_json(LIST_API, {"key": category, "year": str(CURRENT_YEAR)})
            except Exception as e:
                logger.warning("Failed to fetch %s/%d: %s", category, CURRENT_YEAR, e)
                continue

            if isinstance(items, list):
                for item in items:
                    yield item
                    self.rate_limiter.wait()

    def normalize(self, raw: Any) -> Optional[Dict[str, Any]]:
        """Normalize a raw oficio record with full text from PDF."""
        if not isinstance(raw, dict):
            return None
        oficio_num = raw.get("pubNumOficio", "")
        year = raw.get("pubYear", "")
        category = raw.get("pubTipoImpuesto", "")
        blob_id = raw.get("idBlobArchPublica", "")
        filename = raw.get("nombreArchPublica", "")
        pub_date = _parse_date(raw.get("pubFechaPubli", ""))
        summary = raw.get("pubResumen", "")
        legal_refs = raw.get("pubLegal", "")

        if not oficio_num or not blob_id:
            return None

        # Download and extract full text from PDF
        text = ""
        if blob_id and filename:
            pdf_bytes = _download_pdf(blob_id, filename)
            if pdf_bytes:
                text = _extract_text_from_pdf(pdf_bytes)

        if not text:
            logger.warning("No text extracted for oficio %s/%s", year, oficio_num)
            return None

        doc_id = f"CL-SII-OFI-{year}-{oficio_num}"
        doc_url = f"https://www.sii.cl/normativa_legislacion/jurisprudencia_702.html"

        return {
            "_id": doc_id,
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Oficio N° {oficio_num} ({category}) - {summary[:120]}",
            "text": text,
            "date": pub_date,
            "url": doc_url,
            "oficio_number": oficio_num,
            "year": year,
            "category": category,
            "legal_references": legal_refs,
            "summary": summary,
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/SII-Oficios bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--sample-size", type=int, default=15, help="Sample size")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SIIOficios(str(Path(__file__).parent))

    if args.command == "test":
        logger.info("Testing connectivity to SII oficios API...")
        try:
            items = _fetch_json(LIST_API, {"key": "RENTA", "year": str(CURRENT_YEAR)})
            if isinstance(items, list) and len(items) > 0:
                logger.info("SUCCESS: API returned %d oficios for RENTA/%d", len(items), CURRENT_YEAR)
            else:
                logger.error("FAILED: API returned empty or invalid response")
                sys.exit(1)
        except Exception as e:
            logger.error("FAILED: %s", e)
            sys.exit(1)

    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info("Bootstrap result: %s", json.dumps(result, indent=2, default=str))

    elif args.command == "update":
        result = scraper.bootstrap(sample_mode=False)
        logger.info("Update result: %s", json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
