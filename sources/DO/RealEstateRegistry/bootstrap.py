#!/usr/bin/env python3
"""
DO/RealEstateRegistry -- Registro Inmobiliario / Jurisdicción Inmobiliaria
(Dominican Republic)

Fetches the full text of the "Resoluciones sobre Recursos Jerárquicos" issued
by the Dirección Nacional de Registro de Títulos (DNRT) of the Dominican Real
Estate Jurisdiction. These are adjudicatory decisions resolving hierarchical
administrative appeals (recursos jerárquicos) filed against acts of the title
registry — each carries an expediente number, the parties, the legal reasoning,
and the resolved outcome. The Jurisdicción Inmobiliaria is mandated to publish
all of its acts under Ley No. 108-05 de Registro Inmobiliario.

Strategy:
  The decisions are published as full-text PDFs and indexed on a single WordPress
  page (ri.gob.do/?page_id=3929), grouped by year and month. Each link points to
  ri.gob.do/wp-content/uploads/YYYY/MM/DNRT-R-YYYY-NNNNN.pdf.
  1. GET the index page and extract every DNRT-R-*.pdf link.
  2. Download each PDF and extract its text with pdfplumber (with per-page cache
     flushing to keep memory bounded on large documents).
  3. Parse the resolution number, expediente, and date from the document text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py test               # Quick connectivity test

Note: ri.gob.do serves a certificate chain that some clients reject; we disable
TLS verification (the documents are public official acts, integrity is not a
confidentiality concern here).
"""

import sys
import io
import re
import ssl
import json
import time
import logging
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.RealEstateRegistry")

INDEX_URL = "https://ri.gob.do/?page_id=3929"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
MIN_TEXT_CHARS = 200

# DNRT resolution PDF links, e.g. .../DNRT-R-2025-00250.pdf
PDF_LINK_RE = re.compile(r'href="([^"]*?DNRT-R-\d{4}-\d+\.pdf)"', re.I)
RES_NUM_RE = re.compile(r'(DNRT-R-\d{4}-\d+)', re.I)
EXPEDIENTE_RE = re.compile(r'(DNRT-E-\d{4}-\d+)', re.I)
# The PDF header carries "FECHA <dd-mm-yyyy>"; extracted text sometimes splits
# it as "FEC HA". Allow optional whitespace inside the word.
FECHA_RE = re.compile(r'FEC\s*HA\s*[:\s]*?(\d{1,2})-(\d{1,2})-(\d{4})', re.I)

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
        return resp.read()


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber with per-page cache flush
    to keep peak memory bounded on large multi-page documents."""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not available")
        return ""
    parts: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                parts.append(page.extract_text() or "")
                page.flush_cache()
                try:
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
        return ""
    return "\n\n".join(parts).strip()


class RealEstateRegistryScraper(BaseScraper):
    """Scraper for the Dominican Real Estate Jurisdiction (DNRT resolutions)."""

    def _list_pdf_urls(self) -> List[str]:
        """Fetch the index page and return all DNRT resolution PDF URLs
        (newest first, deduplicated, order preserved)."""
        html = _http_get(INDEX_URL).decode("utf-8", "replace")
        seen = set()
        urls = []
        for m in PDF_LINK_RE.finditer(html):
            url = m.group(1)
            if url.startswith("/"):
                url = "https://ri.gob.do" + url
            if url not in seen:
                seen.add(url)
                urls.append(url)
        return urls

    def _build_raw(self, url: str) -> Optional[dict]:
        try:
            pdf_bytes = _http_get(url)
        except Exception as e:
            logger.debug(f"Download failed for {url}: {e}")
            return None
        text = _extract_pdf_text(pdf_bytes)
        rm = RES_NUM_RE.search(url)
        resolution_number = rm.group(1).upper() if rm else url.rsplit("/", 1)[-1]
        return {"url": url, "resolution_number": resolution_number, "text": text}

    def fetch_all(self) -> Generator[dict, None, None]:
        urls = self._list_pdf_urls()
        logger.info(f"Found {len(urls)} DNRT resolution PDFs")
        for url in urls:
            raw = self._build_raw(url)
            if raw is not None:
                yield raw
            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental: re-scan the index; idempotent dedup skips known
        resolution numbers, so only newly published PDFs are written."""
        since_year = since.year if since else 0
        for url in self._list_pdf_urls():
            ym = re.search(r'DNRT-R-(\d{4})', url, re.I)
            if ym and int(ym.group(1)) < since_year:
                continue
            raw = self._build_raw(url)
            if raw is not None:
                yield raw
            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        text = (raw.get("text") or "").strip()
        if len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text for {raw.get('resolution_number')}")
            return None

        resolution_number = raw["resolution_number"]

        # Date: prefer the FECHA field in the document header; fall back to the
        # year embedded in the resolution number.
        date = None
        fm = FECHA_RE.search(text[:1500])
        if fm:
            day, month, year = int(fm.group(1)), int(fm.group(2)), int(fm.group(3))
            try:
                date = f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                date = None
        if not date:
            ym = re.search(r'DNRT-R-(\d{4})', resolution_number)
            if ym:
                date = f"{ym.group(1)}-01-01"

        em = EXPEDIENTE_RE.search(text[:2000])
        expediente = em.group(1).upper() if em else ""

        return {
            "_id": f"DO-RI-{resolution_number}",
            "_source": "DO/RealEstateRegistry",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "resolution_number": resolution_number,
            "expediente": expediente,
            "title": f"Resolución {resolution_number} — Recurso Jerárquico (DNRT)",
            "text": text,
            "date": date,
            "court": "Dirección Nacional de Registro de Títulos (Jurisdicción Inmobiliaria)",
            "url": raw["url"],
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="DO/RealEstateRegistry scraper")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    source_dir = Path(__file__).resolve().parent
    scraper = RealEstateRegistryScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing connectivity...")
        urls = scraper._list_pdf_urls()
        logger.info(f"Index lists {len(urls)} resolution PDFs")
        if urls:
            raw = scraper._build_raw(urls[0])
            logger.info(f"First: {raw['resolution_number']} — {len(raw['text'])} chars")
            rec = scraper.normalize(raw)
            if rec:
                logger.info(f"Normalized date={rec['date']} expediente={rec['expediente']}")
                logger.info(rec["text"][:300])
        logger.info("Test passed!")
        return

    if args.command in ("bootstrap", "bootstrap-fast"):
        sample = args.sample and not args.full
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2)}")
    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {json.dumps(result, indent=2)}")


if __name__ == "__main__":
    main()
