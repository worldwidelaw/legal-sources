#!/usr/bin/env python3
"""
INTL/IACtHR-ProvisionalMeasures -- Inter-American Court of Human Rights
                                   Provisional Measures (Medidas Provisionales)

Fetches the provisional-measures resolutions of the Inter-American Court of
Human Rights (Corte IDH). Under Article 63(2) of the American Convention, the
Court orders provisional measures in cases of extreme gravity and urgency; it
issues binding resolutions granting, monitoring, expanding or lifting them.
Distinct from INTL/IACtHR (contentious judgments, Series C) and
INTL/IACtHR-Advisory (advisory opinions, Series A).

Strategy:
  - POST to the AJAX endpoint with nId_Tipo_Jurisprudencia=MP to list all
    resolutions (HTML fragment).
  - Parse each result: case/matter name + the main born-digital document under
    docs/medidas/ (prefer PDF, fall back to DOCX), excluding votos/vsa_* votes.
  - Download and extract full text (born-digital, no OCR for PDFs).
  - Parse the issue date from the resolution header.
  - Normalize records to the standard schema.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # Full pull (fleet alias)
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IACtHR-ProvisionalMeasures")

LISTING_URL = "https://corteidh.or.cr/get_jurisprudencia_search_tipo.cfm"
BASE_URL = "https://corteidh.or.cr"
REFERER = "https://corteidh.or.cr/medidas_provisionales.cfm?lang=es"

ES_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


class IACtHRProvisionalMeasuresScraper(BaseScraper):
    """
    Scraper for INTL/IACtHR-ProvisionalMeasures.
    Country: INTL   Data types: case_law   Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Referer": REFERER,
        })

    def _fetch_listing(self) -> str:
        """Fetch the full provisional-measures listing from the AJAX endpoint."""
        data = {
            "lang": "es",
            "Texto_busqueda_TXT": "",
            "nId_estado_NUM": "T",
            "sYear": "1987",
            "sYear2": str(datetime.now(timezone.utc).year + 1),
            "page_rows": "5000",
            "nId_Tipo_Jurisprudencia": "MP",
            "startrow": "1",
            "search_param": "name",
        }
        r = self.session.post(LISTING_URL, data=data, timeout=90)
        r.raise_for_status()
        return r.text

    @staticmethod
    def _pick_doc(li) -> Optional[str]:
        """Pick the main document: born-digital PDF (preferred) else DOCX.

        Excludes separate votes (votos/vsa_*). Returns absolute https URL.
        """
        pdfs, docs = [], []
        for a in li.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if "/docs/medidas/" not in low:
                continue
            bn = low.rsplit("/", 1)[-1]
            if "/votos/" in low or "voto" in bn or bn.startswith(("vsa", "vsc")):
                continue
            if bn.endswith(".pdf"):
                pdfs.append(href)
            elif bn.endswith((".docx", ".doc")):
                docs.append(href)
        chosen = pdfs[0] if pdfs else (docs[0] if docs else None)
        if not chosen:
            return None
        if chosen.startswith("http://"):
            chosen = "https://" + chosen[len("http://"):]
        elif not chosen.startswith("http"):
            chosen = BASE_URL + chosen
        return chosen

    def _parse_listing(self, html: str) -> list:
        """Parse the HTML listing into raw records with the main document URL."""
        soup = BeautifulSoup(html, "html.parser")
        records = []
        seen = set()
        for li in soup.find_all("li", class_="search-result"):
            doc_url = self._pick_doc(li)
            if not doc_url:
                continue
            stem = re.sub(r"\.(pdf|docx?|)$", "", doc_url.rsplit("/", 1)[-1],
                          flags=re.IGNORECASE)
            if not stem or stem in seen:
                continue
            seen.add(stem)

            citation = li.get_text(" ", strip=True)
            citation = re.sub(r"^Medidas?\s+Provisionales?\s+Corte\s+IDH\.?\s*", "",
                              citation, flags=re.IGNORECASE).strip()

            records.append({
                "resolution_id": stem,
                "citation": citation,
                "doc_url": doc_url,
            })
        return records

    def _download_and_extract(self, url: str) -> str:
        """Download the document and extract full text."""
        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return ""
        low = url.lower()
        if r.content[:5] == b"%PDF-":
            return extract_pdf_markdown(
                source="INTL/IACtHR-ProvisionalMeasures",
                source_id="",
                pdf_bytes=r.content,
                table="case_law",
            ) or ""
        if low.endswith((".docx", ".doc")):
            return self._extract_docx(r.content)
        logger.warning(f"Unrecognized document format for {url}")
        return ""

    @staticmethod
    def _extract_docx(content: bytes) -> str:
        """Extract text from a DOCX (best-effort; skips if python-docx absent)."""
        try:
            import docx  # python-docx
            d = docx.Document(io.BytesIO(content))
            return "\n\n".join(p.text for p in d.paragraphs if p.text.strip())
        except Exception as e:
            logger.warning(f"DOCX extraction unavailable/failed: {e}")
            return ""

    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        """Parse the issue date from the resolution header (Spanish)."""
        head = text[:1500]
        m = re.search(
            r"\b(\d{1,2})\s+DE\s+([A-Za-zÁÉÍÓÚáéíóúñ]+)\s+DE\s+(\d{4})",
            head, flags=re.IGNORECASE)
        if not m:
            return None
        day = int(m.group(1))
        month = ES_MONTHS.get(m.group(2).lower())
        year = int(m.group(3))
        if not month or not (1 <= day <= 31) or not (1980 <= year <= 2100):
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"

    def normalize(self, raw: dict) -> dict:
        """Transform a raw record into the standard schema."""
        text = raw.get("text", "") or ""
        citation = raw.get("citation", "").strip()
        title = citation.split(".")[0].strip() if citation else raw["resolution_id"]
        # Include the resolution-type phrase if short and present.
        if citation:
            title = citation[:200].rsplit(" ", 1)[0] + ("…" if len(citation) > 200 else "")
        title = f"Corte IDH — Medidas Provisionales: {title}"

        return {
            "_id": f"IACtHR-MP-{raw['resolution_id']}",
            "_source": "INTL/IACtHR-ProvisionalMeasures",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": self._parse_date(text),
            "url": raw.get("doc_url", ""),
            "resolution_id": raw["resolution_id"],
            "language": "es",
            "court": "Inter-American Court of Human Rights",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all provisional-measures resolutions (yields raw records)."""
        logger.info("Fetching provisional-measures listing from AJAX endpoint...")
        html = self._fetch_listing()
        records = self._parse_listing(html)
        logger.info(f"Found {len(records)} resolutions in listing")
        if not records:
            raise RuntimeError(
                "No provisional-measures resolutions found — corteidh.or.cr AJAX "
                "endpoint returned no results (possible IP block or layout change)"
            )

        for i, rec in enumerate(records):
            logger.info(f"[{i+1}/{len(records)}] {rec['resolution_id']} "
                        f"from {rec['doc_url']}")
            text = self._download_and_extract(rec["doc_url"])
            if not text:
                logger.warning(f"No text extracted for {rec['resolution_id']}")
            rec["text"] = text
            yield rec
            time.sleep(2)  # rate limit

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch resolutions issued since a date."""
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            since_dt = None
        for raw in self.fetch_all():
            date = self._parse_date(raw.get("text", "") or "")
            if since_dt and date:
                try:
                    if datetime.fromisoformat(date) < since_dt:
                        continue
                except ValueError:
                    pass
            yield raw


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="INTL/IACtHR-ProvisionalMeasures data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    bf = subparsers.add_parser("bootstrap-fast", help="Full fetch (fleet alias)")
    bf.add_argument("--sample", action="store_true")
    bf.add_argument("--sample-size", type=int, default=15)

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IACtHRProvisionalMeasuresScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            html = scraper._fetch_listing()
            recs = scraper._parse_listing(html)
            logger.info(f"OK: Found {len(recs)} resolutions")
            if recs:
                logger.info(f"First: {recs[0]['resolution_id']} - {recs[0]['citation'][:60]}")
                logger.info(f"Last:  {recs[-1]['resolution_id']} - {recs[-1]['citation'][:60]}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(
            sample_mode=getattr(args, "sample", False),
            sample_size=getattr(args, "sample_size", 15),
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
