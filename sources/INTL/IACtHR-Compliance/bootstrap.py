#!/usr/bin/env python3
"""
INTL/IACtHR-Compliance -- Inter-American Court of Human Rights
                          Monitoring Compliance with Judgment
                          (Supervisión de Cumplimiento de Sentencia)

Fetches the compliance-monitoring resolutions of the Inter-American Court of
Human Rights (Corte IDH). After it delivers a contentious judgment, the Court
retains jurisdiction to supervise its execution and issues periodic binding
resolutions (Resoluciones de Supervisión de Cumplimiento de Sentencia) assessing
whether the state has complied with the ordered reparations. These resolutions
are a distinct, authoritative body of case law.

Distinct from:
  - INTL/IACtHR                    (contentious judgments, Series C)
  - INTL/IACtHR-Advisory           (advisory opinions, Series A)
  - INTL/IACtHR-ProvisionalMeasures (provisional measures, Medidas Provisionales)

Strategy:
  - GET the two "casos en supervisión por país" listing pages (active +
    archived). Each case is a table row: case name, judgment date, and one
    dated resolution per compliance resolution under /docs/supervisiones/.
  - Keep only anchors whose visible text is a Spanish date (these are the real
    resolutions; the undated "{case}c.pdf"/"{case}p.pdf" status-summary docs and
    party briefs are excluded).
  - Download and extract full text (born-digital PDFs, no OCR).
  - Parse the issue date from the anchor text, falling back to the PDF header.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # Full pull (fleet alias)
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

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
logger = logging.getLogger("legal-data-hunter.INTL.IACtHR-Compliance")

BASE_URL = "https://corteidh.or.cr"
LISTING_PAGES = [
    "https://corteidh.or.cr/casos_en_supervision_por_pais.cfm?lang=es",
    "https://corteidh.or.cr/casos_en_supervision_por_pais_archivados.cfm?lang=es",
]

ES_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}

DATE_ANCHOR_RE = re.compile(
    r"\b(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóúñ]+)\s+de\s+(\d{4})", re.IGNORECASE)
SUPERVISION_HREF_RE = re.compile(r"/docs/supervisiones/.+\.pdf", re.IGNORECASE)
STATUS_LABELS = {
    "declaradas cumplidas", "declarada cumplida", "pendientes de cumplimiento",
    "cumplimiento total", "cumplimiento parcial", "escritos", "casos",
    "resoluciones", "sentencia",
}


class IACtHRComplianceScraper(BaseScraper):
    """
    Scraper for INTL/IACtHR-Compliance.
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
            "Referer": BASE_URL + "/casos_en_supervision.cfm?lang=es",
        })

    def _fetch_page(self, url: str) -> str:
        r = self.session.get(url, timeout=90)
        r.raise_for_status()
        return r.text

    @staticmethod
    def _es_date(text: str) -> Optional[str]:
        """Parse a Spanish 'DD de MES de YYYY' date into ISO 8601."""
        m = DATE_ANCHOR_RE.search(text or "")
        if not m:
            return None
        day = int(m.group(1))
        month = ES_MONTHS.get(m.group(2).lower())
        year = int(m.group(3))
        if not month or not (1 <= day <= 31) or not (1980 <= year <= 2100):
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"

    @staticmethod
    def _abs_url(href: str) -> str:
        if href.startswith("http://"):
            return "https://" + href[len("http://"):]
        if not href.startswith("http"):
            return BASE_URL + ("" if href.startswith("/") else "/") + href
        return href

    def _row_case_name(self, tr) -> Optional[str]:
        """Extract the case name from a table row (the cell without resolution
        links that carries alphabetic text and is neither a date nor a status
        label)."""
        for td in tr.find_all("td"):
            if td.find("a", href=SUPERVISION_HREF_RE):
                continue
            t = td.get_text(" ", strip=True)
            if not t or t.isdigit() or DATE_ANCHOR_RE.search(t):
                continue
            if t.strip().lower() in STATUS_LABELS:
                continue
            if sum(c.isalpha() for c in t) < 3:
                continue
            return t
        return None

    def _parse_page(self, html: str) -> list:
        """Parse a listing page into raw resolution records."""
        soup = BeautifulSoup(html, "html.parser")
        records = []
        for tr in soup.find_all("tr"):
            anchors = [
                a for a in tr.find_all("a", href=SUPERVISION_HREF_RE)
                if DATE_ANCHOR_RE.search(a.get_text(" ", strip=True))
            ]
            if not anchors:
                continue
            case_name = self._row_case_name(tr)
            for a in anchors:
                href = self._abs_url(a["href"])
                stem = re.sub(r"\.pdf$", "", href.rsplit("/", 1)[-1],
                              flags=re.IGNORECASE)
                anchor_text = a.get_text(" ", strip=True)
                records.append({
                    "resolution_id": stem,
                    "case_name": case_name,
                    "doc_url": href,
                    "list_date": self._es_date(anchor_text),
                })
        return records

    def _collect_records(self) -> list:
        """Fetch both listing pages and dedup by resolution filename."""
        seen = set()
        out = []
        for url in LISTING_PAGES:
            logger.info(f"Fetching listing: {url}")
            try:
                html = self._fetch_page(url)
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                continue
            recs = self._parse_page(html)
            logger.info(f"  parsed {len(recs)} resolution links")
            for rec in recs:
                if rec["resolution_id"] in seen:
                    continue
                seen.add(rec["resolution_id"])
                out.append(rec)
            time.sleep(1)
        return out

    def _download_and_extract(self, url: str) -> str:
        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return ""
        if r.content[:5] == b"%PDF-":
            return extract_pdf_markdown(
                source="INTL/IACtHR-Compliance",
                source_id="",
                pdf_bytes=r.content,
                table="case_law",
            ) or ""
        logger.warning(f"Not a PDF: {url}")
        return ""

    @staticmethod
    def _header_date(text: str) -> Optional[str]:
        """Parse the issue date from the resolution header (Spanish)."""
        head = text[:2000]
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
        case = (raw.get("case_name") or "").strip()
        date = raw.get("list_date") or self._header_date(text)
        if case and date:
            title = (f"Corte IDH — Supervisión de Cumplimiento de Sentencia: "
                     f"Caso {case} ({date})")
        elif case:
            title = f"Corte IDH — Supervisión de Cumplimiento de Sentencia: Caso {case}"
        else:
            title = f"Corte IDH — Supervisión de Cumplimiento: {raw['resolution_id']}"

        return {
            "_id": f"IACtHR-SC-{raw['resolution_id']}",
            "_source": "INTL/IACtHR-Compliance",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("doc_url", ""),
            "resolution_id": raw["resolution_id"],
            "case_name": case or None,
            "language": "es",
            "court": "Inter-American Court of Human Rights",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all compliance-monitoring resolutions (yields raw records)."""
        records = self._collect_records()
        logger.info(f"Found {len(records)} compliance resolutions total")
        if not records:
            raise RuntimeError(
                "No compliance resolutions found — corteidh.or.cr listing pages "
                "returned no dated resolutions (possible IP block or layout change)"
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
            date = raw.get("list_date") or self._header_date(raw.get("text", "") or "")
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
        description="INTL/IACtHR-Compliance data fetcher")
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

    scraper = IACtHRComplianceScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            recs = scraper._collect_records()
            logger.info(f"OK: Found {len(recs)} compliance resolutions")
            if recs:
                logger.info(f"First: {recs[0]['resolution_id']} - "
                            f"{recs[0]['case_name']} ({recs[0]['list_date']})")
                logger.info(f"Last:  {recs[-1]['resolution_id']} - "
                            f"{recs[-1]['case_name']} ({recs[-1]['list_date']})")
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
