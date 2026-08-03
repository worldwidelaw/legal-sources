#!/usr/bin/env python3
"""
INTL/IACtHR-Advisory -- Inter-American Court of Human Rights
                        Advisory Opinions (Opiniones Consultivas)

Fetches the Advisory Opinions (Opiniones Consultivas, Series A) of the
Inter-American Court of Human Rights (Corte IDH). These interpret the American
Convention on Human Rights and other inter-American treaties at the request of
OAS member states/organs. Distinct from INTL/IACtHR (contentious judgments,
Series C).

Strategy:
  - POST to the AJAX endpoint with nId_Tipo_Jurisprudencia=OC to get the full
    listing (HTML fragment) of all Advisory Opinions.
  - Parse each result: subject/title and the main Spanish born-digital PDF
    (docs/opiniones/seriea_{NN}_esp.pdf), excluding resumen_* summaries and
    votos/vsa_* separate votes.
  - Download the PDF and extract full text (born-digital, no OCR).
  - Parse the OC number and issue date from the PDF header.
  - Normalize records to the standard schema.

Data:
  - 32 Advisory Opinions, OC-1/82 through OC-32/25.
  - Full text from born-digital PDF documents (Spanish).

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
logger = logging.getLogger("legal-data-hunter.INTL.IACtHR-Advisory")

LISTING_URL = "https://corteidh.or.cr/get_jurisprudencia_search_tipo.cfm"
BASE_URL = "https://corteidh.or.cr"
REFERER = "https://corteidh.or.cr/opiniones_consultivas.cfm?lang=es"

ES_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


class IACtHRAdvisoryScraper(BaseScraper):
    """
    Scraper for INTL/IACtHR-Advisory -- Inter-American Court of Human Rights
    Advisory Opinions (Opiniones Consultivas).

    Country: INTL
    Data types: case_law
    Auth: none
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
        """Fetch the full Advisory Opinions listing from the AJAX endpoint."""
        data = {
            "lang": "es",
            "Texto_busqueda_TXT": "",
            "nId_estado_NUM": "T",
            "sYear": "1982",
            "sYear2": str(datetime.now(timezone.utc).year + 1),
            "page_rows": "3000",
            "nId_Tipo_Jurisprudencia": "OC",
            "startrow": "1",
            "search_param": "name",
        }
        r = self.session.post(LISTING_URL, data=data, timeout=60)
        r.raise_for_status()
        return r.text

    @staticmethod
    def _pick_main_pdf(li) -> Optional[str]:
        """Pick the main Spanish born-digital PDF, excluding summaries/votes."""
        esp, eng, other = [], [], []
        for a in li.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if "/votos/" in low:
                continue
            bn = low.rsplit("/", 1)[-1]
            if not bn.startswith("seriea_"):
                continue
            if not bn.endswith(".pdf"):
                continue
            if "resumen" in bn or "voto" in bn or bn.startswith(("vsa", "vsc")):
                continue
            if re.search(r"_(es|esp)\d?\.pdf$", bn):
                esp.append(href)
            elif re.search(r"_(ing|eng)\d?\.pdf$", bn):
                eng.append(href)
            else:
                other.append(href)
        chosen = esp or eng or other
        return chosen[0] if chosen else None

    def _parse_listing(self, html: str) -> list:
        """Parse the HTML listing into raw records with the main PDF URL."""
        soup = BeautifulSoup(html, "html.parser")
        records = []
        for li in soup.find_all("li", class_="search-result"):
            doc_url = self._pick_main_pdf(li)
            if not doc_url:
                continue
            m = re.search(r"seriea_(\d+)", doc_url)
            if not m:
                continue
            series_a = int(m.group(1))
            # Absolute URL; on-page hrefs sometimes use http:// which 404s.
            if doc_url.startswith("http://"):
                doc_url = "https://" + doc_url[len("http://"):]
            elif not doc_url.startswith("http"):
                doc_url = BASE_URL + doc_url

            subject = li.get_text(" ", strip=True)
            subject = re.sub(r"^Opiniones?\s+Consultivas?\s+Corte\s+IDH\.?\s*", "",
                             subject, flags=re.IGNORECASE).strip()

            records.append({
                "series_a": series_a,
                "subject": subject,
                "doc_url": doc_url,
            })
        # Sort ascending by number for deterministic sampling (OC-1 first)
        records.sort(key=lambda r: r["series_a"])
        return records

    def _download_and_extract(self, url: str) -> str:
        """Download a PDF and extract full text (born-digital, no OCR)."""
        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return ""
        ct = r.headers.get("Content-Type", "").lower()
        if r.content[:5] != b"%PDF-":
            logger.warning(f"Not a PDF for {url} (ct={ct})")
            return ""
        return extract_pdf_markdown(
            source="INTL/IACtHR-Advisory",
            source_id="",
            pdf_bytes=r.content,
            table="case_law",
        ) or ""

    @staticmethod
    def _parse_header(text: str) -> tuple:
        """Parse the OC number and issue date from the PDF header.

        Header example:
          "OPINIÓN CONSULTIVA OC-5/85 DEL 13 DE NOVIEMBRE DE 1985"
        Returns (oc_label, iso_date) — either may be None.
        """
        head = text[:2000]
        oc_label = None
        m = re.search(r"OC[-\s]?(\d+)\s*/\s*(\d{2,4})", head)
        if m:
            oc_label = f"OC-{int(m.group(1))}/{m.group(2)}"

        iso_date = None
        d = re.search(
            r"DE(?:L)?\s+(\d{1,2})\s+DE\s+([A-Za-zÁÉÍÓÚáéíóúñ]+)\s+DE\s+(\d{4})",
            head, flags=re.IGNORECASE)
        if d:
            day = int(d.group(1))
            month = ES_MONTHS.get(d.group(2).lower())
            year = int(d.group(3))
            if month:
                iso_date = f"{year:04d}-{month:02d}-{day:02d}"
        return oc_label, iso_date

    def normalize(self, raw: dict) -> dict:
        """Transform a raw record into the standard schema."""
        series_a = raw["series_a"]
        text = raw.get("text", "") or ""
        oc_label, date = self._parse_header(text)
        if not oc_label:
            oc_label = f"OC-{series_a}"

        subject = raw.get("subject", "").strip()
        # Trim overly long subjects for the title (keep full in metadata).
        short_subject = subject.split(";")[0].strip()
        if len(short_subject) > 200:
            short_subject = short_subject[:200].rsplit(" ", 1)[0] + "…"
        title = f"Opinión Consultiva {oc_label}"
        if short_subject:
            title += f": {short_subject}"

        return {
            "_id": f"IACtHR-OC-{series_a}",
            "_source": "INTL/IACtHR-Advisory",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("doc_url", ""),
            "series_a_number": series_a,
            "oc_number": oc_label,
            "subject": subject,
            "language": "es",
            "court": "Inter-American Court of Human Rights",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Advisory Opinions (yields raw records)."""
        logger.info("Fetching Advisory Opinions listing from AJAX endpoint...")
        html = self._fetch_listing()
        records = self._parse_listing(html)
        logger.info(f"Found {len(records)} Advisory Opinions in listing")
        if not records:
            raise RuntimeError(
                "No Advisory Opinions found in listing — corteidh.or.cr AJAX "
                "endpoint returned no results (possible IP block or layout change)"
            )

        for i, rec in enumerate(records):
            logger.info(f"[{i+1}/{len(records)}] OC seriea={rec['series_a']} "
                        f"from {rec['doc_url']}")
            text = self._download_and_extract(rec["doc_url"])
            if not text:
                logger.warning(f"No text extracted for seriea_{rec['series_a']}")
            rec["text"] = text
            yield rec
            time.sleep(2)  # rate limit

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions issued/updated since a date."""
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            since_dt = None
        for raw in self.fetch_all():
            _, date = self._parse_header(raw.get("text", "") or "")
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

    parser = argparse.ArgumentParser(description="INTL/IACtHR-Advisory data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    # Fleet alias: bootstrap-fast == full bootstrap
    bf = subparsers.add_parser("bootstrap-fast", help="Full fetch (fleet alias)")
    bf.add_argument("--sample", action="store_true")
    bf.add_argument("--sample-size", type=int, default=15)

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = IACtHRAdvisoryScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            html = scraper._fetch_listing()
            recs = scraper._parse_listing(html)
            logger.info(f"OK: Found {len(recs)} Advisory Opinions")
            if recs:
                logger.info(f"First: seriea_{recs[0]['series_a']} - {recs[0]['subject'][:60]}")
                logger.info(f"Last:  seriea_{recs[-1]['series_a']} - {recs[-1]['subject'][:60]}")
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
