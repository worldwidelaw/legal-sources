#!/usr/bin/env python3
"""
IT/Lazio-Legislation -- Lazio Regional Legislation

Fetches Lazio regional laws (leggi regionali) with full coordinated text
from the Consiglio Regionale del Lazio website.

Strategy:
  - Paginate year-by-year listing at /?vw=leggiregionali&sv=vigente&annoLegge=YEAR&pg=PAGE
  - Extract law IDs from detail links
  - Fetch each law's full text from the detail page
  - ~2,500 laws from 1974 to present

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from current year
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Lazio-Legislation")

BASE_URL = "https://www.consiglio.regione.lazio.it"
LIST_URL = f"{BASE_URL}/"
DETAIL_URL = f"{BASE_URL}/consiglio-regionale/"

FIRST_YEAR = 1974
CURRENT_YEAR = datetime.now().year

MONTHS_IT = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}


class LazioLegislationScraper(BaseScraper):
    SOURCE_ID = "IT/Lazio-Legislation"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html",
        })

    def _get_html(self, url: str, params: Optional[Dict] = None) -> str:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.encoding = "ISO-8859-1"
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 * (attempt + 1))

    def _extract_ids_from_listing(self, html: str) -> List[int]:
        ids = []
        for match in re.finditer(r'leggiregionalidettaglio&(?:amp;)?id=(\d+)', html):
            law_id = int(match.group(1))
            if law_id not in ids:
                ids.append(law_id)
        return ids

    def _parse_detail_page(self, html: str, law_id: int) -> Optional[Dict]:
        soup = BeautifulSoup(html, "html.parser")

        title_el = soup.find(id="titolo_contenuto")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        metadata = {}
        dati_el = soup.find(id="dati_legge")
        if dati_el:
            dati_text = dati_el.get_text(separator=" ", strip=True)
            num_match = re.search(r'Numero della legge\s*:\s*(\d+)', dati_text)
            if num_match:
                metadata["law_number"] = num_match.group(1)
            date_match = re.search(r'Data\s*:\s*(\d{1,2})\s+(\w+)\s+(\d{4})', dati_text)
            if date_match:
                day = int(date_match.group(1))
                month_name = date_match.group(2).lower()
                year = int(date_match.group(3))
                month = MONTHS_IT.get(month_name, 0)
                if month:
                    metadata["date"] = f"{year}-{month:02d}-{day:02d}"
            bur_match = re.search(r'Numero BUR\s*:\s*(.+?)(?:Data BUR|$)', dati_text)
            if bur_match:
                metadata["bur_number"] = bur_match.group(1).strip()
            bur_date_match = re.search(r'Data BUR\s*:\s*(\d{1,2}/\d{1,2}/\d{4})', dati_text)
            if bur_date_match:
                metadata["bur_date"] = bur_date_match.group(1)

        content_el = soup.find(id="contenuto_legge")
        if not content_el:
            return None
        # Clean HTML to text
        for tag in content_el(["script", "style"]):
            tag.decompose()
        text = content_el.get_text(separator="\n")
        text = re.sub(r"\n{3,}", "\n\n", text).strip()

        if not text:
            return None

        return {
            "id": law_id,
            "title": title,
            "text": text,
            **metadata,
        }

    def _fetch_laws_for_year(self, year: int) -> Generator[Dict, None, None]:
        page = 1
        while True:
            self.rate_limiter.wait()
            params = {
                "vw": "leggiregionali",
                "sv": "vigente",
                "annoLegge": year,
                "pg": page,
                "invia": " Cerca ",
            }
            html = self._get_html(LIST_URL, params=params)
            ids = self._extract_ids_from_listing(html)
            if not ids:
                break

            for law_id in ids:
                self.rate_limiter.wait()
                try:
                    detail_html = self._get_html(
                        DETAIL_URL,
                        params={"vw": "leggiregionalidettaglio", "id": law_id, "sv": "vigente"},
                    )
                    parsed = self._parse_detail_page(detail_html, law_id)
                    if parsed:
                        yield parsed
                    else:
                        logger.warning("Could not parse law id=%d", law_id)
                except Exception as e:
                    logger.warning("Failed to fetch law id=%d: %s", law_id, e)

            page += 1

    def test_connection(self) -> bool:
        try:
            html = self._get_html(LIST_URL, params={
                "vw": "leggiregionali", "sv": "vigente",
                "annoLegge": CURRENT_YEAR, "pg": 1, "invia": " Cerca ",
            })
            ids = self._extract_ids_from_listing(html)
            logger.info("Connection OK: found %d laws for %d on page 1", len(ids), CURRENT_YEAR)
            return len(ids) > 0
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        logger.info("Fetching Lazio regional laws from %d to %d...", FIRST_YEAR, CURRENT_YEAR)
        for year in range(CURRENT_YEAR, FIRST_YEAR - 1, -1):
            logger.info("Fetching year %d...", year)
            count = 0
            for law in self._fetch_laws_for_year(year):
                yield law
                count += 1
            logger.info("Year %d: %d laws", year, count)

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        year = since.year
        logger.info("Fetching laws from %d to %d", year, CURRENT_YEAR)
        for y in range(CURRENT_YEAR, year - 1, -1):
            for law in self._fetch_laws_for_year(y):
                yield law

    def normalize(self, raw: dict) -> dict:
        date_str = raw.get("date")
        year = ""
        if date_str:
            try:
                year = date_str[:4]
            except (ValueError, IndexError):
                pass

        law_num = raw.get("law_number", "")
        law_id = raw.get("id", 0)

        return {
            "_id": f"IT-Lazio-L{law_num}-{year}" if law_num and year else f"IT-Lazio-{law_id}",
            "_source": "IT/Lazio-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": f"{DETAIL_URL}?vw=leggiregionalidettaglio&id={law_id}&sv=vigente",
            "law_number": law_num,
            "bur_number": raw.get("bur_number", ""),
            "bur_date": raw.get("bur_date", ""),
        }

    def run_bootstrap(self, sample: bool = False):
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        label = "SAMPLE" if sample else "FULL"
        logger.info("Running %s bootstrap", label)

        count = 0
        for raw in self.fetch_all():
            normalized = self.normalize(raw)

            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1

            text_len = len(normalized.get("text", ""))
            logger.info("  [%d] %s -> %d chars", count, normalized["title"][:60], text_len)

            if sample and count >= 15:
                break

        logger.info("%s bootstrap complete: %d records saved", label, count)
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IT/Lazio-Legislation Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LazioLegislationScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(since=datetime.now(timezone.utc)):
            scraper.normalize(raw)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
