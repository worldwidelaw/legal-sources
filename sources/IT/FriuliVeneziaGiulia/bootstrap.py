#!/usr/bin/env python3
"""
IT/FriuliVeneziaGiulia -- Legislazione Regionale Friuli Venezia Giulia

Fetches regional laws from the LexView database of the Consiglio Regionale
del Friuli Venezia Giulia.

Strategy:
  - Year-by-year POST search to ricerca.aspx to get law list
  - For each law, GET xmlLex.aspx to fetch full text in NIR HTML format
  - Extract title from div.h3, full text from div.NIR
  - Coverage: 1964-present (first legislature onward)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch laws from recent years
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.FriuliVeneziaGiulia")

BASE_URL = "https://lexview-int.regione.fvg.it/FontiNormative/xml"
SEARCH_URL = f"{BASE_URL}/ricerca.aspx"
LAW_URL = f"{BASE_URL}/xmlLex.aspx"

FIRST_YEAR = 1964
CURRENT_YEAR = datetime.now().year

ITALIAN_MONTHS = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
}


def _parse_italian_date(text: str) -> Optional[str]:
    """Parse Italian date like '28 marzo 2024' to ISO 8601."""
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month_num = ITALIAN_MONTHS.get(month_name)
    if not month_num:
        return None
    return f"{year}-{month_num}-{int(day):02d}"


class FriuliVeneziaGiuliaScraper(BaseScraper):
    SOURCE_ID = "IT/FriuliVeneziaGiulia"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.encoding = "utf-8"
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    def _search_year(self, year: int) -> List[Tuple[int, int]]:
        """Search for all laws in a given year. Returns list of (year, number) tuples."""
        # Step 1: GET the search page to obtain __VIEWSTATE
        r1 = self._get(f"{SEARCH_URL}?db=DBC")
        soup1 = BeautifulSoup(r1.text, "html.parser")

        vs_el = soup1.find("input", {"name": "__VIEWSTATE"})
        vsg_el = soup1.find("input", {"name": "__VIEWSTATEGENERATOR"})
        if not vs_el or not vsg_el:
            logger.error("Could not find __VIEWSTATE for year %d", year)
            return []

        # Step 2: POST the search form
        data = {
            "__VIEWSTATE": vs_el["value"],
            "__VIEWSTATEGENERATOR": vsg_el["value"],
            "ctl00$PageBody$tbAnno": str(year),
            "ctl00$PageBody$legge": "",
            "ctl00$PageBody$ART": "",
            "ctl00$PageBody$AG1": "",
            "ctl00$PageBody$AG2": "",
            "ctl00$PageBody$COM": "",
            "ctl00$PageBody$CAG1": "",
            "ctl00$PageBody$CAG2": "",
            "ctl00$PageBody$materia": "",
            "ctl00$PageBody$parola": "",
            "ctl00$PageBody$rbSearchType": "rbNelTesto",
            "ctl00$PageBody$rbModalita": "rbTutteLeParole",
            "ctl00$PageBody$btnRicerca": "Cerca",
        }

        for attempt in range(3):
            try:
                r2 = self.session.post(f"{SEARCH_URL}?db=DBC", data=data, timeout=60)
                r2.encoding = "utf-8"
                r2.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == 2:
                    logger.error("POST search failed for year %d: %s", year, e)
                    return []
                logger.warning("POST attempt %d failed: %s", attempt + 1, e)
                time.sleep(2 ** attempt)

        # Step 3: Parse results — extract law numbers from IndiceLex links
        soup2 = BeautifulSoup(r2.text, "html.parser")
        results = []
        for a in soup2.find_all("a", href=lambda h: h and "IndiceLex.aspx" in str(h)):
            href = a["href"]
            m = re.search(r"anno=(\d+)&legge=(\d+)", href)
            if m:
                results.append((int(m.group(1)), int(m.group(2))))

        return results

    def _fetch_law_text(self, year: int, number: int) -> Optional[Dict[str, Any]]:
        """Fetch full text and metadata for a single law."""
        url = f"{LAW_URL}?anno={year}&legge={number}&lista=1&fx="
        try:
            resp = self._get(url)
        except requests.RequestException as e:
            logger.error("Failed to fetch law %d/%d: %s", year, number, e)
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title from div.h3 inside div.intestazione
        title = ""
        h3_div = soup.find("div", class_="h3")
        if h3_div:
            raw_title = h3_div.get_text(" ", strip=True)
            # Title format: "Legge regionale DD month YYYY , n. N Title text."
            # Extract just the title part after the law number
            title_match = re.search(
                r"Legge\s+regionale\s+.+?,\s*n\.\s*\d+\s+(.*)",
                raw_title,
                re.DOTALL,
            )
            if title_match:
                title = title_match.group(1).strip()
            else:
                title = raw_title

        # Extract date from the header
        date_iso = None
        if h3_div:
            header_text = h3_div.get_text(" ", strip=True)
            date_iso = _parse_italian_date(header_text)

        # Extract metadata
        metadata = {}
        sotto = soup.find("div", class_="sottotitoloDoc")
        if sotto:
            fonte_div = sotto.find("div", class_="voce")
            if fonte_div:
                metadata["fonte"] = fonte_div.get_text(strip=True)

            # Entry into force date
            for puntato in sotto.find_all("div", class_="puntato"):
                txt = puntato.get_text(strip=True)
                if "entrata in vigore" in txt.lower():
                    sibling = puntato.find_next_sibling()
                    if sibling:
                        metadata["entry_into_force"] = sibling.get_text(strip=True)

        # Extract full text from div.NIR
        nir = soup.find("div", class_="NIR")
        if not nir:
            logger.warning("No NIR div found for law %d/%d", year, number)
            return None

        text = nir.get_text("\n", strip=True)
        if len(text) < 50:
            logger.warning("Short text for law %d/%d (%d chars)", year, number, len(text))
            return None

        return {
            "year": year,
            "number": number,
            "title": title,
            "date": date_iso,
            "text": text,
            "url": f"https://lexview-int.regione.fvg.it/FontiNormative/xml/xmlLex.aspx?anno={year}&legge={number}&lista=1&fx=",
            **metadata,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        year = raw.get("year", "")
        number = raw.get("number", "")
        return {
            "_id": f"IT/FriuliVeneziaGiulia/LR-{year}-{number}",
            "_source": "IT/FriuliVeneziaGiulia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "law_number": f"LR {number}/{year}" if year and number else "",
            "fonte": raw.get("fonte", ""),
            "entry_into_force": raw.get("entry_into_force", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for year in range(CURRENT_YEAR, FIRST_YEAR - 1, -1):
            logger.info("Searching year %d...", year)
            try:
                laws = self._search_year(year)
            except Exception as e:
                logger.error("Failed to search year %d: %s", year, e)
                continue

            logger.info("Found %d laws for %d", len(laws), year)
            for law_year, law_num in laws:
                time.sleep(1.5)
                raw = self._fetch_law_text(law_year, law_num)
                if not raw:
                    continue
                yield self.normalize(raw)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        try:
            since_year = int(since[:4])
        except (ValueError, IndexError):
            since_year = CURRENT_YEAR - 1

        for year in range(CURRENT_YEAR, since_year - 1, -1):
            logger.info("Updating year %d...", year)
            try:
                laws = self._search_year(year)
            except Exception as e:
                logger.error("Failed to search year %d: %s", year, e)
                continue

            for law_year, law_num in laws:
                time.sleep(1.5)
                raw = self._fetch_law_text(law_year, law_num)
                if raw:
                    yield self.normalize(raw)

    def test(self) -> bool:
        try:
            resp = self._get(f"{SEARCH_URL}?db=DBC")
            return "Leggi regionali" in resp.text
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = FriuliVeneziaGiuliaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test()
        print("OK" if ok else "FAIL")
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 15 if sample_mode else 999999

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(
                "[%d] %s — %d chars",
                count,
                record.get("law_number", record["_id"]),
                len(record.get("text", "")),
            )
            if count >= max_records:
                break

        logger.info("Done: %d records saved to %s", count, sample_dir)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else str(CURRENT_YEAR - 1)
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
