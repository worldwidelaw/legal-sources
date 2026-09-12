#!/usr/bin/env python3
"""
IT/Marche -- Legislazione Regionale Marche

Fetches regional laws and regulations of the Marche Region from the
Consiglio Regionale delle Marche's legislative database.

Strategy:
  - Each act is served as an HTML page at:
        https://www.consiglio.marche.it/banche_dati_e_documentazione/leggi/dettaglio.php?arc=vig&idl=N
    where N is an internal database ID (1 to ~2362).
  - For active laws, the "vigente" (arc=vig) version has full consolidated text.
  - For repealed laws, the vigente version only has metadata; the "storico"
    (arc=sto) version has the original text.
  - Metadata (law reference, title, date, publication) is extracted from the
    table inside div#atto.
  - Full text is extracted from the remaining content in div#atto.
  - Server sends ISO-8859-1 encoding despite HTML meta declaring UTF-8.
  - Coverage: 1971-present, ~2300 acts.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent laws (idl > 2200)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.Marche")

BASE_URL = "https://www.consiglio.marche.it/banche_dati_e_documentazione/leggi"
DETAIL_URL = BASE_URL + "/dettaglio.php?arc={arc}&idl={idl}"

MIN_IDL = 1
MAX_IDL = 2500

ITALIAN_MONTHS = {
    "gennaio": "01", "febbraio": "02", "marzo": "03", "aprile": "04",
    "maggio": "05", "giugno": "06", "luglio": "07", "agosto": "08",
    "settembre": "09", "ottobre": "10", "novembre": "11", "dicembre": "12",
}


class MarcheScraper(BaseScraper):
    SOURCE_ID = "IT/Marche"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                resp.encoding = "latin-1"
                return resp
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                logger.warning("GET attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("unreachable")

    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        """Parse Italian date like '17 dicembre 1999' to ISO format."""
        m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text.strip())
        if not m:
            return None
        day, month_name, year = m.groups()
        month = ITALIAN_MONTHS.get(month_name.lower())
        if not month:
            return None
        return f"{year}-{month}-{int(day):02d}"

    @staticmethod
    def _clean_text(raw_text: str) -> str:
        """Clean extracted text: normalize whitespace, remove artifacts."""
        text = html_mod.unescape(raw_text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _parse_metadata(self, table) -> Dict[str, str]:
        """Extract metadata from the detail table."""
        meta = {}
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).rstrip(":")
                value = cells[1].get_text(strip=True)
                meta[label] = value
        return meta

    def _extract_text(self, atto_div, table) -> str:
        """Extract full law text from div#atto, excluding the metadata table."""
        table.decompose()
        sommario = atto_div.find("h2")
        if sommario:
            sommario.decompose()
        toc = atto_div.find("div", style=lambda s: s and "display:block" in s)
        if toc:
            toc.decompose()
        raw = atto_div.get_text(separator="\n", strip=False)
        return self._clean_text(raw)

    def _fetch_detail(self, idl: int, arc: str = "vig") -> Optional[Dict[str, Any]]:
        """Fetch and parse a single law detail page."""
        url = DETAIL_URL.format(arc=arc, idl=idl)
        resp = self._get(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        atto = soup.find("div", id="atto")
        if not atto:
            return None

        table = atto.find("table", summary="dettaglio documento")
        if not table:
            table = atto.find("table")
        if not table:
            return None

        meta = self._parse_metadata(table)
        atto_ref = meta.get("Atto", "")
        title = meta.get("Titolo", "")
        pubblicazione = meta.get("Pubblicazione", "")
        stato = meta.get("Stato", "")

        text = self._extract_text(atto, table)

        # If vigente has no real text (repealed law), try storico
        if arc == "vig" and len(text) < 100:
            logger.debug("IDL %d vigente has no text, trying storico", idl)
            return self._fetch_detail(idl, arc="sto")

        if len(text) < 50:
            return None

        # Parse law number and date from Atto field
        law_number = None
        law_date = None
        # Pattern: "LEGGE REGIONALE 17 dicembre 1999, n. 35"
        # or "REGOLAMENTO REGIONALE 5 maggio 2020, n. 3"
        m = re.match(
            r"(?:LEGGE|REGOLAMENTO)\s+(?:REGIONALE|STATUTARIA)\s+"
            r"(\d{1,2}\s+\w+\s+\d{4}),?\s*n\.\s*(\d+)",
            atto_ref,
        )
        if m:
            law_date = self._parse_date(m.group(1))
            law_number = m.group(2)

        # Determine act type
        act_type = "LR"
        if "REGOLAMENTO" in atto_ref.upper():
            act_type = "RR"
        elif "STATUTARIA" in atto_ref.upper():
            act_type = "LS"

        year = None
        if law_date:
            year = int(law_date[:4])
        else:
            year_m = re.search(r"\b(19\d{2}|20\d{2})\b", atto_ref)
            if year_m:
                year = int(year_m.group(1))

        return {
            "idl": idl,
            "arc": arc,
            "act_type": act_type,
            "law_number": law_number,
            "year": year,
            "title": title,
            "date": law_date,
            "pubblicazione": pubblicazione,
            "stato": stato,
            "text": text,
            "url": DETAIL_URL.format(arc=arc, idl=idl),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        idl = raw.get("idl")
        act_type = raw.get("act_type", "LR")
        law_number = raw.get("law_number")
        year = raw.get("year")

        if law_number and year:
            _id = f"IT/Marche/{act_type}-{year}-{law_number}"
            law_ref = f"{act_type} {law_number}/{year}"
        else:
            _id = f"IT/Marche/idl-{idl}"
            law_ref = ""

        title = raw.get("title", "")
        if law_ref and title and not title.startswith(act_type):
            title = f"{law_ref} — {title}"
        elif law_ref and not title:
            title = law_ref

        return {
            "_id": _id,
            "_source": "IT/Marche",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "law_number": law_ref,
            "stato": raw.get("stato", ""),
            "pubblicazione": raw.get("pubblicazione", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        consecutive_empty = 0
        for idl in range(MIN_IDL, MAX_IDL + 1):
            try:
                raw = self._fetch_detail(idl)
            except Exception as e:
                logger.warning("Error fetching IDL %d: %s", idl, e)
                consecutive_empty += 1
                if consecutive_empty >= 30:
                    logger.info("30 consecutive empty at IDL %d, stopping.", idl)
                    break
                continue

            if raw is None:
                consecutive_empty += 1
                if consecutive_empty >= 30:
                    logger.info("30 consecutive empty at IDL %d, stopping.", idl)
                    break
                continue

            consecutive_empty = 0
            yield self.normalize(raw)
            time.sleep(1.0)

            if idl % 100 == 0:
                logger.info("Progress: IDL %d", idl)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent laws — scan from IDL 2200+ for new entries."""
        start_idl = 2200
        try:
            if since.isdigit() and int(since) > 1000:
                start_idl = int(since)
        except (ValueError, AttributeError):
            pass

        consecutive_empty = 0
        for idl in range(start_idl, MAX_IDL + 1):
            try:
                raw = self._fetch_detail(idl)
            except Exception as e:
                logger.warning("Error fetching IDL %d: %s", idl, e)
                consecutive_empty += 1
                if consecutive_empty >= 30:
                    break
                continue

            if raw is None:
                consecutive_empty += 1
                if consecutive_empty >= 30:
                    break
                continue

            consecutive_empty = 0
            yield self.normalize(raw)
            time.sleep(1.0)

    def test(self) -> bool:
        try:
            raw = self._fetch_detail(1301)
            if not raw:
                return False
            return len(raw.get("text", "")) > 100 and raw.get("law_number") is not None
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = MarcheScraper()

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
        since = sys.argv[2] if len(sys.argv) > 2 else "2200"
        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info("[%d] %s", count, record.get("law_number", record["_id"]))
        logger.info("Update done: %d records", count)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
